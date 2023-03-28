import asyncio
import logging
from functools import lru_cache
from typing import List, Any, Dict

import httpx
from cachetools import TTLCache
from fastapi import Depends
from starlette.status import HTTP_429_TOO_MANY_REQUESTS, HTTP_503_SERVICE_UNAVAILABLE, HTTP_504_GATEWAY_TIMEOUT
from tenacity import retry, retry_if_exception_type, stop_after_attempt, RetryError, wait_fixed

from src.clients.api_models import BookV1ApiRequest, UserReviewV1BatchRequest, UserReviewBatchResponse, \
    ApiUserReviewBatchResponse, ApiBookExistsBatchResponse, \
    ApiBookExistsBatchRequest, ApiBookPopularityResponse, UserBookPopularityResponse, SingleBookPopularityResponse
from src.clients.utils.cache_utils import get_user_read_book_cache
from src.dependencies import Properties
from src.services.book_task_enqueuer_service import BOOK_POPULARITY_THRESHOLD

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class BookRecommenderApiClientException(Exception):
    pass


class BookRecommenderApiServerException(Exception):
    pass


class RetryableException(Exception):
    pass


class NonRetryableException(Exception):
    pass


class BookRecommenderApiClient(object):
    def __init__(self, properties, user_read_books_cache: TTLCache):
        self.base_url = properties.book_recommender_api_base_url
        self.user_read_books_cache = user_read_books_cache

    async def is_ready(self):
        url = f"{self.base_url}/"
        try:
            response = httpx.get(url)
            if not response.is_error:
                return True
        except Exception as e:
            logger.error("Could not reach Book Recommender API for readiness check: {}".format(e))

        return False

    async def create_book(self, book_dict: Dict[str, Any]):
        book_id = book_dict.get("book_id")
        book = BookV1ApiRequest(**book_dict)
        url = f"{self.base_url}/books/{book_id}"
        try:
            response = httpx.put(url, json=book.dict())
            if not response.is_error:
                logger.info("Successfully wrote book: {}".format(book_id))
                return
            elif response.is_client_error:
                logger.error(
                    "Received 4xx exception from server with body: {} URL: {} "
                    "book_id: {}".format(response.text, url, book_id))
                raise BookRecommenderApiClientException(
                    "4xx Exception encountered {} for book_id: {}".format(response.text, book_id))
            elif response.is_server_error:
                logger.error(
                    "Received 5xx exception from server with body: {} URL: {} "
                    "book_id: {}".format(response.text, url, book_id))
                raise BookRecommenderApiServerException(
                    "5xx Exception encountered {} for book_id: {}".format(response.text, book_id))
        except httpx.HTTPError as e:
            raise BookRecommenderApiServerException("HTTP Exception encountered: {} for URL {}".format(e, url))

    async def get_books_read_by_user(self, user_id) -> List[int]:
        """
        Function to return the list of book reviews we already have for a user. This is used to avoid unecessary
        recreations of the same book reviews.

        It uses a TTL cache to avoid hitting the API too often. It's very likely you'll get dozens of books for the
        same user right after each other, so we want to avoid hitting the same API call for each one.
        :param user_id: goodreads profile ID
        :return: List(int) of book_ids
        """
        if user_id in self.user_read_books_cache:
            logging.info("Cache hit! get_books_read_by_user() user_id: {}".format(user_id))
            return self.user_read_books_cache[user_id]

        url = f"{self.base_url}/users/{user_id}/book-ids"
        try:
            response = httpx.get(url)
            if not response.is_error:
                book_ids = response.json().get("book_ids", [])
                self.user_read_books_cache[user_id] = book_ids
                return book_ids
            elif response.is_client_error:
                logger.info("Received 4xx exception from server, assuming user_id: {} does not exist. URL: {} ".format(
                    user_id, url))
                # We still want to cache this empty list, because 4xx is a business valid response (we should index all
                # the user reviews, because they haven't read any books yet)
                self.user_read_books_cache[user_id] = []
                return []
            elif response.is_server_error:
                logger.error(
                    "Received 5xx exception from server with body: {} URL: {} user_id: {}".format(response.text, url,
                                                                                                  user_id))
                raise BookRecommenderApiServerException(
                    "5xx Exception encountered {} for user_id: {}".format(response.text, user_id))
        except httpx.HTTPError as e:
            raise BookRecommenderApiServerException("HTTP Exception encountered: {} for URL {}".format(e, url))

    async def get_already_indexed_books(self, book_ids: List[int]) -> ApiBookExistsBatchResponse:
        """
        Function which will query book_recommender_api to see if we have that book indexed already

        :param book_ids: List of book IDs to check
        :return: List(int) of book_ids that exist from within your input list
        """
        # We can pop all the IDs which already exist from the cache, because that means we have checked them already
        url = f"{self.base_url}/books/batch/exists"
        try:
            request = ApiBookExistsBatchRequest(book_ids=book_ids)
            response = httpx.post(url, json=request.dict())
            if not response.is_error:
                # Find ones which don't exist in our cache, but are already indexed
                return ApiBookExistsBatchResponse(**response.json())
            elif response.status_code == HTTP_429_TOO_MANY_REQUESTS:
                logger.error("Received 429 response code from server. URL: {} ".format(url))
            elif response.is_server_error:
                logger.error(
                    "Received 5xx exception from server with body: {} URL: {} book_ids: {}".format(response.text, url,
                                                                                                   book_ids))
                raise BookRecommenderApiServerException(
                    "5xx Exception encountered {} for book_ids: {}".format(response.text, book_ids))
        except httpx.HTTPError as e:
            logger.error(
                "HTTP Error received {} on URL: {} book_ids: {}".format(e, url, book_ids))
            raise BookRecommenderApiServerException("HTTP Exception encountered: {} for URL {}".format(e, url))

    async def get_book_popularity(self, book_ids: List[int]) -> ApiBookPopularityResponse:
        """
        Function which will query book_recommender_api to see the number of users who reference this book ID. We expect
        large batch sizes, so we will use asyncio to make the request complete as fast as possible.

        :param book_ids: List of book IDs to check
        :return: Dict of book_id -> popularity
        """
        tasks = []
        for book_ids in book_ids:
            task = asyncio.create_task(self._make_book_popularity_request(book_ids))
            tasks.append(task)

        book_info = {}
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            # This suppresses the exception from spoiling the batch,
            if type(result) in (RetryError, NonRetryableException):
                # We either exhausted the retries, or are choosing to not retry
                continue
            if issubclass(type(result), Exception):
                logging.warning("Uncaught exception trying to get book popularity: {} {}".format(type(result), result))
                continue
            book_info[result.book_id] = result.user_count

        return ApiBookPopularityResponse(book_info=book_info)

    @retry(retry=retry_if_exception_type(RetryableException), stop=stop_after_attempt(3), wait=wait_fixed(.5))
    async def _make_book_popularity_request(self, book_id: int) -> SingleBookPopularityResponse:
        """
        Function which will return the future of the book popularity request. This is used to make the request
        asynchronously, and then we can gather all the results together.

        :param book_id:
        :return: Future(ApiBookPopularityResponse) Single book popularity response
        """
        async with httpx.AsyncClient() as client:
            url = f"{self.base_url}/users/book-popularity/{book_id}?limit={BOOK_POPULARITY_THRESHOLD}"
            response = await client.get(url)
            if not response.is_error:
                response = UserBookPopularityResponse(**response.json())
                return SingleBookPopularityResponse(book_id=book_id, user_count=response.user_count)
            elif response.status_code in [HTTP_504_GATEWAY_TIMEOUT,
                                          HTTP_503_SERVICE_UNAVAILABLE,
                                          HTTP_429_TOO_MANY_REQUESTS]:
                logging.warning("Retryable http status encountered {}, retrying!".format(response.status_code))
                raise RetryableException()
            else:
                logging.warning("Non retryable http status encountered {} "
                                "when querying for book_id: {} popularity".format(response.status_code, book_id))
                raise NonRetryableException()

    async def create_batch_user_reviews(self, user_review_batch: List[Dict[str, Any]]) -> UserReviewBatchResponse:
        user_review = UserReviewV1BatchRequest(user_reviews=user_review_batch)
        url = f"{self.base_url}/users/batch/create"
        try:
            response = httpx.post(url, json=user_review.dict())
            if not response.is_error:
                # Probably excessive to deserialize and reserialize the same response, but I really don't like digging
                # in raw JSON by key
                api_response = ApiUserReviewBatchResponse(**response.json())
                logger.info("Successfully indexed {} user reviews".format(api_response.indexed))
                return UserReviewBatchResponse(indexed=api_response.indexed)
            elif response.status_code == HTTP_429_TOO_MANY_REQUESTS:
                logger.error("Received 429 response code from server. URL: {} ".format(url))
                raise BookRecommenderApiServerException("Received HTTP_429_TOO_MANY_REQUESTS from server")
            elif response.is_server_error:
                logger.error(
                    "Received 5xx exception from server with body: {} URL: {}".format(response.text, url))
                raise BookRecommenderApiServerException(
                    "5xx Exception encountered {} for URL: {}".format(response.text, url))
        except httpx.HTTPError as e:
            raise BookRecommenderApiServerException(
                "HTTP Exception encountered: {} for URL {}".format(e, url))


def get_book_recommender_api_client(properties: Properties = Depends(get_properties),
                                    user_read_book_cache: TTLCache = Depends(get_user_read_book_cache)):
    return BookRecommenderApiClient(properties, user_read_book_cache)

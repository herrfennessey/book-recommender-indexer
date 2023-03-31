import asyncio
import logging
from functools import lru_cache
from typing import List

import httpx
from cachetools import TTLCache
from fastapi import Depends
from starlette.status import HTTP_429_TOO_MANY_REQUESTS, HTTP_503_SERVICE_UNAVAILABLE, HTTP_504_GATEWAY_TIMEOUT
from tenacity import retry, retry_if_exception_type, stop_after_attempt, RetryError, wait_fixed

from src.clients.api_models import ApiBookPopularityResponse, UserBookPopularityResponse, SingleBookPopularityResponse
from src.dependencies import Properties

logger = logging.getLogger(__name__)

BOOK_POPULARITY_THRESHOLD = 5


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


class BookRecommenderApiClientV2(object):
    def __init__(self, properties):
        self.base_url = properties.book_recommender_api_base_url_v2

    async def is_ready(self):
        url = f"{self.base_url}/health"
        try:
            response = httpx.get(url)
            if not response.is_error:
                return True
        except Exception as e:
            logger.error("Could not reach Book Recommender API V2 for readiness check: {}".format(e))

        return False

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


def get_book_recommender_api_client_v2(properties: Properties = Depends(get_properties)):
    return BookRecommenderApiClientV2(properties)

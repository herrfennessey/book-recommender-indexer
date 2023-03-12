import logging
from functools import lru_cache
from typing import List, Any, Dict

import httpx
from asyncache import cached
from cachetools import TTLCache, LRUCache
from fastapi import Depends

from src.clients.api_models import BookV1ApiRequest, UserReviewV1ApiRequest
from src.dependencies import Properties

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class BookRecommenderApiClient(object):

    def __init__(self, properties: Properties = Depends(get_properties)):
        self.base_url = properties.book_recommender_api_base_url
        self.seen_books = LRUCache(maxsize=4000)

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
            logging.error(
                "Uncaught Exception: {} encountered for URL: {} for book_id: {}".format(e, url, book_id))
            raise BookRecommenderApiServerException("Uncaught Exception encountered for book id: {}".format(book_id))

    @cached(TTLCache(maxsize=1024, ttl=600))
    async def get_books_read_by_user(self, user_id) -> List[int]:
        url = f"{self.base_url}/users/{user_id}/book-ids"
        try:
            response = httpx.get(url)
            if not response.is_error:
                return response.json().get("book_ids", [])
            elif response.is_client_error:
                logger.info("Received 4xx exception from server, assuming user_id: {} does not exist. URL: {} ".format(
                    user_id, url))
                return []
            elif response.is_server_error:
                logger.error(
                    "Received 5xx exception from server with body: {} URL: {} user_id: {}".format(response.text, url,
                                                                                                  user_id))
                raise BookRecommenderApiServerException(
                    "5xx Exception encountered {} for user_id: {}".format(response.text, user_id))
        except httpx.HTTPError as e:
            logging.error(
                "Uncaught Exception: {} encountered for URL: {} for user_id: {}".format(e, url, user_id))
            raise BookRecommenderApiServerException("Uncaught Exception encountered for user_id: {}".format(user_id))

    async def create_user_review(self, user_review_dict: Dict[str, Any]):
        user_id = user_review_dict.get("user_id")
        book_id = user_review_dict.get("book_id")
        user_review = UserReviewV1ApiRequest(**user_review_dict)
        url = f"{self.base_url}/users/{user_id}/reviews/{book_id}"
        try:
            response = httpx.put(url, data=user_review.json())
            if not response.is_error:
                logger.info("Successfully wrote user review: {}".format(book_id))
                return
            elif response.is_client_error:
                logger.error(
                    "Received 4xx exception from server with body: {} URL: {} "
                    "user_id: {} book_id: {}".format(response.text, url, user_id, book_id))
                raise BookRecommenderApiClientException(
                    "4xx Exception encountered {} for user_id: {} book_id: {}".format(response.text, user_id, book_id))
            elif response.is_server_error:
                logger.error(
                    "Received 5xx exception from server with body: {} URL: {} "
                    "user_id: {} book_id: {}".format(response.text, url, user_id, book_id))
                raise BookRecommenderApiServerException(
                    "5xx Exception encountered {} for user_id: {} book_id: {}".format(response.text, user_id, book_id))
        except httpx.HTTPError as e:
            logging.error(
                "Uncaught Exception: {} encountered for URL: {} for user_id: {} book_id: {}".format(e, url, user_id,
                                                                                                    book_id))
            raise BookRecommenderApiServerException(
                "Uncaught Exception encountered for user_id: {} book_id: {}".format(user_id, book_id))

    async def does_book_exist(self, book_id):
        """
        Function which will query book_recommender_api to see if we have that book indexed already

        I didn't use a decorator here because I wanted to be able to set the LRU cache manually
        :param book_id: book_id to check
        :return: boolean True if book exists, False if not, or it throws an Exception if you're fancy
        """
        # Hacky LRU cache to cut down on API calls
        if self.seen_books.get(book_id):
            return True

        url = f"{self.base_url}/books/{book_id}"
        try:
            response = httpx.get(url)
            if not response.is_error:
                # Set LRU Cache, so we don't have to hit the API again
                self.seen_books[book_id] = True
                return True
            elif response.is_client_error:
                logger.info("Received 4xx exception from server, assuming book_id: {} does not exist. URL: {} ".format(
                    book_id, url))
                return False
            elif response.is_server_error:
                logger.error(
                    "Received 5xx exception from server with body: {} URL: {} book_id: {}".format(response.text, url,
                                                                                                  book_id))
                raise BookRecommenderApiServerException(
                    "5xx Exception encountered {} for book_id: {}".format(response.text, book_id))
        except httpx.HTTPError as e:
            logging.error(
                "Uncaught Exception: {} encountered for URL: {} for book_id: {}".format(e, url, book_id))
            raise BookRecommenderApiServerException("Uncaught Exception encountered for book id: {}".format(book_id))


class BookRecommenderApiClientException(Exception):
    pass


class BookRecommenderApiServerException(Exception):
    pass


def get_book_recommender_api_client(properties: Properties = Depends(get_properties)):
    return BookRecommenderApiClient(properties)

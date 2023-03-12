import logging
from functools import lru_cache
from typing import List, Any, Dict

import httpx
from cachetools import TTLCache, LRUCache
from fastapi import Depends

from src.clients.api_models import BookV1ApiRequest, UserReviewV1ApiRequest
from src.clients.utils.cache_utils import get_user_read_book_cache, get_book_exists_cache
from src.dependencies import Properties

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class BookRecommenderApiClient(object):
    def __init__(self, properties, user_read_books_cache: TTLCache, book_exists_cache: LRUCache):
        self.base_url = properties.book_recommender_api_base_url
        self.user_read_books_cache = user_read_books_cache
        self.book_exists_cache = book_exists_cache

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
            logging.error(
                "Uncaught Exception: {} encountered for URL: {} for user_id: {}".format(e, url, user_id))
            raise BookRecommenderApiServerException("Uncaught Exception encountered for user_id: {}".format(user_id))

    async def does_book_exist(self, book_id):
        """
        Function which will query book_recommender_api to see if we have that book indexed already

        The cachetools decorator doesn't seem to work with async fastapi stuff, because of how it instantiates fresh
        every single time, so I just do it in the global memory space
        :param book_id: book_id to check
        :return: boolean True if book exists, False if not, or it throws an Exception if you're fancy
        """
        # LRU cache to cut down on API calls
        if self.book_exists_cache.get(book_id):
            logging.info("Cache hit! does_book_exist() book_id: {}".format(book_id))
            return True

        url = f"{self.base_url}/books/{book_id}"
        try:
            response = httpx.get(url)
            if not response.is_error:
                # Set LRU Cache, so we don't have to hit the API again
                self.book_exists_cache[book_id] = True
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

    async def create_user_review(self, user_review_dict: Dict[str, Any]):
        user_id = user_review_dict.get("user_id")
        book_id = user_review_dict.get("book_id")
        user_review = UserReviewV1ApiRequest(**user_review_dict)
        url = f"{self.base_url}/users/{user_id}/reviews/{book_id}"
        try:
            response = httpx.put(url, json=user_review.dict())
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


class BookRecommenderApiClientException(Exception):
    pass


class BookRecommenderApiServerException(Exception):
    pass


def get_book_recommender_api_client(properties: Properties = Depends(get_properties),
                                    user_read_book_cache: TTLCache = Depends(get_user_read_book_cache),
                                    book_exists_cache: LRUCache = Depends(get_book_exists_cache)):
    return BookRecommenderApiClient(properties, user_read_book_cache, book_exists_cache)

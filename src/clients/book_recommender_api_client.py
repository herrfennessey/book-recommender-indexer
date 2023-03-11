import logging
from functools import lru_cache

import httpx
from fastapi import Depends

from src.clients.api_models import BookDataV1
from src.dependencies import Properties

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class BookRecommenderApiClient(object):

    def __init__(self, properties: Properties = Depends(get_properties)):
        self.base_url = properties.book_recommender_api_base_url

    async def create_book(self, book: BookDataV1):
        book_id = book.book_id
        url = f"{self.base_url}/books/{book_id}"
        try:
            response = httpx.put(url, data=book.json())
            if not response.is_error:
                logger.info("Successfully wrote book: {}".format(book_id))
                return response
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


class BookRecommenderApiClientException(Exception):
    pass


class BookRecommenderApiServerException(Exception):
    pass


def get_book_recommender_api_client(properties: Properties = Depends(get_properties)):
    return BookRecommenderApiClient(properties)

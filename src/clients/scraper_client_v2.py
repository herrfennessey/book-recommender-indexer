import logging
from functools import lru_cache

import httpx
from fastapi import Depends
from pydantic import BaseModel

from src.dependencies import Properties

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


"""
{
    "spider_name": "book",
    "start_requests": true,
    "crawl_args": {
        "books": "4",
        "project_id": "book-suggestion-please",
        "topic_name": "scraper-book-v1"
    }
}
"""


class BookScrapeRequestArgs(BaseModel):
    books: str
    project_id: str
    topic_name: str


class ScrapeRequest(BaseModel):
    spider_name: str
    start_requests: bool = True
    crawl_args: BookScrapeRequestArgs


class ScraperClientV2(object):

    def __init__(self, properties: Properties = Depends(get_properties)):
        self.base_url = properties.scraper_client_base_url
        self.gcp_project_name = properties.gcp_project_name
        self.pubsub_book_topic_name = properties.pubsub_book_topic_name

    async def trigger_book_scrape(self, book_id: int):
        scrape_request = ScrapeRequest(spider_name="book",
                                       crawl_args=BookScrapeRequestArgs(books=book_id, project_id=self.gcp_project_name,
                                                                        topic_name=self.pubsub_book_topic_name))
        url = f"{self.base_url}/crawl.json"
        try:
            response = httpx.post(url, data=scrape_request.json())
            if not response.is_error:
                logger.info("Successfully triggered book scrape for book_id: {}".format(book_id))
            else:
                raise ScraperClientV2Exception("Received a 4xx or 5xx server exception".format(book_id))
        except Exception as e:
            logger.error("Unable to scrape book_id: {} due to exception: {}".format(book_id, e))

class ScraperClientV2Exception(Exception):
    pass


def get_scraper_client_v2(properties: Properties = Depends(get_properties)):
    return ScraperClientV2(properties)

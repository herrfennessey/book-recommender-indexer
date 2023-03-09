import logging
from datetime import datetime
from functools import lru_cache
from typing import Optional, Dict, List

from fastapi import Depends
from pydantic import BaseModel

from src.dependencies import Properties

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class BookDataV1(BaseModel):
    # Work Details
    work_internal_id: str
    work_id: int
    publish_date: Optional[datetime]
    original_title: Optional[str]
    author: str
    author_url: str

    # Work Statistics
    num_ratings: Optional[int]
    num_reviews: Optional[int]
    avg_rating: float
    rating_histogram: Dict[str, int] = {}

    # Book Information
    book_id: int
    book_url: str
    book_title: str
    book_description: Optional[str]
    num_pages: Optional[int]
    language: Optional[str]
    isbn: Optional[str]
    isbn13: Optional[str]
    asin: Optional[str]
    series: Optional[str]
    genres: List[str] = list()
    scrape_time: datetime


class BookRecommenderApiClient(object):

    def __init__(self, properties: Properties = Depends(get_properties)):
        self.base_url = properties.book_recommendations_api_base_url

    def create_book(self, book: BookDataV1):
        logger.info(f"Creating book {book}")
        pass

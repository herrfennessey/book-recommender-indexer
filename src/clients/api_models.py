from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, validator


class BookV1ApiRequest(BaseModel):
    # Work Details
    work_internal_id: str
    work_id: int
    publish_date: Optional[str]
    original_title: Optional[str]
    author: str
    author_url: str

    # Work Statistics
    num_ratings: Optional[int]
    num_reviews: Optional[int]
    avg_rating: float
    rating_histogram: List[int]

    # Book Information
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
    scrape_time: str

    @validator("publish_date", pre=True, allow_reuse=True)
    def convert_publish_date_to_string_if_exists(cls, publish_date):
        if publish_date:
            # httpx doesn't like datetime objects in its json serializer
            return str(publish_date.isoformat())



class UserReviewV1ApiRequest(BaseModel):
    user_rating: int
    date_read: str
    scrape_time: str

    @validator("date_read", pre=True, allow_reuse=True)
    def convert_date_read_to_string(cls, date_read):
        return str(date_read.isoformat())

    @validator("scrape_time", pre=True, allow_reuse=True)
    def convert_date_read_to_string(cls, scrape_time):
        return str(scrape_time.isoformat())

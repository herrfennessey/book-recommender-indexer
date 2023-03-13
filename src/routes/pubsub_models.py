from datetime import datetime
from typing import Dict, Any, Optional, List

from dateutil.parser import parse
from pydantic import BaseModel, Extra, validator


class MessagePayload(BaseModel):
    attributes: Dict[str, Any] = {}
    data: str
    message_id: str
    publish_time: str

    class Config:
        extra = Extra.ignore


class PubSubMessage(BaseModel):
    message: MessagePayload
    subscription: str

    class Config:
        extra = Extra.ignore


class PubSubBookV1(BaseModel):
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
    rating_histogram: List[int]

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

    @validator("publish_date", "scrape_time", pre=True)
    def parse_datetime_fields(cls, publish_date):
        return parse(publish_date)


class PubSubUserReviewV1(BaseModel):
    user_id: int
    book_id: int
    user_rating: int
    date_read: datetime
    scrape_time: datetime

    @validator("date_read", "scrape_time", pre=True)
    def parse_datetime_fields(cls, publish_date):
        return parse(publish_date)

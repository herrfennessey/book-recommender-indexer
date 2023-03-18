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

    @validator("publish_date", "scrape_time", pre=True, allow_reuse=True)
    def convert_dates_and_times_to_strings(cls, input_datetime):
        if input_datetime and isinstance(input_datetime, datetime):
            # httpx doesn't like datetime objects in its json serializer
            return str(input_datetime.isoformat())
        else:
            return str(input_datetime)


class UserReviewV1BatchItem(BaseModel):
    user_id: int
    book_id: int
    user_rating: int
    date_read: str
    scrape_time: str


class UserReviewV1BatchRequest(BaseModel):
    user_reviews: List[UserReviewV1BatchItem]


class ApiBookExistsBatchRequest(BaseModel):
    book_ids: List[int]


class ApiBookExistsBatchResponse(BaseModel):
    book_ids: List[int]


class ApiUserReviewBatchResponse(BaseModel):
    indexed: int


class UserReviewBatchResponse(BaseModel):
    indexed: int

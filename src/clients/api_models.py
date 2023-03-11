from datetime import datetime
from typing import Optional, List

import isbnlib
from dateutil.parser import parse
from pydantic import BaseModel, validator


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
    scrape_time: datetime

    @validator("isbn", allow_reuse=True)
    def ensure_isbn_is_valid(cls, isbn):
        if isbn:
            assert isbnlib.is_isbn10(isbn), "ISBN10 is invalid"
        return isbn

    @validator("isbn13", allow_reuse=True)
    def ensure_isbn13_is_valid(cls, isbn):
        if isbn:
            assert isbnlib.is_isbn13(isbn), "ISBN13 is invalid"
        return isbn

    @validator("num_pages", allow_reuse=True)
    def validate_page_count(cls, pages):
        if pages is not None:
            assert pages >= 0, "Pages must be greater than or equal to 0"
        return pages

    @validator("num_ratings", allow_reuse=True)
    def validate_num_ratings(cls, num_ratings):
        if num_ratings is not None:
            assert num_ratings >= 0, "Number of ratings must be greater than 0"
        return num_ratings

    @validator("publish_date", pre=True, allow_reuse=True)
    def ensure_publish_date_is_reasonable(cls, publish_date):
        parsed_ts = parse(publish_date)
        assert (
                datetime(1900, 1, 1, 0, 0).timestamp()
                < parsed_ts.timestamp()
                <= datetime.now().timestamp()
        ), "Publish Date must be between Jan 1, 1900 and now"
        return parsed_ts

    @validator("rating_histogram", allow_reuse=True)
    def validate_rating_histogram(cls, histogram):
        assert len(histogram) == 5, "We must have star ratings for 1, 2, 3, 4, 5 stars"
        return histogram

    @validator("scrape_time", pre=True, allow_reuse=True)
    def ensure_scrape_time_is_reasonable(cls, scrape_time):
        parsed_ts = parse(scrape_time)
        assert (
                datetime(1900, 1, 1, 0, 0).timestamp()
                < parsed_ts.timestamp()
                <= datetime.now().timestamp()
        ), "Parse date must be between Jan 1, 1900 and now"
        return parsed_ts

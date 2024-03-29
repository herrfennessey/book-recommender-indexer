import json
from datetime import datetime
from typing import Any, Dict

import httpx
import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from cachetools import TTLCache

from src.clients.api_models import ApiBookExistsBatchResponse, ApiBookPopularityResponse
from src.clients.book_recommender_api_client_v2 import (
    BOOK_POPULARITY_THRESHOLD,
    BookRecommenderApiClientException,
    BookRecommenderApiClientV2,
    BookRecommenderApiServerException,
)
from src.dependencies import Properties

TEST_PROPERTIES = Properties(
    book_recommender_api_base_url_v2="https://testurl", env_name="test"
)


@pytest.fixture
def book_recommender_api_client_v2():
    return BookRecommenderApiClientV2(
        properties=TEST_PROPERTIES, user_read_books_cache=TTLCache(maxsize=100, ttl=60)
    )


@pytest.mark.asyncio
async def test_200_on_book_popularity_request(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given

    httpx_mock.add_response(
        json={"user_count": 5},
        status_code=200,
        url=f"https://testurl/book-popularity/1?limit={BOOK_POPULARITY_THRESHOLD}",
    )
    httpx_mock.add_response(
        json={"user_count": 0},
        status_code=200,
        url=f"https://testurl/book-popularity/2?limit={BOOK_POPULARITY_THRESHOLD}",
    )

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(
        ApiBookPopularityResponse(book_info={"1": 5, "2": 0})
    )


@pytest.mark.parametrize("status_code", [429, 503, 504])
@pytest.mark.asyncio
async def test_retryable_exception_doesnt_error_batch_and_doesnt_retry(
    status_code,
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    httpx_mock.add_response(
        json={"user_count": 5},
        status_code=200,
        url=f"https://testurl/book-popularity/1?limit={BOOK_POPULARITY_THRESHOLD}",
    )
    httpx_mock.add_response(
        status_code=status_code,
        url=f"https://testurl/book-popularity/2?limit={BOOK_POPULARITY_THRESHOLD}",
    )

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(ApiBookPopularityResponse(book_info={"1": 5}))
    assert_that(len(httpx_mock.get_requests())).is_equal_to(
        4
    )  # We currently retry twice, .5 seconds apart


@pytest.mark.asyncio
async def test_non_retryable_exception_doesnt_error_batch_and_retries(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    httpx_mock.add_response(
        json={"user_count": 5},
        status_code=200,
        url=f"https://testurl/book-popularity/1?limit={BOOK_POPULARITY_THRESHOLD}",
    )
    httpx_mock.add_response(
        status_code=500,
        url=f"https://testurl/book-popularity/2?limit={BOOK_POPULARITY_THRESHOLD}",
    )

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(ApiBookPopularityResponse(book_info={"1": 5}))
    assert_that(caplog.text).contains("Non retryable http status encountered", "500")
    assert_that(len(httpx_mock.get_requests())).is_equal_to(2)


@pytest.mark.asyncio
async def test_http_exception_on_book_popularity_throws_server_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(ApiBookPopularityResponse(book_info={}))
    assert_that(caplog.text).contains("ReadTimeout", "Unable to read within timeout")


@pytest.mark.asyncio
async def test_successful_book_put(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(json={}, status_code=200, url="https://testurl/books/1")

    # When
    await book_recommender_api_client_v2.create_book(_a_random_book())

    # Then
    assert_that(caplog.text).contains("Successfully wrote book: 1")


@pytest.mark.asyncio
async def test_empty_response_to_see_if_book_exists(
    httpx_mock, book_recommender_api_client_v2: BookRecommenderApiClientV2
):
    # Given
    httpx_mock.add_response(
        json={"book_ids": []}, status_code=200, url="https://testurl/books/batch/exists"
    )

    # When
    response = await book_recommender_api_client_v2.get_already_indexed_books([1])

    # Then
    assert_that(response).is_equal_to(ApiBookExistsBatchResponse(book_ids=[]))


@pytest.mark.asyncio
async def test_200_to_see_if_book_exists(
    httpx_mock, book_recommender_api_client_v2: BookRecommenderApiClientV2
):
    # Given
    httpx_mock.add_response(
        json={"book_ids": [1]},
        status_code=200,
        url="https://testurl/books/batch/exists",
    )

    # When
    response = await book_recommender_api_client_v2.get_already_indexed_books([1])

    # Then
    assert_that(response).is_equal_to(ApiBookExistsBatchResponse(book_ids=[1]))


@pytest.mark.asyncio
async def test_5xx_when_querying_if_book_exists_throws_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    httpx_mock.add_response(status_code=500, url="https://testurl/books/batch/exists")

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.get_already_indexed_books([1])


@pytest.mark.asyncio
async def test_unhandled_exceptions_when_querying_if_book_exists_throws_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.get_already_indexed_books([1, 2, 3])

    assert_that(caplog.text).contains(
        "HTTP Error", "Unable to read within timeout", "[1, 2, 3]"
    )


@pytest.mark.parametrize("expected_response_code", [500, 501, 502, 503, 504])
@pytest.mark.asyncio
async def test_5xx_custom_exception_on_book_put(
    expected_response_code,
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(
        json=json_response,
        status_code=expected_response_code,
        url="https://testurl/books/1",
    )

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Received 5xx exception from server",
        json.dumps(json_response),
        "https://testurl/books/1",
        "book_id: 1",
    )


@pytest.mark.parametrize("expected_response_code", [400, 401, 402, 403, 404])
@pytest.mark.asyncio
async def test_4xx_custom_exception_on_book_put(
    expected_response_code,
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(
        json=json_response,
        status_code=expected_response_code,
        url="https://testurl/books/1",
    )

    # When / Then
    with pytest.raises(BookRecommenderApiClientException):
        await book_recommender_api_client_v2.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Received 4xx exception from server",
        json.dumps(json_response),
        "https://testurl/books/1",
        "book_id: 1",
    )


@pytest.mark.asyncio
async def test_uncaught_exception_on_put_book(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("ERROR", logger="book_recommender_api_client_v2")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException) as e:
        await book_recommender_api_client_v2.create_book(_a_random_book())

    assert_that(e.value.args[0]).contains(
        "Unable to read within timeout", "https://testurl/books/1"
    )


@pytest.mark.asyncio
async def test_successful_get_books_read(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(
        json={"book_ids": [3, 4, 5]},
        status_code=200,
        url="https://testurl/reviews/1/book-ids",
    )

    # When
    response = await book_recommender_api_client_v2.get_books_read_by_user(1)

    # Then
    assert_that(response).contains_only(3, 4, 5)


@pytest.mark.asyncio
async def test_4xx_when_getting_books_read_returns_empty_array(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(status_code=404, url="https://testurl/reviews/1/book-ids")

    # When
    response = await book_recommender_api_client_v2.get_books_read_by_user(1)

    # Then
    assert_that(caplog.text).contains(
        "Received 4xx exception from server",
        "user_id: 1",
        "https://testurl/reviews/1/book-ids",
    )
    assert_that(response).is_equal_to(list())


@pytest.mark.asyncio
async def test_4xx_still_gets_cached(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(status_code=404, url="https://testurl/reviews/1/book-ids")

    # When
    for _ in range(10):
        response = await book_recommender_api_client_v2.get_books_read_by_user(1)

    # Then
    assert_that(caplog.text).contains(
        "Received 4xx exception from server",
        "user_id: 1",
        "https://testurl/reviews/1/book-ids",
    )
    assert_that(response).is_equal_to(list())
    assert_that(httpx_mock.get_requests()).is_length(1)


@pytest.mark.asyncio
async def test_5xx_when_getting_books_read_throws_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(status_code=500, url="https://testurl/reviews/1/book-ids")

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.get_books_read_by_user(1)

    assert_that(caplog.text).contains(
        "Received 5xx exception from server",
        "user_id: 1",
        "https://testurl/reviews/1/book-ids",
    )


@pytest.mark.asyncio
async def test_unhandled_exceptions_when_getting_books_read_throws_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException) as e:
        await book_recommender_api_client_v2.get_books_read_by_user(1)

    assert_that(e.value.args[0]).contains(
        "Unable to read within timeout", "https://testurl/reviews/1/book-ids"
    )


@pytest.mark.asyncio
async def test_successful_user_review_creation(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    httpx_mock.add_response(
        json={"indexed": 1}, status_code=200, url="https://testurl/reviews/batch/create"
    )
    reviews = [_a_random_review()]

    # When
    await book_recommender_api_client_v2.create_batch_user_reviews(reviews)

    # Then
    assert_that(caplog.text).contains("Successfully indexed 1 user reviews")


@pytest.mark.asyncio
async def test_batch_correctly_takes_indexed_from_api_not_input(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    httpx_mock.add_response(
        json={"indexed": 2}, status_code=200, url="https://testurl/reviews/batch/create"
    )
    reviews = [_a_random_review(), _a_random_review(), _a_random_review()]

    # When
    await book_recommender_api_client_v2.create_batch_user_reviews(reviews)

    # Then
    assert_that(caplog.text).contains("Successfully indexed 2 user reviews")


@pytest.mark.asyncio
async def test_429_user_review_creation_throws_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(status_code=429, url="https://testurl/reviews/batch/create")
    review = _a_random_review()

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.create_batch_user_reviews([review])

    assert_that(caplog.text).contains(
        "Received 429 response code", "https://testurl/reviews/batch/create"
    )


@pytest.mark.asyncio
async def test_5xx_user_review_creation_throws_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_response(status_code=500, url="https://testurl/reviews/batch/create")
    review = _a_random_review()

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.create_batch_user_reviews([review])

    assert_that(caplog.text).contains(
        "Received 5xx exception from server", "https://testurl/reviews/batch/create"
    )


@pytest.mark.asyncio
async def test_unhandled_exceptions_when_creating_user_review_throws_exception(
    httpx_mock,
    caplog: LogCaptureFixture,
    book_recommender_api_client_v2: BookRecommenderApiClientV2,
):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client_v2")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))
    review = _a_random_review()

    # When / Then
    with pytest.raises(BookRecommenderApiServerException) as e:
        await book_recommender_api_client_v2.create_batch_user_reviews([review])

    assert_that(e.value.args[0]).contains(
        "Unable to read within timeout", "https://testurl/reviews/batch/create"
    )


@pytest.fixture
def assert_all_responses_were_requested() -> bool:
    return False


def _a_random_review() -> Dict[str, Any]:
    return {
        "user_id": "1",
        "book_id": "2",
        "date_read": datetime(2017, 1, 1),
        "scrape_time": datetime.now(),
        "user_rating": 5,
    }


def _a_random_book() -> Dict[str, Any]:
    return {
        "work_internal_id": "A Random Work Internal ID",
        "work_id": 12345,
        "author": "A Random Author",
        "author_url": "A Random Author URL",
        "avg_rating": 4.5,
        "rating_histogram": [1, 2, 3, 4, 5],
        "book_id": 1,
        "book_title": "A Random Book Title",
        "book_url": "www.bookurl.com",
        "image_url": "www.imageurl.com",
        "scrape_time": "2022-09-01T00:00:00.000000",
    }

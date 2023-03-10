import json

import httpx
import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that

from src.clients.api_models import BookDataV1
from src.clients.book_recommender_api_client import BookRecommenderApiClient, BookRecommenderApiServerException, \
    BookRecommenderApiClientException
from src.dependencies import Properties

TEST_PROPERTIES = Properties(book_recommender_api_base_url="https://testurl", env_name="test")


@pytest.mark.asyncio
async def test_successful_response_from_user_info_client(httpx_mock, caplog: LogCaptureFixture):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(json={"goofed": "you did"}, status_code=200, url="https://testurl/books/1")
    client = BookRecommenderApiClient(properties=TEST_PROPERTIES)

    # When
    response = await client.create_book(_a_random_book())

    # Then
    assert_that(caplog.text).contains("Successfully wrote book: 1")
    assert_that(response.status_code).is_equal_to(200)


@pytest.mark.parametrize("expected_response_code", [500, 501, 502, 503, 504])
@pytest.mark.asyncio
async def test_5xx_custom_exception_from_user_info_client(expected_response_code, httpx_mock,
                                                          caplog: LogCaptureFixture):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_response(json=json_response, status_code=expected_response_code,
                            url="https://testurl/books/1")
    client = BookRecommenderApiClient(properties=TEST_PROPERTIES)

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await client.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Received 5xx exception from server",
        json.dumps(json_response),
        "https://testurl/books/1",
        "book_id: 1"
    )


@pytest.mark.parametrize("expected_response_code", [400, 401, 402, 403, 404])
@pytest.mark.asyncio
async def test_4xx_custom_exception_from_user_info_client(expected_response_code, httpx_mock,
                                                          caplog: LogCaptureFixture):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_response(json=json_response, status_code=expected_response_code,
                            url="https://testurl/books/1")
    client = BookRecommenderApiClient(properties=TEST_PROPERTIES)

    # When / Then
    with pytest.raises(BookRecommenderApiClientException):
        await client.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Received 4xx exception from server",
        json.dumps(json_response),
        "https://testurl/books/1",
        "book_id: 1"
    )


@pytest.mark.asyncio
async def test_uncaught_exception_from_user_info_client(httpx_mock, caplog: LogCaptureFixture):
    # Given
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))
    client = BookRecommenderApiClient(properties=TEST_PROPERTIES)

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await client.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Unable to read within timeout",
        "https://testurl/books/1",
        "book_id: 1"
    )

@pytest.fixture
def assert_all_responses_were_requested() -> bool:
    return False


def _a_random_book() -> BookDataV1:
    return BookDataV1(
        work_internal_id="A Random Work Internal ID",
        work_id=12345,
        author="A Random Author",
        author_url="A Random Author URL",
        avg_rating=4.5,
        rating_histogram=[1, 2, 3, 4, 5],
        book_id=1,
        book_title="A Random Book Title",
        book_url="www.bookurl.com",
        scrape_time="2022-09-01T00:00:00.000000",
    )

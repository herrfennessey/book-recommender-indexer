import json

import httpx
import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that

from src.clients.api_models import ApiBookPopularityResponse, ApiBookExistsBatchResponse
from src.clients.book_recommender_api_client_v2 import BookRecommenderApiClientV2, BOOK_POPULARITY_THRESHOLD, \
    BookRecommenderApiClientException, BookRecommenderApiServerException
from src.dependencies import Properties
from tests.clients.test_book_recommender_api_client import _a_random_book

TEST_PROPERTIES = Properties(book_recommender_api_base_url_v2="https://testurl", env_name="test")


@pytest.fixture
def book_recommender_api_client_v2():
    return BookRecommenderApiClientV2(properties=TEST_PROPERTIES)


@pytest.mark.asyncio
async def test_200_on_book_popularity_request(httpx_mock, caplog: LogCaptureFixture,
                                              book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given

    httpx_mock.add_response(json={"user_count": 5}, status_code=200,
                            url=f"https://testurl/users/book-popularity/1?limit={BOOK_POPULARITY_THRESHOLD}")
    httpx_mock.add_response(json={"user_count": 0}, status_code=200,
                            url=f"https://testurl/users/book-popularity/2?limit={BOOK_POPULARITY_THRESHOLD}")

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(ApiBookPopularityResponse(book_info={"1": 5, "2": 0}))


@pytest.mark.parametrize("status_code", [429, 503, 504])
@pytest.mark.asyncio
async def test_retryable_exception_doesnt_error_batch_and_doesnt_retry(status_code, httpx_mock,
                                                                       caplog: LogCaptureFixture,
                                                                       book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    httpx_mock.add_response(json={"user_count": 5}, status_code=200,
                            url=f"https://testurl/users/book-popularity/1?limit={BOOK_POPULARITY_THRESHOLD}")
    httpx_mock.add_response(status_code=status_code,
                            url=f"https://testurl/users/book-popularity/2?limit={BOOK_POPULARITY_THRESHOLD}")

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(ApiBookPopularityResponse(book_info={"1": 5}))
    assert_that(len(httpx_mock.get_requests())).is_equal_to(4)  # We currently retry twice, .5 seconds apart


@pytest.mark.asyncio
async def test_non_retryable_exception_doesnt_error_batch_and_retries(httpx_mock, caplog: LogCaptureFixture,
                                                                      book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    httpx_mock.add_response(json={"user_count": 5}, status_code=200,
                            url=f"https://testurl/users/book-popularity/1?limit={BOOK_POPULARITY_THRESHOLD}")
    httpx_mock.add_response(status_code=500,
                            url=f"https://testurl/users/book-popularity/2?limit={BOOK_POPULARITY_THRESHOLD}")

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(ApiBookPopularityResponse(book_info={"1": 5}))
    assert_that(caplog.text).contains("Non retryable http status encountered", "500")
    assert_that(len(httpx_mock.get_requests())).is_equal_to(2)


@pytest.mark.asyncio
async def test_http_exception_on_book_popularity_throws_server_exception(httpx_mock, caplog: LogCaptureFixture,
                                                                         book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When
    response = await book_recommender_api_client_v2.get_book_popularity([1, 2])

    # Then
    assert_that(response).is_equal_to(ApiBookPopularityResponse(book_info={}))
    assert_that(caplog.text).contains("ReadTimeout", "Unable to read within timeout")


@pytest.mark.asyncio
async def test_successful_book_put(httpx_mock, caplog: LogCaptureFixture,
                                   book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(json={}, status_code=200, url="https://testurl/books/1")

    # When
    await book_recommender_api_client_v2.create_book(_a_random_book())

    # Then
    assert_that(caplog.text).contains("Successfully wrote book: 1")


@pytest.mark.asyncio
async def test_empty_response_to_see_if_book_exists(httpx_mock,
                                                    book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    httpx_mock.add_response(json={"book_ids": []}, status_code=200, url="https://testurl/books/batch/exists")

    # When
    response = await book_recommender_api_client_v2.get_already_indexed_books([1])

    # Then
    assert_that(response).is_equal_to(ApiBookExistsBatchResponse(book_ids=[]))


@pytest.mark.asyncio
async def test_200_to_see_if_book_exists(httpx_mock,
                                         book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    httpx_mock.add_response(json={"book_ids": [1]}, status_code=200, url="https://testurl/books/batch/exists")

    # When
    response = await book_recommender_api_client_v2.get_already_indexed_books([1])

    # Then
    assert_that(response).is_equal_to(ApiBookExistsBatchResponse(book_ids=[1]))


@pytest.mark.asyncio
async def test_5xx_when_querying_if_book_exists_throws_exception(httpx_mock, caplog: LogCaptureFixture,
                                                                 book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    httpx_mock.add_response(status_code=500, url="https://testurl/books/batch/exists")

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.get_already_indexed_books([1])


@pytest.mark.asyncio
async def test_unhandled_exceptions_when_querying_if_book_exists_throws_exception(httpx_mock,
                                                                                  caplog: LogCaptureFixture,
                                                                                  book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.get_already_indexed_books([1, 2, 3])

    assert_that(caplog.text).contains("HTTP Error", "Unable to read within timeout", "[1, 2, 3]")

@pytest.mark.parametrize("expected_response_code", [500, 501, 502, 503, 504])
@pytest.mark.asyncio
async def test_5xx_custom_exception_on_book_put(expected_response_code, httpx_mock,
                                                caplog: LogCaptureFixture,
                                                book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_response(json=json_response, status_code=expected_response_code,
                            url="https://testurl/books/1")

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client_v2.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Received 5xx exception from server",
        json.dumps(json_response),
        "https://testurl/books/1",
        "book_id: 1"
    )


@pytest.mark.parametrize("expected_response_code", [400, 401, 402, 403, 404])
@pytest.mark.asyncio
async def test_4xx_custom_exception_on_book_put(expected_response_code, httpx_mock,
                                                caplog: LogCaptureFixture,
                                                book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_response(json=json_response, status_code=expected_response_code,
                            url="https://testurl/books/1")

    # When / Then
    with pytest.raises(BookRecommenderApiClientException):
        await book_recommender_api_client_v2.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Received 4xx exception from server",
        json.dumps(json_response),
        "https://testurl/books/1",
        "book_id: 1"
    )


@pytest.mark.asyncio
async def test_uncaught_exception_on_put_book(httpx_mock, caplog: LogCaptureFixture,
                                              book_recommender_api_client_v2: BookRecommenderApiClientV2):
    # Given
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException) as e:
        await book_recommender_api_client_v2.create_book(_a_random_book())

    assert_that(e.value.args[0]).contains("Unable to read within timeout", "https://testurl/books/1")


@pytest.fixture
def assert_all_responses_were_requested() -> bool:
    return False

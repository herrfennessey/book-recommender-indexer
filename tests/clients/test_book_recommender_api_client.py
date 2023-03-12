import json
from typing import Dict, Any

import httpx
import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from cachetools import TTLCache, LRUCache

from src.clients.book_recommender_api_client import BookRecommenderApiClient, BookRecommenderApiServerException, \
    BookRecommenderApiClientException
from src.dependencies import Properties

TEST_PROPERTIES = Properties(book_recommender_api_base_url="https://testurl", env_name="test")


@pytest.fixture
def book_recommender_api_client():
    return BookRecommenderApiClient(properties=TEST_PROPERTIES,
                                    user_read_books_cache=TTLCache(maxsize=100, ttl=60),
                                    book_exists_cache=LRUCache(maxsize=100))


@pytest.mark.asyncio
async def test_successful_book_put(httpx_mock, caplog: LogCaptureFixture,
                                   book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(json={}, status_code=200, url="https://testurl/books/1")

    # When
    await book_recommender_api_client.create_book(_a_random_book())

    # Then
    assert_that(caplog.text).contains("Successfully wrote book: 1")


@pytest.mark.parametrize("expected_response_code", [500, 501, 502, 503, 504])
@pytest.mark.asyncio
async def test_5xx_custom_exception_on_book_put(expected_response_code, httpx_mock,
                                                caplog: LogCaptureFixture,
                                                book_recommender_api_client: BookRecommenderApiClient):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_response(json=json_response, status_code=expected_response_code,
                            url="https://testurl/books/1")

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client.create_book(_a_random_book())

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
                                                book_recommender_api_client: BookRecommenderApiClient):
    # Given
    json_response = {"goofed": "you did"}
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_response(json=json_response, status_code=expected_response_code,
                            url="https://testurl/books/1")

    # When / Then
    with pytest.raises(BookRecommenderApiClientException):
        await book_recommender_api_client.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Received 4xx exception from server",
        json.dumps(json_response),
        "https://testurl/books/1",
        "book_id: 1"
    )


@pytest.mark.asyncio
async def test_uncaught_exception_on_put_book(httpx_mock, caplog: LogCaptureFixture,
                                              book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("ERROR", logger="book_recommender_api_client")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client.create_book(_a_random_book())

    assert_that(caplog.text).contains(
        "Unable to read within timeout",
        "https://testurl/books/1",
        "book_id: 1"
    )


@pytest.mark.asyncio
async def test_successful_get_books_read(httpx_mock, caplog: LogCaptureFixture,
                                         book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(json={"book_ids": [3, 4, 5]}, status_code=200, url="https://testurl/users/1/book-ids")

    # When
    response = await book_recommender_api_client.get_books_read_by_user(1)

    # Then
    assert_that(response).contains_only(3, 4, 5)


@pytest.mark.asyncio
async def test_cache_prevent_more_than_one_call_to_get_books_read_by_user(httpx_mock, caplog: LogCaptureFixture,
                                                                          book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(json={"book_ids": [3, 4, 5]}, status_code=200, url="https://testurl/users/1/book-ids")
    httpx_mock.add_response(json={"book_ids": [6, 7, 8]}, status_code=200, url="https://testurl/users/2/book-ids")

    # When
    for _ in range(0, 10):
        await book_recommender_api_client.get_books_read_by_user(1)

    await book_recommender_api_client.get_books_read_by_user(2)

    # Then
    assert_that(httpx_mock.get_requests()).is_length(2)
    assert_that(caplog.text).contains("Cache hit!")


@pytest.mark.asyncio
async def test_4xx_when_getting_books_read_returns_empty_array(httpx_mock, caplog: LogCaptureFixture,
                                                               book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(status_code=404, url="https://testurl/users/1/book-ids")

    # When
    response = await book_recommender_api_client.get_books_read_by_user(1)

    # Then
    assert_that(caplog.text).contains("Received 4xx exception from server", "user_id: 1",
                                      "https://testurl/users/1/book-ids")
    assert_that(response).is_equal_to(list())


@pytest.mark.asyncio
async def test_4xx_still_gets_cached(httpx_mock, caplog: LogCaptureFixture,
                                     book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(status_code=404, url="https://testurl/users/1/book-ids")

    # When
    for _ in range(10):
        response = await book_recommender_api_client.get_books_read_by_user(1)

    # Then
    assert_that(caplog.text).contains("Received 4xx exception from server", "user_id: 1",
                                      "https://testurl/users/1/book-ids")
    assert_that(response).is_equal_to(list())
    assert_that(httpx_mock.get_requests()).is_length(1)


@pytest.mark.asyncio
async def test_5xx_when_getting_books_read_throws_exception(httpx_mock, caplog: LogCaptureFixture,
                                                            book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(status_code=500, url="https://testurl/users/1/book-ids")

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client.get_books_read_by_user(1)

    assert_that(caplog.text).contains("Received 5xx exception from server", "user_id: 1",
                                      "https://testurl/users/1/book-ids")


@pytest.mark.asyncio
async def test_unhandled_exceptions_when_getting_books_read_throws_exception(httpx_mock, caplog: LogCaptureFixture,
                                                                             book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client.get_books_read_by_user(1)

    assert_that(caplog.text).contains("Uncaught Exception", "Unable to read within timeout", "user_id: 1",
                                      "https://testurl/users/1/book-ids")


@pytest.mark.asyncio
async def test_404_to_see_if_book_exists(httpx_mock,
                                         book_recommender_api_client: BookRecommenderApiClient):
    # Given
    httpx_mock.add_response(status_code=404, url="https://testurl/books/1")

    # When
    response = await book_recommender_api_client.does_book_exist(1)

    # Then
    assert_that(response).is_false()


@pytest.mark.asyncio
async def test_200_to_see_if_book_exists(httpx_mock,
                                         book_recommender_api_client: BookRecommenderApiClient):
    # Given
    httpx_mock.add_response(status_code=200, url="https://testurl/books/1")

    # When
    response = await book_recommender_api_client.does_book_exist(1)

    # Then
    assert_that(response).is_true()


@pytest.mark.asyncio
async def test_cache_prevent_more_than_one_call_to_check_if_book_exists(httpx_mock,
                                                                        book_recommender_api_client: BookRecommenderApiClient):
    # Given
    httpx_mock.add_response(status_code=200, url="https://testurl/books/1")

    # When
    for _ in range(0, 10):
        response = await book_recommender_api_client.does_book_exist(1)

    # Then
    assert_that(response).is_true()
    assert_that(httpx_mock.get_requests()).is_length(1)


@pytest.mark.asyncio
async def test_cache_doesnt_save_404_on_check_if_book_exists(httpx_mock,
                                                             book_recommender_api_client: BookRecommenderApiClient):
    # Given
    httpx_mock.add_response(status_code=404, url="https://testurl/books/1")

    # When
    for _ in range(0, 10):
        response = await book_recommender_api_client.does_book_exist(1)

    # Then
    assert_that(response).is_false()
    assert_that(httpx_mock.get_requests()).is_length(10)


@pytest.mark.asyncio
async def test_4xx_when_querying_if_book_exists_returns_false(httpx_mock, caplog: LogCaptureFixture,
                                                              book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(status_code=404, url="https://testurl/books/1")

    # When
    response = await book_recommender_api_client.does_book_exist(1)

    # Then
    assert_that(response).is_false()
    assert_that(caplog.text).contains("does not exist", "book_id: 1", "https://testurl/books/1")


@pytest.mark.asyncio
async def test_5xx_when_querying_if_book_exists_throws_exception(httpx_mock, caplog: LogCaptureFixture,
                                                                 book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_response(status_code=500, url="https://testurl/books/1")

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client.does_book_exist(1)

    assert_that(caplog.text).contains("Received 5xx exception from server", "book_id: 1",
                                      "https://testurl/books/1")


@pytest.mark.asyncio
async def test_unhandled_exceptions_when_querying_if_book_exists_throws_exception(httpx_mock,
                                                                                  caplog: LogCaptureFixture,
                                                                                  book_recommender_api_client: BookRecommenderApiClient):
    # Given
    caplog.set_level("INFO", logger="book_recommender_api_client")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read within timeout"))

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        await book_recommender_api_client.does_book_exist(1)

    assert_that(caplog.text).contains("Uncaught Exception", "Unable to read within timeout", "book_id: 1",
                                      "https://testurl/books/1")


@pytest.fixture
def assert_all_responses_were_requested() -> bool:
    return False


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
        "scrape_time": "2022-09-01T00:00:00.000000",
    }

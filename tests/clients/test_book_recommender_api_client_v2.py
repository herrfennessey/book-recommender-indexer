import httpx
import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that

from src.clients.api_models import ApiBookPopularityResponse
from src.clients.book_recommender_api_client_v2 import BookRecommenderApiClientV2, BOOK_POPULARITY_THRESHOLD
from src.dependencies import Properties

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


@pytest.fixture
def assert_all_responses_were_requested() -> bool:
    return False

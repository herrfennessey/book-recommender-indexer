from typing import List, Dict
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from assertpy import assert_that

from src.clients.api_models import ApiBookPopularityResponse, ApiBookExistsBatchResponse
from src.clients.book_recommender_api_client import BookRecommenderApiServerException
from src.clients.book_recommender_api_client_v2 import BOOK_POPULARITY_THRESHOLD
from src.services.book_task_enqueuer_service import BookTaskEnqueuerService

USER_ID = 1
BOOK_ID = 2


@pytest.fixture()
def book_recommender_api_client():
    with patch('src.clients.book_recommender_api_client') as mock_book_recommender_api_client:
        mock_book_recommender_api_client.reset_mock()
        yield mock_book_recommender_api_client


@pytest.fixture()
def book_recommender_api_client_v2():
    with patch('src.clients.book_recommender_api_client_v2') as mock_book_recommender_api_client_v2:
        mock_book_recommender_api_client_v2.reset_mock()
        yield mock_book_recommender_api_client_v2


@pytest.fixture()
def task_client():
    with patch('src.clients.task_client') as mock_task_client:
        mock_task_client.enqueue_book = MagicMock(side_effect=lambda book_id: f"book-{book_id}")
        yield mock_task_client


@pytest.mark.asyncio
async def test_book_task_enqueuer_correctly_filters_by_threshold(book_recommender_api_client,
                                                                 book_recommender_api_client_v2, task_client):
    # Given
    book_task_enqueuer_service = BookTaskEnqueuerService(book_recommender_api_client,
                                                         book_recommender_api_client_v2, task_client)
    return_book_info = {
        "1": 0,
        "2": 1,
        "3": 6
    }
    _get_book_popularity_call_returns_dict(book_recommender_api_client_v2, return_book_info)
    _already_indexed_books_call_returns(book_recommender_api_client, [])

    # When
    response = await book_task_enqueuer_service.enqueue_books_if_necessary([1, 2, 3])

    # Then
    assert_that(book_recommender_api_client.get_already_indexed_books.call_count).is_equal_to(1)
    book_recommender_api_client.get_already_indexed_books.assert_called_with([3])
    assert_that(response.tasks).is_length(1)


@pytest.mark.asyncio
async def test_book_task_enqueuer_correctly_filters_by_books_already_indexed(book_recommender_api_client,
                                                                             book_recommender_api_client_v2,
                                                                             task_client):
    # Given
    book_task_enqueuer_service = BookTaskEnqueuerService(book_recommender_api_client,
                                                         book_recommender_api_client_v2, task_client)
    return_book_info = {
        "1": BOOK_POPULARITY_THRESHOLD
    }
    _get_book_popularity_call_returns_dict(book_recommender_api_client_v2, return_book_info)
    _already_indexed_books_call_returns(book_recommender_api_client, [1])

    # When
    response = await book_task_enqueuer_service.enqueue_books_if_necessary([1])

    # Then
    task_client.enqueue_books.assert_not_called()
    assert_that(response.tasks).is_length(0)


@pytest.mark.asyncio
async def test_task_queue_correctly_enqueues_and_returns_task_name(book_recommender_api_client,
                                                                   book_recommender_api_client_v2, task_client):
    # Given
    book_task_enqueuer_service = BookTaskEnqueuerService(book_recommender_api_client,
                                                         book_recommender_api_client_v2, task_client)
    return_book_info = {
        "1": BOOK_POPULARITY_THRESHOLD,
        "2": BOOK_POPULARITY_THRESHOLD,
    }
    _get_book_popularity_call_returns_dict(book_recommender_api_client_v2, return_book_info)
    _already_indexed_books_call_returns(book_recommender_api_client, [])

    # When
    response = await book_task_enqueuer_service.enqueue_books_if_necessary([1, 2, 3])

    # Then
    assert_that(response.tasks).contains("book-1", "book-2")


@pytest.mark.asyncio
async def test_book_popularity_call_service_exceptions_bubble_up(book_recommender_api_client,
                                                                 book_recommender_api_client_v2, task_client):
    # Given
    book_task_enqueuer_service = BookTaskEnqueuerService(book_recommender_api_client,
                                                         book_recommender_api_client_v2, task_client)
    book_recommender_api_client_v2.get_book_popularity = AsyncMock(side_effect=BookRecommenderApiServerException)

    # When
    with pytest.raises(BookRecommenderApiServerException):
        await book_task_enqueuer_service.enqueue_books_if_necessary([1, 2, 3])


@pytest.mark.asyncio
async def test_already_indexed_book_call_exceptions_bubble_up(book_recommender_api_client,
                                                              book_recommender_api_client_v2, task_client):
    # Given
    book_task_enqueuer_service = BookTaskEnqueuerService(book_recommender_api_client,
                                                         book_recommender_api_client_v2, task_client)
    return_book_info = {
        "1": BOOK_POPULARITY_THRESHOLD,
        "2": BOOK_POPULARITY_THRESHOLD,
    }
    _get_book_popularity_call_returns_dict(book_recommender_api_client_v2, return_book_info)
    book_recommender_api_client.get_already_indexed_books = AsyncMock(side_effect=BookRecommenderApiServerException)

    # When
    with pytest.raises(BookRecommenderApiServerException):
        await book_task_enqueuer_service.enqueue_books_if_necessary([1, 2, 3])


@pytest.mark.asyncio
async def test_that_payload_full_of_ineligible_books_does_not_make_book_exists_call(book_recommender_api_client,
                                                                                    task_client):
    # Given
    book_task_enqueuer_service = BookTaskEnqueuerService(book_recommender_api_client,
                                                         book_recommender_api_client_v2, task_client)
    return_book_info = {
        "1": 0,
        "2": 0,
        "3": 0
    }
    _get_book_popularity_call_returns_dict(book_recommender_api_client_v2, return_book_info)

    # When
    response = await book_task_enqueuer_service.enqueue_books_if_necessary([1, 2, 3])

    # Then
    assert_that(response.tasks).is_empty()


def _get_book_popularity_call_returns_dict(book_recommender_api_client_v2, return_book_info: Dict[str, int]):
    book_recommender_api_client_v2.get_book_popularity = AsyncMock(
        return_value=ApiBookPopularityResponse(book_info=return_book_info))


def _already_indexed_books_call_returns(book_recommender_api_client, return_ids: List[int]):
    book_recommender_api_client.get_already_indexed_books = AsyncMock(
        return_value=ApiBookExistsBatchResponse(book_ids=return_ids))

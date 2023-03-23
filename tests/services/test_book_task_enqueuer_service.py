from typing import List, Dict
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from assertpy import assert_that

from src.clients.api_models import UserReviewBatchResponse, ApiBookPopularityResponse, ApiBookExistsBatchResponse
from src.clients.book_recommender_api_client import BookRecommenderApiClient
from src.clients.pubsub_audit_client import PubSubAuditClient
from src.clients.task_client import TaskClient
from src.routes.pubsub_models import PubSubUserReviewV1
from src.services.book_task_enqueuer_service import BookTaskEnqueuerService
from src.services.user_review_service import UserReviewService

USER_ID = 1
BOOK_ID = 2


@pytest.fixture()
def book_recommender_api_client():
    with patch('src.clients.book_recommender_api_client') as mock_book_recommender_api_client:
        yield mock_book_recommender_api_client


@pytest.fixture()
def task_client():
    with patch('src.clients.task_client') as mock_task_client:
        mock_task_client.enqueue_book = MagicMock(side_effect=lambda book_id: f"book-{book_id}")
        yield mock_task_client


@pytest.mark.asyncio
async def test_book_task_enqueuer_correctly_filters_by_threshold(book_recommender_api_client, task_client):
    # Given
    book_task_enqueuer_service = BookTaskEnqueuerService(book_recommender_api_client, task_client)
    return_book_info = {
        "1": 0,
        "2": 1,
        "3": 6
    }
    _get_book_popularity_call_returns_dict(book_recommender_api_client, return_book_info)
    _already_indexed_books_call_returns(book_recommender_api_client, [])

    # When
    response = await book_task_enqueuer_service.enqueue_books_if_necessary([1, 2, 3])

    # Then
    assert_that(response.tasks).is_length(1)


def _get_book_popularity_call_returns_dict(book_recommender_api_client, return_book_info: Dict[str, int]):
    book_recommender_api_client.get_book_popularity = AsyncMock(
        return_value=ApiBookPopularityResponse(book_info=return_book_info))


def _already_indexed_books_call_returns(book_recommender_api_client, return_ids: List[int]):
    book_recommender_api_client.get_already_indexed_books = AsyncMock(
        return_value=ApiBookExistsBatchResponse(book_ids=return_ids))

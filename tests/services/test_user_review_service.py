from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from assertpy import assert_that

from src.clients.book_recommender_api_client import BookRecommenderApiClient
from src.clients.task_client import TaskClient
from src.routes.pubsub_models import PubSubUserReviewV1
from src.services.user_review_service import UserReviewService

USER_ID = 1
BOOK_ID = 2


@pytest.fixture()
def book_recommender_api_client():
    with patch('src.clients.book_recommender_api_client') as mock_book_recommender_api_client:
        mock_book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[])
        mock_book_recommender_api_client.does_book_exist = AsyncMock(return_value=False)
        mock_book_recommender_api_client.create_user_review = AsyncMock(return_value=None)
        yield mock_book_recommender_api_client


@pytest.fixture()
def task_client():
    with patch('src.clients.task_client') as mock_task_client:
        mock_task_client.enqueue_book = MagicMock(return_value="foo")
        yield mock_task_client


@pytest.mark.asyncio
async def test_review_exists_book_exists(book_recommender_api_client: BookRecommenderApiClient,
                                         task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[BOOK_ID])
    book_recommender_api_client.does_book_exist = AsyncMock(return_value=True)
    service = UserReviewService(book_recommender_api_client, task_client)
    review = _a_pubsub_user_review()

    # When
    response = await service.process_pubsub_message(review)

    # Then
    assert_that(response.indexed_review).is_false()
    assert_that(response.scraped_book_task).is_none()

    book_recommender_api_client.create_user_review.assert_not_called()
    task_client.enqueue_book.assert_not_called()


@pytest.mark.asyncio
async def test_review_doesnt_exist_book_exists(book_recommender_api_client: BookRecommenderApiClient,
                                               task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client.does_book_exist = AsyncMock(return_value=True)
    service = UserReviewService(book_recommender_api_client, task_client)
    review = _a_pubsub_user_review()

    # When
    response = await service.process_pubsub_message(review)

    # Then
    assert_that(response.indexed_review).is_true()
    assert_that(response.scraped_book_task).is_none()
    book_recommender_api_client.create_user_review.assert_called_with(review.dict())
    task_client.enqueue_book.assert_not_called()


@pytest.mark.asyncio
async def test_review_exists_book_doesnt_exist(book_recommender_api_client: BookRecommenderApiClient,
                                               task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[BOOK_ID])
    book_recommender_api_client.does_book_exist = AsyncMock(return_value=False)
    service = UserReviewService(book_recommender_api_client, task_client)
    review = _a_pubsub_user_review()

    # When
    response = await service.process_pubsub_message(review)

    # Then
    assert_that(response.indexed_review).is_false()
    assert_that(response.scraped_book_task).is_equal_to("foo")
    book_recommender_api_client.create_user_review.assert_not_called()
    task_client.enqueue_book.assert_called_with(BOOK_ID)


@pytest.mark.asyncio
async def test_review_doesnt_exist_book_doesnt_exist(book_recommender_api_client: BookRecommenderApiClient,
                                                     task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client.does_book_exist = AsyncMock(return_value=False)

    service = UserReviewService(book_recommender_api_client, task_client)
    review = _a_pubsub_user_review()

    # When
    response = await service.process_pubsub_message(review)

    # Then
    assert_that(response.indexed_review).is_true()
    assert_that(response.scraped_book_task).is_equal_to("foo")
    book_recommender_api_client.create_user_review.assert_called_with(review.dict())
    task_client.enqueue_book.assert_called_with(BOOK_ID)


def _a_pubsub_user_review():
    return PubSubUserReviewV1(
        user_id=USER_ID,
        book_id=BOOK_ID,
        user_rating=3,
        date_read="2022-01-01",
        scrape_time="2022-03-12T12:00:00"
    )

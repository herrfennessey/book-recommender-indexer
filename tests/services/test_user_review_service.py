from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from assertpy import assert_that

from src.clients.api_models import UserReviewBatchResponse
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
        mock_book_recommender_api_client.get_already_indexed_books = AsyncMock(return_value=[])
        mock_book_recommender_api_client.create_batch_user_reviews = AsyncMock(
            return_value=UserReviewBatchResponse(indexed=0))
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
    book_recommender_api_client.get_already_indexed_books = AsyncMock(return_value=[BOOK_ID])
    service = UserReviewService(book_recommender_api_client, task_client)
    reviews_to_index = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews_to_index)

    # Then
    assert_that(response.indexed).is_equal_to(0)
    assert_that(response.tasks).is_empty()

    book_recommender_api_client.create_batch_user_reviews.assert_not_called()
    task_client.enqueue_book.assert_not_called()


@pytest.mark.asyncio
async def test_one_review_exists_one_book_exists(book_recommender_api_client: BookRecommenderApiClient,
                                                 task_client: TaskClient):
    # Given
    new_book_id = 5
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[BOOK_ID])
    book_recommender_api_client.get_already_indexed_books = AsyncMock(return_value=[BOOK_ID])
    book_recommender_api_client.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=1))
    service = UserReviewService(book_recommender_api_client, task_client)
    reviews_to_index = [_a_pubsub_user_review(book_id=BOOK_ID), _a_pubsub_user_review(book_id=new_book_id)]

    # When
    response = await service.process_pubsub_batch_message(reviews_to_index)

    # Then
    assert_that(response.indexed).is_equal_to(1)
    assert_that(response.tasks).is_length(1)

    book_recommender_api_client.create_batch_user_reviews.assert_called_once()
    task_client.enqueue_book.assert_called_with(new_book_id)


@pytest.mark.asyncio
async def test_review_doesnt_exist_book_exists(book_recommender_api_client: BookRecommenderApiClient,
                                               task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client.get_already_indexed_books = AsyncMock(return_value=[BOOK_ID])
    book_recommender_api_client.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=1))
    service = UserReviewService(book_recommender_api_client, task_client)
    reviews = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_equal_to(1)
    assert_that(response.tasks).is_empty()

    book_recommender_api_client.create_batch_user_reviews.assert_called_once()
    task_client.enqueue_book.assert_not_called()


@pytest.mark.asyncio
async def test_review_exists_book_doesnt_exist(book_recommender_api_client: BookRecommenderApiClient,
                                               task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[BOOK_ID])
    book_recommender_api_client.get_already_indexed_books = AsyncMock(return_value=[])

    service = UserReviewService(book_recommender_api_client, task_client)
    reviews = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_equal_to(0)
    assert_that(response.tasks).is_length(1)

    book_recommender_api_client.create_batch_user_reviews.assert_not_called()
    task_client.enqueue_book.assert_called_with(BOOK_ID)


@pytest.mark.asyncio
async def test_review_doesnt_exist_book_doesnt_exist(book_recommender_api_client: BookRecommenderApiClient,
                                                     task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client.get_already_indexed_books = AsyncMock(return_value=[])
    book_recommender_api_client.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=1))

    service = UserReviewService(book_recommender_api_client, task_client)
    reviews = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_equal_to(1)
    assert_that(response.tasks).is_length(1)

    book_recommender_api_client.create_batch_user_reviews.assert_called_once()
    task_client.enqueue_book.assert_called_with(BOOK_ID)


@pytest.mark.asyncio
async def test_multiple_books_multiple_reviews_indexed(book_recommender_api_client: BookRecommenderApiClient,
                                                       task_client: TaskClient):
    # Given
    book_recommender_api_client.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client.get_already_indexed_books = AsyncMock(return_value=[])
    book_recommender_api_client.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=3))

    service = UserReviewService(book_recommender_api_client, task_client)
    reviews = []
    for i in range(5):
        reviews.append(_a_pubsub_user_review(book_id=BOOK_ID + i))

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_equal_to(3)
    assert_that(response.tasks).is_length(len(reviews))

    book_recommender_api_client.create_batch_user_reviews.assert_called_once()
    assert_that(task_client.enqueue_book.call_count).is_equal_to(len(reviews))


def _a_pubsub_user_review(user_id: int = USER_ID, book_id: int = BOOK_ID):
    return PubSubUserReviewV1(
        user_id=user_id,
        book_id=book_id,
        user_rating=3,
        date_read="2022-01-01",
        scrape_time="2022-03-12T12:00:00"
    )

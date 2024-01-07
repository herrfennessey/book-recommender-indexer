from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from assertpy import assert_that

from src.clients.api_models import UserReviewBatchResponse
from src.clients.book_recommender_api_client_v2 import BookRecommenderApiClientV2
from src.clients.pubsub_audit_client import PubSubAuditClient
from src.routes.pubsub_models import PubSubUserReviewV1
from src.services.user_review_service import UserReviewService

USER_ID = 1
BOOK_ID = 2


@pytest.fixture()
def book_recommender_api_client_v2():
    with patch('src.clients.book_recommender_api_client_v2') as mock_book_recommender_api_client_v2:
        mock_book_recommender_api_client_v2.get_books_read_by_user = AsyncMock(return_value=[])
        mock_book_recommender_api_client_v2.create_batch_user_reviews = AsyncMock(
            return_value=UserReviewBatchResponse(indexed=0))
        yield mock_book_recommender_api_client_v2


@pytest.fixture()
def pubsub_audit_client():
    with patch('src.clients.pubsub_audit_client') as mock_pubsub_audit_client:
        mock_pubsub_audit_client.send_batch = MagicMock()
        yield mock_pubsub_audit_client


@pytest.mark.asyncio
async def test_review_exists_book_exists(book_recommender_api_client_v2: BookRecommenderApiClientV2,
                                         pubsub_audit_client: PubSubAuditClient):
    # Given
    book_recommender_api_client_v2.get_books_read_by_user = AsyncMock(return_value=[BOOK_ID])
    service = UserReviewService(book_recommender_api_client_v2, pubsub_audit_client)
    reviews_to_index = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews_to_index)

    # Then
    assert_that(response.indexed).is_length(0)

    book_recommender_api_client_v2.create_batch_user_reviews.assert_not_called()
    assert_that(pubsub_audit_client.send_batch.call_count).is_equal_to(0)


@pytest.mark.asyncio
async def test_one_review_exists_one_book_exists(book_recommender_api_client_v2: BookRecommenderApiClientV2,pubsub_audit_client: PubSubAuditClient):
    # Given
    new_book_id = 5
    book_recommender_api_client_v2.get_books_read_by_user = AsyncMock(return_value=[BOOK_ID])
    book_recommender_api_client_v2.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=1))
    service = UserReviewService(book_recommender_api_client_v2, pubsub_audit_client)
    reviews_to_index = [_a_pubsub_user_review(book_id=BOOK_ID), _a_pubsub_user_review(book_id=new_book_id)]

    # When
    response = await service.process_pubsub_batch_message(reviews_to_index)

    # Then
    assert_that(response.indexed).is_length(1)

    book_recommender_api_client_v2.create_batch_user_reviews.assert_called_once()
    assert_that(pubsub_audit_client.send_batch.call_count).is_equal_to(1)


@pytest.mark.asyncio
async def test_review_doesnt_exist_book_exists(book_recommender_api_client_v2: BookRecommenderApiClientV2, pubsub_audit_client: PubSubAuditClient):
    # Given
    book_recommender_api_client_v2.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client_v2.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=1))
    service = UserReviewService(book_recommender_api_client_v2, pubsub_audit_client)
    reviews = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_length(1)

    book_recommender_api_client_v2.create_batch_user_reviews.assert_called_once()
    assert_that(pubsub_audit_client.send_batch.call_count).is_equal_to(1)


@pytest.mark.asyncio
async def test_review_exists_book_doesnt_exist(book_recommender_api_client_v2: BookRecommenderApiClientV2,pubsub_audit_client: PubSubAuditClient):
    # Given
    book_recommender_api_client_v2.get_books_read_by_user = AsyncMock(return_value=[BOOK_ID])

    service = UserReviewService(book_recommender_api_client_v2, pubsub_audit_client)
    reviews = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_length(0)

    book_recommender_api_client_v2.create_batch_user_reviews.assert_not_called()
    assert_that(pubsub_audit_client.send_batch.call_count).is_equal_to(0)


@pytest.mark.asyncio
async def test_review_doesnt_exist_book_doesnt_exist(book_recommender_api_client_v2: BookRecommenderApiClientV2, pubsub_audit_client: PubSubAuditClient):
    # Given
    book_recommender_api_client_v2.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client_v2.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=1))

    service = UserReviewService(book_recommender_api_client_v2, pubsub_audit_client)
    reviews = [_a_pubsub_user_review()]

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_length(1)

    book_recommender_api_client_v2.create_batch_user_reviews.assert_called_once()
    assert_that(pubsub_audit_client.send_batch.call_count).is_equal_to(1)


@pytest.mark.asyncio
async def test_multiple_books_multiple_reviews_indexed(book_recommender_api_client_v2: BookRecommenderApiClientV2,pubsub_audit_client: PubSubAuditClient):
    # Given
    book_recommender_api_client_v2.get_books_read_by_user = AsyncMock(return_value=[])
    book_recommender_api_client_v2.create_batch_user_reviews = AsyncMock(return_value=UserReviewBatchResponse(indexed=5))

    service = UserReviewService(book_recommender_api_client_v2, pubsub_audit_client)
    reviews = []
    for i in range(5):
        reviews.append(_a_pubsub_user_review(book_id=BOOK_ID + i))

    # When
    response = await service.process_pubsub_batch_message(reviews)

    # Then
    assert_that(response.indexed).is_length(5)

    assert_that(pubsub_audit_client.send_batch.call_count).is_equal_to(1)

    book_recommender_api_client_v2.create_batch_user_reviews.assert_called_once()


def _a_pubsub_user_review(user_id: int = USER_ID, book_id: int = BOOK_ID):
    return PubSubUserReviewV1(
        user_id=user_id,
        book_id=book_id,
        user_rating=3,
        date_read="2022-01-01",
        scrape_time="2022-03-12T12:00:00"
    )

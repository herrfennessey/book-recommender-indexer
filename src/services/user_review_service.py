import logging
from typing import List, Dict

from fastapi import Depends
from pydantic import BaseModel

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client
from src.clients.pubsub_audit_client import PubSubAuditClient, get_pubsub_audit_client, ItemTopic
from src.clients.task_client import get_task_client, TaskClient
from src.routes.pubsub_models import PubSubUserReviewV1

logger = logging.getLogger(__name__)


class UserReviewServiceResponse(BaseModel):
    indexed: List[PubSubUserReviewV1]


class UserReviewService(object):
    def __init__(self, book_recommender_api_client: BookRecommenderApiClient, task_client: TaskClient,
                 audit_client: PubSubAuditClient):
        self.book_recommender_api_client = book_recommender_api_client
        self.task_client = task_client
        self.audit_client = audit_client

    async def process_pubsub_batch_message(self, pubsub_message: List[PubSubUserReviewV1]) -> UserReviewServiceResponse:
        service_response = UserReviewServiceResponse(indexed=[])

        # Partition by user ID - it should already be, but can't be too certain
        user_to_review_batch_dict: Dict[int, List[PubSubUserReviewV1]] = {}
        for user_review in pubsub_message:
            if user_review.user_id not in user_to_review_batch_dict:
                user_to_review_batch_dict[user_review.user_id] = []
            user_to_review_batch_dict[user_review.user_id].append(user_review)

        for user_id, user_reviews in user_to_review_batch_dict.items():
            remaining_reviews_to_index = await self._remove_reviews_already_indexed(user_id, user_reviews)

            if len(remaining_reviews_to_index) > 0:
                batch_user_reviews = [review.dict() for review in remaining_reviews_to_index]
                await self.book_recommender_api_client.create_batch_user_reviews(
                    batch_user_reviews)
                self.audit_client.send_batch(ItemTopic.USER_REVIEW, batch_user_reviews)
                service_response.indexed.extend(remaining_reviews_to_index)
                # We intentionally allow 5xx and uncaught exceptions to bubble up to the caller
            else:
                logger.info("All reviews for user_id: {} already indexed".format(user_id))

        return service_response

    async def _remove_reviews_already_indexed(self, user_id,
                                              user_reviews: List[PubSubUserReviewV1]) -> List[PubSubUserReviewV1]:
        reviews_to_index = user_reviews.copy()
        books_read_by_user = await self.book_recommender_api_client.get_books_read_by_user(user_id)
        for review in user_reviews:
            if review.book_id in books_read_by_user:
                reviews_to_index.remove(review)
        return reviews_to_index


def get_user_review_service(
        book_recommender_api_client: BookRecommenderApiClient = Depends(get_book_recommender_api_client),
        task_client: TaskClient = Depends(get_task_client),
        pubsub_audit_client: PubSubAuditClient = Depends(get_pubsub_audit_client)
) -> UserReviewService:
    return UserReviewService(book_recommender_api_client, task_client, pubsub_audit_client)

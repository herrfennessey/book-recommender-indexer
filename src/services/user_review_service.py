import logging
from typing import List, Dict

from fastapi import Depends
from pydantic import BaseModel

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client, \
    BookRecommenderApiClientException
from src.clients.task_client import get_task_client, TaskClient
from src.routes.pubsub_models import PubSubUserReviewV1

logger = logging.getLogger(__name__)


class UserReviewServiceResponse(BaseModel):
    indexed: int = 0
    tasks: List[str] = []


class UserReviewService(object):
    def __init__(self, book_recommender_api_client: BookRecommenderApiClient, task_client: TaskClient):
        self.book_recommender_api_client = book_recommender_api_client
        self.task_client = task_client

    async def process_pubsub_batch_message(self, pubsub_message: List[PubSubUserReviewV1]) -> UserReviewServiceResponse:
        service_response = UserReviewServiceResponse()

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
                create_response = await self.book_recommender_api_client.create_batch_user_reviews(
                    batch_user_reviews)
                service_response.indexed += create_response.indexed
                # We intentionally allow 5xx and uncaught exceptions to bubble up to the caller
            else:
                logger.info("All reviews for user_id: {} already indexed".format(user_id))

            # Now kick off an asynchronous scrape for books which also don't exist yet
            books_in_reviews = [review.book_id for review in user_reviews]
            books_to_scrape = await self._remove_books_already_indexed(books_in_reviews)
            for book_id in books_to_scrape:
                logging.info("Attempting to enqueue book_id: %s", book_id)
                task_name = self.task_client.enqueue_book(book_id)
                service_response.tasks.append(task_name)

        return service_response

    async def _remove_reviews_already_indexed(self, user_id,
                                              user_reviews: List[PubSubUserReviewV1]) -> List[PubSubUserReviewV1]:
        candidates = user_reviews.copy()
        books_read_by_user = await self.book_recommender_api_client.get_books_read_by_user(user_id)
        books_read_by_user_set = set(books_read_by_user)
        for review in candidates:
            if review.book_id in books_read_by_user_set:
                candidates.remove(review)
        return candidates

    async def _remove_books_already_indexed(self, books_in_reviews: List[int]) -> List[int]:
        candidates = set(books_in_reviews.copy())
        books_in_api = await self.book_recommender_api_client.get_already_indexed_books(books_in_reviews)
        books_in_api_set = set(books_in_api)
        candidates = candidates - books_in_api_set
        return list(candidates)


def get_user_review_service(
        book_recommender_api_client: BookRecommenderApiClient = Depends(get_book_recommender_api_client),
        task_client: TaskClient = Depends(get_task_client)
) -> UserReviewService:
    return UserReviewService(book_recommender_api_client, task_client)

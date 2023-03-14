import logging

from fastapi import Depends
from pydantic import BaseModel

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client, \
    BookRecommenderApiClientException
from src.clients.task_client import get_task_client, TaskClient
from src.routes.pubsub_models import PubSubUserReviewV1

logger = logging.getLogger(__name__)


class UserReviewServiceResponse(BaseModel):
    indexed_review: bool = False
    scraped_book_task: str = None


class UserReviewService(object):
    def __init__(self, book_recommender_api_client: BookRecommenderApiClient, task_client: TaskClient):
        self.book_recommender_api_client = book_recommender_api_client
        self.task_client = task_client

    async def process_pubsub_message(self, pubsub_message: PubSubUserReviewV1) -> UserReviewServiceResponse:
        response = UserReviewServiceResponse()

        book_id = pubsub_message.book_id
        user_id = pubsub_message.user_id
        if await self._do_we_need_to_index_user_review(user_id, book_id):
            try:
                await self.book_recommender_api_client.create_user_review(pubsub_message.dict())
                response.indexed_review = True
            except BookRecommenderApiClientException as e:
                logger.error("Received 4xx response from API - Failed to index user review: {}".format(e))
                return response
            # We intentionally allow 5xx and uncaught exceptions to bubble up to the caller
        else:
            logger.info("User review already indexed for user_id: {} and book_id: {}".format(
                user_id, book_id))

        # If this is a new book, we should also trigger a background task to scrape it, but the user shouldn't wait
        book_exists = await self.book_recommender_api_client.does_book_exist(book_id)
        if not book_exists:
            response.scraped_book_task = self.task_client.enqueue_book(book_id)
        return response

    async def _do_we_need_to_index_user_review(self, user_id, book_id):
        books_read_by_user = await self.book_recommender_api_client.get_books_read_by_user(user_id)
        return book_id not in books_read_by_user


def get_user_review_service(
        book_recommender_api_client: BookRecommenderApiClient = Depends(get_book_recommender_api_client),
        task_client: TaskClient = Depends(get_task_client)
) -> UserReviewService:
    return UserReviewService(book_recommender_api_client, task_client)

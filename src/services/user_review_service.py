import logging

from fastapi import Depends

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client, \
    BookRecommenderApiClientException
from src.routes.pubsub_models import PubSubUserReviewV1

logger = logging.getLogger(__name__)

class UserReviewService(object):
    def __init__(self, book_recommender_api_client: BookRecommenderApiClient):
        self.book_recommender_api_client = book_recommender_api_client

    async def process_pubsub_message(self, pubsub_message: PubSubUserReviewV1):
        if self._do_we_need_to_index_user_review(pubsub_message.user_id, pubsub_message.book_id):
            try:
                await self.book_recommender_api_client.create_user_review(pubsub_message.dict())
            except BookRecommenderApiClientException as e:
                logger.error("Received 4xx response from API - Failed to index user review: {}".format(e))
            # We intentionally allow 5xx and uncaught exceptions to bubble up to the caller

    async def _do_we_need_to_index_user_review(self, user_id, book_id):
        books_read_by_user = await self.book_recommender_api_client.get_books_read_by_user(user_id)
        return book_id not in books_read_by_user

def get_user_review_service(
        book_recommender_api_client: BookRecommenderApiClient = Depends(get_book_recommender_api_client)):
    return UserReviewService(book_recommender_api_client)

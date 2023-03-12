import logging

from fastapi import Depends, BackgroundTasks
from pydantic import BaseModel

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client, \
    BookRecommenderApiClientException
from src.clients.scraper_client_v2 import ScraperClientV2, get_scraper_client_v2, get_background_tasks
from src.routes.pubsub_models import PubSubUserReviewV1

logger = logging.getLogger(__name__)


class UserReviewServiceResponse(BaseModel):
    indexed_review: bool = False
    book_missing: bool = False


class UserReviewService(object):
    def __init__(self, book_recommender_api_client: BookRecommenderApiClient,
                 scraper_client: ScraperClientV2,
                 background_tasks: BackgroundTasks):
        self.book_recommender_api_client = book_recommender_api_client
        self.scraper_client = scraper_client
        self.background_tasks = background_tasks

    async def process_pubsub_message(self, pubsub_message: PubSubUserReviewV1) -> UserReviewServiceResponse:
        response = UserReviewServiceResponse()

        if self._do_we_need_to_index_user_review(pubsub_message.user_id, pubsub_message.book_id):
            try:
                await self.book_recommender_api_client.create_user_review(pubsub_message.dict())
                response.indexed_review = True
            except BookRecommenderApiClientException as e:
                logger.error("Received 4xx response from API - Failed to index user review: {}".format(e))
            # We intentionally allow 5xx and uncaught exceptions to bubble up to the caller

        await self._trigger_book_scrape_if_book_doesnt_exist(pubsub_message.book_id)
        return response

    async def _do_we_need_to_index_user_review(self, user_id, book_id):
        books_read_by_user = await self.book_recommender_api_client.get_books_read_by_user(user_id)
        return book_id not in books_read_by_user

    async def _trigger_book_scrape_if_book_doesnt_exist(self, book_id):
        book_exists = self.book_recommender_api_client.does_book_exist(book_id)
        if not book_exists:
            logger.info("Book with ID {} does not exist in our DB. Triggering scrape".format(book_id))
            self.background_tasks.add_task(self.scraper_client.trigger_book_scrape, book_id)



def get_user_review_service(
        book_recommender_api_client: BookRecommenderApiClient = Depends(get_book_recommender_api_client),
        scraper_client: ScraperClientV2 = Depends(get_scraper_client_v2),
        background_tasks: BackgroundTasks = Depends(get_background_tasks)):
    return UserReviewService(book_recommender_api_client, scraper_client, background_tasks)

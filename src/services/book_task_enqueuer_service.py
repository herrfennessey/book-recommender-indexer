import logging
from typing import List

from fastapi import Depends

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client
from src.clients.task_client import TaskClient, get_task_client

BOOK_POPULARITY_THRESHOLD = 5

logger = logging.getLogger(__name__)


class BookTaskEnqueuerResponse:
    tasks: List[str] = []


class BookTaskEnqueuerService(object):
    def __init__(self, book_recommender_api_client: BookRecommenderApiClient, task_client: TaskClient):
        self.task_client = task_client
        self.book_recommender_api_client = book_recommender_api_client

    async def enqueue_books_if_necessary(self, book_ids: List[int]) -> BookTaskEnqueuerResponse:
        # Kick off two async requests simultaneously to get the books already indexed and the book popularity
        # We will use both responses to filter out which tasks we actually want to create

        response = BookTaskEnqueuerResponse()

        candidates = await self._get_candidates_above_indexing_threshold(book_ids)
        if len(candidates) > 0:
            already_indexed = await self.book_recommender_api_client.get_already_indexed_books(candidates)
            books_we_should_index = set(candidates) - set(already_indexed.book_ids)
            for book_id in books_we_should_index:
                logging.info("Attempting to enqueue book_id: %s", book_id)
                task_name = self.task_client.enqueue_book(book_id)
                response.tasks.append(task_name)

        return response

    async def _get_candidates_above_indexing_threshold(self, book_ids: List[int]) -> List[int]:
        candidates = []
        book_popularity_response = await self.book_recommender_api_client.get_book_popularity(book_ids)

        for book_id, user_reviews in book_popularity_response.book_info.items():
            if user_reviews >= BOOK_POPULARITY_THRESHOLD:
                candidates.append(int(book_id))

        return candidates


def get_book_task_enqueuer_service(
        book_recommender_api_client: BookRecommenderApiClient = Depends(get_book_recommender_api_client),
        task_client: TaskClient = Depends(get_task_client)):
    return BookTaskEnqueuerService(book_recommender_api_client, task_client)

import logging
from typing import List

from fastapi import Depends
from pydantic import BaseModel

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client
from src.clients.book_recommender_api_client_v2 import BookRecommenderApiClientV2, get_book_recommender_api_client_v2, \
    BOOK_POPULARITY_THRESHOLD
from src.clients.task_client import TaskClient, get_task_client

logger = logging.getLogger(__name__)


class BookTaskEnqueuerResponse(BaseModel):
    tasks: List[str] = []


class BookTaskEnqueuerService(object):
    def __init__(self, book_recommender_api_client_v2: BookRecommenderApiClientV2,
                 task_client: TaskClient):
        self.task_client = task_client
        self.book_recommender_api_client_v2 = book_recommender_api_client_v2

    async def enqueue_books_if_necessary(self, book_ids: List[int]) -> BookTaskEnqueuerResponse:
        response = BookTaskEnqueuerResponse(tasks=[])

        candidates = await self._get_candidates_above_indexing_threshold(book_ids)
        if len(candidates) > 0:
            already_indexed = await self.book_recommender_api_client_v2.get_already_indexed_books(candidates)
            books_we_should_index = set(candidates) - set(already_indexed.book_ids)
            for book_id in books_we_should_index:
                logging.info("Attempting to enqueue book_id: %s", book_id)
                task_name = self.task_client.enqueue_book(book_id)
                response.tasks.append(task_name)

        return response

    async def _get_candidates_above_indexing_threshold(self, book_ids: List[int]) -> List[int]:
        candidates = []
        book_popularity_response = await self.book_recommender_api_client_v2.get_book_popularity(book_ids)

        for book_id, user_reviews in book_popularity_response.book_info.items():
            if user_reviews >= BOOK_POPULARITY_THRESHOLD and int(book_id) in book_ids:
                candidates.append(int(book_id))
            else:
                logging.info("book_id: %s has too few reviews: %s. Skipping!", book_id, user_reviews)

        return candidates


def get_book_task_enqueuer_service(
        book_recommender_api_client_v2: BookRecommenderApiClientV2 = Depends(get_book_recommender_api_client_v2),
        task_client: TaskClient = Depends(get_task_client)):
    return BookTaskEnqueuerService(book_recommender_api_client_v2, task_client)

import logging

from fastapi import APIRouter, Depends
from pydantic import ValidationError

from src.routes.pubsub_models import PubSubMessage, PubSubUserReviewV1, IndexerResponse
from src.routes.pubsub_utils import _unpack_envelope
from src.services.book_task_enqueuer_service import BookTaskEnqueuerService, get_book_task_enqueuer_service
from src.services.user_review_service import get_user_review_service, UserReviewService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pubsub/user-reviews")

"""
The message pubsub sends us roughly follows this schema - data is base 64 encoded

{
    "message": {
        "attributes": {
            "key": "value"
        },
        "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
        "message_id": "2070443601311540",
        "publish_time": "2021-02-26T19:13:55.749Z"
    },
   "subscription": "projects/myproject/subscriptions/mysubscription"
}
"""


@router.post("/handle", tags=["user-reviews"], status_code=200)
async def handle_pubsub_message(
        request: PubSubMessage,
        user_review_service: UserReviewService = Depends(get_user_review_service),
        book_task_enqueuer_service: BookTaskEnqueuerService = Depends(get_book_task_enqueuer_service)
):
    """
    Handle a pubsub POST call. We do not use the actual pubsub library, but instead receive the message
    payload via a POST call. This is because we're badasses. We don't need no stinkin' libraries.

    Malformed messages will automatically get a 422 response from fastapi. However, if the message is well
    formed, but doesn't follow our model, we "ack" it with a 200, but discard the bad payload
    """
    logging.debug("Handling message with ID %s - Publish Time %s - Attributes %s", request.message.message_id,
                  request.message.publish_time, request.message.attributes)

    batch = _unpack_envelope(request)
    items = []
    for item in batch.items:
        try:
            items.append(PubSubUserReviewV1(**item))
        except ValidationError as e:
            logging.error("Error converting item into PubSubUserReviewV1 object. Received: %s Error: %s", batch.items,
                          e)

    indexer_response = IndexerResponse()
    if len(items) > 0:
        user_review_service_response = await user_review_service.process_pubsub_batch_message(items)
        indexer_response.indexed = len(user_review_service_response.indexed)

        book_ids_to_enqueue = [item.book_id for item in items]
        try:
            book_task_enqueuer_response = await book_task_enqueuer_service.enqueue_books_if_necessary(
                book_ids_to_enqueue)
            indexer_response.tasks = book_task_enqueuer_response.tasks
        except Exception as e:
            logging.error("Error enqueuing book tasks. Error: %s", e)

    return indexer_response

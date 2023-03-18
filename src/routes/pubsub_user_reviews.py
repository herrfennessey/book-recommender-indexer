import logging

from fastapi import APIRouter, Depends
from pydantic import ValidationError

from src.routes.pubsub_models import PubSubMessage, PubSubUserReviewV1, IndexerResponse
from src.routes.pubsub_utils import _unpack_envelope
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
        user_review_service: UserReviewService = Depends(get_user_review_service)
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

    if len(items) > 0:
        service_response = await user_review_service.process_pubsub_batch_message(items)
        return IndexerResponse(**service_response.dict())

    return IndexerResponse()


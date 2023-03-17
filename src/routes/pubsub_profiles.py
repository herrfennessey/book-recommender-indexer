import logging
from typing import List

from fastapi import APIRouter, Depends
from pydantic import ValidationError, BaseModel

from src.clients.task_client import get_task_client, TaskClient
from src.routes.pubsub_models import PubSubMessage, PubSubProfileV1, IndexerResponse
from src.routes.pubsub_utils import _unpack_envelope

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pubsub/profiles")

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


@router.post("/handle", tags=["profiles"], status_code=200)
async def handle_pubsub_message(
        request: PubSubMessage,
        task_client: TaskClient = Depends(get_task_client)
) -> IndexerResponse:
    """
    Handle a pubsub POST call. We do not use the actual pubsub library, but instead receive the message
    payload via a POST call. This is because we're badasses. We don't need no stinkin' libraries.

    Malformed messages will automatically get a 422 response from fastapi. However, if the message is well
    formed, but doesn't follow our model, we "ack" it with a 200, but discard the bad payload
    """
    response = IndexerResponse()
    tasks = []

    profile_batch = _unpack_envelope(request)
    for profile in profile_batch.items:
        try:
            serialized_profile = PubSubProfileV1(**profile)
        except ValidationError as e:
            logging.error("Error converting profile into PubSubProfileV1 object. Received: %s Error: %s", profile, e)
            continue
        logging.info("Attempting to enqueue profile: %s", serialized_profile.user_id)
        task_name = task_client.enqueue_user_scrape(serialized_profile.user_id)
        tasks.append(task_name)

    response.tasks = tasks
    return response

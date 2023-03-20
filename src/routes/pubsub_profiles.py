import logging
from functools import lru_cache

from fastapi import APIRouter, Depends
from pydantic import ValidationError

from src.clients.pubsub_audit_client import PubsubAuditClient, get_pubsub_audit_client
from src.clients.task_client import get_task_client, TaskClient
from src.dependencies import Properties
from src.routes.pubsub_models import PubSubMessage, PubSubProfileV1, IndexerResponse
from src.routes.pubsub_utils import _unpack_envelope

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pubsub/profiles")


@lru_cache()
def get_properties():
    return Properties()


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
        task_client: TaskClient = Depends(get_task_client),
        pubsub_audit_client: PubsubAuditClient = Depends(get_pubsub_audit_client)
) -> IndexerResponse:
    """
    Handle a pubsub POST call. We do not use the actual pubsub library, but instead receive the message
    payload via a POST call. This is because we're badasses. We don't need no stinkin' libraries.

    Malformed messages will automatically get a 422 response from fastapi. However, if the message is well
    formed, but doesn't follow our model, we "ack" it with a 200, but discard the bad payload
    """
    response = IndexerResponse()
    tasks = []
    successful_messages = []
    profile_batch = _unpack_envelope(request)
    for profile in profile_batch.items:
        try:
            serialized_profile = PubSubProfileV1(**profile)
        except ValidationError as e:
            logging.error("Error converting item into PubSubProfileV1 object. Received: %s Error: %s", profile, e)
            continue
        logging.info("Attempting to enqueue profile: %s", serialized_profile.user_id)
        task_name = task_client.enqueue_user_scrape(serialized_profile.user_id)
        tasks.append(task_name)
        successful_messages.append(profile)

    if len(successful_messages) > 0:
        pubsub_audit_client.send_batch(get_properties().pubsub_profiles_audit_topic_name, successful_messages)
    response.tasks = tasks

    return response

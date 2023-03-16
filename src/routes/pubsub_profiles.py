import base64
import json
import logging
from json import JSONDecodeError

from fastapi import APIRouter, Depends
from pydantic import ValidationError, BaseModel

from src.clients.task_client import get_task_client, TaskClient
from src.routes.pubsub_models import PubSubMessage, PubSubProfileV1

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


class ProfileScrapeResponse(BaseModel):
    task_name: str = None


@router.post("/handle", tags=["profiles"], status_code=200)
async def handle_pubsub_message(
        request: PubSubMessage,
        task_client: TaskClient = Depends(get_task_client)
) -> ProfileScrapeResponse:
    """
    Handle a pubsub POST call. We do not use the actual pubsub library, but instead receive the message
    payload via a POST call. This is because we're badasses. We don't need no stinkin' libraries.

    Malformed messages will automatically get a 422 response from fastapi. However, if the message is well
    formed, but doesn't follow our model, we "ack" it with a 200, but discard the bad payload
    """
    logging.debug("Handling message with ID %s - Publish Time %s - Attributes %s", request.message.message_id,
                  request.message.publish_time, request.message.attributes)

    response = ProfileScrapeResponse()
    try:
        payload = base64.b64decode(request.message.data).decode("utf-8")
        json_payload = json.loads(payload)
        profile = PubSubProfileV1(**json_payload)
        logging.info("Attempting to enqueue profile: %s", profile.user_id)
        response.task_name = task_client.enqueue_user_scrape(profile.user_id)
    except JSONDecodeError as _:
        logging.error("Payload was not in JSON - received %s", payload)
    except ValidationError as e:
        logging.error("Error converting payload into profiles object. Received: %s Error: %s", json_payload, e)
    except Exception as e:
        logging.error("Uncaught Exception while handling pubsub message. Exception: %s. Message: %s", e, request.dict())

    return response

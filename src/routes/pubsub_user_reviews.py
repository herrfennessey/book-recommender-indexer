import base64
import json
import logging
from json import JSONDecodeError

from fastapi import APIRouter, Depends
from pydantic import ValidationError
from starlette.responses import Response
from starlette.status import HTTP_200_OK

from src.routes.pubsub_models import PubSubMessage, PubSubUserReviewV1
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
    try:
        payload = base64.b64decode(request.message.data).decode("utf-8")
        json_payload = json.loads(payload)
        user_review = PubSubUserReviewV1(**json_payload)
    except JSONDecodeError as _:
        logging.error("Payload was not in JSON - received %s", payload)
        return Response(status_code=HTTP_200_OK)
    except ValidationError as e:
        logging.error("Error converting payload into user review object. Received: %s Error: %s", json_payload, e)
        return Response(status_code=HTTP_200_OK)

    return await user_review_service.process_pubsub_message(user_review)
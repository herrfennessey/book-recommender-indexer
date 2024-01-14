import base64
import json
import logging
from json import JSONDecodeError

from pydantic import ValidationError

from src.routes.pubsub_models import PubSubItemBatch, PubSubMessage


def _unpack_envelope(request: PubSubMessage) -> PubSubItemBatch:
    """Unpacks an envelope from a Pub/Sub message.

    Args:
        envelope (dict): The Pub/Sub message, and all of its weird base64 data fields

    Returns:
        dict: The unpacked envelope into a nice python dict
    """
    logging.debug(
        "Handling message with ID %s - Publish Time %s - Attributes %s",
        request.message.message_id,
        request.message.publish_time,
        request.message.attributes,
    )
    try:
        payload = base64.b64decode(request.message.data).decode("utf-8")
        json_payload = json.loads(payload)
        return PubSubItemBatch(**json_payload)
    except JSONDecodeError as f:
        logging.error("Payload was not in JSON - received %s. Error: %s", payload, f)
    except ValidationError as e:
        logging.error(
            "Error converting payload into object. Received: %s Error: %s",
            json_payload,
            e,
        )
    except Exception as e:
        logging.error(
            "Uncaught Exception while handling pubsub message. Exception: %s. Message: %s",
            e,
            request.dict(),
        )

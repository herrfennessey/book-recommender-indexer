import logging
from functools import lru_cache

from fastapi import APIRouter, Depends
from pydantic import ValidationError
from starlette.responses import Response
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client, \
    BookRecommenderApiClientException, BookRecommenderApiServerException
from src.clients.pubsub_audit_client import get_pubsub_audit_client, PubsubAuditClient
from src.dependencies import Properties
from src.routes.pubsub_models import PubSubMessage, IndexerResponse, PubSubBookV1
from src.routes.pubsub_utils import _unpack_envelope


@lru_cache()
def get_properties():
    return Properties()


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pubsub/books")

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


@router.post("/handle", tags=["books"], status_code=200)
async def handle_pubsub_message(
        request: PubSubMessage,
        client: BookRecommenderApiClient = Depends(get_book_recommender_api_client),
        pubsub_audit_client: PubsubAuditClient = Depends(get_pubsub_audit_client)
):
    """
    Handle a pubsub POST call. We do not use the actual pubsub library, but instead receive the message
    payload via a POST call. This is because we're badasses. We don't need no stinkin' libraries.

    Malformed messages will automatically get a 422 response from fastapi. However, if the message is well
    formed, but doesn't follow our model, we "ack" it with a 200, but discard the bad payload
    """
    indexed = 0
    batch = _unpack_envelope(request)
    successful_books = []
    # We have a valid book batch, so let's send it to the book recommender API
    try:
        for book in batch.items:
            try:
                serialized_book = PubSubBookV1(**book)
            except ValidationError as e:
                logging.error("Error converting item into PubSubBookV1 object. Received: %s Error: %s", book, e)
                continue

            await client.create_book(serialized_book.dict())
            indexed += 1
            successful_books.append(book)
    except BookRecommenderApiClientException as e:
        logging.error("API returned 4xx exception when called with payload %s - exception: %s", serialized_book, e)
    except BookRecommenderApiServerException as e:
        logging.error("API returned 5xx Exception when called with payload %s - exception: %s", serialized_book, e)
        return Response(status_code=HTTP_500_INTERNAL_SERVER_ERROR)

    if len(successful_books) > 0:
        pubsub_audit_client.send_batch(get_properties().pubsub_book_audit_topic_name, successful_books)

    # We need to return 200 to pubsub, otherwise it will retry
    return IndexerResponse(indexed=indexed)

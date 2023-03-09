# import base64
# import json
# import logging
# from typing import Dict, Any
#
# from fastapi import APIRouter, Depends
# from pydantic import BaseModel, Extra, ValidationError
# from starlette.responses import Response
# from starlette.status import HTTP_200_OK
#
# from src.clients.book_recommender_api_client import BookRecommenderApiClient, BookDataV1
#
# logger = logging.getLogger(__name__)
# router = APIRouter(prefix="/pubsub-books")
#
# """
# The message pubsub sends us roughly follows this schema - data is base 64 encoded
#
# {
#     "message": {
#         "attributes": {
#             "key": "value"
#         },
#         "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
#         "message_id": "2070443601311540",
#         "publish_time": "2021-02-26T19:13:55.749Z"
#     },
#    "subscription": "projects/myproject/subscriptions/mysubscription"
# }
# """
#
#
#
# @router.post("/handle", tags=["books"], status_code=200)
# async def handle_pubsub_message(
#         request: PubSubMessage,
#         client: Depends(BookRecommenderApiClient)
# ):
#     """
#     Handle a pubsub POST call. We do not use the actual pubsub library, but instead receive the message
#     payload via a POST call. This is because we're badasses. We don't need no stinkin' libraries.
#
#     Malformed messages will automatically get a 422 response from fastapi. However, if the message is well
#     formed, but doesn't follow our model, we "ack" it with a 200, but discard the bad payload
#     """
#     logging.debug("Handling message with ID %s - Publish Time %s - Attributes %s", request.message.message_id,
#                   request.message.publish_time, request.message.attributes)
#     try:
#         payload = base64.b64decode(request.message.data).decode("utf-8")
#         json_payload = json.loads(payload)
#         book_info = BookDataV1(**json_payload)
#         await client.create_book(book_info)
#     except ValidationError as er:
#         logging.error("Error converting payload into book object. Received: %s Error: %s", json_payload, er)
#     except Exception as e:
#         logging.error("Uncaught Exception while handling pubsub message. Error: %s", e)
#
#     # We need to return 200 to pubsub, otherwise it will retry
#     return Response(status_code=HTTP_200_OK)

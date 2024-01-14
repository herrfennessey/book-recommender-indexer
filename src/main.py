from __future__ import annotations

import logging.config
import uuid
from os import path

from fastapi import Depends, FastAPI, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette import status
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.clients.book_recommender_api_client_v2 import (
    BookRecommenderApiClientV2,
    get_book_recommender_api_client_v2,
)
from src.clients.task_client import TaskClient, get_task_client
from src.routes import pubsub_books, pubsub_profiles, pubsub_user_reviews

# setup loggers to display more information
log_file_path = path.join(path.dirname(path.abspath(__file__)), "logging.conf")
logging.config.fileConfig(log_file_path, disable_existing_loggers=False)

# get root logger
logger = logging.getLogger(__name__)

app = FastAPI(
    contact={"email": "dave@dfennessey.com"},
    description="Book Recommender Indexer - Powered by PubSub subscriptions",
)


@app.get("/", tags=["welcome"])
def welcome():
    return {"status": "Ready to Rock!"}


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request, e):
    return await http_exception_handler(request, e)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    uuid_str = str(uuid.uuid4())
    exc_str = f"{uuid_str} - {exc}".replace("\n", " ").replace("   ", " ")
    logger.error(exc_str)
    content = {"status_code": 10422, "message": exc_str, "data": None}
    return JSONResponse(
        content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )


@app.get("/health", tags=["healthcheck"])
async def health(
    task_client: TaskClient = Depends(get_task_client),
    book_api_client: BookRecommenderApiClientV2 = Depends(
        get_book_recommender_api_client_v2
    ),
):
    book_health_status = await book_api_client.is_ready()
    task_client_health_status = task_client.is_ready()
    if book_health_status and task_client_health_status:
        return JSONResponse({"status": "Healthy"}, status_code=200)
    else:
        return JSONResponse({"status": "Not Healthy"}, status_code=500)


app.include_router(pubsub_books.router)
app.include_router(pubsub_user_reviews.router)
app.include_router(pubsub_profiles.router)

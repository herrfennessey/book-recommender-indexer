from __future__ import annotations

import logging.config
import uuid
from os import path

from fastapi import FastAPI, Request
from fastapi.exception_handlers import (
    http_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette import status
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.routes import pubsub_books, pubsub_user_reviews

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


app.include_router(pubsub_books.router)
app.include_router(pubsub_user_reviews.router)

import base64
import json
import logging
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient

from src.clients.book_recommender_api_client import BookRecommenderApiClient, get_book_recommender_api_client
from src.clients.scraper_client_v2 import get_scraper_client_v2, ScraperClientV2
from src.dependencies import Properties
from src.main import app
from src.services.user_review_service import get_user_review_service, UserReviewService, UserReviewServiceResponse

file_root_path = Path(os.path.dirname(__file__))


@pytest.fixture()
def scraper_client():
    scraper_client = get_scraper_client_v2(Properties())
    scraper_client.trigger_book_scrape = AsyncMock()
    yield scraper_client


@pytest.fixture()
def user_review_service():
    user_review_service = get_user_review_service(BookRecommenderApiClient(Properties()))
    user_review_service.process_pubsub_message = AsyncMock()
    yield user_review_service


@pytest.fixture(autouse=True)
def run_around_tests(user_review_service: UserReviewService, scraper_client: ScraperClientV2):
    # Code run before all tests
    _stub_user_review_service(user_review_service)
    _stub_scraper_client(scraper_client)

    yield
    # Code that will run after each test
    app.dependency_overrides = {}


def test_handle_endpoint_doesnt_allow_gets(test_client: TestClient):
    response = test_client.get("/pubsub/user-reviews/handle")
    assert_that(response.status_code).is_equal_to(405)


def test_handle_endpoint_rejects_malformed_requests(test_client: TestClient):
    # Given
    request = {"malformed_request": 123}

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=request)

    # Then
    assert_that(response.status_code).is_equal_to(422)


def test_well_formed_request_but_payload_not_json_returns_200(test_client: TestClient, caplog: LogCaptureFixture):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_user_reviews")
    message = _an_example_pubsub_post_call()

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Payload was not in JSON")


def test_well_formed_request_but_not_a_valid_user_review_returns_200(test_client: TestClient,
                                                                     caplog: LogCaptureFixture):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_user_reviews")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_random_json_object()

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Error converting payload into user review object")

def _stub_scraper_client(scraper_client):
    app.dependency_overrides[get_scraper_client_v2] = lambda: scraper_client


def _stub_user_review_service(user_review_service):
    app.dependency_overrides[get_user_review_service] = lambda: user_review_service


def _an_example_pubsub_post_call():
    return {
        "message": {
            "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
            "message_id": "2070443601311540",
            "publish_time": "2021-02-26T19:13:55.749Z"},
        "subscription": "projects/myproject/subscriptions/mysubscription"
    }


def _a_base_64_encoded_random_json_object():
    random_json = {"random": "json"}
    doc_bytes = json.dumps(random_json).encode("utf-8")
    doc_encoded = base64.b64encode(doc_bytes)
    return str(doc_encoded, 'utf-8')


def _a_base_64_encoded_user_review():
    with open(file_root_path / "resources/hedge_knight_user_review.json", "r") as f:
        doc = json.load(f)
        doc_bytes = json.dumps(doc).encode("utf-8")
        doc_encoded = base64.b64encode(doc_bytes)
        return str(doc_encoded, 'utf-8')

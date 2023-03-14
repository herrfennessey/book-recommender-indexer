import base64
import json
import logging
import os
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient

file_root_path = Path(os.path.dirname(__file__))


@pytest.fixture
def non_mocked_hosts() -> list:
    # We don't want to mock the actual service endpoint, just the underlying httpx calls
    return ["testserver"]


def test_handle_endpoint_doesnt_allow_gets(test_client: TestClient):
    response = test_client.get("/pubsub/books/handle")
    assert_that(response.status_code).is_equal_to(405)


def test_handle_endpoint_logs_error_but_suppresses_exception(test_client: TestClient, caplog: LogCaptureFixture):
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _invalid_base_64_object()

    response = test_client.post("/pubsub/books/handle", json=message)

    assert_that(caplog.text).contains("Uncaught Exception", "Incorrect padding", _invalid_base_64_object())
    assert_that(response.status_code).is_equal_to(200)


def test_handle_endpoint_rejects_non_book_objects(test_client: TestClient):
    # Given
    request = {"malformed_request": 123}

    # When
    response = test_client.post("/pubsub/books/handle", json=request)

    # Then
    assert_that(response.status_code).is_equal_to(422)


def test_book_recommender_client_error_suppressed(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture):
    # Given
    _put_call_receives_4xx(httpx_mock)
    caplog.set_level(logging.ERROR, logger="pubsub_books")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_book()

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("API returned 4xx exception when called with payload")


def test_book_recommender_server_error_propagates(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture):
    # Given
    _put_call_receives_5xx(httpx_mock)
    caplog.set_level(logging.ERROR, logger="pubsub_books")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_book()

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(500)
    assert_that(caplog.text).contains("API returned 5xx Exception when called with payload")


def test_well_formed_request_but_payload_not_json_returns_200(test_client: TestClient, caplog: LogCaptureFixture):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_books")
    message = _an_example_pubsub_post_call()

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Payload was not in JSON")


def test_well_formed_request_but_not_a_valid_book_returns_200(test_client: TestClient, caplog: LogCaptureFixture):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_books")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_random_json_object()

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Error converting payload into book object")


def test_successful_book_write(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture):
    # Given
    _put_call_is_successful(httpx_mock)
    caplog.set_level(logging.INFO, logger="pubsub_books")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_book()

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Successfully wrote book: 4")


def _put_call_is_successful(httpx_mock):
    httpx_mock.add_response(status_code=200, url="http://localhost:9000/books/4", method="PUT")


def _put_call_receives_4xx(httpx_mock):
    httpx_mock.add_response(status_code=422, url="http://localhost:9000/books/4", method="PUT")


def _put_call_receives_5xx(httpx_mock):
    httpx_mock.add_response(status_code=500, url="http://localhost:9000/books/4", method="PUT")


def _an_example_pubsub_post_call():
    return {
        "message": {
            "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
            "message_id": "2070443601311540",
            "publish_time": "2021-02-26T19:13:55.749Z"},
        "subscription": "projects/myproject/subscriptions/mysubscription"
    }


def _invalid_base_64_object():
    # incorrectly padded base 64 object - should throw a gnarly error
    return "ABHPdSaxrhjAWA="


def _a_base_64_encoded_random_json_object():
    random_json = {"random": "json"}
    doc_bytes = json.dumps(random_json).encode("utf-8")
    doc_encoded = base64.b64encode(doc_bytes)
    return str(doc_encoded, 'utf-8')


def _a_base_64_encoded_book():
    with open(file_root_path.parents[0] / "resources/harry_potter.json", "r") as f:
        doc = json.load(f)
        doc_bytes = json.dumps(doc).encode("utf-8")
        doc_encoded = base64.b64encode(doc_bytes)
        return str(doc_encoded, 'utf-8')

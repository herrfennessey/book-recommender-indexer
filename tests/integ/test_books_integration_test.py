import json
import os
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient
from google.pubsub_v1 import SubscriberClient

from src.clients.pubsub_audit_client import ItemTopic
from src.dependencies import Properties
from tests.integ.integ_utils import _base_64_encode

file_root_path = Path(os.path.dirname(__file__))

properties = Properties()

SUBSCRIBER_NAME = "test-subscriber"


@pytest.fixture
def non_mocked_hosts() -> list:
    # We don't want to mock the actual service endpoint, just the underlying httpx calls
    return ["testserver"]


@pytest.fixture(autouse=True)
def test_setup(publisher_client, subscriber_client):
    publisher_client.create_topic(request={"name": _get_topic_path()})
    subscriber_client.create_subscription(request={"name": _get_subscription_path(), "topic": _get_topic_path()})
    yield
    subscriber_client.delete_subscription(request={"subscription": _get_subscription_path()})
    publisher_client.delete_topic(request={"topic": _get_topic_path()})


def _consume_messages(client: SubscriberClient):
    response = client.pull(
        request={"subscription": _get_subscription_path(), "max_messages": 100}, timeout=2)
    ack_ids = [received_message.ack_id for received_message in response.received_messages]
    if len(ack_ids) > 0:
        client.acknowledge(request={"subscription": _get_subscription_path(), "ack_ids": ack_ids})
    return response


def test_handle_endpoint_doesnt_allow_gets(test_client: TestClient):
    response = test_client.get("/pubsub/books/handle")
    assert_that(response.status_code).is_equal_to(405)


def test_handle_endpoint_rejects_non_book_objects(test_client: TestClient):
    # Given
    request = {"malformed_request": 123}

    # When
    response = test_client.post("/pubsub/books/handle", json=request)

    # Then
    assert_that(response.status_code).is_equal_to(422)


def test_book_recommender_client_error_suppressed(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                  subscriber_client: SubscriberClient):
    # Given
    _put_call_receives_4xx(httpx_mock)
    payload = json.dumps({"items": [_a_random_book_dict()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("API returned 4xx exception when called with payload")
    assert_that(_consume_messages(subscriber_client).received_messages).is_empty()


def test_book_recommender_server_error_propagates(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                  subscriber_client: SubscriberClient):
    # Given
    _put_call_receives_5xx(httpx_mock)
    payload = json.dumps({"items": [_a_random_book_dict()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(500)
    assert_that(caplog.text).contains("API returned 5xx Exception when called with payload")
    assert_that(_consume_messages(subscriber_client).received_messages).is_empty()


def test_well_formed_request_but_not_a_valid_book_returns_200(test_client: TestClient, caplog: LogCaptureFixture,
                                                              subscriber_client: SubscriberClient):
    # Given
    payload = json.dumps({"items": [{"abc": 123}]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Error converting item into PubSubBookV1 object")
    assert_that(_consume_messages(subscriber_client).received_messages).is_empty()


def test_successful_book_write(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                               subscriber_client: SubscriberClient):
    # Given
    _put_call_is_successful(httpx_mock)
    payload = json.dumps({"items": [_a_random_book_dict()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Successfully wrote book: 4")
    assert_that(response.json().get("indexed")).is_equal_to(1)
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(1)


def test_audit_message_looks_exactly_like_input_model(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                      subscriber_client: SubscriberClient):
    # Given
    _put_call_is_successful(httpx_mock)
    book = _a_random_book_dict()
    payload = json.dumps({"items": [book]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    test_client.post("/pubsub/books/handle", json=message)

    # Then
    audit_message = _consume_messages(subscriber_client).received_messages[0]
    assert_that(audit_message.message.data.decode("utf-8")).is_equal_to(json.dumps(book))


def test_invalid_item_in_batch_doesnt_prevent_other_writes(httpx_mock,
                                                           test_client: TestClient,
                                                           caplog: LogCaptureFixture,
                                                           subscriber_client: SubscriberClient):
    # Given
    _put_call_is_successful(httpx_mock)
    payload = json.dumps({"items": [_a_random_book_dict(), {"abc": 123}]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Successfully wrote book: 4")
    assert_that(response.json().get("indexed")).is_equal_to(1)
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(1)


def test_multiple_valid_books_with_successful_puts(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                   subscriber_client: SubscriberClient):
    # Given
    _put_call_is_successful(httpx_mock, 1)
    _put_call_is_successful(httpx_mock, 2)

    book_1 = _a_random_book_dict()
    book_1["book_id"] = 1

    book_2 = _a_random_book_dict()
    book_2["book_id"] = 2

    payload = json.dumps({"items": [book_1, book_2]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Successfully wrote book: 1", "Successfully wrote book: 2")
    assert_that(response.json().get("indexed")).is_equal_to(2)
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(2)


def test_multiple_valid_books_with_one_4xx_put_fails_gracefully(httpx_mock,
                                                                test_client: TestClient,
                                                                caplog: LogCaptureFixture,
                                                                subscriber_client: SubscriberClient):
    # Given
    _put_call_is_successful(httpx_mock, 1)
    _put_call_receives_4xx(httpx_mock, 2)

    book_1 = _a_random_book_dict()
    book_1["book_id"] = 1

    book_2 = _a_random_book_dict()
    book_2["book_id"] = 2

    payload = json.dumps({"items": [book_1, book_2]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Successfully wrote book: 1")
    assert_that(response.json().get("indexed")).is_equal_to(1)
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(1)


def test_multiple_valid_books_with_one_5xx_put_fails_entire_batch(httpx_mock, test_client: TestClient,
                                                                  caplog: LogCaptureFixture,
                                                                  subscriber_client: SubscriberClient):
    # Given
    _put_call_is_successful(httpx_mock, 1)
    _put_call_receives_5xx(httpx_mock, 2)

    book_1 = _a_random_book_dict()
    book_1["book_id"] = 1

    book_2 = _a_random_book_dict()
    book_2["book_id"] = 2

    payload = json.dumps({"items": [book_1, book_2]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/books/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(500)
    assert_that(_consume_messages(subscriber_client).received_messages).is_empty()


def _put_call_is_successful(httpx_mock, book_id=4):
    httpx_mock.add_response(status_code=200, url=f"http://localhost_v2:9000/books/{book_id}", method="PUT")


def _put_call_receives_4xx(httpx_mock, book_id=4):
    httpx_mock.add_response(status_code=422, url=f"http://localhost_v2:9000/books/{book_id}", method="PUT")


def _put_call_receives_5xx(httpx_mock, book_id=4):
    httpx_mock.add_response(status_code=500, url=f"http://localhost_v2:9000/books/{book_id}", method="PUT")


def _an_example_pubsub_post_call():
    return {
        "message": {
            "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
            "message_id": "2070443601311540",
            "publish_time": "2021-02-26T19:13:55.749Z"},
        "subscription": "projects/myproject/subscriptions/mysubscription"
    }


def _a_random_book_dict():
    with open(file_root_path.parents[0] / "resources/harry_potter.json", "r") as f:
        doc = json.load(f)
        return doc


def _get_topic_path():
    return f"projects/{properties.gcp_project_name}/topics/{ItemTopic.BOOK}"


def _get_subscription_path():
    return f"projects/{properties.gcp_project_name}/subscriptions/{SUBSCRIBER_NAME}"

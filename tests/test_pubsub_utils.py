import logging

from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient

from src.routes.pubsub_models import PubSubMessage
from src.routes.pubsub_utils import _unpack_envelope


def test_well_formed_request_but_payload_not_json_returns_200(test_client: TestClient, caplog: LogCaptureFixture):
    # Given
    message = _an_example_pubsub_post_call()
    pub_sub_message = PubSubMessage(**message)

    # When
    _unpack_envelope(pub_sub_message)

    # Then
    assert_that(caplog.text).contains("Payload was not in JSON")


def test_handle_endpoint_logs_error_but_suppresses_exception(test_client: TestClient, caplog: LogCaptureFixture):
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _invalid_base_64_object()
    pub_sub_message = PubSubMessage(**message)

    _unpack_envelope(pub_sub_message)

    assert_that(caplog.text).contains("Uncaught Exception", "Incorrect padding", _invalid_base_64_object())


def _invalid_base_64_object():
    # incorrectly padded base 64 object - should throw a gnarly error
    return "ABHPdSaxrhjAWA="


def _an_example_pubsub_post_call():
    return {
        "message": {
            "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
            "message_id": "2070443601311540",
            "publish_time": "2021-02-26T19:13:55.749Z"},
        "subscription": "projects/myproject/subscriptions/mysubscription"
    }

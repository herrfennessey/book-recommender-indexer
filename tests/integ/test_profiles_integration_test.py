import json
import logging
import os
import random
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient
from google.cloud.tasks_v2 import CloudTasksClient

from tests.integ.integ_utils import _base_64_encode

file_root_path = Path(os.path.dirname(__file__))


@pytest.fixture
def non_mocked_hosts() -> list:
    # We don't want to mock the actual service endpoint, just the underlying httpx calls
    return ["testserver"]


def test_handle_endpoint_doesnt_allow_gets(test_client: TestClient):
    response = test_client.get("/pubsub/profiles/handle")
    assert_that(response.status_code).is_equal_to(405)


def test_handle_endpoint_rejects_malformed_requests(test_client: TestClient):
    # Given
    request = {"malformed_request": 123}

    # When
    response = test_client.post("/pubsub/profiles/handle", json=request)

    # Then
    assert_that(response.status_code).is_equal_to(422)


def test_well_formed_request_but_not_a_valid_profile_returns_200(test_client: TestClient,
                                                                 caplog: LogCaptureFixture):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_profiles")
    profile_batch = json.dumps({"items": [{"not_a_profile": "123"}]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(profile_batch)

    # When
    response = test_client.post("/pubsub/profiles/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Error converting profile into PubSubProfileV1 object")


def test_successful_profile_task_enqueues_correctly(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                    cloud_tasks: CloudTasksClient):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_profiles")
    profile_batch = json.dumps({"items": [_a_random_profile_item() for _ in range(0, 10)]})

    pub_sub_message = _an_example_pubsub_post_call()
    pub_sub_message["message"]["data"] = _base_64_encode(profile_batch)

    # When
    response = test_client.post("/pubsub/profiles/handle", json=pub_sub_message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    for task in response.json().get("tasks"):
        assert_that(cloud_tasks.get_task(name=task)).is_not_none()


def test_one_bad_profile_doesnt_spoil_the_batch(test_client: TestClient, caplog: LogCaptureFixture,
                                                cloud_tasks: CloudTasksClient):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_profiles")
    message = _an_example_pubsub_post_call()
    items = [_a_random_profile_item() for _ in range(0, 10)]
    items.append({"not_a_profile": 1234})
    profile_batch = json.dumps({"items": items})

    message["message"]["data"] = _base_64_encode(profile_batch)

    # When
    response = test_client.post("/pubsub/profiles/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(response.json().get("tasks")).is_length(10)


def _an_example_pubsub_post_call():
    return {
        "message": {
            "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
            "message_id": "2070443601311540",
            "publish_time": "2021-02-26T19:13:55.749Z"},
        "subscription": "projects/myproject/subscriptions/mysubscription"
    }


def _a_random_profile_item():
    return {
        "user_id": random.randint(1, 100000),
    }

import base64
import json
import logging
import os
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient
from google.cloud.tasks_v2 import CloudTasksClient

from src.clients.book_recommender_api_client import BookRecommenderApiServerException
from src.main import app
from src.services.user_review_service import get_user_review_service

file_root_path = Path(os.path.dirname(__file__))


@pytest.fixture
def non_mocked_hosts() -> list:
    # We don't want to mock the actual service endpoint, just the underlying httpx calls
    return ["testserver"]


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


def test_user_review_doesnt_exist_book_exists(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                              cloud_tasks):
    # Given
    _user_review_doesnt_exist(httpx_mock)
    _user_review_put_successful(httpx_mock)
    _book_exists(httpx_mock)

    caplog.set_level(logging.INFO, logger="book_recommender_api_client")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Successfully wrote review for user_id: 1 book_id: 13501")
    assert_that(response.json().get("task_name")).is_none()


def test_user_review_exists_book_doesnt_exist(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                              cloud_tasks):
    # Given
    _user_review_exists(httpx_mock)
    _book_doesnt_exist(httpx_mock)

    caplog.set_level(logging.INFO, logger="book_recommender_api_client")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(cloud_tasks.get_task(name=response.json().get("task_name"))).is_not_none()


def test_user_review_exists_book_exists(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture, cloud_tasks):
    # Given
    _user_review_exists(httpx_mock)
    _book_exists(httpx_mock)

    caplog.set_level(logging.INFO, logger="book_recommender_api_client")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(response.json().get("task_name")).is_none()


def test_user_review_doesnt_exist_book_doesnt_exist(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                    cloud_tasks: CloudTasksClient):
    # Given
    _user_review_doesnt_exist(httpx_mock)
    _user_review_put_successful(httpx_mock)
    _book_doesnt_exist(httpx_mock)

    caplog.set_level(logging.INFO, logger="book_recommender_api_client")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Successfully wrote review for user_id: 1 book_id: 13501")
    assert_that(cloud_tasks.get_task(name=response.json().get("task_name"))).is_not_none()


def test_user_review_doesnt_exist_but_put_receives_client_error_handled_gracefully(httpx_mock, test_client: TestClient,
                                                                                   caplog: LogCaptureFixture,
                                                                                   cloud_tasks: CloudTasksClient):
    # Given
    _user_review_doesnt_exist(httpx_mock)
    _user_review_put_gets_client_error(httpx_mock)

    caplog.set_level(logging.ERROR, logger="user_review_service")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Received 4xx response from API - Failed to index user review")
    assert_that(response.json().get("index_review")).is_none()
    assert_that(response.json().get("task_name")).is_none()


def test_user_review_doesnt_exist_put_receives_server_error_propagates_exception_back(httpx_mock,
                                                                                      test_client: TestClient,
                                                                                      caplog: LogCaptureFixture,
                                                                                      cloud_tasks: CloudTasksClient):
    # Given
    _user_review_doesnt_exist(httpx_mock)
    _user_review_put_gets_server_error(httpx_mock)

    caplog.set_level(logging.ERROR, logger="user_review_service")
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert response.status_code == 500


def test_book_existence_check_throwing_500_propagates_error_upward(httpx_mock,
                                                                   test_client: TestClient,
                                                                   caplog: LogCaptureFixture,
                                                                   cloud_tasks: CloudTasksClient):
    # Given
    _user_review_doesnt_exist(httpx_mock)
    _user_review_put_successful(httpx_mock)
    _book_existence_check_throws_server_error(httpx_mock)

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert response.status_code == 500


def test_user_review_existence_check_throwing_500_propagates_error_upward(httpx_mock,
                                                                          test_client: TestClient,
                                                                          caplog: LogCaptureFixture,
                                                                          cloud_tasks: CloudTasksClient):
    # Given
    _user_review_existence_check_throws_server_error(httpx_mock)

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _a_base_64_encoded_user_review()

    # When
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert response.status_code == 500


def _user_review_exists(httpx_mock):
    httpx_mock.add_response(json={"book_ids": [13501]}, status_code=200, url="http://localhost:9000/users/1/book-ids")


def _user_review_doesnt_exist(httpx_mock):
    httpx_mock.add_response(json={"book_ids": []}, status_code=200, url="http://localhost:9000/users/1/book-ids")


def _user_review_existence_check_throws_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url="http://localhost:9000/users/1/book-ids")


def _book_exists(httpx_mock):
    httpx_mock.add_response(status_code=200, url="http://localhost:9000/books/13501")


def _book_doesnt_exist(httpx_mock):
    httpx_mock.add_response(status_code=404, url="http://localhost:9000/books/13501")


def _book_existence_check_throws_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url="http://localhost:9000/books/13501")


def _user_review_put_successful(httpx_mock):
    httpx_mock.add_response(status_code=200, url=f"http://localhost:9000/users/1/reviews/13501")


def _user_review_put_gets_client_error(httpx_mock):
    httpx_mock.add_response(status_code=422, url=f"http://localhost:9000/users/1/reviews/13501")


def _user_review_put_gets_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url=f"http://localhost:9000/users/1/reviews/13501")


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
    with open(file_root_path.parents[0] / "resources/hedge_knight_user_review.json", "r") as f:
        doc = json.load(f)
        doc_bytes = json.dumps(doc).encode("utf-8")
        doc_encoded = base64.b64encode(doc_bytes)
        return str(doc_encoded, 'utf-8')

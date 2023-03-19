import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient
from google.cloud.tasks_v2 import CloudTasksClient

from src.clients.book_recommender_api_client import BookRecommenderApiServerException
from src.main import app
from src.services.user_review_service import get_user_review_service
from tests.integ.integ_utils import _base_64_encode

file_root_path = Path(os.path.dirname(__file__))

USER_ID = 1
BOOK_ID = 2


@pytest.fixture
def non_mocked_hosts() -> list:
    # We don't want to mock the actual service endpoint, just the underlying httpx calls
    return ["testserver"]


def test_handle_endpoint_doesnt_allow_gets(test_client: TestClient):
    response = test_client.get("/pubsub/user-reviews/handle")
    assert_that(response.status_code).is_equal_to(405)


def test_well_formed_request_but_not_a_valid_user_review_batch_returns_200(test_client: TestClient,
                                                                           caplog: LogCaptureFixture):
    # Given
    caplog.set_level(logging.ERROR, logger="pubsub_user_reviews")
    payload = json.dumps({"items": [{"garbage": "123"}]})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains("Error converting item into PubSubUserReviewV1 object")


def test_multiple_review_multiple_book_creation(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                cloud_tasks: CloudTasksClient):
    # Given
    num_reviews = 5
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock, 5)
    _book_doesnt_exist(httpx_mock)

    reviews = [_a_random_user_review(book_id=i) for i in range(num_reviews)]
    payload = json.dumps({"items": reviews})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains(f"Successfully indexed {num_reviews} user reviews")
    assert_that(response.json().get("indexed")).is_equal_to(num_reviews)
    assert_that(response.json().get("tasks")).is_length(num_reviews)


def test_indexer_correctly_takes_into_account_existing_items(httpx_mock, test_client: TestClient,
                                                             caplog: LogCaptureFixture,
                                                             cloud_tasks: CloudTasksClient):
    # Given
    total_num_reviews = 5
    expected_num_reviews_indexed = 2
    expected_num_books_enqueued = 3
    _user_has_read_books(httpx_mock, [0, 1, 2])
    _book_exists_in_db(httpx_mock, [3, 4])
    _user_review_batch_create_successful(httpx_mock, expected_num_reviews_indexed)

    reviews = [_a_random_user_review(book_id=i) for i in range(total_num_reviews)]
    payload = json.dumps({"items": reviews})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains(f"Successfully indexed {expected_num_reviews_indexed} user reviews")
    assert_that(response.json().get("indexed")).is_equal_to(expected_num_reviews_indexed)
    assert_that(response.json().get("tasks")).is_length(expected_num_books_enqueued)


def test_multiple_users_in_one_batch_doesnt_mess_things_up(httpx_mock, test_client: TestClient,
                                                           caplog: LogCaptureFixture,
                                                           cloud_tasks: CloudTasksClient):
    # Given
    _user_has_read_no_books(httpx_mock, user_id=1)
    _user_has_read_books(httpx_mock, book_ids=[1, 2, 3, 4, 5], user_id=2)
    _user_review_batch_create_successful(httpx_mock, 1)  # A bit sloppy, but both are going to create one item
    _book_exists_in_db(httpx_mock, book_ids=[1, 2, 3])

    reviews = [_a_random_user_review(user_id=1, book_id=1),  # Unread by user 1
               _a_random_user_review(user_id=2, book_id=4),  # Read by user 2
               _a_random_user_review(user_id=2, book_id=6)]  # Unread by user 1

    payload = json.dumps({"items": reviews})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    # Two separate users should get two separate calls to create user review batches
    create_calls = [True for message in caplog.messages if message == "Successfully indexed 1 user reviews"]
    assert_that(create_calls).is_length(2)
    # But their index count should be aggregated together
    assert_that(response.json().get("indexed")).is_equal_to(2)
    # And the number of tasks should be the number of books that need to be enqueued, regardless of who enqueued them
    assert_that(response.json().get("tasks")).is_length(2)


def test_duplicate_books_correctly_only_create_one_task(httpx_mock, test_client: TestClient,
                                                           caplog: LogCaptureFixture,
                                                           cloud_tasks: CloudTasksClient):
    # Given
    _user_has_read_no_books(httpx_mock, user_id=1)
    _user_has_read_no_books(httpx_mock, user_id=2)

    _user_review_batch_create_successful(httpx_mock, 1)  # A bit sloppy, but both are going to create one item
    _book_exists_in_db(httpx_mock, book_ids=[])

    reviews = [_a_random_user_review(user_id=1, book_id=1),  # Unread by user 1
               _a_random_user_review(user_id=2, book_id=1)]  # Unread by user 1

    payload = json.dumps({"items": reviews})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    # Two separate users should get two separate calls to create user review batches
    create_calls = [True for message in caplog.messages if message == "Successfully indexed 1 user reviews"]
    assert_that(create_calls).is_length(2)
    # But their index count should be aggregated together
    assert_that(response.json().get("indexed")).is_equal_to(2)
    # And the number of tasks should be the number of books that need to be enqueued, regardless of who enqueued them
    assert_that(response.json().get("tasks")).is_length(2)
    assert_that(response.json().get("tasks")).contains("duplicate")


def test_user_review_doesnt_exist_book_exists(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                              cloud_tasks):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock)
    _book_exists_in_db(httpx_mock)

    payload = json.dumps({"items": [_a_random_user_review()]})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains(f"Successfully indexed {1} user reviews")
    assert_that(response.json().get("indexed")).is_equal_to(1)
    assert_that(response.json().get("tasks")).is_length(0)


def test_user_review_exists_book_doesnt_exist(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                              cloud_tasks):
    # Given
    _user_has_read_books(httpx_mock)
    _book_doesnt_exist(httpx_mock)
    payload = json.dumps({"items": [_a_random_user_review()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(response.json().get("indexed")).is_equal_to(0)
    assert_that(response.json().get("tasks")).is_length(1)


def test_user_review_exists_book_exists(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture, cloud_tasks):
    # Given
    _user_has_read_books(httpx_mock)
    _book_exists_in_db(httpx_mock)
    payload = json.dumps({"items": [_a_random_user_review()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(response.json().get("indexed")).is_equal_to(0)
    assert_that(response.json().get("tasks")).is_length(0)


def test_user_review_doesnt_exist_book_doesnt_exist(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                    cloud_tasks: CloudTasksClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock)
    _book_doesnt_exist(httpx_mock)
    payload = json.dumps({"items": [_a_random_user_review()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(response.json().get("indexed")).is_equal_to(1)
    assert_that(response.json().get("tasks")).is_length(1)


def test_user_review_doesnt_exist_and_we_get_429_response(httpx_mock, test_client: TestClient,
                                                          caplog: LogCaptureFixture,
                                                          cloud_tasks: CloudTasksClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_gets_too_many_requests_response(httpx_mock)
    payload = json.dumps({"items": [_a_random_user_review()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert_that(response.status_code).is_equal_to(500)


def test_user_review_doesnt_exist_put_receives_server_error_propagates_exception_back(httpx_mock,
                                                                                      test_client: TestClient,
                                                                                      caplog: LogCaptureFixture,
                                                                                      cloud_tasks: CloudTasksClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_gets_server_error(httpx_mock)
    payload = json.dumps({"items": [_a_random_user_review()]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert_that(response.status_code).is_equal_to(500)


def test_book_existence_check_throwing_500_propagates_error_upward(httpx_mock,
                                                                   test_client: TestClient,
                                                                   caplog: LogCaptureFixture,
                                                                   cloud_tasks: CloudTasksClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock)
    _book_existence_check_throws_server_error(httpx_mock)

    payload = json.dumps({"items": [_a_random_user_review()]})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert_that(response.status_code).is_equal_to(500)


def test_user_review_existence_check_throwing_500_propagates_error_upward(httpx_mock,
                                                                          test_client: TestClient,
                                                                          caplog: LogCaptureFixture,
                                                                          cloud_tasks: CloudTasksClient):
    # Given
    _user_review_existence_check_throws_server_error(httpx_mock)

    payload = json.dumps({"items": [_a_random_user_review()]})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert response.status_code == 500


def _user_has_read_books(httpx_mock, book_ids=[BOOK_ID], user_id=USER_ID):
    httpx_mock.add_response(json={"book_ids": book_ids}, status_code=200,
                            url=f"http://localhost:9000/users/{user_id}/book-ids")


def _user_has_read_no_books(httpx_mock, user_id=USER_ID):
    httpx_mock.add_response(json={"book_ids": []}, status_code=200,
                            url=f"http://localhost:9000/users/{user_id}/book-ids")


def _user_review_existence_check_throws_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url="http://localhost:9000/users/1/book-ids")


def _book_exists_in_db(httpx_mock, book_ids=[BOOK_ID]):
    httpx_mock.add_response(json={"book_ids": book_ids}, status_code=200,
                            url="http://localhost:9000/books/batch/exists",
                            method="POST")


def _book_doesnt_exist(httpx_mock):
    httpx_mock.add_response(json={"book_ids": []}, status_code=200, url="http://localhost:9000/books/batch/exists",
                            method="POST")


def _book_existence_check_throws_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url="http://localhost:9000/books/batch/exists", method="POST")


def _user_review_batch_create_successful(httpx_mock, indexed=1):
    httpx_mock.add_response(json={"indexed": indexed}, status_code=200, url=f"http://localhost:9000/users/batch/create",
                            method="POST")


def _user_review_batch_create_gets_too_many_requests_response(httpx_mock):
    httpx_mock.add_response(status_code=429, url=f"http://localhost:9000/users/batch/create", method="POST")


def _user_review_batch_create_gets_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url=f"http://localhost:9000/users/batch/create", method="POST")


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


def _a_random_user_review(user_id: int = USER_ID, book_id: int = BOOK_ID):
    return {
        "user_id": user_id,
        "book_id": book_id,
        "user_rating": 5,
        "date_read": "2017-09-29T00:00:00",
        "scrape_time": datetime.now().isoformat(),
    }

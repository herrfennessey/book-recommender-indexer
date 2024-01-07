import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi.testclient import TestClient
from google.cloud.tasks_v2 import CloudTasksClient
from google.pubsub_v1 import SubscriberClient

from src.clients.book_recommender_api_client_v2 import BOOK_POPULARITY_THRESHOLD, BookRecommenderApiServerException
from src.dependencies import Properties
from src.main import app
from src.services.user_review_service import get_user_review_service
from tests.integ.integ_utils import _base_64_encode

file_root_path = Path(os.path.dirname(__file__))

properties = Properties()

USER_ID = 1
BOOK_ID = 2
SUBSCRIBER_NAME = "test-subscriber"
PARENT_QUEUE = f"projects/{properties.gcp_project_name}/locations/{properties.cloud_task_region}/queues/{properties.task_queue_name}"


@pytest.fixture
def non_mocked_hosts() -> list:
    # We don't want to mock the actual service endpoint, just the underlying httpx calls
    return ["testserver"]


@pytest.fixture(autouse=True)
def test_setup(publisher_client, subscriber_client, cloud_tasks):
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
    response = test_client.get("/pubsub/user-reviews/handle")
    assert_that(response.status_code).is_equal_to(405)


def test_well_formed_request_but_not_a_valid_user_review_batch_returns_200(test_client: TestClient,
                                                                           caplog: LogCaptureFixture,
                                                                           subscriber_client: SubscriberClient):
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_empty()


def test_multiple_review_multiple_book_creation(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                cloud_tasks: CloudTasksClient, subscriber_client: SubscriberClient):
    # Given
    num_reviews = 5
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock, 5)
    _books_referenced_by_enough_reviewers_to_index(httpx_mock, [0, 1, 2, 3, 4])
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(num_reviews)


def test_indexer_correctly_takes_into_account_existing_items(httpx_mock, test_client: TestClient,
                                                             caplog: LogCaptureFixture,
                                                             cloud_tasks: CloudTasksClient,
                                                             subscriber_client: SubscriberClient):
    # Given
    total_num_reviews = 5
    expected_num_reviews_indexed = 2
    expected_num_books_enqueued = 3
    _user_has_read_books(httpx_mock, [0, 1, 2])
    _books_referenced_by_enough_reviewers_to_index(httpx_mock, [0, 1, 2, 3, 4])
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(expected_num_reviews_indexed)


def test_popularity_filtering_correctly_blocks_some_books_indexing(httpx_mock, test_client: TestClient,
                                                                   caplog: LogCaptureFixture,
                                                                   cloud_tasks: CloudTasksClient,
                                                                   subscriber_client: SubscriberClient):
    # Given
    total_num_reviews = 5
    expected_num_reviews_indexed = 5
    expected_num_books_enqueued = 3
    _user_has_read_no_books(httpx_mock)
    _book_exists_in_db(httpx_mock, [])
    _book_popularity_returns_payload(httpx_mock, {"0": 5, "1": 5, "2": 5, "3": 0, "4": 0})
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(expected_num_reviews_indexed)


def test_http_errors_on_popularity_calls_dont_break_entire_request(httpx_mock, test_client: TestClient,
                                                                   caplog: LogCaptureFixture,
                                                                   cloud_tasks: CloudTasksClient,
                                                                   subscriber_client: SubscriberClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _book_exists_in_db(httpx_mock, [])
    # 1 should get a successful response
    httpx_mock.add_response(json={"user_count": 5}, status_code=200,
                            url=f"http://localhost_v2:9000/book-popularity/1?limit={BOOK_POPULARITY_THRESHOLD}")
    # 2 should get a 500 (no retry)
    httpx_mock.add_response(status_code=500,
                            url=f"http://localhost_v2:9000/book-popularity/2?limit={BOOK_POPULARITY_THRESHOLD}")

    # 3 should first get a retryable error
    httpx_mock.add_response(status_code=503,
                            url=f"http://localhost_v2:9000/book-popularity/3?limit={BOOK_POPULARITY_THRESHOLD}")
    # 3 request should succeed second time
    httpx_mock.add_response(json={"user_count": 5}, status_code=200,
                            url=f"http://localhost_v2:9000/book-popularity/3?limit={BOOK_POPULARITY_THRESHOLD}")
    _user_review_batch_create_successful(httpx_mock, 3)

    reviews = [_a_random_user_review(book_id=i) for i in range(1, 4)]
    payload = json.dumps({"items": reviews})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(caplog.text).contains(f"Successfully indexed 3 user reviews")
    assert_that(response.json().get("indexed")).is_equal_to(3)
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(3)
    assert_that(response.json().get("tasks")).is_length(2)


def test_multiple_users_in_one_batch_doesnt_mess_things_up(httpx_mock, test_client: TestClient,
                                                           caplog: LogCaptureFixture,
                                                           cloud_tasks: CloudTasksClient,
                                                           subscriber_client: SubscriberClient):
    # Given
    _user_has_read_no_books(httpx_mock, user_id=1)
    _user_has_read_books(httpx_mock, book_ids=[1, 2, 3, 4], user_id=2)
    _user_review_batch_create_successful(httpx_mock, 1)  # A bit sloppy, but both are going to create one item
    _book_exists_in_db(httpx_mock, book_ids=[1, 2, 3])

    reviews = [_a_random_user_review(user_id=1, book_id=1),  # Unread by user 1
               _a_random_user_review(user_id=2, book_id=4),  # Read by user 2
               _a_random_user_review(user_id=2, book_id=5)]  # Unread by user 2
    # All books which are referenced by the reviews need to be mocked
    _books_referenced_by_enough_reviewers_to_index(httpx_mock, [1, 4, 5])

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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(2)


def test_duplicate_books_correctly_only_create_one_task(httpx_mock, test_client: TestClient,
                                                        caplog: LogCaptureFixture,
                                                        cloud_tasks: CloudTasksClient,
                                                        subscriber_client: SubscriberClient):
    # Given
    _user_has_read_no_books(httpx_mock, user_id=1)
    _user_has_read_no_books(httpx_mock, user_id=2)

    _user_review_batch_create_successful(httpx_mock, 1)  # A bit sloppy, but both are going to create one item
    _books_referenced_by_enough_reviewers_to_index(httpx_mock, [1])
    _book_doesnt_exist(httpx_mock)

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
    assert_that(response.json().get("tasks")).is_length(1)
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(2)


def test_book_queue_task_will_not_duplicate_preexisting_task(httpx_mock, test_client: TestClient,
                                                             caplog: LogCaptureFixture,
                                                             cloud_tasks: CloudTasksClient,
                                                             subscriber_client: SubscriberClient):
    # Given
    print(list(cloud_tasks.list_tasks(parent=PARENT_QUEUE)))
    cloud_tasks.create_task(parent=PARENT_QUEUE,
                            task=_a_task_for_book_scrape(BOOK_ID))  # task already exists for book 1

    _user_has_read_no_books(httpx_mock, user_id=USER_ID)
    _user_review_batch_create_successful(httpx_mock, 1)  # A bit sloppy, but both are going to create one item
    _books_referenced_by_enough_reviewers_to_index(httpx_mock)
    _book_doesnt_exist(httpx_mock)

    reviews = [_a_random_user_review(user_id=USER_ID, book_id=BOOK_ID)]

    payload = json.dumps({"items": reviews})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(response.status_code).is_equal_to(200)
    assert_that(response.json().get("indexed")).is_equal_to(1)
    assert_that(response.json().get("tasks")).is_equal_to(["duplicate"])
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(1)


def test_user_review_doesnt_exist_book_exists(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                              cloud_tasks, subscriber_client: SubscriberClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock)
    _books_referenced_by_enough_reviewers_to_index(httpx_mock)
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(1)


def test_audit_message_looks_exactly_like_input_model(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                      cloud_tasks, subscriber_client: SubscriberClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock)
    _books_referenced_by_enough_reviewers_to_index(httpx_mock)
    _book_doesnt_exist(httpx_mock)
    review = _a_random_user_review()
    payload = json.dumps({"items": [review]})

    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    audit_message = _consume_messages(subscriber_client).received_messages[0]
    assert_that(audit_message.message.data.decode("utf-8")).is_equal_to(json.dumps(review))


def test_user_review_exists_book_doesnt_exist(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                              cloud_tasks, subscriber_client: SubscriberClient):
    # Given
    _user_has_read_books(httpx_mock)
    _books_referenced_by_enough_reviewers_to_index(httpx_mock)
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(0)


def test_user_review_exists_book_exists(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture, cloud_tasks,
                                        subscriber_client: SubscriberClient):
    # Given
    _user_has_read_books(httpx_mock)
    _books_referenced_by_enough_reviewers_to_index(httpx_mock)
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(0)


def test_user_review_doesnt_exist_book_doesnt_exist(httpx_mock, test_client: TestClient, caplog: LogCaptureFixture,
                                                    cloud_tasks: CloudTasksClient, subscriber_client: SubscriberClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock)
    _books_referenced_by_enough_reviewers_to_index(httpx_mock)
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
    assert_that(_consume_messages(subscriber_client).received_messages).is_length(1)


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


def test_book_existence_check_throwing_500_suppresses_exception(httpx_mock,
                                                                test_client: TestClient,
                                                                caplog: LogCaptureFixture,
                                                                cloud_tasks: CloudTasksClient):
    # Given
    _user_has_read_no_books(httpx_mock)
    _user_review_batch_create_successful(httpx_mock)
    _books_referenced_by_enough_reviewers_to_index(httpx_mock)
    _book_existence_check_throws_server_error(httpx_mock)

    payload = json.dumps({"items": [_a_random_user_review()]})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When
    response = test_client.post("/pubsub/user-reviews/handle", json=message)

    # Then
    assert_that(caplog.text).contains("Error enqueuing book tasks", "book_ids: [2]")
    assert_that(response.status_code).is_equal_to(200)


def test_user_review_existence_check_throwing_500_propagates_error_upward(httpx_mock,
                                                                          test_client: TestClient,
                                                                          caplog: LogCaptureFixture,
                                                                          cloud_tasks: CloudTasksClient):
    # Given
    _user_review_existence_check_throws_server_error(httpx_mock)
    payload = json.dumps({"items": [_a_random_user_review()]})
    message = _an_example_pubsub_post_call()
    message["message"]["data"] = _base_64_encode(payload)

    # When / Then
    with pytest.raises(BookRecommenderApiServerException):
        response = test_client.post("/pubsub/user-reviews/handle", json=message)
        assert_that(response.status_code).is_equal_to(500)
        assert_that(caplog.text).contains("5xx Exception encountered", "user_id: 1")


def _books_referenced_by_enough_reviewers_to_index(httpx_mock, book_ids: List[int] = [BOOK_ID]):
    for book in book_ids:
        _book_popularity_returns_payload(httpx_mock, {str(book): BOOK_POPULARITY_THRESHOLD})


def _book_popularity_returns_payload(httpx_mock, book_to_popularity_dict: Dict[str, int]):
    for book_id, popularity in book_to_popularity_dict.items():
        httpx_mock.add_response(json={"user_count": popularity}, status_code=200,
                                url=f"http://localhost_v2:9000/book-popularity/{book_id}?limit={BOOK_POPULARITY_THRESHOLD}")


def _user_has_read_books(httpx_mock, book_ids=[BOOK_ID], user_id=USER_ID):
    httpx_mock.add_response(json={"book_ids": book_ids}, status_code=200,
                            url=f"http://localhost_v2:9000/reviews/{user_id}/book-ids")


def _user_has_read_no_books(httpx_mock, user_id=USER_ID):
    httpx_mock.add_response(json={"book_ids": []}, status_code=200,
                            url=f"http://localhost_v2:9000/reviews/{user_id}/book-ids")


def _user_review_existence_check_throws_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url="http://localhost_v2:9000/reviews/1/book-ids")


def _book_exists_in_db(httpx_mock, book_ids=[BOOK_ID]):
    httpx_mock.add_response(json={"book_ids": book_ids}, status_code=200,
                            url="http://localhost_v2:9000/books/batch/exists",
                            method="POST")


def _book_doesnt_exist(httpx_mock):
    httpx_mock.add_response(json={"book_ids": []}, status_code=200, url="http://localhost_v2:9000/books/batch/exists",
                            method="POST")


def _book_existence_check_throws_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url="http://localhost_v2:9000/books/batch/exists", method="POST")


def _user_review_batch_create_successful(httpx_mock, indexed=1):
    httpx_mock.add_response(json={"indexed": indexed}, status_code=200, url=f"http://localhost_v2:9000/reviews/batch/create",
                            method="POST")


def _user_review_batch_create_gets_too_many_requests_response(httpx_mock):
    httpx_mock.add_response(status_code=429, url=f"http://localhost_v2:9000/reviews/batch/create", method="POST")


def _user_review_batch_create_gets_server_error(httpx_mock):
    httpx_mock.add_response(status_code=500, url=f"http://localhost_v2:9000/reviews/batch/create", method="POST")


def _stub_user_review_service(user_review_service):
    app.dependency_overrides[get_user_review_service] = lambda: user_review_service


def _a_task_for_book_scrape(book_id):
    return {
        "name": f"{PARENT_QUEUE}/tasks/book-{book_id}",
        "http_request": {"url": "http://localhost:12000/foo/bar/baz", "body": b"asdf"}
    }


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


def _get_topic_path():
    return f"projects/{properties.gcp_project_name}/topics/{properties.pubsub_book_audit_topic_name}"


def _get_subscription_path():
    return f"projects/{properties.gcp_project_name}/subscriptions/{SUBSCRIBER_NAME}"

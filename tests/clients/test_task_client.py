import pytest
from assertpy import assert_that
from google.cloud.tasks_v2 import CloudTasksClient

from src.clients.task_client import TaskClient
from src.dependencies import Properties

default_properties = Properties()
PARENT_QUEUE = f"projects/{default_properties.gcp_project_name}/locations/{default_properties.cloud_task_region}/queues/{default_properties.task_queue_name}"


@pytest.fixture(autouse=True)
def test_setup(cloud_tasks):
    for task in cloud_tasks.list_tasks(request={"parent": PARENT_QUEUE}):
        cloud_tasks.delete_task(request={"name": task.name})
    yield


def test_task_client_ready_validates_our_queues_exist(cloud_tasks: CloudTasksClient):
    # Given
    task_client = TaskClient(cloud_tasks, default_properties)

    # When
    response = task_client.is_ready()

    # Then
    assert_that(response).is_true()


def test_task_client_fails_with_unknown_queue(cloud_tasks: CloudTasksClient):
    # Given
    properties = Properties(task_queue_name="unknown_queue")
    task_client = TaskClient(cloud_tasks, properties)

    # When
    response = task_client.is_ready()

    # Then
    assert_that(response).is_false()


def test_task_queue_successfully_deduplicates_user_tasks(cloud_tasks: CloudTasksClient):
    # Given
    task_client = TaskClient(cloud_tasks, default_properties)

    # When
    task_name = task_client.enqueue_user_scrape("abc123")
    task_name_2 = task_client.enqueue_user_scrape("abc123")

    # Then
    assert_that(task_name).is_equal_to(f"{PARENT_QUEUE}/tasks/user-abc123")
    assert_that(task_name_2).is_equal_to("duplicate")
    assert_that(list(cloud_tasks.list_tasks(parent=PARENT_QUEUE))).is_length(1)


def test_task_queue_successfully_deduplicates_book_tasks(cloud_tasks: CloudTasksClient):
    # Given
    task_client = TaskClient(cloud_tasks, default_properties)

    # When
    task_name = task_client.enqueue_book(12345)
    task_name_2 = task_client.enqueue_book(12345)

    # Then
    assert_that(task_name).is_equal_to(f"{PARENT_QUEUE}/tasks/book-12345")
    assert_that(task_name_2).is_equal_to("duplicate")
    assert_that(list(cloud_tasks.list_tasks(parent=PARENT_QUEUE))).is_length(1)

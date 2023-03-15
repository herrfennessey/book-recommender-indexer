from assertpy import assert_that
from google.cloud.tasks_v2 import CloudTasksClient

from src.clients.task_client import TaskClient
from src.dependencies import Properties


def test_task_client_ready_validates_our_queues_exist(cloud_tasks: CloudTasksClient):
    # Given
    properties = Properties()
    task_client = TaskClient(cloud_tasks, properties)

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

from google.cloud.tasks_v2 import CloudTasksClient
from pytest_asyncio import fixture
from testcontainers.core.waiting_utils import wait_for_logs

from src.dependencies import Properties
from tests.cloud_tasks_container import CloudTasksContainer


def purge_queues(task_queue: CloudTasksClient) -> None:
    properties = Properties()
    queue_path = task_queue.queue_path(properties.gcp_project_name, properties.cloud_task_region, properties.book_task_queue)
    task_queue.purge_queue(name=queue_path)


@fixture(autouse=True)
def run_before_tests(task_queue: CloudTasksClient):
    # Things to happen before
    purge_queues(task_queue)
    yield  # this is where the testing happens
    # This happens afterwards


@fixture(scope="session", autouse=True)
def task_queue():
    with CloudTasksContainer() as container:
        wait_for_logs(container, "Creating initial queue", 20)
        yield container.get_client()

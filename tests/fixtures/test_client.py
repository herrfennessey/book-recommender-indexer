import pytest
from fastapi.testclient import TestClient
from google.cloud.tasks_v2 import CloudTasksClient

from src.clients.task_client import get_cloud_tasks_client
from src.main import app


@pytest.fixture(scope="session", autouse=True)
def test_client(cloud_tasks: CloudTasksClient):
    app.dependency_overrides[get_cloud_tasks_client] = lambda: cloud_tasks
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

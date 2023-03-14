import pytest
from cachetools import TTLCache, LRUCache
from fastapi.testclient import TestClient
from google.cloud.tasks_v2 import CloudTasksClient

from src.clients.task_client import get_cloud_tasks_client
from src.clients.utils.cache_utils import get_book_exists_cache, get_user_read_book_cache
from src.main import app


@pytest.fixture(scope="session", autouse=True)
def test_client(cloud_tasks: CloudTasksClient):
    # Clear caches between runs
    app.dependency_overrides[get_user_read_book_cache] = lambda: TTLCache(maxsize=1000, ttl=60)
    app.dependency_overrides[get_book_exists_cache] = lambda: LRUCache(maxsize=1000)
    # Stub the cloud tasks client to use the docker container instead
    app.dependency_overrides[get_cloud_tasks_client] = lambda: cloud_tasks
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture(scope="session", autouse=True)
def test_client():
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

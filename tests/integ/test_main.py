import pytest
from fastapi.testclient import TestClient

from src.clients.task_client import get_properties
from src.dependencies import Properties
from src.main import app


@pytest.fixture
def non_mocked_hosts() -> list:
    # We don't want to mock the actual service endpoint, just the underlying httpx calls
    return ["testserver"]


def test_read_main(test_client: TestClient):
    # When
    response = test_client.get("/")

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "Ready to Rock!"}


def test_health_check_with_both_services_healthy(httpx_mock, test_client: TestClient):
    # Given
    properties = Properties()
    httpx_mock.add_response(
        url=f"{properties.book_recommender_api_base_url_v2}",
        json={"status": "Healthy"},
        status_code=200,
        method="GET",
    )
    app.dependency_overrides[get_properties] = lambda: properties

    # When
    response = test_client.get("/health")

    # Then
    assert response.status_code == 200
    assert response.json() == {"status": "Healthy"}
    app.dependency_overrides.pop(get_properties, None)


def test_health_check_with_recommendation_api_unhealthy(
    httpx_mock, test_client: TestClient
):
    # Given
    properties = Properties()
    httpx_mock.add_response(
        url=f"{properties.book_recommender_api_base_url_v2}",
        json={"status": "Healthy"},
        status_code=500,
        method="GET",
    )

    # When
    response = test_client.get("/health")

    # Then
    assert response.json() == {"status": "Not Healthy"}
    assert response.status_code == 500
    app.dependency_overrides.pop(get_properties, None)


def test_health_check_with_task_client_unhealthy(httpx_mock, test_client: TestClient):
    # Given
    properties = Properties()
    properties.task_queue_name = "boom"
    httpx_mock.add_response(
        url=f"{properties.book_recommender_api_base_url_v2}",
        json={"status": "Healthy"},
        status_code=200,
        method="GET",
    )
    app.dependency_overrides[get_properties] = lambda: properties

    # When
    response = test_client.get("/health")

    # Then
    assert response.json() == {"status": "Not Healthy"}
    assert response.status_code == 500
    app.dependency_overrides.pop(get_properties, None)

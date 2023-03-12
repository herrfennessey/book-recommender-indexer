from typing import Dict, Any
from unittest.mock import patch, MagicMock

import httpx
import pytest
from _pytest.logging import LogCaptureFixture
from assertpy import assert_that
from fastapi import BackgroundTasks

from src.clients.scraper_client_v2 import ScraperClientV2
from src.dependencies import Properties

TEST_PROPERTIES = Properties(scraper_client_base_url="https://testurl", env_name="test",
                             gcp_project_name="test-project", pubsub_book_topic_name="test-topic")


@pytest.fixture()
def background_tasks():
    with patch("fastapi.BackgroundTasks") as mock:
        mock.add_task.return_value = MagicMock(return_value=None)
        yield mock


@pytest.mark.asyncio
async def test_successful_book_scrape(httpx_mock, caplog: LogCaptureFixture, background_tasks: BackgroundTasks):
    # Given
    caplog.set_level("INFO", logger="scraper_client_v2")
    httpx_mock.add_response(json={}, status_code=200, url="https://testurl/crawl.json")
    client = ScraperClientV2(TEST_PROPERTIES, background_tasks)

    # When
    await client.trigger_book_scrape(1)

    # Then
    assert_that(caplog.text).contains("Successfully triggered book scrape for book_id: 1")


@pytest.mark.asyncio
async def test_5xx_custom_exception_on_book_scrape(httpx_mock, caplog: LogCaptureFixture,
                                                   background_tasks: BackgroundTasks):
    # Given
    caplog.set_level("ERROR", logger="scraper_client_v2")
    httpx_mock.add_response(json={}, status_code=500, url="https://testurl/crawl.json")
    client = ScraperClientV2(TEST_PROPERTIES, background_tasks)

    # When / Then
    await client.trigger_book_scrape(1)

    assert_that(caplog.text).contains(
        "Received a 4xx or 5xx server exception",
        "book_id: 1"
    )


@pytest.mark.asyncio
async def test_background_tasks_get_called(httpx_mock, caplog: LogCaptureFixture, background_tasks: BackgroundTasks):
    """
    Can't really unit test the background tasks mechanism itself, so we'll have to assume that part actually works
    """
    # Given
    caplog.set_level("INFO", logger="scraper_client_v2")
    client = ScraperClientV2(TEST_PROPERTIES, background_tasks)

    # When / Then
    await client.trigger_background_task_book_scrape(1)

    background_tasks.add_task.assert_called_once()
    assert_that(caplog.text).contains("book_id: 1", "Triggering background scrape")


@pytest.mark.asyncio
async def test_uncaught_exception_on_book_scrape(httpx_mock, caplog: LogCaptureFixture,
                                                 background_tasks: BackgroundTasks):
    # Given
    caplog.set_level("ERROR", logger="scraper_client_v2")
    httpx_mock.add_exception(httpx.ReadTimeout("Unable to read"), url="https://testurl/crawl.json")
    client = ScraperClientV2(TEST_PROPERTIES, background_tasks)

    # When / Then
    await client.trigger_book_scrape(1)

    assert_that(caplog.text).contains(
        "Unable to read",
        "book_id: 1"
    )


@pytest.fixture
def assert_all_responses_were_requested() -> bool:
    return False


def _a_random_book() -> Dict[str, Any]:
    return {
        "work_internal_id": "A Random Work Internal ID",
        "work_id": 12345,
        "author": "A Random Author",
        "author_url": "A Random Author URL",
        "avg_rating": 4.5,
        "rating_histogram": [1, 2, 3, 4, 5],
        "book_id": 1,
        "book_title": "A Random Book Title",
        "book_url": "www.bookurl.com",
        "scrape_time": "2022-09-01T00:00:00.000000",
    }

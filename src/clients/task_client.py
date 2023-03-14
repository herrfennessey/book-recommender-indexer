import logging
from functools import lru_cache
from typing import Union

import grpc
from fastapi import Depends
from google.cloud import tasks_v2
from google.cloud.tasks_v2 import CloudTasksClient
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport
from pydantic import BaseModel

from src.dependencies import Properties

BOOK_SPIDER_NAME = "book"
USER_REVIEWS_SPIDER_NAME = "user_reviews"

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class BookScrapeRequestArgs(BaseModel):
    books: str
    project_id: str
    topic_name: str


class UserReviewsScrapeRequestArgs(BaseModel):
    profiles: str
    project_id: str
    topic_name: str


class ScrapeRequest(BaseModel):
    spider_name: str
    start_requests: bool = True
    crawl_args: Union[BookScrapeRequestArgs, UserReviewsScrapeRequestArgs]


class TaskQueuePayload(BaseModel):
    url: str
    body: bytes
    http_method: int = tasks_v2.HttpMethod.POST
    headers: dict = {"Content-Type": "application/json"}


class TaskQueueRequest(BaseModel):
    http_request: TaskQueuePayload


class TaskClient(object):
    def __init__(self, client: CloudTasksClient, properties: Properties):
        self.properties = properties
        self.client = client

    def enqueue_book(self, book_id: int) -> str:
        book_scrape_request = ScrapeRequest(spider_name=BOOK_SPIDER_NAME,
                                            crawl_args=BookScrapeRequestArgs(books=book_id,
                                                                             project_id=self.properties.gcp_project_name,
                                                                             topic_name=self.properties.pubsub_book_topic_name))
        book_json = book_scrape_request.json()
        # cloud tasks expects bytes
        book_bytes = book_json.encode("utf-8")
        task = TaskQueueRequest(
            http_request=TaskQueuePayload(url=f"{self.properties.scraper_client_base_url}/crawl.json", body=book_bytes))

        parent = self.client.queue_path(self.properties.gcp_project_name, self.properties.cloud_task_region,
                                        self.properties.task_queue_name)
        response = self.client.create_task(parent=parent, task=task.dict())
        logging.info("Created task for book: {}".format(book_id))
        return response.name

    def enqueue_user_scrape(self, user_profile_id: str) -> str:
        user_scrape_request = ScrapeRequest(spider_name=USER_REVIEWS_SPIDER_NAME,
                                            crawl_args=UserReviewsScrapeRequestArgs(profiles=user_profile_id,
                                                                                    project_id=self.properties.gcp_project_name,
                                                                                    topic_name=self.properties.pubsub_user_review_topic_name))
        user_scrape_request_json = user_scrape_request.json()
        # cloud tasks expects bytes
        user_scrape_request_bytes = user_scrape_request_json.encode("utf-8")
        task = TaskQueueRequest(
            http_request=TaskQueuePayload(url=f"{self.properties.scraper_client_base_url}/crawl.json",
                                          body=user_scrape_request_bytes))

        parent = self.client.queue_path(self.properties.gcp_project_name, self.properties.cloud_task_region,
                                        self.properties.task_queue_name)
        response = self.client.create_task(parent=parent, task=task.dict())
        logging.info("Created task for user ID: {}".format(user_profile_id))
        return response.name


def get_cloud_tasks_client(properties: Properties = Depends(get_properties)):
    if properties.env_name == "local":
        transport = CloudTasksGrpcTransport(channel=grpc.insecure_channel('localhost:8123'))
        return CloudTasksClient(transport=transport)
    else:
        return CloudTasksClient()


def get_task_client(client: CloudTasksClient = Depends(get_cloud_tasks_client),
                    properties: Properties = Depends(get_properties)):
    return TaskClient(client, properties)

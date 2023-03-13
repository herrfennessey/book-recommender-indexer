import logging
from functools import lru_cache

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


class BookScrapeRequest(BaseModel):
    spider_name: str
    start_requests: bool = True
    crawl_args: BookScrapeRequestArgs


class TaskQueuePayload(BaseModel):
    url: str
    body: BookScrapeRequest
    http_method: int = tasks_v2.HttpMethod.POST
    headers: dict = {"Content-Type": "application/json"}


class TaskQueueRequest(BaseModel):
    http_request: TaskQueuePayload


class TaskClient(object):
    def __init__(self, client: CloudTasksClient, properties: Properties):
        self.properties = properties
        self.client = client

    def enqueue_book(self, book_id: int):
        book_scrape_request = BookScrapeRequest(spider_name=BOOK_SPIDER_NAME,
                                                crawl_args=BookScrapeRequestArgs(books=book_id,
                                                                                 project_id=self.properties.gcp_project_name,
                                                                                 topic_name=self.properties.pubsub_book_topic_name))
        book_json = book_scrape_request.json()
        # cloud tasks expects bytes
        book_bytes = book_json.encode("utf-8")
        task = TaskQueueRequest(
            http_request=TaskQueuePayload(url=f"{self.properties.scraper_client_base_url}/crawl.json", body=book_bytes))

        parent = self.client.queue_path(self.properties.gcp_project_name, self.properties.cloud_task_region,
                                        self.properties.book_task_queue)
        return self.client.create_task(parent=parent, task=task.dict())


def get_cloud_tasks_client(properties: Properties = Depends(get_properties)):
    if properties.env_name == "local":
        transport = CloudTasksGrpcTransport(channel=grpc.insecure_channel('localhost:8123'))
        return CloudTasksClient(transport=transport)
    else:
        return CloudTasksClient()


def get_task_client(client: CloudTasksClient = Depends(get_cloud_tasks_client),
                     properties: Properties = Depends(get_properties)):
    return TaskClient(client, properties)

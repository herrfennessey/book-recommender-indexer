import logging
from functools import lru_cache
from typing import Union

import grpc
from fastapi import Depends
from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2
from google.cloud.tasks_v2 import CloudTasksClient
from google.cloud.tasks_v2.services.cloud_tasks.transports import (
    CloudTasksGrpcTransport,
)
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
    name: str
    http_request: TaskQueuePayload


class TaskClient(object):
    def __init__(self, client: CloudTasksClient, properties: Properties):
        self.properties = properties
        self.client = client

    def is_ready(self) -> bool:
        # This is kind of like the parent "folder" that you can run the list_queues command on
        location_path = self.client.common_location_path(
            self.properties.gcp_project_name, self.properties.cloud_task_region
        )

        # This is the actual queue path we expect to find in there
        queue_path = self._generate_parent_path()

        queue_generator = self.client.list_queues(parent=location_path)
        # We want to make sure our queue exists before we start sending stuff to it
        for queue in queue_generator:
            if queue.name == queue_path:
                return True
        return False

    def enqueue_book(self, book_id: int) -> str:
        book_scrape_request = ScrapeRequest(
            spider_name=BOOK_SPIDER_NAME,
            crawl_args=BookScrapeRequestArgs(
                books=book_id,
                project_id=self.properties.gcp_project_name,
                topic_name=self.properties.pubsub_book_topic_name,
            ),
        )
        book_json = book_scrape_request.json()
        # cloud tasks expects bytes
        book_bytes = book_json.encode("utf-8")

        # We use the book ID as part of the task name to deduplicate the task queue.
        task_name = f"book-{book_id}"
        task = TaskQueueRequest(
            name=self._generate_task_path(task_name),
            http_request=TaskQueuePayload(
                url=f"{self.properties.scraper_client_base_url}/crawl.json",
                body=book_bytes,
            ),
        )

        parent = self._generate_parent_path()

        try:
            response = self.client.create_task(parent=parent, task=task.dict())
            logging.info("Created task for book: {}".format(book_id))
            return response.name
        except AlreadyExists as e:
            logging.info(
                "Task already exists for book: {}. Exception: {}".format(book_id, e)
            )
            return "duplicate"

    def enqueue_user_scrape(self, user_profile_id: str) -> str:
        user_scrape_request = ScrapeRequest(
            spider_name=USER_REVIEWS_SPIDER_NAME,
            crawl_args=UserReviewsScrapeRequestArgs(
                profiles=user_profile_id,
                project_id=self.properties.gcp_project_name,
                topic_name=self.properties.pubsub_user_review_topic_name,
            ),
        )
        user_scrape_request_json = user_scrape_request.json()
        # cloud tasks expects bytes
        user_scrape_request_bytes = user_scrape_request_json.encode("utf-8")

        # We use the user profile ID as part of the task name to deduplicate the task queue.
        task_name = f"user-{user_profile_id}"
        task = TaskQueueRequest(
            name=self._generate_task_path(task_name),
            http_request=TaskQueuePayload(
                url=f"{self.properties.scraper_client_base_url}/crawl.json",
                body=user_scrape_request_bytes,
            ),
        )

        parent = self._generate_parent_path()
        try:
            response = self.client.create_task(parent=parent, task=task.dict())
            logging.info("Created task for user ID: {}".format(user_profile_id))
        except AlreadyExists as e:
            logging.info(
                "Task already exists for user ID: {}. Exception: {}".format(
                    user_profile_id, e
                )
            )
            return "duplicate"

        return response.name

    def _generate_parent_path(self):
        """
        The parent path is the absolute path to the queue we want to send tasks to

        :return: str : Fully qualified path to the queue
        """
        parent = self.client.queue_path(
            self.properties.gcp_project_name,
            self.properties.cloud_task_region,
            self.properties.task_queue_name,
        )
        return parent

    def _generate_task_path(self, task_name: str) -> str:
        """
        The task path is the absolute path of the task itself. We can set this explicitly, or it will be auto set
        by the cloud tasks client.

        :return: str : Fully qualified path to the task itself
        """
        parent = self.client.task_path(
            self.properties.gcp_project_name,
            self.properties.cloud_task_region,
            self.properties.task_queue_name,
            task_name,
        )
        return parent


def get_cloud_tasks_client(properties: Properties = Depends(get_properties)):
    if properties.env_name == "local":
        transport = CloudTasksGrpcTransport(
            channel=grpc.insecure_channel(
                "localhost:8123", options=[("grpc.enable_http_proxy", 0)]
            )
        )
        return CloudTasksClient(transport=transport)
    else:
        return CloudTasksClient()


def get_task_client(
    client: CloudTasksClient = Depends(get_cloud_tasks_client),
    properties: Properties = Depends(get_properties),
):
    return TaskClient(client, properties)

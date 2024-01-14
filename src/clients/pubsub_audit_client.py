import json
import logging
import time
from concurrent import futures
from functools import lru_cache
from typing import Any, Callable, Dict, List

from fastapi import Depends
from google.cloud import pubsub_v1
from google.pubsub_v1 import PublisherClient

from src.clients.utils.json_utils import json_timestamp_friendly_serializer
from src.dependencies import Properties

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class ItemTopic(object):
    USER_REVIEW = get_properties().pubsub_user_review_audit_topic_name
    BOOK = get_properties().pubsub_book_audit_topic_name
    PROFILE = get_properties().pubsub_profiles_audit_topic_name


class PubSubAuditClient(object):
    """
    Client to audit all indexed data and send it to PubSub as well

    It accepts a topic name and a list of JSON like objects. This class will take care of all of the data processing
    and transport mechanisms to send it to PubSub
    """

    def __init__(self, publisher_client: PublisherClient, properties: Properties):
        self.publisher_client = publisher_client
        self.properties = properties

    def send_batch(self, audit_item: ItemTopic, messages: List[Dict[str, Any]]):
        start_time = time.time() * 1000
        topic_path = self.publisher_client.topic_path(
            self.properties.gcp_project_name, audit_item
        )
        publish_futures = []
        for message in messages:
            data_payload = str(
                json.dumps(message, default=json_timestamp_friendly_serializer)
            )
            publish_future = self.publisher_client.publish(
                topic_path, data_payload.encode("utf-8")
            )
            publish_future.add_done_callback(
                self.get_callback(publish_future, data_payload)
            )
            publish_futures.append(publish_future)

        futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info(
            "Sent {} items to {} in {} milliseconds".format(
                len(messages), audit_item, time.time() * 1000 - start_time
            )
        )

    @staticmethod
    def get_callback(
        publish_future: pubsub_v1.publisher.futures.Future, data: str
    ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        """
        Taken from the GCP documentation page on publishing to PubSub topics. I have no idea what this code monstrosity
        is doing...
        https://cloud.google.com/pubsub/docs/publisher
        """

        def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                # Wait 60 seconds for the publish call to succeed.
                publish_future.result(timeout=60)
            except futures.TimeoutError:
                logger.error(f"Publishing {data} timed out.")

        return callback


def get_pubsub_audit_publisher():
    """
    Easier testing + dependency injection
    """
    return pubsub_v1.PublisherClient()


def get_pubsub_audit_client(
    pubsub_publisher: PublisherClient = Depends(get_pubsub_audit_publisher),
    properties: Properties = Depends(get_properties),
):
    return PubSubAuditClient(pubsub_publisher, properties)

import json
import logging
import time
from concurrent import futures
from functools import lru_cache
from typing import List, Any, Dict, Callable

from fastapi import Depends
from google.cloud import pubsub_v1
from google.pubsub_v1 import PublisherClient

from src.dependencies import Properties

logger = logging.getLogger(__name__)


@lru_cache()
def get_properties():
    return Properties()


class PubsubAuditClient(object):
    """
    Client to audit all indexed data and send it to PubSub as well

    It accepts a topic name and a list of JSON like objects. This class will take care of all of the data processing
    and transport mechanisms to send it to PubSub
    """

    def __init__(self, publisher_client: PublisherClient, properties: Properties):
        self.publisher_client = publisher_client
        self.properties = properties

    def send_batch(self, topic_name, messages: List[Dict[str, Any]]):
        start_time = time.time() * 1000
        topic_path = self.publisher_client.topic_path(self.properties.gcp_project_name, topic_name)
        publish_futures = []
        for message in messages:
            data_payload = str(json.dumps(message))
            publish_future = self.publisher_client.publish(topic_path, data_payload.encode("utf-8"))
            publish_future.add_done_callback(self.get_callback(publish_future, data_payload))
            publish_futures.append(publish_future)

        futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info(
            "Sent {} items to PubSub in {} milliseconds".format(len(messages), time.time() * 1000 - start_time))

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
                logger.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logger.info(f"Publishing {data} timed out.")

        return callback


def get_pubsub_audit_publisher():
    """
    Easier testing + dependency injection
    """
    return pubsub_v1.PublisherClient()


def get_pubsub_audit_client(pubsub_publisher: PublisherClient = Depends(get_pubsub_audit_publisher),
                            properties: Properties = Depends(get_properties)):
    return PubsubAuditClient(pubsub_publisher, properties)

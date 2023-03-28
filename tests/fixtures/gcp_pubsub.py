from _pytest.fixtures import fixture
from google.pubsub_v1 import PublisherClient, SubscriberClient
from testcontainers.core.waiting_utils import wait_for_logs

from tests.pubsub_container import PubSubContainer


@fixture(scope="session", autouse=True)
def pubsub_container():
    with PubSubContainer() as container:
        wait_for_logs(container, "Server started, listening", 40)
        yield container


@fixture(scope="session", autouse=True)
def publisher_client(pubsub_container: PubSubContainer) -> PublisherClient:
    return pubsub_container.get_publisher_client()


@fixture(scope="session", autouse=True)
def subscriber_client(pubsub_container: PubSubContainer) -> SubscriberClient:
    return pubsub_container.get_subscriber_client()

import pytest
from assertpy import assert_that
from google.pubsub_v1 import SubscriberClient

from src.dependencies import Properties

properties = Properties()
SUBSCRIBER_NAME = "test-topic-sub"


@pytest.fixture(autouse=True)
def test_setup(publisher_client, subscriber_client):
    publisher_client.create_topic(request={"name": _get_topic_path()})
    subscriber_client.create_subscription(request={"name": _get_subscription_path(), "topic": _get_topic_path()})
    yield
    subscriber_client.delete_subscription(request={"subscription": _get_subscription_path()})
    publisher_client.delete_topic(request={"topic": _get_topic_path()})


def _consume_one_message(client: SubscriberClient):
    response = client.pull(request={"subscription": _get_subscription_path(), "max_messages": 1}, timeout=2)
    ack_ids = [received_message.ack_id for received_message in response.received_messages]
    client.acknowledge(request={"subscription": _get_subscription_path(), "ack_ids": ack_ids})
    return response


def test_pubsub_testcontainers_works(publisher_client, subscriber_client):
    message_id = publisher_client.publish(_get_topic_path(), b"test message").result()
    message = _consume_one_message(client=subscriber_client)
    for received_message in message.received_messages:
        assert_that(received_message.message.message_id).is_equal_to(message_id)
        assert_that(received_message.message.data).is_equal_to(b"test message")


def _get_topic_path():
    return f"projects/{properties.gcp_project_name}/topics/{properties.pubsub_profiles_audit_topic_name}"


def _get_subscription_path():
    return f"projects/{properties.gcp_project_name}/subscriptions/{SUBSCRIBER_NAME}"

from os import environ

from testcontainers.general import DockerContainer


class PubSubContainer(DockerContainer):
    """
    Pubsub emulator
    """

    def __init__(
        self,
        image="gcr.io/google.com/cloudsdktool/google-cloud-cli:latest",
        project="test-project",
        port=8800,
        **kwargs,
    ):
        super(PubSubContainer, self).__init__(image=image, **kwargs)
        self.project = project
        self.port = port
        self.with_exposed_ports(self.port)
        self.with_command(
            f"gcloud beta emulators pubsub start --project={self.project} --host-port=0.0.0.0:{self.port}"
        )

    def get_publisher_client(self):
        from google.cloud.pubsub_v1 import PublisherClient

        emulator_host = (
            f"{self.get_container_host_ip()}:{self.get_exposed_port(self.port)}"
        )
        environ["PUBSUB_EMULATOR_HOST"] = emulator_host

        return PublisherClient()

    def get_subscriber_client(self):
        from google.cloud.pubsub_v1 import SubscriberClient

        emulator_host = (
            f"{self.get_container_host_ip()}:{self.get_exposed_port(self.port)}"
        )
        environ["PUBSUB_EMULATOR_HOST"] = emulator_host

        return SubscriberClient()

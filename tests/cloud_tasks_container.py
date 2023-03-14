import grpc
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport
from testcontainers.general import DockerContainer


class CloudTasksContainer(DockerContainer):
    """
    Cloud Tasks Emulator used for integration testing
    """

    def __init__(
            self,
            image="ghcr.io/aertje/cloud-tasks-emulator:latest",
            project="test-project",
            port=8123,
            **kwargs,
    ):
        super(CloudTasksContainer, self).__init__(image=image, **kwargs)
        self.project = project
        self.port = port
        self.with_exposed_ports(self.port)
        self.with_command(
            f'-host 0.0.0.0 -port {self.port} -queue "projects/test-project/locations/here/queues/test-book-queue"'
        )

    def get_client(self):
        from google.cloud.tasks_v2 import CloudTasksClient

        transport = CloudTasksGrpcTransport(
            channel=grpc.insecure_channel(f"{self.get_container_host_ip()}:{self.get_exposed_port(self.port)}"))
        return CloudTasksClient(transport=transport)

    def cleanup(self):
        self.get_client()

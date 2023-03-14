from google.cloud.tasks_v2 import CloudTasksClient


def test_cloud_tasks_client(cloud_tasks):
    # Create a normal http task that should succeed
    cloud_tasks.create_task(task={'http_request': {'http_method': 'GET', 'url': 'https://www.google.com'}},
                            parent="projects/test-project/locations/here/queues/test-book-queue")  # 200

from pydantic import BaseSettings


class Properties(BaseSettings):
    app_name: str = "Book Recommender API"
    env_name: str = "local"
    book_recommender_api_base_url: str = "http://localhost:9000"
    gcp_project_name: str = "test-project"
    pubsub_book_topic_name: str = "test-topic"
    pubsub_user_review_topic_name: str = "test-topic"
    cloud_task_region: str = "here"
    book_task_queue: str = "test-book-queue"

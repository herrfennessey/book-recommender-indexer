from pydantic import BaseSettings


class Properties(BaseSettings):
    app_name: str = "Book Recommender API"
    env_name: str = "local"
    book_recommender_api_base_url_v2: str = "http://localhost_v2:9000"
    scraper_client_base_url: str = "http://localhost:9080"
    gcp_project_name: str = "test-project"
    pubsub_book_topic_name: str = "test-topic"
    pubsub_user_review_topic_name: str = "test-topic"
    pubsub_profiles_audit_topic_name = "test-topic"
    pubsub_book_audit_topic_name = "test-topic"
    pubsub_user_review_audit_topic_name = "test-topic"
    cloud_task_region: str = "here"
    task_queue_name: str = "test-queue"

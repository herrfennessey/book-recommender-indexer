from pydantic import BaseSettings


class Properties(BaseSettings):
    app_name: str = "Book Recommender API"
    env_name: str = "local"
    gcp_project_name: str = "test-project"
    pubsub_book_topic_name: str = "test-topic"
    book_recommender_api_base_url: str = "http://localhost:9000"
    scraper_client_base_url: str = "http://localhost:9080"

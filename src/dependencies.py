from pydantic import BaseSettings


class Properties(BaseSettings):
    app_name: str = "Book Recommender API"
    env_name: str = "local"
    book_recommendations_api_base_url: str = "http://localhost:8999"

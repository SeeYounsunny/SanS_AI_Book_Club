from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    telegram_bot_token: str = Field(alias="TELEGRAM_BOT_TOKEN")
    member_chat_id: str = Field(alias="MEMBER_CHAT_ID")
    admin_chat_id: str = Field(alias="ADMIN_CHAT_ID")

    timezone: str = Field(default="Asia/Seoul", alias="TIMEZONE")
    db_path: str = Field(default="./data/reading_club.sqlite3", alias="DB_PATH")
    database_url: Optional[str] = Field(default=None, alias="DATABASE_URL")
    bookmarks_max_per_user: int = Field(default=100, alias="BOOKMARKS_MAX_PER_USER")

    # Embeddings (for taste snapshot without generative LLM)
    embeddings_provider: str = Field(default="none", alias="EMBEDDINGS_PROVIDER")
    openai_api_key: Optional[str] = Field(default=None, alias="OPENAI_API_KEY")
    # Cheapest general-purpose OpenAI embeddings model
    openai_embeddings_model: str = Field(default="text-embedding-3-small", alias="OPENAI_EMBEDDINGS_MODEL")
    # Keep defaults modest to control cost
    taste_bookmarks_limit: int = Field(default=30, alias="TASTE_BOOKMARKS_LIMIT")
    taste_max_clusters: int = Field(default=4, alias="TASTE_MAX_CLUSTERS")

    # LLM summary (optional)
    openai_summary_model: str = Field(default="gpt-4o-mini", alias="OPENAI_SUMMARY_MODEL")
    taste_summary_max_quotes: int = Field(default=6, alias="TASTE_SUMMARY_MAX_QUOTES")

    # Webhook mode: if webhook_url is set, we will start webhook server.
    webhook_url: Optional[str] = Field(default=None, alias="WEBHOOK_URL")
    port: int = Field(default=8080, alias="PORT")
    webhook_secret_token: Optional[str] = Field(default=None, alias="WEBHOOK_SECRET_TOKEN")

    # External book metadata
    # Optional: used for /book_search and /book_select
    google_books_api_key: Optional[str] = Field(default=None, alias="GOOGLE_BOOKS_API_KEY")


def get_settings() -> Settings:
    return Settings()


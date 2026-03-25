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

    # Webhook mode: if webhook_url is set, we will start webhook server.
    webhook_url: Optional[str] = Field(default=None, alias="WEBHOOK_URL")
    port: int = Field(default=8080, alias="PORT")
    webhook_secret_token: Optional[str] = Field(default=None, alias="WEBHOOK_SECRET_TOKEN")


def get_settings() -> Settings:
    return Settings()


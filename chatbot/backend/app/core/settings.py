from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    app_name: str = "Lakehouse Analyst Chatbot"
    environment: str = "development"
    host: str = "0.0.0.0"
    port: int = 8090
    log_level: str = "INFO"
    cors_allowed_origins: list[str] = Field(default_factory=lambda: ["http://127.0.0.1:4173"])
    trusted_hosts: list[str] = Field(default_factory=lambda: ["127.0.0.1", "localhost", "testserver"])
    session_secret: str = "replace-me"
    cookie_name: str = "lakehouse_chatbot_session"
    session_ttl_hours: int = 24 * 7

    trino_host: str = "127.0.0.1"
    trino_port: int = 8085
    trino_user: str = "chatbot"
    trino_catalog: str = "iceberg"
    trino_schema: str = "demo"
    trino_http_scheme: str = "http"
    trino_query_timeout_seconds: int = 25
    trino_max_rows: int = 500

    vertex_model: str = "gemini-2.5-flash"
    vertex_api_key: str | None = None
    vertex_project_id: str | None = None
    vertex_region: str = "global"
    allow_mock_llm: bool = True

    sql_default_limit: int = 50
    sql_hard_limit: int = 500
    feedback_note_max_length: int = 1000

    data_dir: Path = BASE_DIR / "data"

    model_config = SettingsConfigDict(
        env_prefix="CHATBOT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("cors_allowed_origins", "trusted_hosts", mode="before")
    @classmethod
    def _coerce_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        raise TypeError("expected string or list")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    return settings

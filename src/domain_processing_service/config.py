from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class AppSettings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="DOMAIN_PROCESSING_",
        env_file=".env",
        extra="ignore",
    )

    app_name: str = "domain-processing-service"
    environment: str = "local"
    log_level: LogLevel = "INFO"

    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/domain_processing"
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)
    db_pool_timeout_seconds: float = Field(default=5.0, gt=0)

    max_domains_per_request: int = Field(default=1000, ge=1)
    default_page_size: int = Field(default=100, ge=1)
    max_page_size: int = Field(default=1000, ge=1)

    worker_concurrency: int = Field(default=50, ge=1)
    worker_queue_capacity: int = Field(default=100, ge=1)
    task_lease_seconds: int = Field(default=120, ge=1)
    shutdown_grace_seconds: int = Field(default=30, ge=1)

    dns_timeout_seconds: float = Field(default=3.0, gt=0)
    dns_nameservers: list[str] | str = Field(
        default_factory=lambda: ["8.8.8.8", "1.1.1.1", "8.8.4.4"],
        description="Upstream DNS nameservers for aiodns resolution",
    )
    connect_timeout_seconds: float = Field(default=5.0, gt=0)
    tls_timeout_seconds: float = Field(default=5.0, gt=0)
    read_timeout_seconds: float = Field(default=10.0, gt=0)
    total_processing_timeout_seconds: float = Field(default=20.0, gt=0)
    max_response_read_bytes: int = Field(default=50 * 1024, ge=1)

    domain_detail_freshness_seconds: int = Field(default=24 * 60 * 60, ge=1)
    refresh_interval_seconds: int = Field(default=14 * 24 * 60 * 60, ge=1)
    max_attempts: int = Field(default=3, ge=1)

    @field_validator("dns_nameservers", mode="before")
    @classmethod
    def parse_dns_nameservers(cls, value: object) -> list[str]:
        if isinstance(value, str):
            return [s.strip() for s in value.split(",") if s.strip()]
        if isinstance(value, (list, tuple)):
            return [str(s).strip() for s in value if str(s).strip()]
        return ["8.8.8.8", "1.1.1.1", "8.8.4.4"]

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        if not value.strip():
            msg = "database_url must not be empty"
            raise ValueError(msg)
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, value: object) -> object:
        if isinstance(value, str):
            return value.upper()
        return value

    @field_validator("max_page_size")
    @classmethod
    def validate_page_size_bounds(cls, value: int, info: object) -> int:
        data = getattr(info, "data", {})
        default_page_size = data.get("default_page_size")
        if isinstance(default_page_size, int) and value < default_page_size:
            msg = "max_page_size must be greater than or equal to default_page_size"
            raise ValueError(msg)
        return value


@lru_cache
def get_settings() -> AppSettings:
    return AppSettings()

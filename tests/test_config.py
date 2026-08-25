import pytest
from pydantic import ValidationError

from domain_processing_service.config import AppSettings


def test_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DOMAIN_PROCESSING_DATABASE_URL",
        "postgresql+asyncpg://test:test@db.example.local:5432/testdb",
    )
    monkeypatch.setenv("DOMAIN_PROCESSING_LOG_LEVEL", "debug")
    monkeypatch.setenv("DOMAIN_PROCESSING_DB_POOL_SIZE", "3")
    monkeypatch.setenv("DOMAIN_PROCESSING_WORKER_CONCURRENCY", "12")

    settings = AppSettings()

    assert settings.database_url == "postgresql+asyncpg://test:test@db.example.local:5432/testdb"
    assert settings.log_level == "DEBUG"
    assert settings.db_pool_size == 3
    assert settings.worker_concurrency == 12


def test_settings_reject_invalid_bounds() -> None:
    with pytest.raises(ValidationError):
        AppSettings(db_pool_size=0)


def test_settings_rejects_max_page_size_below_default() -> None:
    with pytest.raises(ValidationError):
        AppSettings(default_page_size=50, max_page_size=25)


def test_safety_guard_rejects_protected_databases() -> None:
    from tests.conftest import assert_safe_test_database_url

    with pytest.raises(RuntimeError, match="SAFETY VIOLATION.*protected database 'domain_processing'"):
        assert_safe_test_database_url("postgresql://user:password@localhost:5432/domain_processing")

    with pytest.raises(RuntimeError, match="SAFETY VIOLATION.*protected database 'postgres'"):
        assert_safe_test_database_url("postgresql://user:password@localhost:5432/postgres")

    with pytest.raises(RuntimeError, match="SAFETY VIOLATION.*Database URL cannot be empty"):
        assert_safe_test_database_url("")


def test_safety_guard_accepts_isolated_test_databases() -> None:
    from tests.conftest import assert_safe_test_database_url

    # Should not raise
    assert_safe_test_database_url("postgresql+psycopg2://user:password@localhost:5432/domain_processing_test")
    assert_safe_test_database_url("postgresql+asyncpg://user:password@localhost:5432/domain_processing_test")
    assert_safe_test_database_url("postgresql://localhost/test_analytics")


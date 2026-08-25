"""Pytest configuration and shared fixtures."""

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from alembic import command
from domain_processing_service.config import AppSettings
from domain_processing_service.models import Domain, Job

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_DATABASE_URL = (
    "postgresql+psycopg2://user:password@127.0.0.1:5432/domain_processing_test"
)
DEFAULT_TEST_ASYNC_DATABASE_URL = (
    "postgresql+asyncpg://user:password@127.0.0.1:5432/domain_processing_test"
)
FORBIDDEN_DATABASES = frozenset({
    "domain_processing",
    "postgres",
    "template0",
    "template1",
    "",
    None,
})


def assert_safe_test_database_url(url: str | None) -> None:
    """Refuse to run tests if configured against production or development database."""
    if not url:
        raise RuntimeError("SAFETY VIOLATION: Database URL cannot be empty for tests.")

    from sqlalchemy.engine.url import make_url

    parsed = make_url(url)
    database_name = parsed.database

    if database_name in FORBIDDEN_DATABASES:
        raise RuntimeError(
            f"SAFETY VIOLATION: Tests are configured against protected database '{database_name}'. "
            "Tests MUST only run against isolated test databases such as 'domain_processing_test'."
        )

    if not (
        (database_name and database_name.endswith("_test"))
        or (database_name and database_name.startswith("test_"))
    ):
        raise RuntimeError(
            f"SAFETY VIOLATION: Test database '{database_name}' must contain '_test' suffix or 'test_' prefix."
        )


def get_test_database_url() -> str:
    url = os.environ.get("DOMAIN_PROCESSING_TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    assert_safe_test_database_url(url)
    return url


def get_test_async_database_url() -> str:
    url = os.environ.get(
        "DOMAIN_PROCESSING_TEST_ASYNC_DATABASE_URL", DEFAULT_TEST_ASYNC_DATABASE_URL
    )
    assert_safe_test_database_url(url)
    return url


def ensure_test_database_exists() -> None:
    """Ensure that the test database exists; create it via maintenance connection if missing."""
    test_sync_url = get_test_database_url()
    assert_safe_test_database_url(test_sync_url)

    from sqlalchemy.engine.url import make_url

    parsed = make_url(test_sync_url)
    test_db_name = parsed.database
    admin_url = os.environ.get(
        "DOMAIN_PROCESSING_TEST_ADMIN_DATABASE_URL",
        parsed.set(database="domain_processing"),
    )

    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": test_db_name},
            ).scalar()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{test_db_name}"'))
    finally:
        admin_engine.dispose()


def alembic_config() -> Config:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", get_test_database_url())
    return config


def reset_schema() -> None:
    test_url = get_test_database_url()
    assert_safe_test_database_url(test_url)
    engine = create_engine(test_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DROP TABLE IF EXISTS
                        idempotency_record,
                        domain_detail,
                        task,
                        domain,
                        job,
                        alembic_version
                    CASCADE
                    """
                )
            )
            connection.execute(text("DROP TYPE IF EXISTS task_type CASCADE"))
            connection.execute(text("DROP TYPE IF EXISTS task_status CASCADE"))
    finally:
        engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def _migrated_database() -> Iterator[None]:
    """Session-scoped autouse fixture that runs migrations once on domain_processing_test."""
    ensure_test_database_exists()
    assert_safe_test_database_url(get_test_database_url())
    assert_safe_test_database_url(get_test_async_database_url())

    config = alembic_config()
    reset_schema()
    command.upgrade(config, "head")

    # Verify tables exist after migration in test database
    verify_engine = create_engine(get_test_database_url())
    try:
        with verify_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name IN (
                    'job', 'task', 'domain', 'domain_detail', 'idempotency_record'
                )
            """))
            tables = {row[0] for row in result}
            expected = {'job', 'task', 'domain', 'domain_detail', 'idempotency_record'}
            missing = expected - tables
            if missing:
                raise RuntimeError(f"Migration did not create tables: {missing}")
    finally:
        verify_engine.dispose()

    yield

    # Clean up test database only at end of session
    assert_safe_test_database_url(get_test_database_url())
    command.downgrade(config, "base")


@pytest.fixture(scope="session")
async def async_engine() -> AsyncIterator[AsyncEngine]:
    """Session-scoped async engine shared across all tests (pointing to domain_processing_test)."""
    async_url = get_test_async_database_url()
    assert_safe_test_database_url(async_url)

    engine = create_async_engine(
        async_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args={
            "prepared_statement_cache_size": 0
        },  # Disable prepared statement cache to avoid OID cache issues
    )
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture()
async def async_db_session(async_engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """Function-scoped async session per test with TRUNCATE cleanup on test DB after test."""
    async_session_maker = async_sessionmaker(
        bind=async_engine, class_=AsyncSession, expire_on_commit=False
    )

    # Create session for test (without context manager to avoid double-close)
    session = async_session_maker()
    try:
        yield session
    finally:
        await session.close()

    # Perform TRUNCATE on domain_processing_test
    async with async_session_maker() as cleanup_session:
        await cleanup_session.execute(text("""
            TRUNCATE TABLE
                idempotency_record,
                domain_detail,
                task,
                domain,
                job
            RESTART IDENTITY CASCADE
        """))
        await cleanup_session.commit()


@pytest.fixture()
async def test_client(async_engine: AsyncEngine) -> AsyncIterator[TestClient]:
    """Create a TestClient that uses the shared test async engine."""
    from domain_processing_service.app import create_app
    from domain_processing_service.config import AppSettings

    # Create a database wrapper that uses the shared test async engine
    class TestDatabase:
        def __init__(self, engine: AsyncEngine):
            self._engine = engine

        async def connect(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def is_ready(self) -> bool:
            return True

        @property
        def engine(self) -> AsyncEngine:
            return self._engine

        @property
        def session_maker(self) -> async_sessionmaker:
            return async_sessionmaker(
                bind=self._engine, class_=AsyncSession, expire_on_commit=False
            )

        async def _ping(self) -> None:
            pass

    test_database = TestDatabase(async_engine)
    async_url = get_test_async_database_url()
    assert_safe_test_database_url(async_url)
    settings = AppSettings(database_url=async_url)
    object.__setattr__(settings, "redis_db", 1)
    app = create_app(settings, test_database)

    with TestClient(app) as client:
        yield client


@pytest.fixture()
async def async_client(async_engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    """Create an async HTTP client for testing pointing to test database and Redis DB 1."""
    from domain_processing_service.app import create_app
    from domain_processing_service.config import AppSettings

    # Create a database wrapper that uses the shared test async engine
    class TestDatabase:
        def __init__(self, engine: AsyncEngine):
            self._engine = engine

        async def connect(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def is_ready(self) -> bool:
            return True

        @property
        def engine(self) -> AsyncEngine:
            return self._engine

        @property
        def session_maker(self) -> async_sessionmaker:
            return async_sessionmaker(
                bind=self._engine, class_=AsyncSession, expire_on_commit=False
            )

        async def _ping(self) -> None:
            pass

    test_database = TestDatabase(async_engine)
    async_url = get_test_async_database_url()
    assert_safe_test_database_url(async_url)
    settings = AppSettings(database_url=async_url)
    object.__setattr__(settings, "redis_db", 1)
    app = create_app(settings, test_database)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture()
async def test_settings() -> AppSettings:
    """Create test settings pointing exclusively to test DB and Redis DB 1."""
    async_url = get_test_async_database_url()
    assert_safe_test_database_url(async_url)
    settings = AppSettings(
        database_url=async_url,
        worker_concurrency=5,
        worker_queue_capacity=10,
        task_lease_seconds=120,
        shutdown_grace_seconds=10,
    )
    object.__setattr__(settings, "redis_db", 1)
    return settings


@pytest.fixture()
async def sample_domain(async_db_session: AsyncSession) -> Domain:
    """Create a sample domain for testing."""

    now = datetime.now(UTC)
    domain = Domain(
        id=uuid.uuid4(),
        normalized_domain="example.com",
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    async_db_session.add(domain)
    await async_db_session.commit()
    await async_db_session.refresh(domain)
    return domain


@pytest.fixture()
async def sample_job(async_db_session: AsyncSession) -> Job:
    """Create a sample job for testing."""
    from domain_processing_service.models import TaskStatus

    now = datetime.now(UTC)
    job = Job(
        id=uuid.uuid4(),
        status=TaskStatus.PENDING,
        created_at=now,
        updated_at=now,
    )
    async_db_session.add(job)
    await async_db_session.commit()
    await async_db_session.refresh(job)
    return job
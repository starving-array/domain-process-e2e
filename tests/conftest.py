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
from domain_processing_service.models import Domain, Job

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/domain_processing"
DEFAULT_TEST_ASYNC_DATABASE_URL = (
    "postgresql+asyncpg://user:password@localhost:5432/domain_processing"
)


def get_test_database_url() -> str:
    return os.environ.get("DOMAIN_PROCESSING_TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)


def get_test_async_database_url() -> str:
    return os.environ.get(
        "DOMAIN_PROCESSING_TEST_ASYNC_DATABASE_URL", DEFAULT_TEST_ASYNC_DATABASE_URL
    )


def alembic_config() -> Config:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", get_test_database_url())
    return config


def reset_schema() -> None:
    engine = create_engine(get_test_database_url())
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
    """Session-scoped autouse fixture that runs migrations once.

    Does not yield to other fixtures.
    """
    config = alembic_config()
    reset_schema()
    command.upgrade(config, "head")
    # Verify tables exist after migration
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

    # Teardown at end of session
    command.downgrade(config, "base")


@pytest.fixture(scope="session")
async def async_engine() -> AsyncIterator[AsyncEngine]:
    """Session-scoped async engine shared across all tests (single connection pool)."""
    engine = create_async_engine(
        get_test_async_database_url(),
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
    """Function-scoped async session per test with TRUNCATE cleanup after test."""
    async_session_maker = async_sessionmaker(
        bind=async_engine, class_=AsyncSession, expire_on_commit=False
    )
    
    # Create session for test (without context manager to avoid double-close)
    session = async_session_maker()
    try:
        yield session
    finally:
        await session.close()
    
    # Perform TRUNCATE using a new session from the shared engine
    # This runs after the test's session is fully closed
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
    """Create a TestClient that uses the shared async engine."""
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
    settings = AppSettings(database_url=get_test_async_database_url())
    app = create_app(settings, test_database)

    with TestClient(app) as client:
        yield client


@pytest.fixture()
async def async_client(async_engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    """Create an async HTTP client for testing that shares the same engine as the test fixtures."""
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
    settings = AppSettings(database_url=get_test_async_database_url())
    app = create_app(settings, test_database)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


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
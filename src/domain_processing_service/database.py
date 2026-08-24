import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from domain_processing_service.config import AppSettings
from domain_processing_service.logging import log_event

logger = logging.getLogger(__name__)


class DatabaseLifecycle(Protocol):
    async def connect(self) -> None:
        """Initialize database resources and verify connectivity."""

    async def close(self) -> None:
        """Release database resources."""

    async def is_ready(self) -> bool:
        """Return whether PostgreSQL is reachable."""

    @property
    def session_maker(self) -> async_sessionmaker[AsyncSession]:
        """Return the session maker for creating database sessions."""
        ...


class SqlAlchemyDatabase:
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._engine: AsyncEngine | None = None
        self._session_maker: async_sessionmaker[AsyncSession] | None = None

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            msg = "database engine has not been initialized"
            raise RuntimeError(msg)
        return self._engine

    @property
    def session_maker(self) -> async_sessionmaker[AsyncSession]:
        if self._session_maker is None:
            msg = "session maker has not been initialized"
            raise RuntimeError(msg)
        return self._session_maker

    async def connect(self) -> None:
        log_event(logger, "database.connection.started", dependency="postgresql")
        self._engine = create_async_engine(
            self._settings.database_url,
            pool_pre_ping=True,
            pool_size=self._settings.db_pool_size,
            max_overflow=self._settings.db_max_overflow,
            pool_timeout=self._settings.db_pool_timeout_seconds,
        )
        self._session_maker = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        try:
            await self._ping()
        except Exception:
            log_event(
                logger,
                "database.connection.failed",
                level=logging.ERROR,
                dependency="postgresql",
                reason="ping_failed",
            )
            raise
        log_event(logger, "database.connection.ready", dependency="postgresql")

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_maker = None

    async def is_ready(self) -> bool:
        if self._engine is None:
            return False
        try:
            await self._ping()
        except Exception:
            return False
        return True

    async def _ping(self) -> None:
        async with self.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))


@asynccontextmanager
async def database_lifespan(database: DatabaseLifecycle) -> AsyncIterator[None]:
    await database.connect()
    try:
        yield
    finally:
        await database.close()

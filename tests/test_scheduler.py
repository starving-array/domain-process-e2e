"""Phase 11: Refresh Scheduler Tests."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy import select

from domain_processing_service.config import AppSettings
from domain_processing_service.models import Domain, DomainDetail, Task, TaskStatus, TaskType
from domain_processing_service.repositories import DomainDetailRepository, TaskRepository
from domain_processing_service.scheduler import RefreshScheduler
from domain_processing_service.worker import WorkerPool


class TestRefreshScheduler:
    """Tests for the RefreshScheduler class."""

    @pytest.fixture()
    def settings(self) -> AppSettings:
        """Test settings with fast intervals."""
        return AppSettings(
            refresh_interval_seconds=1,  # Fast for testing
            worker_concurrency=10,
            worker_queue_capacity=20,
            task_lease_seconds=120,
            max_attempts=3,
        )

    @pytest.fixture()
    def session_maker(self, async_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        """Create a session maker from the test async engine."""
        return async_sessionmaker(
            bind=async_engine, class_=AsyncSession, expire_on_commit=False
        )

    @pytest.fixture()
    async def sample_active_domain(
        self, async_db_session: AsyncSession
    ) -> Domain:
        """Create an active domain with a stale DomainDetail."""
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="example.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain)
        await async_db_session.flush()

        # Create DomainDetail that needs refresh (next_refresh_at in the past)
        detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Example Domain",
            response_time=100,
            response_headers={},
            fetched_at=now - timedelta(hours=24),
            next_refresh_at=now - timedelta(hours=1),  # Stale - needs refresh
            version=1,
        )
        async_db_session.add(detail)
        await async_db_session.commit()
        await async_db_session.refresh(domain)
        return domain

    @pytest.fixture()
    async def sample_inactive_domain(
        self, async_db_session: AsyncSession
    ) -> Domain:
        """Create an inactive domain with a stale DomainDetail."""
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="inactive.example.com",
            is_active=False,
            deactivated_at=now,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain)
        await async_db_session.flush()

        detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Inactive Domain",
            response_time=100,
            response_headers={},
            fetched_at=now - timedelta(hours=24),
            next_refresh_at=now - timedelta(hours=1),  # Stale but inactive
            version=1,
        )
        async_db_session.add(detail)
        await async_db_session.commit()
        await async_db_session.refresh(domain)
        return domain

    @pytest.fixture()
    async def sample_fresh_domain(
        self, async_db_session: AsyncSession
    ) -> Domain:
        """Create an active domain with a fresh DomainDetail (not needing refresh)."""
        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="fresh.example.com",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(domain)
        await async_db_session.flush()

        detail = DomainDetail(
            domain_id=domain.id,
            ip_addresses=["93.184.216.34"],
            dns_records={"A": ["93.184.216.34"]},
            http_status=200,
            page_title="Fresh Domain",
            response_time=100,
            response_headers={},
            fetched_at=now,
            next_refresh_at=now + timedelta(days=14),  # Fresh - doesn't need refresh
            version=1,
        )
        async_db_session.add(detail)
        await async_db_session.commit()
        await async_db_session.refresh(domain)
        return domain

    @pytest.fixture()
    def mock_worker_pool(self) -> MagicMock:
        """Create a mock worker pool with available capacity."""
        pool = MagicMock(spec=WorkerPool)
        pool.available_capacity = 10
        pool.queue = MagicMock()
        pool.queue.size = 0
        pool.queue.capacity = 20
        return pool

    async def test_scheduler_starts_and_stops(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
    ) -> None:
        """Test that scheduler starts and stops correctly."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        # Test start
        await scheduler.start()
        assert scheduler._running is True
        assert scheduler._task is not None

        # Test stop
        await scheduler.stop()
        assert scheduler._running is False

    async def test_scheduler_tick_creates_refresh_tasks(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
        sample_active_domain: Domain,
    ) -> None:
        """Test that scheduler tick creates REFRESH tasks for stale domains."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        # Run a single tick
        await scheduler._tick()

        # Verify refresh task was created
        async with session_maker() as session:
            task_repo = TaskRepository(session)
            stmt = select(Task).where(
                Task.domain_id == sample_active_domain.id,
                Task.type == TaskType.REFRESH,
            )
            result = await session.execute(stmt)
            refresh_tasks = list(result.scalars().all())
            assert len(refresh_tasks) == 1
            task = refresh_tasks[0]
            assert task.domain_id == sample_active_domain.id
            assert task.type == TaskType.REFRESH
            assert task.status == TaskStatus.PENDING
            assert task.job_id is None  # REFRESH tasks have no job

    async def test_scheduler_skips_inactive_domains(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
        sample_inactive_domain: Domain,
    ) -> None:
        """Test that scheduler skips inactive domains."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        await scheduler._tick()

        async with session_maker() as session:
            task_repo = TaskRepository(session)
            tasks = await task_repo.get_by_job_id(sample_inactive_domain.id)
            refresh_tasks = [t for t in tasks if t.type == TaskType.REFRESH]
            assert len(refresh_tasks) == 0

    async def test_scheduler_skips_fresh_domains(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
        sample_fresh_domain: Domain,
    ) -> None:
        """Test that scheduler skips domains that are not yet due for refresh."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        await scheduler._tick()

        async with session_maker() as session:
            task_repo = TaskRepository(session)
            tasks = await task_repo.get_by_job_id(sample_fresh_domain.id)
            refresh_tasks = [t for t in tasks if t.type == TaskType.REFRESH]
            assert len(refresh_tasks) == 0

    async def test_scheduler_prevents_duplicate_refresh_tasks(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
        sample_active_domain: Domain,
    ) -> None:
        """Test that scheduler doesn't create duplicate REFRESH tasks."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        # Run first tick
        await scheduler._tick()
        # Run second tick immediately
        await scheduler._tick()

        async with session_maker() as session:
            task_repo = TaskRepository(session)
            stmt = select(Task).where(
                Task.domain_id == sample_active_domain.id,
                Task.type == TaskType.REFRESH,
            )
            result = await session.execute(stmt)
            refresh_tasks = list(result.scalars().all())
            # Should only have one REFRESH task (the first one is still PENDING)
            assert len(refresh_tasks) == 1

    async def test_scheduler_respects_backpressure(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        sample_active_domain: Domain,
    ) -> None:
        """Test that scheduler respects worker pool backpressure."""
        # Create mock worker pool with zero capacity
        mock_pool = MagicMock(spec=WorkerPool)
        mock_pool.available_capacity = 0
        mock_pool.queue = MagicMock()
        mock_pool.queue.size = 20
        mock_pool.queue.capacity = 20

        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_pool,
        )

        await scheduler._tick()

        async with session_maker() as session:
            task_repo = TaskRepository(session)
            stmt = select(Task).where(
                Task.domain_id == sample_active_domain.id,
                Task.type == TaskType.REFRESH,
            )
            result = await session.execute(stmt)
            refresh_tasks = list(result.scalars().all())
            assert len(refresh_tasks) == 0

    async def test_scheduler_handles_empty_candidate_set(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
    ) -> None:
        """Test that scheduler handles empty candidate set correctly."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        # No domains in database
        await scheduler._tick()

        async with session_maker() as session:
            task_repo = TaskRepository(session)
            # Should not create any tasks
            stmt = select(Task).where(Task.type == TaskType.REFRESH)
            result = await session.execute(stmt)
            tasks = list(result.scalars().all())
            assert len(tasks) == 0

    async def test_scheduler_handles_database_error(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
    ) -> None:
        """Test that scheduler handles database errors gracefully."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        # Mock session to raise an error
        with patch.object(
            session_maker, "__call__", side_effect=Exception("DB error")
        ):
            await scheduler._tick()
            # Should not crash, error should be logged

    async def test_scheduler_cancellation_is_clean(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
    ) -> None:
        """Test that scheduler shutdown/cancellation is clean."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        await scheduler.start()
        task = scheduler._task
        assert task is not None

        await scheduler.stop()
        assert scheduler._running is False
        # Task should be cancelled
        assert task.cancelled() or task.done()

    async def test_scheduler_multiple_ticks_behave_correctly(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        mock_worker_pool: MagicMock,
        sample_active_domain: Domain,
    ) -> None:
        """Test that repeated scheduler ticks behave correctly."""
        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        # Run multiple ticks
        for _ in range(3):
            await scheduler._tick()

        async with session_maker() as session:
            task_repo = TaskRepository(session)
            stmt = select(Task).where(
                Task.domain_id == sample_active_domain.id,
                Task.type == TaskType.REFRESH,
            )
            result = await session.execute(stmt)
            refresh_tasks = list(result.scalars().all())
            # Should still only have one REFRESH task (no duplicates)
            assert len(refresh_tasks) == 1

    async def test_scheduler_respects_batch_limit(
        self,
        async_db_session: AsyncSession,
        settings: AppSettings,
        mock_worker_pool: MagicMock,
    ) -> None:
        """Test that scheduler respects the batch size limit."""
        now = datetime.now(UTC)
        # Create multiple stale domains
        domains = []
        for i in range(15):  # More than worker_concurrency (10)
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"batch{i}.example.com",
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            detail = DomainDetail(
                domain_id=domain.id,
                ip_addresses=["93.184.216.34"],
                dns_records={"A": ["93.184.216.34"]},
                http_status=200,
                page_title=f"Batch Domain {i}",
                response_time=100,
                response_headers={},
                fetched_at=now - timedelta(hours=24),
                next_refresh_at=now - timedelta(hours=1),
                version=1,
            )
            async_db_session.add(detail)
            await async_db_session.commit()
            domains.append(domain)

        # Get session maker from the async_db_session's bind
        session_maker = async_sessionmaker(
            bind=async_db_session.bind, class_=AsyncSession, expire_on_commit=False
        )

        scheduler = RefreshScheduler(
            session_maker=session_maker,
            settings=settings,
            worker_pool=mock_worker_pool,
        )

        await scheduler._tick()

        async with session_maker() as session:
            task_repo = TaskRepository(session)
            stmt = select(Task).where(Task.type == TaskType.REFRESH)
            result = await session.execute(stmt)
            tasks = list(result.scalars().all())
            # Should be limited by worker_concurrency (10)
            assert len(tasks) <= settings.worker_concurrency

    async def test_domain_detail_repo_get_domains_needing_refresh(
        self,
        async_db_session: AsyncSession,
        sample_active_domain: Domain,
        sample_inactive_domain: Domain,
        sample_fresh_domain: Domain,
    ) -> None:
        """Test DomainDetailRepository.get_domains_needing_refresh method."""
        repo = DomainDetailRepository(async_db_session)
        now = datetime.now(UTC)

        domains = await repo.get_domains_needing_refresh(limit=10, now=now)

        # Should only return the active stale domain
        assert len(domains) == 1
        assert domains[0][0] == sample_active_domain.id
        assert domains[0][1] == sample_active_domain.normalized_domain

    async def test_task_repo_create_refresh_tasks(
        self,
        async_db_session: AsyncSession,
        sample_active_domain: Domain,
    ) -> None:
        """Test TaskRepository.create_refresh_tasks method."""
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)

        # Create refresh tasks
        created = await repo.create_refresh_tasks(
            domain_ids=[sample_active_domain.id],
            now=now,
        )

        assert len(created) == 1
        task = created[0]
        assert task.domain_id == sample_active_domain.id
        assert task.type == TaskType.REFRESH
        assert task.status == TaskStatus.PENDING
        assert task.job_id is None
        assert task.attempts == 0

    async def test_task_repo_create_refresh_tasks_skips_existing(
        self,
        async_db_session: AsyncSession,
        sample_active_domain: Domain,
    ) -> None:
        """Test that create_refresh_tasks skips domains with existing refresh tasks."""
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)

        # Create an existing PENDING refresh task
        existing_task = Task(
            id=uuid.uuid4(),
            job_id=None,
            domain_id=sample_active_domain.id,
            type=TaskType.REFRESH,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            lease_expires_at=None,
            error_payload=None,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(existing_task)
        await async_db_session.commit()

        # Try to create refresh tasks for the same domain
        created = await repo.create_refresh_tasks(
            domain_ids=[sample_active_domain.id],
            now=now,
        )

        # Should skip because there's already a PENDING refresh task
        assert len(created) == 0

    async def test_task_repo_get_pending_refresh_task_for_domain(
        self,
        async_db_session: AsyncSession,
        sample_active_domain: Domain,
    ) -> None:
        """Test TaskRepository.get_pending_refresh_task_for_domain method."""
        repo = TaskRepository(async_db_session)
        now = datetime.now(UTC)

        # No existing task
        task = await repo.get_pending_refresh_task_for_domain(sample_active_domain.id)
        assert task is None

        # Create a PENDING refresh task
        existing_task = Task(
            id=uuid.uuid4(),
            job_id=None,
            domain_id=sample_active_domain.id,
            type=TaskType.REFRESH,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=now,
            lease_expires_at=None,
            error_payload=None,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(existing_task)
        await async_db_session.commit()

        task = await repo.get_pending_refresh_task_for_domain(sample_active_domain.id)
        assert task is not None
        assert task.id == existing_task.id

        # Test with PROCESSING task
        existing_task.status = TaskStatus.PROCESSING
        await async_db_session.commit()

        task = await repo.get_pending_refresh_task_for_domain(sample_active_domain.id)
        assert task is not None
        assert task.status == TaskStatus.PROCESSING

        # Test with COMPLETED task (should not be found)
        existing_task.status = TaskStatus.COMPLETED
        await async_db_session.commit()

        task = await repo.get_pending_refresh_task_for_domain(sample_active_domain.id)
        assert task is None


# Need to import select for the test
from sqlalchemy import select
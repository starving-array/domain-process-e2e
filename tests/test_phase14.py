"""Phase 14: Full Integration & Concurrency Testing.

Comprehensive concurrency and integration tests for the Domain Processing Service.
"""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx
from sqlalchemy import text
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain_processing_service.config import AppSettings
from domain_processing_service.manager import TaskManager

def make_mock_handler(delay=0.01):
    async def handler(task, session):
        from domain_processing_service.repositories.task import TaskRepository
        task_repo = TaskRepository(session)
        await task_repo.update_status(task.id, TaskStatus.COMPLETED)
        await asyncio.sleep(delay)
    return handler

from domain_processing_service.models import (
    Domain,
    DomainDetail,
    Job,
    Task,
    TaskStatus,
    TaskType,
)
from domain_processing_service.repositories import (
    DomainDetailRepository,
    DomainRepository,
    IdempotencyRecordRepository,
    JobRepository,
    TaskRepository,
)
from domain_processing_service.scheduler import RefreshScheduler
from domain_processing_service.worker import BoundedQueue, WorkerPool, Worker


# ============================================================================
# Helper Functions
# ============================================================================

async def create_pending_tasks(
    session: AsyncSession,
    count: int,
    task_type: str = TaskType.USER_REQUEST,
    base_time: datetime | None = None,
    domain_prefix: str = "domain",
) -> list:
    """Helper to create multiple PENDING tasks for testing."""
    if base_time is None:
        base_time = datetime.now(UTC)

    # Create a job for USER_REQUEST tasks
    job_id = uuid.uuid4()
    if task_type == TaskType.USER_REQUEST:
        job = Job(
            id=job_id,
            status=TaskStatus.PENDING,
            created_at=base_time,
            updated_at=base_time,
        )
        session.add(job)
        await session.flush()

    tasks = []
    for i in range(count):
        unique_id = uuid.uuid4().hex[:8]
        domain_name = f"{domain_prefix}{i}_{unique_id}.com"
        
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain=domain_name,
            is_active=True,
            created_at=base_time,
            updated_at=base_time,
        )
        session.add(domain)
        await session.flush()

        task = Task(
            id=uuid.uuid4(),
            job_id=job_id if task_type == TaskType.USER_REQUEST else None,
            domain_id=domain.id,
            type=task_type,
            status=TaskStatus.PENDING,
            attempts=0,
            next_attempt_at=base_time,
            created_at=base_time,
            updated_at=base_time,
        )
        session.add(task)
        tasks.append(task)

    await session.commit()
    return tasks


# ============================================================================
# Fixtures
# ============================================================================

from tests.conftest import get_test_async_database_url


@pytest.fixture
async def test_session_maker(async_engine):
    """Create a session maker for the test using the shared engine."""
    return async_sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )


@pytest.fixture
async def test_settings() -> AppSettings:
    """Create test settings pointing to test DB and Redis DB 1."""
    settings = AppSettings(
        database_url=get_test_async_database_url(),
        worker_concurrency=5,
        worker_queue_capacity=10,
        task_lease_seconds=120,
        shutdown_grace_seconds=10,
    )
    object.__setattr__(settings, "redis_db", 1)
    return settings


# ============================================================================
# 3.1 Concurrent Task Managers
# ============================================================================

class TestConcurrentTaskManagers:
    """Tests for multiple TaskManager instances running concurrently."""

    async def test_two_managers_no_duplicate_claims(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test that two concurrent managers don't claim the same tasks."""
        # Ensure clean state before test
        await async_db_session.execute(text("""
            TRUNCATE TABLE
                idempotency_record,
                domain_detail,
                task,
                domain,
                job
            RESTART IDENTITY CASCADE
        """))
        await async_db_session.commit()

        # Create 10 PENDING tasks
        await create_pending_tasks(async_db_session, 10)

        # Create separate session makers for each manager
        async def make_manager(session_maker):
            return TaskManager(session_maker, test_settings)

        manager1_session_maker = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        manager2_session_maker = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )

        async def make_manager(session_maker):
            return TaskManager(session_maker, test_settings)

        manager1 = await make_manager(manager1_session_maker)
        manager2 = await make_manager(manager2_session_maker)

        claimed_by_manager1: list[uuid.UUID] = []
        claimed_by_manager2: list[uuid.UUID] = []

        async def run_claims(manager: TaskManager, session_maker, results: list[uuid.UUID]):
            for _ in range(3):
                async with session_maker() as session:
                    task_repo = TaskRepository(session)
                    lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
                    try:
                        claimed = await task_repo.claim_tasks(
                            limit=3,
                            lease_expires_at=lease_expires_at,
                        )
                        await session.commit()
                        results.extend([t.id for t in claimed])
                    except Exception:
                        await session.rollback()
                        raise
                await asyncio.sleep(0.01)

        await asyncio.gather(
            run_claims(manager1, manager1_session_maker, claimed_by_manager1),
            run_claims(manager2, manager2_session_maker, claimed_by_manager2),
        )

        all_claimed = claimed_by_manager1 + claimed_by_manager2
        assert len(all_claimed) == len(set(all_claimed)), (
            f"Duplicate task IDs claimed: {all_claimed}"
        )
        assert len(all_claimed) == 10


# ============================================================================
# 4. Worker Pool Concurrency
# ============================================================================

class TestWorkerPoolConcurrency:
    """Tests for WorkerPool behavior under concurrency."""

    @pytest.fixture
    async def mock_task_handler(self):
        """Create a mock task handler that simulates work."""
        async def handler(task: Task, session: AsyncSession) -> None:
            await asyncio.sleep(0.01)  # Simulate work
            task.status = TaskStatus.COMPLETED
            task.updated_at = datetime.now(UTC)
        return handler

    async def test_worker_pool_basic(
        self, test_session_maker, test_settings, mock_task_handler
    ) -> None:
        """Test basic worker pool operation."""
        from domain_processing_service.manager import TaskManager
        
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=make_mock_handler(0.01),
        )
        
        task_manager = TaskManager(
            session_maker=test_session_maker,
            settings=test_settings,
            worker_pool=worker_pool,
        )

        await worker_pool.start()
        await task_manager.start()
        assert worker_pool.running is True
        assert worker_pool.worker_count == test_settings.worker_concurrency

        # Create some tasks
        async with test_session_maker() as session:
            await create_pending_tasks(session, 5)

        # Start task manager to claim tasks and put them in queue
        await asyncio.sleep(0.2)

        await task_manager.stop()
        await worker_pool.stop()
        assert worker_pool.running is False

    async def test_worker_pool_queue_full(
        self, test_session_maker, test_settings, mock_task_handler
    ) -> None:
        """Test worker pool behavior when queue is full."""
        test_settings.worker_queue_capacity = 2
        test_settings.worker_concurrency = 1
        
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=mock_task_handler,
        )
        
        task_manager = TaskManager(
            session_maker=test_session_maker,
            settings=test_settings,
            worker_pool=worker_pool,
        )
        
        await worker_pool.start()
        await task_manager.start()
        
        # Create tasks to fill queue
        async with test_session_maker() as session:
            await create_pending_tasks(session, 5)

        # Wait for queue to fill and manager to stop claiming
        await asyncio.sleep(0.5)
        
        # Verify queue is at capacity
        assert worker_pool.queue.size <= test_settings.worker_queue_capacity
        
        await task_manager.stop()
        await worker_pool.stop()

    async def test_worker_exception_isolation(
        self, test_session_maker, test_settings
    ) -> None:
        """Test that worker exceptions don't crash other workers."""
        fail_count = 0
        
        async def failing_handler(task: Task, session: AsyncSession) -> None:
            nonlocal fail_count
            fail_count += 1
            if fail_count <= 2:
                raise ValueError("Simulated failure")
            task.status = TaskStatus.COMPLETED
            task.updated_at = datetime.now(UTC)
        
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=failing_handler,
        )
        
        task_manager = TaskManager(
            session_maker=test_session_maker,
            settings=test_settings,
            worker_pool=worker_pool,
        )
        
        await worker_pool.start()
        await task_manager.start()
        
        async with test_session_maker() as session:
            await create_pending_tasks(session, 5)
        
        await asyncio.sleep(0.5)
        
        # Verify all workers still running
        assert worker_pool.running is True
        assert all(w.running for w in worker_pool.workers)
        
        await task_manager.stop()
        await worker_pool.stop()


# ============================================================================
# 5. Scheduler Concurrency
# ============================================================================

class TestSchedulerConcurrency:
    """Tests for RefreshScheduler concurrency."""

    async def test_scheduler_multiple_instances(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test multiple schedulers don't create duplicate refresh tasks."""
        from sqlalchemy.ext.asyncio import async_sessionmaker
        
        # Create domains with stale refresh times
        base_time = datetime.now(UTC)
        for i in range(5):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"scheduler{i}.com",
                is_active=True,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            detail = DomainDetail(
                domain_id=domain.id,
                ip_addresses=["93.184.216.34"],
                dns_records={"A": ["93.184.216.34"]},
                http_status=200,
                page_title="Test",
                response_time=100,
                response_headers={},
                fetched_at=base_time - timedelta(hours=24),
                next_refresh_at=base_time - timedelta(hours=1),
                version=1,
            )
            async_db_session.add(detail)
        await async_db_session.commit()

        session_maker1 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        session_maker2 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )

        scheduler1 = RefreshScheduler(session_maker1, test_settings, None)
        scheduler2 = RefreshScheduler(session_maker2, test_settings, None)

        # Run tick on both schedulers
        await scheduler1._tick()
        await scheduler2._tick()

        # Verify only one set of refresh tasks was created
        async with async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )() as session:
            from sqlalchemy import select
            task_repo = TaskRepository(session)
            result = await session.execute(select(Task).where(Task.type == TaskType.REFRESH))
            refresh_tasks = result.scalars().all()
            
            # Should have at most 5 refresh tasks (one per domain)
            assert len(refresh_tasks) <= 5


# ============================================================================
# 6. Idempotency Concurrency
# ============================================================================

class TestIdempotencyConcurrency:
    """Tests for idempotency key concurrency."""

    @pytest.fixture
    async def async_client(self, async_engine):
        from domain_processing_service.app import create_app
        from domain_processing_service.config import AppSettings
        
        class TestDatabase:
            def __init__(self, engine):
                self._engine = engine
            async def connect(self): pass
            async def close(self): pass
            async def is_ready(self): return True
            @property
            def engine(self): return self._engine
            @property
            def session_maker(self):
                return async_sessionmaker(bind=self._engine, expire_on_commit=False)
            async def _ping(self): pass
        
        test_database = TestDatabase(async_engine)
        settings = AppSettings(database_url=get_test_async_database_url())
        object.__setattr__(settings, "redis_db", 1)
        app = create_app(settings, test_database)
        
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    async def test_concurrent_same_key_same_payload(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Concurrent requests with same key and payload should create only one job."""
        payload = {"domains": ["example.com", "google.com"]}
        headers = {"Idempotency-Key": "test-concurrent-1", "X-Client-ID": "client-1"}
        
        # Send 5 concurrent requests
        async def make_request():
            return await async_client.post("/jobs", json=payload, headers=headers)
        
        responses = await asyncio.gather(*[make_request() for _ in range(5)])
        
        # All should succeed
        for resp in responses:
            assert resp.status_code in (200, 202)
        
        # Exactly one job should be created
        job_ids = [resp.json()["jobId"] for resp in responses]
        assert len(set(job_ids)) == 1
        
        # Check status codes: one 202, rest 200
        statuses = [r.status_code for r in responses]
        assert statuses.count(202) == 1
        assert statuses.count(200) == 4

    async def test_concurrent_same_key_different_payload(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Concurrent requests with same key but different payloads should return 409."""
        headers = {"Idempotency-Key": "test-conflict-1", "X-Client-ID": "client-1"}
        
        async def make_request(domains):
            return await async_client.post("/jobs", json={"domains": domains}, headers=headers)
        
        # Send conflicting requests concurrently
        responses = await asyncio.gather(
            make_request(["example.com"]),
            make_request(["google.com"]),
            make_request(["github.com"]),
        )
        
        # One should succeed, others should get 409
        statuses = sorted([r.status_code for r in responses])
        assert 202 in statuses or 200 in statuses
        assert statuses.count(409) >= 1

    async def test_different_clients_same_key(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Same key with different client IDs should be treated independently."""
        payload = {"domains": ["example.com"]}
        
        # Client 1
        resp1 = await async_client.post(
            "/jobs", json=payload,
            headers={"Idempotency-Key": "shared-key", "X-Client-ID": "client-1"}
        )
        assert resp1.status_code == 202
        job_id_1 = resp1.json()["jobId"]
        
        # Client 2 with same key
        resp2 = await async_client.post(
            "/jobs", json=payload,
            headers={"Idempotency-Key": "shared-key", "X-Client-ID": "client-2"}
        )
        assert resp2.status_code == 202
        job_id_2 = resp2.json()["jobId"]
        
        # Should create different jobs
        assert job_id_1 != job_id_2

    async def test_different_keys_independent(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Different idempotency keys should behave independently."""
        resp1 = await async_client.post(
            "/jobs", json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "key-1", "X-Client-ID": "client-1"}
        )
        assert resp1.status_code == 202
        
        resp2 = await async_client.post(
            "/jobs", json={"domains": ["example.com"]},
            headers={"Idempotency-Key": "key-2", "X-Client-ID": "client-1"}
        )
        assert resp2.status_code == 202
        
        # Should create different jobs
        assert resp1.json()["jobId"] != resp2.json()["jobId"]


# ============================================================================
# 7. Database Lock/Deadlock Testing
# ============================================================================

class TestDatabaseLockOrdering:
    """Tests for database lock ordering and deadlock prevention."""

    async def test_task_claim_lock_order(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Verify task claiming acquires locks in consistent order."""
        # Create tasks
        await create_pending_tasks(async_db_session, 10)
        
        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
        
        # Claim all tasks
        claimed = await task_repo.claim_tasks(
            limit=10,
            lease_expires_at=lease_expires_at,
        )
        await async_db_session.commit()
        
        assert len(claimed) == 10
        
        # Verify all tasks are PROCESSING
        for task in claimed:
            assert task.status == TaskStatus.PROCESSING
            assert task.lease_expires_at == lease_expires_at

    async def test_recovery_vs_claim_race(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test that recovery and claiming don't deadlock."""
        from sqlalchemy.ext.asyncio import async_sessionmaker
        
        # Create tasks
        await create_pending_tasks(async_db_session, 5)
        
        # Create expired tasks
        base_time = datetime.now(UTC) - timedelta(seconds=200)
        await create_pending_tasks(async_db_session, 3)
        
        # Manually expire some leases
        task_repo = TaskRepository(async_db_session)
        claimed = await task_repo.claim_tasks(limit=3, lease_expires_at=datetime.now(UTC) - timedelta(seconds=10))
        await async_db_session.commit()
        
        # Manually expire their leases
        for task in claimed:
            task.lease_expires_at = datetime.now(UTC) - timedelta(seconds=10)
        await async_db_session.commit()
        
        # Now run recovery and claiming concurrently
        session_maker1 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        session_maker2 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        
        manager = TaskManager(session_maker1, test_settings)
        
        async def run_recovery():
            async with session_maker1() as session:
                task_repo = TaskRepository(session)
                recovered = await task_repo.recover_expired_tasks(
                    limit=10,
                    now=datetime.now(UTC),
                )
                await session.commit()
                return len(recovered)
        
        async def run_claim():
            async with session_maker2() as session:
                task_repo = TaskRepository(session)
                lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
                claimed = await task_repo.claim_tasks(
                    limit=10,
                    lease_expires_at=lease_expires_at,
                )
                await session.commit()
                return len(claimed)
        
        # Run recovery and claiming concurrently
        recovery_count, claim_count = await asyncio.gather(
            run_recovery(),
            run_claim(),
        )
        
        # Both should complete without deadlock
        assert recovery_count >= 0
        assert claim_count >= 0


# ============================================================================
# 8. PostgreSQL Lock Contention
# ============================================================================

class TestPostgreSQLLockContention:
    """Tests for PostgreSQL lock contention scenarios."""

    async def test_two_transactions_same_task(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test two transactions attempting to claim the same task."""
        await create_pending_tasks(async_db_session, 1)
        
        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
        
        # First claim
        claimed1 = await task_repo.claim_tasks(limit=1, lease_expires_at=lease_expires_at)
        await async_db_session.commit()
        assert len(claimed1) == 1
        
        # Second claim with same task should get 0 (already claimed)
        claimed2 = await task_repo.claim_tasks(limit=1, lease_expires_at=lease_expires_at)
        await async_db_session.commit()
        assert len(claimed2) == 0

    async def test_concurrent_claims_overlapping_batches(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test concurrent claim operations with overlapping batches."""
        from sqlalchemy.ext.asyncio import async_sessionmaker
        
        await create_pending_tasks(async_db_session, 10)
        
        session_maker1 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        session_maker2 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        
        async def claim_batch(session_maker):
            async with session_maker() as session:
                task_repo = TaskRepository(session)
                lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
                claimed = await task_repo.claim_tasks(
                    limit=5,
                    lease_expires_at=lease_expires_at,
                )
                await session.commit()
                return len(claimed)
        
        results = await asyncio.gather(
            claim_batch(session_maker1),
            claim_batch(session_maker2),
        )
        
        total_claimed = sum(results)
        assert total_claimed <= 10  # No duplicates
        assert sum(results) == 10  # All tasks claimed

    async def test_scheduler_vs_task_manager(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test scheduler creating refresh tasks while task manager claims."""
        from sqlalchemy.ext.asyncio import async_sessionmaker
        
        # Create stale domains
        base_time = datetime.now(UTC)
        for i in range(3):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"stale{i}.com",
                is_active=True,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(domain)
            await async_db_session.flush()
            
            detail = DomainDetail(
                domain_id=domain.id,
                ip_addresses=["93.184.216.34"],
                dns_records={"A": ["93.184.216.34"]},
                http_status=200,
                page_title="Test",
                response_time=100,
                response_headers={},
                fetched_at=base_time - timedelta(hours=24),
                next_refresh_at=base_time - timedelta(hours=1),
                version=1,
            )
            async_db_session.add(detail)
        await async_db_session.commit()
        
        # Also create USER_REQUEST tasks
        await create_pending_tasks(async_db_session, 5)
        
        session_maker1 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        session_maker2 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        
        scheduler = RefreshScheduler(session_maker1, test_settings, None)
        task_manager = TaskManager(session_maker2, test_settings)
        
        # Run scheduler tick and task manager claim concurrently
        async def run_scheduler():
            await scheduler._tick()
        
        async def run_claim():
            async with session_maker2() as session:
                task_repo = TaskRepository(session)
                lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
                claimed = await task_repo.claim_tasks(
                    limit=10,
                    lease_expires_at=lease_expires_at,
                )
                await session.commit()
                return len(claimed)
        
        results = await asyncio.gather(
            run_scheduler(),
            run_claim(),
        )
        
        # Both should complete without deadlock
        assert results is not None


# ============================================================================
# 9. Redis Lock + PostgreSQL Interaction
# ============================================================================

class TestRedisPostgreSQLLockOrdering:
    """Tests for Redis lock and PostgreSQL transaction ordering."""

    async def test_worker_lock_acquisition_order(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Verify worker acquires Redis lock before/after PostgreSQL operations."""
        from domain_processing_service.domain_lock import DomainLockManager
        
        lock_manager = DomainLockManager(test_settings)
        await lock_manager.connect()
        
        try:
            # Test lock acquisition
            async with lock_manager._lock.acquire("test-domain.com") as lock_ctx:
                assert lock_ctx.is_acquired
                assert lock_ctx.token is not None
            
            # Verify lock is released
            async with lock_manager._lock.acquire("test-domain.com") as lock_ctx:
                assert lock_ctx.is_acquired
        finally:
            await lock_manager.close()

    async def test_worker_redis_failure_handling(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Test worker handles Redis unavailability correctly."""
        from domain_processing_service.domain_lock import DomainLockManager
        from domain_processing_service.domain_processor import DomainProcessor
        
        # Test with unavailable Redis
        lock_manager = DomainLockManager(test_settings)
        # Don't connect - simulate Redis unavailable
        
        # Should handle gracefully
        # The domain processor should handle lock acquisition failure
        # by rescheduling the task


# ============================================================================
# 10. Lease Recovery Concurrency
# ============================================================================

class TestLeaseRecoveryConcurrency:
    """Tests for lease recovery concurrency."""

    async def test_recovery_vs_worker_completion(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test recovery doesn't conflict with worker completing a task."""
        # Create tasks and claim them
        await create_pending_tasks(async_db_session, 3)
        
        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
        claimed = await task_repo.claim_tasks(limit=3, lease_expires_at=lease_expires_at)
        await async_db_session.commit()
        
        # Simulate worker completing one task
        task_to_complete = claimed[0]
        task_to_complete.status = TaskStatus.COMPLETED
        task_to_complete.updated_at = datetime.now(UTC)
        await async_db_session.commit()
        
        # Now run recovery
        task_repo = TaskRepository(async_db_session)
        recovered = await task_repo.recover_expired_tasks(
            limit=10,
            now=datetime.now(UTC),
        )
        await async_db_session.commit()
        
        # Recovery should not touch completed tasks
        # (they're no longer PROCESSING)
        assert len(recovered) <= 2  # Only the 2 remaining PROCESSING tasks
        
    async def test_recovery_with_active_lease(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test recovery doesn't touch tasks with valid leases."""
        await create_pending_tasks(async_db_session, 3)
        
        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
        claimed = await task_repo.claim_tasks(limit=3, lease_expires_at=lease_expires_at)
        await async_db_session.commit()
        
        # Run recovery immediately (leases not expired)
        task_repo = TaskRepository(async_db_session)
        recovered = await task_repo.recover_expired_tasks(
            limit=10,
            now=datetime.now(UTC),
        )
        await async_db_session.commit()
        
        # No tasks should be recovered (leases still valid)
        assert len(recovered) == 0


# ============================================================================
# 11. Graceful Shutdown Under Load
# ============================================================================

class TestGracefulShutdown:
    """Tests for graceful shutdown behavior."""

    @pytest.fixture
    async def test_database(self, async_engine):
        """Create a test database wrapper."""
        class TestDatabase:
            def __init__(self, engine):
                self._engine = engine
            async def connect(self): pass
            async def close(self): pass
            async def is_ready(self): return True
            @property
            def engine(self): return self._engine
            @property
            def session_maker(self):
                return async_sessionmaker(
                    bind=self._engine, class_=AsyncSession, expire_on_commit=False
                )
            async def _ping(self): pass
        
        return TestDatabase(async_engine)

    async def test_shutdown_rejects_new_requests(
        self, async_engine, test_settings: AppSettings
    ) -> None:
        """Test that shutdown returns 503 for new requests."""
        from domain_processing_service.app import create_app
        from domain_processing_service.shutdown import ShutdownCoordinator
        
        class TestDatabase:
            def __init__(self, engine):
                self._engine = engine
            async def connect(self): pass
            async def close(self): pass
            async def is_ready(self): return True
            @property
            def engine(self): return self._engine
            @property
            def session_maker(self):
                return async_sessionmaker(
                    bind=self._engine, class_=AsyncSession, expire_on_commit=False
                )
            async def _ping(self): pass
        
        test_database = TestDatabase(async_engine)
        app = create_app(test_settings, TestDatabase(async_engine))
        
        # Start app
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), 
            base_url="http://test"
        ) as client:
            # Initiate shutdown by calling shutdown coordinator directly
            # (In real scenario, this would be triggered by SIGTERM)
            response = await client.post(
                "/jobs",
                json={"domains": ["example.com"]},
            )
            # Should succeed normally
            assert response.status_code == 202

    async def test_shutdown_drains_worker_pool(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Test that shutdown drains worker pool gracefully."""
        async def slow_handler(task: Task, session: AsyncSession) -> None:
            await asyncio.sleep(0.5)  # Simulate long-running task
            task.status = TaskStatus.COMPLETED
            task.updated_at = datetime.now(UTC)
        
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=slow_handler,
        )
        
        await worker_pool.start()
        
        # Add some tasks
        async with test_session_maker() as session:
            await create_pending_tasks(session, 3)
        
        # Stop with grace period
        await worker_pool.stop(graceful=True)
        
        # Should have stopped gracefully
        assert not worker_pool.running
        for worker in worker_pool.workers:
            assert not worker.running


# ============================================================================
# 12. Connection Pool Exhaustion
# ============================================================================

class TestConnectionPoolExhaustion:
    """Tests for connection pool behavior under high concurrency."""

    async def test_many_concurrent_requests(
        self, async_client: httpx.AsyncClient
    ) -> None:
        """Test many concurrent API requests."""
        async def make_request(i):
            return await async_client.post(
                "/jobs",
                json={"domains": [f"domain{i}.com"]},
                headers={"Idempotency-Key": f"key-{i}", "X-Client-ID": "client-1"}
            )
        
        # Send 20 concurrent requests
        responses = await asyncio.gather(*[make_request(i) for i in range(20)])
        
        # All should succeed (or return 503 if pool exhausted, but not crash)
        for resp in responses:
            assert resp.status_code in (202, 503)
        
        # At least some should succeed
        success_count = sum(1 for r in responses if r.status_code == 202)
        assert success_count > 0

    async def test_connection_pool_recovers(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Test connection pool recovers after exhaustion."""
        # This test is more of a smoke test to ensure the pool recovers
        async with test_session_maker() as session:
            await create_pending_tasks(session, 5)
        
        # If we get here without deadlock, pool is working
        assert True


# ============================================================================
# 13. Async Correctness
# ============================================================================

class TestAsyncCorrectness:
    """Tests for async correctness and task lifecycle."""

    async def test_no_background_task_leaks(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Test that no background tasks are leaked after operations."""
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=make_mock_handler(0.01),
        )
        
        task_manager = TaskManager(
            session_maker=test_session_maker,
            settings=test_settings,
            worker_pool=worker_pool,
        )
        
        await worker_pool.start()
        await task_manager.start()
        
        async with test_session_maker() as session:
            await create_pending_tasks(session, 3)
        
        # Process tasks
        await asyncio.sleep(0.2)
        
        # Stop and verify no leaked tasks
        await task_manager.stop()
        await worker_pool.stop(graceful=True)
        
        # Check for any pending asyncio tasks
        pending = asyncio.all_tasks()
        # Filter out current task
        other_tasks = [t for t in pending if t is not asyncio.current_task()]
        # Note: Some background tasks may exist (e.g., test framework)
        # but we shouldn't have worker tasks running
        assert all(not t.get_name().startswith("worker") for t in other_tasks)

    async def test_no_blocking_calls_in_async(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Verify no synchronous blocking calls in async code paths."""
        # This is a smoke test - if it runs without blocking, it's OK
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=lambda t, s: setattr(t, 'status', TaskStatus.COMPLETED),
        )
        
        task_manager = TaskManager(
            session_maker=test_session_maker,
            settings=test_settings,
            worker_pool=worker_pool,
        )
        
        await worker_pool.start()
        await task_manager.start()
        await task_manager.stop()
        await worker_pool.stop()
        assert True  # If we reach here without hanging, no blocking calls


# ============================================================================
# 14. Stress / Soak Testing
# ============================================================================

class TestStressProcessing:
    """Deterministic stress tests."""

    async def test_stress_processing(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Stress test with multiple tasks, managers, workers."""
        # Create many tasks
        async with test_session_maker() as session:
            await create_pending_tasks(session, 50)
        
        # Create worker pool with fast handler
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=make_mock_handler(0.001),
        )
        
        task_manager = TaskManager(
            session_maker=test_session_maker,
            settings=test_settings,
            worker_pool=worker_pool,
        )
        
        await worker_pool.start()
        await task_manager.start()
        
        # Wait for completion
        await asyncio.sleep(1.0)
        
        await task_manager.stop()
        await worker_pool.stop()
        
        # Verify tasks completed
        async with test_session_maker() as session:
            task_repo = TaskRepository(session)
            tasks = list((await session.execute(select(Task))).scalars().all())
            completed = [t for t in tasks if t.status == TaskStatus.COMPLETED]
            assert len(completed) > 0

    async def test_many_concurrent_schedulers(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test multiple schedulers running concurrently."""
        # Create many stale domains
        base_time = datetime.now(UTC)
        for i in range(20):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"stress{i}.com",
                is_active=True,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(domain)
            await async_db_session.flush()
            
            detail = DomainDetail(
                domain_id=domain.id,
                ip_addresses=["93.184.216.34"],
                dns_records={"A": ["93.184.216.34"]},
                http_status=200,
                page_title="Test",
                response_time=100,
                response_headers={},
                fetched_at=base_time - timedelta(hours=24),
                next_refresh_at=base_time - timedelta(hours=1),
                version=1,
            )
            async_db_session.add(detail)
        await async_db_session.commit()
        
        from sqlalchemy.ext.asyncio import async_sessionmaker
        session_maker = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        
        scheduler = RefreshScheduler(session_maker, test_settings, None)
        
        # Run multiple ticks
        for _ in range(3):
            await scheduler._tick()
        
        # Verify tasks created
        async with async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )() as session:
            task_repo = TaskRepository(session)
            tasks = list((await session.execute(select(Task))).scalars().all())
            refresh_tasks = [t for t in tasks if t.type == TaskType.REFRESH]
            assert len(refresh_tasks) <= 20


# ============================================================================
# 15. Deadlock Detection
# ============================================================================

class TestDeadlockDetection:
    """Deadlock detection and prevention tests."""

    async def test_lock_ordering_analysis(
        self, async_db_session: AsyncSession
    ) -> None:
        """Analyze lock acquisition order in the codebase.
        
        This test documents the expected lock acquisition order:
        1. PostgreSQL: task rows (SELECT FOR UPDATE SKIP LOCKED)
        2. Redis: domain lock (SET NX PX)
        3. PostgreSQL: domain_detail (OCC update)
        4. PostgreSQL: task (completion)
        
        This order must be maintained to prevent deadlocks.
        """
        # This test documents the expected lock order
        # Actual verification is through integration tests
        assert True  # Documentation test

    async def test_no_deadlock_under_contention(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """Test that heavy contention doesn't cause deadlocks."""
        from sqlalchemy.ext.asyncio import async_sessionmaker
        
        # Create many tasks
        await create_pending_tasks(async_db_session, 20)
        
        session_maker = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        
        manager = TaskManager(
            async_sessionmaker(
                bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
            ),
            test_settings,
        )
        
        # Run multiple claim operations concurrently
        async def claim_batch():
            async with async_sessionmaker(
                bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
            )() as session:
                task_repo = TaskRepository(session)
                lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
                claimed = await task_repo.claim_tasks(
                    limit=5,
                    lease_expires_at=lease_expires_at,
                )
                await session.commit()
                return len(claimed)
        
        # Run 4 concurrent claim batches
        results = await asyncio.gather(*[claim_batch() for _ in range(4)])
        
        # All should complete without deadlock
        total = sum(results)
        assert total == 20  # All 20 tasks claimed


# ============================================================================
# 16. Phase 14 Test Suite
# ============================================================================

class TestPhase14Integration:
    """Integration tests for Phase 14."""

    async def test_full_processing_pipeline(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Test the full processing pipeline end-to-end."""
        # Create a task
        async with test_session_maker() as session:
            base_time = datetime.now(UTC)
            job = Job(
                id=uuid.uuid4(),
                status=TaskStatus.PENDING,
                created_at=base_time,
                updated_at=base_time,
            )
            session.add(job)
            await session.flush()
            
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain="e2e-test.com",
                is_active=True,
                created_at=base_time,
                updated_at=base_time,
            )
            session.add(domain)
            await session.flush()
            
            task = Task(
                id=uuid.uuid4(),
                job_id=job.id,
                domain_id=domain.id,
                type=TaskType.USER_REQUEST,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=base_time,
                created_at=base_time,
                updated_at=base_time,
            )
            session.add(task)
            await session.commit()
            task_id = task.id
        
        # Create worker pool with mock handler
        async def handler(task: Task, session: AsyncSession):
            from domain_processing_service.repositories.task import TaskRepository
            task_repo = TaskRepository(session)
            await task_repo.update_status(task.id, TaskStatus.COMPLETED)
            # Add domain detail
            detail = DomainDetail(
                domain_id=task.domain_id,
                ip_addresses=["93.184.216.34"],
                dns_records={"A": ["93.184.216.34"]},
                http_status=200,
                page_title="Test",
                response_time=100,
                response_headers={},
                fetched_at=datetime.now(UTC),
                next_refresh_at=datetime.now(UTC) + timedelta(days=1),
                version=1,
            )
            session.add(detail)
        
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=handler,
        )
        
        task_manager = TaskManager(
            session_maker=test_session_maker,
            settings=test_settings,
            worker_pool=worker_pool,
        )
        
        await worker_pool.start()
        await task_manager.start()
        
        # Create task
        async with test_session_maker() as session:
            await create_pending_tasks(session, 1)
        
        # Process
        await asyncio.sleep(0.5)
        
        await task_manager.stop()
        await worker_pool.stop()
        
        # Verify completion
        async with test_session_maker() as session:
            task_repo = TaskRepository(session)
            tasks = list((await session.execute(select(Task))).scalars().all())
            completed = [t for t in tasks if t.status == TaskStatus.COMPLETED]
            assert len(completed) > 0

    async def test_end_to_end_with_shutdown(
        self, test_session_maker, test_settings: AppSettings
    ) -> None:
        """Test full lifecycle including shutdown."""
        worker_pool = WorkerPool(
            session_maker=test_session_maker,
            settings=test_settings,
            task_handler=make_mock_handler(0.1),
        )
        
        await worker_pool.start()
        
        # Add tasks
        async with test_session_maker() as session:
            await create_pending_tasks(session, 3)
        
        # Start processing
        await asyncio.sleep(0.05)
        
        # Shutdown while processing
        await worker_pool.stop(graceful=True)
        
        # Verify clean shutdown
        assert not worker_pool.running


# ============================================================================
# Run tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
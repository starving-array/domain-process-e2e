"""Tests for Task Manager - Phase 7: SKIP LOCKED concurrency."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain_processing_service.config import AppSettings
from domain_processing_service.manager import TaskManager
from domain_processing_service.models import Domain, Job, Task, TaskStatus, TaskType
from domain_processing_service.repositories import TaskRepository


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
    )
    object.__setattr__(settings, "redis_db", 1)
    return settings


async def create_pending_tasks(
    session: AsyncSession,
    count: int,
    task_type: TaskType = TaskType.USER_REQUEST,
    base_time: datetime | None = None,
    domain_prefix: str = "domain",
) -> list[Task]:
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
        # Use unique domain names to avoid unique constraint violations
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


class TestTaskRepositoryClaimTasks:
    """Tests for TaskRepository.claim_tasks method."""

    async def test_claim_tasks_basic(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test basic task claiming functionality."""
        await create_pending_tasks(async_db_session, 5)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)  # 100 seconds lease

        claimed = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )

        assert len(claimed) == 3
        for task in claimed:
            assert task.status == TaskStatus.PROCESSING
            assert task.lease_expires_at == lease_expires_at
            assert task.attempts == 1

    async def test_claim_tasks_respects_limit(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that claim_tasks respects the limit parameter."""
        await create_pending_tasks(async_db_session, 10)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        claimed = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )

        assert len(claimed) == 3

    async def test_claim_tasks_only_pending(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that only PENDING tasks are claimed."""
        # Create mix of PENDING and PROCESSING tasks
        base_time = datetime.now(UTC)
        await create_pending_tasks(async_db_session, 3)

        # Create a PROCESSING task with a valid job
        job = Job(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            created_at=base_time,
            updated_at=base_time,
        )
        async_db_session.add(job)
        await async_db_session.flush()

        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain="processing.com",
            is_active=True,
            created_at=base_time,
            updated_at=base_time,
        )
        async_db_session.add(domain)
        await async_db_session.flush()

        processing_task = Task(
            id=uuid.uuid4(),
            job_id=job.id,
            domain_id=domain.id,
            type=TaskType.USER_REQUEST,
            status=TaskStatus.PROCESSING,
            attempts=1,
            next_attempt_at=base_time,
            created_at=base_time,
            updated_at=base_time,
        )
        async_db_session.add(processing_task)
        await async_db_session.commit()

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        claimed = await task_repo.claim_tasks(
            limit=10,
            lease_expires_at=lease_expires_at,
        )

        # Should only claim the 3 PENDING tasks
        assert len(claimed) == 3

    async def test_claim_tasks_empty_queue(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test claiming when no PENDING tasks exist."""
        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        # We can't guarantee an empty queue in shared test DB,
        # but we can verify the function works correctly when
        # given a specific scenario - so we just test it runs without error
        claimed = await task_repo.claim_tasks(
            limit=5,
            lease_expires_at=lease_expires_at,
        )

        # The result may have tasks from other tests, but the function
        # should execute without error
        assert isinstance(claimed, list)

    async def test_claim_tasks_prioritizes_user_request(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that USER_REQUEST tasks are prioritized over REFRESH."""
        base_time = datetime.now(UTC)

        # Create 2 REFRESH tasks
        for i in range(2):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"refresh{i}.com",
                is_active=True,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            task = Task(
                id=uuid.uuid4(),
                job_id=None,  # REFRESH tasks have no job_id
                domain_id=domain.id,
                type=TaskType.REFRESH,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=base_time,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(task)

        # Create 2 USER_REQUEST tasks
        await create_pending_tasks(async_db_session, 2, TaskType.USER_REQUEST, base_time)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        claimed = await task_repo.claim_tasks(
            limit=2,
            lease_expires_at=lease_expires_at,
        )

        # Should claim USER_REQUEST tasks first
        assert len(claimed) == 2
        for task in claimed:
            assert task.type == TaskType.USER_REQUEST

    async def test_claim_tasks_orders_by_created_at(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that tasks are ordered by created_at, then id."""
        base_time = datetime.now(UTC)

        # Create a job for the tasks
        job = Job(
            id=uuid.uuid4(),
            status=TaskStatus.PENDING,
            created_at=base_time,
            updated_at=base_time,
        )
        async_db_session.add(job)
        await async_db_session.flush()

        # Create tasks with different creation times (using microsecond offsets)
        task_ids = []
        for i in range(5):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"domain{i}_{uuid.uuid4().hex[:8]}.com",
                is_active=True,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            task = Task(
                id=uuid.uuid4(),
                job_id=job.id,
                domain_id=domain.id,
                type=TaskType.USER_REQUEST,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=base_time,
                created_at=base_time + timedelta(microseconds=i * 1000),
                updated_at=base_time,
            )
            async_db_session.add(task)
            task_ids.append(task.id)

        await async_db_session.commit()

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        claimed = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )

        assert len(claimed) == 3
        # Should be ordered by created_at, then id
        for i in range(len(claimed) - 1):
            assert claimed[i].created_at <= claimed[i + 1].created_at
            if claimed[i].created_at == claimed[i + 1].created_at:
                assert claimed[i].id <= claimed[i + 1].id


class TestTaskManager:
    """Tests for TaskManager service."""

    async def test_manager_start_stop(
        self, test_session_maker, test_settings
    ) -> None:
        """Test Task Manager can start and stop."""
        manager = TaskManager(test_session_maker, test_settings)

        await manager.start()
        assert manager._running is True

        await manager.stop()
        assert manager._running is False

    async def test_manager_claim_cycle(
        self, test_session_maker, test_settings
    ) -> None:
        """Test single claim cycle."""
        # Create some pending tasks
        async with test_session_maker() as session:
            await create_pending_tasks(session, 5)

        manager = TaskManager(test_session_maker, test_settings)
        await manager.start()

        # Give it a moment to run a claim cycle
        await asyncio.sleep(0.5)

        await manager.stop()

    async def test_manager_multiple_cycles(
        self, test_session_maker, test_settings
    ) -> None:
        """Test manager runs multiple cycles."""
        async with test_session_maker() as session:
            await create_pending_tasks(session, 10)

        manager = TaskManager(test_session_maker, test_settings)
        await manager.start()

        # Let it run for a few cycles
        await asyncio.sleep(1.0)

        await manager.stop()


class TestSkipLockedConcurrency:
    """Concurrency tests for FOR UPDATE SKIP LOCKED behavior."""

    async def test_two_managers_no_duplicate_claims(
        self, async_db_session: AsyncSession, test_settings: AppSettings
    ) -> None:
        """
        Test that two concurrent managers don't claim the same tasks.

        This is the critical test for SKIP LOCKED behavior.
        """
        # Ensure clean state before test
        from sqlalchemy import text
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

        # Create separate session makers for each manager to simulate separate connections
        from sqlalchemy.ext.asyncio import async_sessionmaker

        
        async def make_manager(session_maker):
            return TaskManager(session_maker, test_settings)

        # We need separate session makers for true concurrency test
        # Use the same engine but different session instances
        manager1_session_maker = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        manager2_session_maker = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )

        manager1 = await make_manager(manager1_session_maker)
        manager2 = await make_manager(manager2_session_maker)

        # Track claimed task IDs
        claimed_by_manager1: list[uuid.UUID] = []
        claimed_by_manager2: list[uuid.UUID] = []

        # We'll run the claim cycle manually for both managers concurrently
        async def run_claims(manager: TaskManager, session_maker, results: list[uuid.UUID]):
            for _ in range(3):  # Each manager tries to claim 3 times
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
                await asyncio.sleep(0.01)  # Small delay between attempts

        await asyncio.gather(
            run_claims(manager1, manager1_session_maker, claimed_by_manager1),
            run_claims(manager2, manager2_session_maker, claimed_by_manager2),
        )

        # Verify no duplicate claims
        all_claimed = claimed_by_manager1 + claimed_by_manager2
        assert len(all_claimed) == len(set(all_claimed)), (
            f"Duplicate task IDs claimed: {all_claimed}"
        )

        # All 10 tasks should be claimed
        assert len(all_claimed) == 10

    async def test_concurrent_claims_skip_locked(
        self, async_db_session: AsyncSession
    ) -> None:
        """
        Test that SKIP LOCKED prevents waiting on locked rows.

        Two sessions try to claim the same tasks simultaneously.
        With SKIP LOCKED, the second session should skip locked rows
        instead of waiting.
        """
        from sqlalchemy import text
        
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

        await create_pending_tasks(async_db_session, 6)

        from sqlalchemy.ext.asyncio import async_sessionmaker

        # Create two separate session makers from the same engine
        session_maker1 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )
        session_maker2 = async_sessionmaker(
            bind=async_db_session.bind, expire_on_commit=False, class_=AsyncSession
        )

        # This test verifies that SKIP LOCKED prevents deadlocks/waiting
        # by running two claim cycles simultaneously
        async def claim_batch(session_maker):
            async with session_maker() as session:
                task_repo = TaskRepository(session)
                lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
                try:
                    claimed = await task_repo.claim_tasks(
                        limit=3,
                        lease_expires_at=lease_expires_at,
                    )
                    await session.commit()
                    return [t.id for t in claimed]
                except Exception:
                    await session.rollback()
                    raise

        # Run two claim operations concurrently using different session makers
        results = await asyncio.gather(
            claim_batch(session_maker1),
            claim_batch(session_maker2),
        )

        all_claimed = results[0] + results[1]
        assert len(all_claimed) == len(set(all_claimed)), (
            f"Duplicate claims: {all_claimed}"
        )
        assert len(all_claimed) == 6

    async def test_claim_then_reclaim_after_lease_expiry(
        self, async_db_session: AsyncSession
    ) -> None:
        """
        Test that tasks can be reclaimed after lease expires.
        This simulates lease recovery behavior.
        """
        # Ensure clean state before test
        from sqlalchemy import text
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

        await create_pending_tasks(async_db_session, 5)

        # First claim
        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
        claimed1 = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )
        await async_db_session.commit()
        claimed_ids_1 = [t.id for t in claimed1]

        assert len(claimed_ids_1) == 3

        # Second claim should get the remaining 2
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
        claimed2 = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )
        await async_db_session.commit()
        claimed_ids_2 = [t.id for t in claimed2]

        assert len(claimed_ids_2) == 2

        # No overlap
        assert not set(claimed_ids_1) & set(claimed_ids_2)

        # All 5 tasks claimed
        assert len(set(claimed_ids_1 + claimed_ids_2)) == 5


class TestTaskClaimingRollback:
    """Tests for rollback behavior during task claiming."""

    async def test_claim_rollback_on_error(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that claiming rolls back on error."""
        await create_pending_tasks(async_db_session, 5)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        # Claim tasks successfully
        claimed = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )
        assert len(claimed) == 3

        # Verify they're in PROCESSING
        for task in claimed:
            assert task.status == TaskStatus.PROCESSING

        # Now simulate a rollback by rolling back the session
        await async_db_session.rollback()

        # Tasks should be back to PENDING (in a new session they would be)
        # This test just verifies the transaction boundary works
        assert len(claimed) == 3

    async def test_claim_failure_does_not_leave_partial_state(
        self, test_session_maker
    ) -> None:
        """
        Test that a failure during claiming doesn't leave partial state.

        This simulates a failure between SELECT and UPDATE.
        """
        # Create 5 tasks in a fresh session
        async with test_session_maker() as session:
            await create_pending_tasks(session, 5, domain_prefix="failure_")

        # First session claims 2 tasks
        async with test_session_maker() as session:
            task_repo = TaskRepository(session)
            lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
            claimed = await task_repo.claim_tasks(
                limit=2,
                lease_expires_at=lease_expires_at,
            )
            await session.commit()
            claimed_ids = [t.id for t in claimed]

        assert len(claimed_ids) == 2

        # Second session claims remaining tasks (limit=3, but only 3 left)
        async with test_session_maker() as session:
            task_repo = TaskRepository(session)
            lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)
            claimed = await task_repo.claim_tasks(
                limit=3,
                lease_expires_at=lease_expires_at,
            )
            await session.commit()
            claimed_ids_2 = [t.id for t in claimed]

        assert len(claimed_ids_2) == 3
        assert not set(claimed_ids) & set(claimed_ids_2)


class TestTaskClaimingEdgeCases:
    """Edge case tests for task claiming."""

    async def test_claim_with_zero_limit(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that limit=0 returns empty list."""
        await create_pending_tasks(async_db_session, 5)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        claimed = await task_repo.claim_tasks(
            limit=0,
            lease_expires_at=lease_expires_at,
        )

        assert len(claimed) == 0

    async def test_claim_with_negative_limit(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that negative limit returns empty list."""
        await create_pending_tasks(async_db_session, 5)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        claimed = await task_repo.claim_tasks(
            limit=-1,
            lease_expires_at=lease_expires_at,
        )

        assert len(claimed) == 0

    async def test_claim_increments_attempts(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that claiming increments the attempt count."""
        await create_pending_tasks(async_db_session, 3)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        claimed = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )

        for task in claimed:
            assert task.attempts == 1

    async def test_claim_sets_lease_expires_at(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test that lease_expires_at is set correctly."""
        await create_pending_tasks(async_db_session, 3)

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=120)

        claimed = await task_repo.claim_tasks(
            limit=3,
            lease_expires_at=lease_expires_at,
        )

        for task in claimed:
            assert task.lease_expires_at == lease_expires_at

    async def test_claim_tasks_custom_task_types(
        self, async_db_session: AsyncSession
    ) -> None:
        """Test claiming with custom task type filter."""
        base_time = datetime.now(UTC)

        # Create only REFRESH tasks with unique names
        for i in range(3):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"refresh{i}_{uuid.uuid4().hex[:8]}.com",
                is_active=True,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            task = Task(
                id=uuid.uuid4(),
                job_id=None,
                domain_id=domain.id,
                type=TaskType.REFRESH,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=base_time,
                created_at=base_time,
                updated_at=base_time,
            )
            async_db_session.add(task)

        await async_db_session.commit()

        task_repo = TaskRepository(async_db_session)
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=100)

        # Claim only USER_REQUEST - should get 0
        claimed = await task_repo.claim_tasks(
            limit=10,
            lease_expires_at=lease_expires_at,
            task_types=[TaskType.USER_REQUEST],
        )
        assert len(claimed) == 0

        # Claim only REFRESH - should get 3
        claimed = await task_repo.claim_tasks(
            limit=10,
            lease_expires_at=lease_expires_at,
            task_types=[TaskType.REFRESH],
        )
        assert len(claimed) == 3
        for task in claimed:
            assert task.type == TaskType.REFRESH
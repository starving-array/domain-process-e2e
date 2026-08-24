"""Task repository for data access."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.models import Task, TaskStatus, TaskType


class TaskRepository:
    """Repository for Task persistence operations."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, task: Task) -> Task:
        """Insert a new Task."""
        self._session.add(task)
        await self._session.flush()
        return task

    async def create_batch(self, tasks: list[Task]) -> list[Task]:
        """Insert multiple Tasks."""
        self._session.add_all(tasks)
        await self._session.flush()
        return tasks

    async def get_pending_refresh_task_for_domain(self, domain_id: uuid.UUID) -> Task | None:
        """Check if there's already a PENDING or PROCESSING REFRESH task for a domain."""
        stmt = select(Task).where(
            Task.domain_id == domain_id,
            Task.type == TaskType.REFRESH,
            Task.status.in_([TaskStatus.PENDING, TaskStatus.PROCESSING]),
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_refresh_tasks(
        self,
        domain_ids: list[uuid.UUID],
        now: datetime,
    ) -> list[Task]:
        """
        Create REFRESH tasks for the given domain IDs.

        Skips domains that already have a PENDING or PROCESSING REFRESH task.

        Args:
            domain_ids: List of domain IDs to create refresh tasks for.
            now: Current timestamp for task creation.

        Returns:
            List of created Task objects.
        """
        created_tasks = []
        for domain_id in domain_ids:
            # Check for existing refresh task
            existing = await self.get_pending_refresh_task_for_domain(domain_id)
            if existing is not None:
                continue

            task = Task(
                id=uuid.uuid4(),
                job_id=None,  # REFRESH tasks are not associated with a user job
                domain_id=domain_id,
                type=TaskType.REFRESH,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=now,
                lease_expires_at=None,
                error_payload=None,
                created_at=now,
                updated_at=now,
            )
            self._session.add(task)
            created_tasks.append(task)

        if created_tasks:
            await self._session.flush()

        return created_tasks

    async def get(self, task_id: uuid.UUID) -> Task | None:
        """Retrieve a Task by ID."""
        stmt = select(Task).where(Task.id == task_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_job_id(self, job_id: uuid.UUID) -> list[Task]:
        """Retrieve all Tasks for a Job."""
        stmt = select(Task).where(Task.job_id == job_id).order_by(Task.created_at)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_job_id_paginated(
        self,
        job_id: uuid.UUID,
        limit: int,
        cursor_created_at: datetime | None = None,
        cursor_task_id: uuid.UUID | None = None,
    ) -> list[Task]:
        """Retrieve Tasks for a Job with cursor-based pagination."""
        stmt = select(Task).where(Task.job_id == job_id)

        if cursor_created_at is not None and cursor_task_id is not None:
            stmt = stmt.where(
                (Task.created_at > cursor_created_at)
                | ((Task.created_at == cursor_created_at) & (Task.id > cursor_task_id))
            )

        stmt = stmt.order_by(Task.created_at, Task.id).limit(limit + 1)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_summary_by_job_id(self, job_id: uuid.UUID) -> dict[TaskStatus, int]:
        """Get task count summary grouped by status for a Job."""
        stmt = (
            select(Task.status, func.count(Task.id))
            .where(Task.job_id == job_id)
            .group_by(Task.status)
        )
        result = await self._session.execute(stmt)
        summary = {status: count for status, count in result.all()}
        return summary

    async def claim_tasks(
        self,
        limit: int,
        lease_expires_at: datetime,
        task_types: list[TaskType] | None = None,
    ) -> list[Task]:
        """
        Claim PENDING tasks for processing using FOR UPDATE SKIP LOCKED.

        This method atomically selects eligible PENDING tasks and transitions
        them to PROCESSING with a lease expiration. Uses PostgreSQL's
        FOR UPDATE SKIP LOCKED to ensure concurrent managers don't claim
        the same tasks.

        Args:
            limit: Maximum number of tasks to claim.
            lease_expires_at: Lease expiration timestamp for claimed tasks.
            task_types: Optional list of task types to claim. Defaults to
                [USER_REQUEST, REFRESH] with USER_REQUEST prioritized.

        Returns:
            List of claimed Task objects (now in PROCESSING state).
        """
        if limit <= 0:
            return []

        # Default to both task types with USER_REQUEST prioritized
        if task_types is None:
            task_types = [TaskType.USER_REQUEST, TaskType.REFRESH]

        # Build the ordering - USER_REQUEST tasks come first
        # We use a CASE expression to prioritize USER_REQUEST over REFRESH
        from sqlalchemy import case

        type_priority = case(
            (Task.type == TaskType.USER_REQUEST, 0),
            (Task.type == TaskType.REFRESH, 1),
            else_=2,
        )

        # Select eligible tasks with FOR UPDATE SKIP LOCKED
        stmt = (
            select(Task)
            .where(
                Task.status == TaskStatus.PENDING,
                Task.type.in_(task_types),
            )
            .order_by(type_priority, Task.created_at, Task.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        result = await self._session.execute(stmt)
        tasks = list(result.scalars().all())

        # Transition claimed tasks to PROCESSING
        now = datetime.now(UTC)
        for task in tasks:
            task.status = TaskStatus.PROCESSING
            task.lease_expires_at = lease_expires_at
            task.updated_at = now
            task.attempts += 1  # Increment attempt count on claim

        if tasks:
            await self._session.flush()

        return tasks

    async def recover_expired_tasks(
        self,
        limit: int,
        now: datetime,
    ) -> list[Task]:
        """
        Recover expired PROCESSING tasks whose lease has expired.

        This method finds PROCESSING tasks whose lease has expired and
        transitions them back to PENDING with incremented attempt count
        and updated next_attempt_at for retry with exponential backoff.
        Uses FOR UPDATE SKIP LOCKED to safely handle concurrent recovery.

        Args:
            limit: Maximum number of tasks to recover.
            now: Current timestamp for lease expiration check.

        Returns:
            List of recovered Task objects (now in PENDING state).
        """
        if limit <= 0:
            return []

        from sqlalchemy import case

        # Build the ordering - USER_REQUEST tasks come first
        type_priority = case(
            (Task.type == TaskType.USER_REQUEST, 0),
            (Task.type == TaskType.REFRESH, 1),
            else_=2,
        )

        # Select expired PROCESSING tasks with FOR UPDATE SKIP LOCKED
        stmt = (
            select(Task)
            .where(
                Task.status == TaskStatus.PROCESSING,
                Task.lease_expires_at.is_not(None),
                Task.lease_expires_at < now,
            )
            .order_by(type_priority, Task.created_at, Task.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        result = await self._session.execute(stmt)
        tasks = list(result.scalars().all())

        if not tasks:
            return []

        now = datetime.now(UTC)
        from domain_processing_service.config import get_settings
        settings = get_settings()

        recovered = []
        for task in tasks:
            task.updated_at = now

            # Check if we've exceeded max attempts
            if task.attempts >= settings.max_attempts:
                # Mark as FAILED - retry budget exhausted
                task.status = TaskStatus.FAILED
                task.error_payload = {
                    "code": "MAX_ATTEMPTS_EXCEEDED",
                    "message": f"Task exceeded maximum attempts ({settings.max_attempts})",
                    "retryable": False,
                }
                task.lease_expires_at = None
                # next_attempt_at is non-nullable, leave as-is when FAILED
            else:
                # Transition back to PENDING for retry
                task.status = TaskStatus.PENDING
                task.lease_expires_at = None
                # Exponential backoff with jitter
                base_delay = 60  # 1 minute base
                delay = min(base_delay * (2 ** (task.attempts - 1)), 3600)
                import random
                jitter = random.randint(0, 60)
                task.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay + jitter)

            recovered.append(task)

        if recovered:
            await self._session.flush()

        return recovered

    async def update_status(
        self,
        task_id: uuid.UUID,
        status: TaskStatus,
        *,
        lease_expires_at: datetime | None = None,
        attempts: int | None = None,
        next_attempt_at: datetime | None = None,
        error_payload: dict[str, Any] | None = None,
    ) -> Task | None:
        """Update Task status and related fields."""
        task = await self.get(task_id)
        if task is None:
            return None

        task.status = status
        task.updated_at = datetime.now(UTC)

        if lease_expires_at is not None:
            task.lease_expires_at = lease_expires_at
        if attempts is not None:
            task.attempts = attempts
        if next_attempt_at is not None:
            task.next_attempt_at = next_attempt_at
        if error_payload is not None:
            task.error_payload = error_payload

        await self._session.flush()
        return task

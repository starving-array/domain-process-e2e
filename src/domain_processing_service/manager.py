"""Task Manager - DB polling coordinator for claiming tasks, dispatching to worker pool,
and recovering expired leases."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain_processing_service.config import AppSettings
from domain_processing_service.logging import log_event
from domain_processing_service.models import Task, TaskStatus
from domain_processing_service.repositories import DomainRepository, TaskRepository
from domain_processing_service.worker import WorkerPool

logger = logging.getLogger(__name__)


class TaskManager:
    """
    Task Manager responsible for polling PostgreSQL for PENDING tasks,
    claiming them, dispatching to the worker pool, and recovering expired leases.

    The Task Manager:
    - Polls the database for eligible PENDING tasks
    - Uses FOR UPDATE SKIP LOCKED to safely claim tasks
    - Transitions claimed tasks to PROCESSING with a lease
    - Dispatches claimed tasks to the bounded worker queue
    - Respects queue capacity limits (backpressure)
    - Recovers expired PROCESSING tasks (lease recovery)
    - Backs off when no work is available or queue is full
    """

    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        worker_pool: "WorkerPool | None" = None,
    ) -> None:
        self._session_maker = session_maker
        self._settings = settings
        self._worker_pool = worker_pool
        self._running = False
        self._task: asyncio.Task[Any] | None = None
        self._recovery_task: asyncio.Task[Any] | None = None

    def set_worker_pool(self, worker_pool: "WorkerPool") -> None:
        """Set the worker pool for task dispatch."""
        self._worker_pool = worker_pool

    async def start(self) -> None:
        """Start the Task Manager polling loop and recovery loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        self._recovery_task = asyncio.create_task(self._recovery_loop())
        log_event(
            logger,
            "task_manager.started",
            level=logging.INFO,
            poll_interval_seconds=self._settings.task_lease_seconds / 4,
            recovery_interval_seconds=self._settings.task_lease_seconds / 4,
            worker_concurrency=self._settings.worker_concurrency,
            worker_queue_capacity=self._settings.worker_queue_capacity,
        )

    async def stop(self) -> None:
        """Stop the Task Manager polling loop and recovery loop."""
        if not self._running:
            return
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._recovery_task is not None:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
        log_event(
            logger,
            "task_manager.stopped",
            level=logging.INFO,
        )

    async def _run_loop(self) -> None:
        """Main polling loop for the Task Manager."""
        while self._running:
            try:
                await self._claim_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_event(
                    logger,
                    "task_manager.cycle_failed",
                    level=logging.ERROR,
                    error=str(e),
                )
                # Back off on error
                await asyncio.sleep(1)

            # Small delay between polling cycles to avoid tight loops
            await asyncio.sleep(0.1)

    async def _recovery_loop(self) -> None:
        """Background loop for recovering expired PROCESSING tasks."""
        while self._running:
            try:
                await self._recovery_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_event(
                    logger,
                    "task_manager.recovery_cycle_failed",
                    level=logging.ERROR,
                    error=str(e),
                )
                # Back off on error
                await asyncio.sleep(1)

            # Recovery runs at the same interval as claim cycle
            await asyncio.sleep(self._settings.task_lease_seconds / 4)

    async def _recovery_cycle(self) -> None:
        """
        Single recovery cycle - find and recover expired PROCESSING tasks.
        
        This method:
        1. Finds PROCESSING tasks with expired leases
        2. Uses FOR UPDATE SKIP LOCKED to safely claim expired tasks
        3. Transitions them back to PENDING with incremented attempts
        4. Applies exponential backoff with jitter for next_attempt_at
        5. Marks as FAILED if max_attempts exceeded
        """
        if self._worker_pool is None:
            log_event(
                logger,
                "task_manager.recovery_no_worker_pool",
                level=logging.WARNING,
            )
            await asyncio.sleep(1)
            return

        # Calculate how many tasks we can recover (limited by queue capacity)
        available_capacity = self._worker_pool.available_capacity
        batch_size = min(available_capacity, self._settings.worker_concurrency)

        if batch_size <= 0:
            # Queue is full, apply backpressure
            log_event(
                logger,
                "worker_pool.queue_full",
                level=logging.DEBUG,
                queue_size=self._worker_pool.queue.size,
                queue_capacity=self._worker_pool.queue.capacity,
            )
            await asyncio.sleep(1)
            return

        log_event(
            logger,
            "task_manager.recovery_attempt",
            level=logging.DEBUG,
            batch_size=batch_size,
            available_capacity=available_capacity,
        )

        async with self._session_maker() as session:
            task_repo = TaskRepository(session)
            try:
                recovered_tasks = await task_repo.recover_expired_tasks(
                    limit=batch_size,
                    now=datetime.now(UTC),
                )
                await session.commit()

                if recovered_tasks:
                    log_event(
                        logger,
                        "task_manager.tasks_recovered",
                        level=logging.INFO,
                        recovered_count=len(recovered_tasks),
                        task_ids=[str(t.id) for t in recovered_tasks],
                        task_types=[t.type.value for t in recovered_tasks],
                    )

                    # Re-dispatch recovered tasks that are now PENDING
                    for task in recovered_tasks:
                        if task.status == TaskStatus.PENDING:
                            try:
                                await self._worker_pool.queue.put(task)
                                log_event(
                                    logger,
                                    "worker_pool.task_dispatched",
                                    level=logging.DEBUG,
                                    task_id=str(task.id),
                                    queue_size=self._worker_pool.queue.size,
                                )
                            except asyncio.QueueFull:
                                # Shouldn't happen since we checked capacity,
                                # but handle gracefully
                                log_event(
                                    logger,
                                    "worker_pool.queue_full_on_dispatch",
                                    level=logging.WARNING,
                                    task_id=str(task.id),
                                )
                        else:
                            # Task was marked FAILED (max attempts exceeded)
                            log_event(
                                logger,
                                "task_manager.task_failed_max_attempts",
                                level=logging.WARNING,
                                task_id=str(task.id),
                                attempts=task.attempts,
                                max_attempts=self._settings.max_attempts,
                            )
                            # Soft deactivate the domain due to max attempts exceeded
                            await self._maybe_deactivate_domain(session, task)
                    else:
                        log_event(
                            logger,
                            "task_manager.no_expired_tasks",
                            level=logging.DEBUG,
                        )
                    # Back off when no expired tasks are available
                    await asyncio.sleep(1)

            except Exception:
                await session.rollback()
                raise

    async def _claim_cycle(self) -> None:
        """
        Single claim cycle - attempt to claim tasks from the database
        and dispatch them to the worker pool.
        """
        # Calculate available queue capacity from worker pool
        if self._worker_pool is None:
            # No worker pool configured, skip claiming
            log_event(
                logger,
                "task_manager.no_worker_pool",
                level=logging.WARNING,
            )
            await asyncio.sleep(1)
            return

        available_capacity = self._worker_pool.available_capacity
        batch_size = min(available_capacity, self._settings.worker_concurrency)

        if batch_size <= 0:
            # Queue is full, apply backpressure
            log_event(
                logger,
                "worker_pool.queue_full",
                level=logging.DEBUG,
                queue_size=self._worker_pool.queue.size,
                queue_capacity=self._worker_pool.queue.capacity,
            )
            await asyncio.sleep(1)
            return

        lease_expires_at = datetime.now(UTC) + timedelta(seconds=self._settings.task_lease_seconds)

        log_event(
            logger,
            "task_manager.claim_attempt",
            level=logging.DEBUG,
            batch_size=batch_size,
            lease_seconds=self._settings.task_lease_seconds,
            available_capacity=available_capacity,
        )

        async with self._session_maker() as session:
            task_repo = TaskRepository(session)
            try:
                claimed_tasks = await task_repo.claim_tasks(
                    limit=batch_size,
                    lease_expires_at=lease_expires_at,
                )
                await session.commit()

                if claimed_tasks:
                    log_event(
                        logger,
                        "task_manager.tasks_claimed",
                        level=logging.INFO,
                        claimed_count=len(claimed_tasks),
                        task_ids=[str(t.id) for t in claimed_tasks],
                        task_types=[t.type.value for t in claimed_tasks],
                    )

                    # Dispatch tasks to worker pool
                    for task in claimed_tasks:
                        try:
                            await self._worker_pool.queue.put(task)
                            log_event(
                                logger,
                                "worker_pool.task_dispatched",
                                level=logging.DEBUG,
                                task_id=str(task.id),
                                queue_size=self._worker_pool.queue.size,
                            )
                        except asyncio.QueueFull:
                            # This shouldn't happen since we checked capacity,
                            # but handle it gracefully
                            log_event(
                                logger,
                                "worker_pool.queue_full_on_dispatch",
                                level=logging.WARNING,
                                task_id=str(task.id),
                            )
                else:
                    log_event(
                        logger,
                        "task_manager.no_tasks_available",
                        level=logging.DEBUG,
                    )
                    # Back off when no tasks are available
                    await asyncio.sleep(1)

            except Exception:
                await session.rollback()
                raise

    async def _maybe_deactivate_domain(self, session: AsyncSession, task: Task) -> None:
        """
        Soft deactivate a domain if the task failed due to max attempts exceeded.
        
        Only deactivates if the domain is currently active and the failure
        is MAX_ATTEMPTS_EXCEEDED.
        """
        error_payload = task.error_payload or {}
        if error_payload.get("code") != "MAX_ATTEMPTS_EXCEEDED":
            return
        
        domain_repo = DomainRepository(session)
        domain = await domain_repo.get(task.domain_id)
        if domain is None or not domain.is_active:
            return
        
        now = datetime.now(UTC)
        domain.is_active = False
        domain.deactivated_at = now
        domain.updated_at = now
        await session.flush()
        
        log_event(
            logger,
            "domain.deactivated",
            level=logging.WARNING,
            domain_id=str(task.domain_id),
            domain=domain.normalized_domain,
            reason="MAX_ATTEMPTS_EXCEEDED",
        )
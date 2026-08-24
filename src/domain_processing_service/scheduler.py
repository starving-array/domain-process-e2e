"""Refresh Scheduler for Phase 11.

Periodically identifies domains that require refresh and creates REFRESH tasks
that enter the same Task Manager and Worker Pool pipeline.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain_processing_service.config import AppSettings
from domain_processing_service.logging import log_event
from domain_processing_service.repositories import DomainDetailRepository, TaskRepository

logger = logging.getLogger(__name__)


class RefreshScheduler:
    """
    Background scheduler that creates REFRESH tasks for stale domains.

    The scheduler:
    - Runs at a configured interval (default ~14 days)
    - Queries active domains with next_refresh_at <= NOW()
    - Creates REFRESH tasks for eligible domains
    - Skips domains that already have a pending/progressing REFRESH task
    - Respects backpressure from the worker pool queue
    - Emits structured lifecycle events
    """

    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        worker_pool: Any | None = None,  # WorkerPool for backpressure awareness
    ) -> None:
        self._session_maker = session_maker
        self._settings = settings
        self._worker_pool = worker_pool
        self._running = False
        self._task: asyncio.Task[Any] | None = None

    def set_worker_pool(self, worker_pool: Any) -> None:
        """Set the worker pool for backpressure awareness."""
        self._worker_pool = worker_pool

    async def start(self) -> None:
        """Start the scheduler loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        log_event(
            logger,
            "scheduler.started",
            level=logging.INFO,
            interval_seconds=self._settings.refresh_interval_seconds,
            batch_size=self._settings.worker_concurrency,
        )

    async def stop(self) -> None:
        """Stop the scheduler loop."""
        if not self._running:
            return
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log_event(
            logger,
            "scheduler.stopped",
            level=logging.INFO,
        )

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_event(
                    logger,
                    "scheduler.error",
                    level=logging.ERROR,
                    error=str(e),
                )
                # Back off on error
                await asyncio.sleep(1)

            # Wait for the next interval
            await asyncio.sleep(self._settings.refresh_interval_seconds)

    async def _tick(self) -> None:
        """Single scheduler tick - discover and create refresh tasks."""
        log_event(
            logger,
            "scheduler.tick_started",
            level=logging.DEBUG,
        )

        # Check backpressure - if worker pool queue is full, skip this tick
        if self._worker_pool is not None:
            available_capacity = self._worker_pool.available_capacity
            if available_capacity <= 0:
                log_event(
                    logger,
                    "scheduler.backpressure",
                    level=logging.DEBUG,
                    queue_size=self._worker_pool.queue.size,
                    queue_capacity=self._worker_pool.queue.capacity,
                )
                return

        async with self._session_maker() as session:
            domain_detail_repo = DomainDetailRepository(session)
            task_repo = TaskRepository(session)

            try:
                # Calculate how many tasks we can create (limited by queue capacity)
                max_tasks = self._settings.worker_concurrency
                if self._worker_pool is not None:
                    max_tasks = min(max_tasks, self._worker_pool.available_capacity)

                if max_tasks <= 0:
                    log_event(
                        logger,
                        "scheduler.backpressure",
                        level=logging.DEBUG,
                        queue_size=self._worker_pool.queue.size if self._worker_pool else 0,
                        queue_capacity=self._worker_pool.queue.capacity if self._worker_pool else 0,
                    )
                    return

                # Discover domains needing refresh
                now = datetime.now(UTC)
                domains_needing_refresh = await domain_detail_repo.get_domains_needing_refresh(
                    limit=max_tasks,
                    now=now,
                )

                if not domains_needing_refresh:
                    log_event(
                        logger,
                        "scheduler.tick_completed",
                        level=logging.DEBUG,
                        candidates_found=0,
                        tasks_created=0,
                    )
                    return

                log_event(
                    logger,
                    "scheduler.refresh_candidates_discovered",
                    level=logging.INFO,
                    candidate_count=len(domains_needing_refresh),
                    domains=[domain for _, domain in domains_needing_refresh],
                )

                # Extract domain IDs
                domain_ids = [domain_id for domain_id, _ in domains_needing_refresh]

                # Create refresh tasks (skips duplicates)
                created_tasks = await task_repo.create_refresh_tasks(
                    domain_ids=domain_ids,
                    now=now,
                )
                await session.commit()

                if created_tasks:
                    log_event(
                        logger,
                        "scheduler.refresh_task_created",
                        level=logging.INFO,
                        created_count=len(created_tasks),
                        task_ids=[str(t.id) for t in created_tasks],
                        domains=[t.domain_id for t in created_tasks],
                    )

                    # Log skipped domains
                    skipped_count = len(domain_ids) - len(created_tasks)
                    if skipped_count > 0:
                        log_event(
                            logger,
                            "scheduler.refresh_task_skipped_duplicate",
                            level=logging.DEBUG,
                            skipped_count=skipped_count,
                        )
                else:
                    log_event(
                        logger,
                        "scheduler.refresh_task_skipped_duplicate",
                        level=logging.DEBUG,
                        skipped_count=len(domain_ids),
                    )

                log_event(
                    logger,
                    "scheduler.tick_completed",
                    level=logging.DEBUG,
                    candidates_found=len(domains_needing_refresh),
                    tasks_created=len(created_tasks),
                )

            except Exception:
                await session.rollback()
                raise
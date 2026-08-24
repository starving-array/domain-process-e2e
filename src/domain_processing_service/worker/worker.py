"""Worker Pool and Bounded Queue for Phase 8."""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain_processing_service.config import AppSettings
from domain_processing_service.logging import log_event
from domain_processing_service.models import Task

if TYPE_CHECKING:
    from domain_processing_service.manager import TaskManager

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class QueueStats:
    """Statistics about the bounded queue."""

    capacity: int
    size: int = 0
    max_size_reached: int = 0
    total_enqueued: int = 0
    total_dequeued: int = 0


class BoundedQueue(Generic[T]):
    """
    A bounded queue with backpressure support.

    Uses asyncio.Queue with a maximum size to enforce bounded capacity.
    Provides statistics and backpressure signaling.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("Queue capacity must be positive")
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=capacity)
        self._capacity = capacity
        self._stats = QueueStats(capacity=capacity)

    @property
    def capacity(self) -> int:
        """Maximum capacity of the queue."""
        return self._capacity

    @property
    def size(self) -> int:
        """Current number of items in the queue."""
        return self._queue.qsize()

    @property
    def available_capacity(self) -> int:
        """Available capacity in the queue."""
        return self._capacity - self._queue.qsize()

    @property
    def is_full(self) -> bool:
        """Check if the queue is full."""
        return self._queue.full()

    @property
    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    @property
    def stats(self) -> QueueStats:
        """Get queue statistics."""
        return self._stats

    async def put(self, item: T) -> None:
        """
        Put an item into the queue.

        If the queue is full, this will block until space is available.
        """
        await self._queue.put(item)
        self._stats.size = self._queue.qsize()
        self._stats.total_enqueued += 1
        if self._stats.size > self._stats.max_size_reached:
            self._stats.max_size_reached = self._stats.size

    async def get(self) -> T:
        """
        Get an item from the queue.

        If the queue is empty, this will block until an item is available.
        """
        item = await self._queue.get()
        self._stats.size = self._queue.qsize()
        self._stats.total_dequeued += 1
        return item

    def put_nowait(self, item: T) -> None:
        """
        Put an item into the queue without blocking.

        Raises:
            asyncio.QueueFull: If the queue is full.
        """
        self._queue.put_nowait(item)
        self._stats.size = self._queue.qsize()
        self._stats.total_enqueued += 1
        if self._stats.size > self._stats.max_size_reached:
            self._stats.max_size_reached = self._stats.size

    def get_nowait(self) -> T:
        """
        Get an item from the queue without blocking.

        Raises:
            asyncio.QueueEmpty: If the queue is empty.
        """
        item = self._queue.get_nowait()
        self._stats.size = self._queue.qsize()
        self._stats.total_dequeued += 1
        return item

    def task_done(self) -> None:
        """Mark a task as done (for join() support)."""
        self._queue.task_done()

    async def join(self) -> None:
        """Wait until all items in the queue have been processed."""
        await self._queue.join()


@dataclass
class WorkerConfig:
    """Configuration for a worker."""

    worker_id: str
    task_handler: Callable[[Task, AsyncSession], Awaitable[None]]
    session_maker: async_sessionmaker[AsyncSession]
    settings: AppSettings


class Worker:
    """
    A single worker that consumes tasks from the queue and processes them.

    For Phase 8, the worker stops at the boundary where domain processing
    would begin (Phase 9). It mocks the processing by simply marking the
    task as COMPLETED.
    """

    def __init__(self, config: WorkerConfig) -> None:
        self._config = config
        self._worker_id = config.worker_id
        self._task_handler = config.task_handler
        self._session_maker = config.session_maker
        self._settings = config.settings
        self._running = False
        self._task: asyncio.Task[Any] | None = None

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def running(self) -> bool:
        return self._running

    async def start(self, queue: BoundedQueue[Task]) -> None:
        """Start the worker, consuming tasks from the queue."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop(queue))

        log_event(
            logger,
            "worker.started",
            level=logging.INFO,
            worker_id=self._worker_id,
        )

    async def stop(self, graceful: bool = True) -> None:
        """
        Stop the worker.

        Args:
            graceful: If True, wait for current task to complete.
                     If False, cancel immediately.
        """
        if not self._running:
            return

        self._running = False

        if graceful and self._task is not None:
            # Wait for the current task to complete
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        elif self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        log_event(
            logger,
            "worker.stopped",
            level=logging.INFO,
            worker_id=self._worker_id,
        )

    async def _run_loop(self, queue: BoundedQueue[Task]) -> None:
        """Main loop: consume tasks from queue and process them."""
        log_event(
            logger,
            "worker.task_received",
            level=logging.DEBUG,
            worker_id=self._worker_id,
            queue_size=queue.size,
        )

        while self._running:
            try:
                # Get a task from the queue (blocks until available)
                try:
                    task = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                log_event(
                    logger,
                    "worker.task_received",
                    level=logging.INFO,
                    worker_id=self._worker_id,
                    task_id=str(task.id),
                    task_type=task.type.value,
                    job_id=str(task.job_id) if task.job_id else None,
                )

                # Process the task
                await self._process_task(task)

                # Mark the queue task as done
                queue.task_done()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_event(
                    logger,
                    "worker.task_failed",
                    level=logging.ERROR,
                    worker_id=self._worker_id,
                    error=str(e),
                )
                # Continue processing other tasks

    async def _process_task(self, task: Task) -> None:
        """
        Process a single task.

        For Phase 8, this mocks the processing by calling the task handler
        which will mark the task as COMPLETED. Phase 9 will implement
        the actual domain processing.
        """
        async with self._session_maker() as session:
            try:
                await self._task_handler(task, session)
                await session.commit()

                log_event(
                    logger,
                    "worker.task_completed",
                    level=logging.INFO,
                    worker_id=self._worker_id,
                    task_id=str(task.id),
                )
            except Exception:
                await session.rollback()
                raise


class WorkerPool:
    """
    Manages a pool of workers that consume tasks from a bounded queue.

    The WorkerPool:
    - Creates and manages a pool of worker coroutines
    - Provides a bounded queue for task dispatch
    - Handles worker lifecycle (start/stop)
    - Provides backpressure awareness to the Task Manager
    """

    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        settings: AppSettings,
        task_handler: Callable[[Task, AsyncSession], Awaitable[None]],
    ) -> None:
        self._session_maker = session_maker
        self._settings = settings
        self._task_handler = task_handler

        # Create the bounded queue
        self._queue: BoundedQueue[Task] = BoundedQueue(
            capacity=settings.worker_queue_capacity
        )

        # Create workers
        self._workers: list[Worker] = []
        self._worker_concurrency = settings.worker_concurrency

        for _i in range(self._worker_concurrency):
            worker_config = WorkerConfig(
                worker_id=f"worker-{uuid.uuid4().hex[:8]}",
                task_handler=task_handler,
                session_maker=session_maker,
                settings=settings,
            )
            worker = Worker(worker_config)
            self._workers.append(worker)

        self._running = False
        self._task_manager: TaskManager | None = None

    @property
    def queue(self) -> BoundedQueue[Task]:
        return self._queue

    @property
    def workers(self) -> list[Worker]:
        return self._workers

    @property
    def running(self) -> bool:
        return self._running

    @property
    def worker_count(self) -> int:
        return len(self._workers)

    @property
    def available_capacity(self) -> int:
        """Available capacity in the queue for the Task Manager to claim."""
        return self._queue.available_capacity

    @property
    def queue_stats(self) -> QueueStats:
        return self._queue.stats

    def set_task_manager(self, task_manager: "TaskManager | None") -> None:
        """Set the Task Manager reference for coordination."""
        self._task_manager = task_manager

    async def start(self) -> None:
        """Start the worker pool and all workers."""
        if self._running:
            return

        self._running = True

        # Start all workers
        for worker in self._workers:
            await worker.start(self._queue)

        log_event(
            logger,
            "worker_pool.started",
            level=logging.INFO,
            worker_count=self._worker_concurrency,
            queue_capacity=self._settings.worker_queue_capacity,
        )

    async def stop(self, graceful: bool = True) -> None:
        """
        Stop the worker pool and all workers.

        Args:
            graceful: If True, wait for workers to finish current tasks.
        """
        if not self._running:
            return

        self._running = False

        # Stop all workers
        for worker in self._workers:
            await worker.stop(graceful=graceful)

        log_event(
            logger,
            "worker_pool.stopped",
            level=logging.INFO,
        )


def create_phase9_task_handler(
    settings: AppSettings,
) -> Callable[[Task, AsyncSession], Awaitable[None]]:
    """
    Create a Phase 9 task handler that performs actual domain processing.
    
    This replaces the mock handler from Phase 8 with actual domain processing logic.
    """
    from domain_processing_service.domain_processor import DomainProcessor
    
    def phase9_task_handler(task: Task, session: AsyncSession) -> Awaitable[None]:
        """
        Phase 9 task handler - performs actual domain processing.
        """
        # Create domain processor
        processor = DomainProcessor(
            settings=settings,
            session=session,
            domain_lock_manager=None,  # Will be created inside
        )
        
        async def _process() -> None:
            try:
                await processor.process_task(task)
                
                # The processor updates the task status directly
                # We just need to ensure the session is flushed
                await session.flush()
                
            finally:
                await processor.close()
        
        return _process()
    
    return phase9_task_handler
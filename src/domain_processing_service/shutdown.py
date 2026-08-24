"""Graceful shutdown coordination for Phase 13."""

import asyncio
import logging
import signal
from dataclasses import dataclass
from typing import Any

from domain_processing_service.config import AppSettings
from domain_processing_service.logging import log_event
from domain_processing_service.manager import TaskManager
from domain_processing_service.scheduler import RefreshScheduler
from domain_processing_service.worker import WorkerPool

logger = logging.getLogger(__name__)

_signal_handlers_installed = False


@dataclass
class ShutdownState:
    """Tracks the current shutdown state."""

    initiated: bool = False
    signal: int | None = None
    api_closed: bool = False
    task_manager_stopped: bool = False
    scheduler_stopped: bool = False
    worker_pool_draining: bool = False
    worker_pool_stopped: bool = False
    database_closed: bool = False
    completed: bool = False


class ShutdownCoordinator:
    """
    Coordinates graceful shutdown across all components.

    Handles SIGTERM/SIGINT signals and orchestrates the shutdown sequence
    according to the architecture specification.
    """

    def __init__(
        self,
        settings: AppSettings,
        task_manager: TaskManager | None = None,
        scheduler: RefreshScheduler | None = None,
        worker_pool: WorkerPool | None = None,
    ) -> None:
        self._settings = settings
        self._task_manager = task_manager
        self._scheduler = scheduler
        self._worker_pool = worker_pool
        self._state = ShutdownState()
        self._shutdown_event = asyncio.Event()

    @property
    def is_shutting_down(self) -> bool:
        """Check if shutdown has been initiated."""
        return self._state.initiated

    @property
    def is_shutdown_complete(self) -> bool:
        """Check if shutdown has completed."""
        return self._state.completed

    def install_signal_handlers(self) -> None:
        """Install signal handlers for SIGTERM and SIGINT."""
        global _signal_handlers_installed
        if _signal_handlers_installed:
            return

        loop = asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig, lambda s=sig: asyncio.create_task(self._on_signal(s))  # type: ignore[misc]
                )
            except NotImplementedError:
                # Windows doesn't support add_signal_handler for SIGTERM/SIGINT
                # We'll rely on the lifespan context manager for cleanup
                logger.warning(
                    "Signal handler not supported on this platform for %s", sig
                )

        _signal_handlers_installed = True

    async def _on_signal(self, sig: int) -> None:
        """Handle shutdown signal."""
        if self._state.initiated:
            logger.warning("Shutdown already initiated, ignoring signal %s", sig)
            return

        log_event(
            logger,
            "shutdown.signal_received",
            level=logging.INFO,
            signal=sig,
        )
        self._state.initiated = True
        self._state.signal = sig
        self._shutdown_event.set()

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()

    async def shutdown(self, database: Any = None) -> None:
        """Execute the full graceful shutdown sequence."""
        if not self._state.initiated:
            log_event(
                logger,
                "shutdown.initiated",
                level=logging.INFO,
                reason="explicit",
            )
            self._state.initiated = True

        grace_seconds = self._settings.shutdown_grace_seconds

        # 1. Close API - reject new requests with 503
        log_event(
            logger,
            "shutdown.api_closed",
            level=logging.INFO,
        )
        self._state.api_closed = True

        # 2. Stop Task Manager polling
        if self._task_manager is not None:
            log_event(
                logger,
                "shutdown.task_manager_stopped",
                level=logging.INFO,
            )
            await self._task_manager.stop()
            self._state.task_manager_stopped = True

        # 2. Stop Scheduler
        if self._scheduler is not None:
            log_event(
                logger,
                "shutdown.scheduler_stopped",
                level=logging.INFO,
            )
            await self._scheduler.stop()
            self._state.scheduler_stopped = True

        # 3. Drain Worker Pool
        if self._worker_pool is not None:
            log_event(
                logger,
                "shutdown.worker_pool_draining",
                level=logging.INFO,
                grace_seconds=grace_seconds,
            )
            self._state.worker_pool_draining = True

            try:
                await asyncio.wait_for(
                    self._worker_pool.stop(graceful=True),
                    timeout=grace_seconds,
                )
            except TimeoutError:
                log_event(
                    logger,
                    "shutdown.timeout",
                    level=logging.WARNING,
                    message="Worker pool drain timed out, forcing stop",
                    grace_seconds=grace_seconds,
                )
                await self._worker_pool.stop(graceful=False)

            log_event(
                logger,
                "shutdown.worker_pool_stopped",
                level=logging.INFO,
            )
            self._state.worker_pool_stopped = True

        # 4. Close Database
        if database is not None:
            log_event(
                logger,
                "shutdown.database_closed",
                level=logging.INFO,
            )
            await database.close()
            self._state.database_closed = True

        log_event(
            logger,
            "shutdown.completed",
            level=logging.INFO,
        )
        self._state.completed = True


# Global shutdown coordinator
_shutdown_coordinator: "ShutdownCoordinator | None" = None


def get_shutdown_coordinator() -> ShutdownCoordinator | None:
    """Get the global shutdown coordinator."""
    return _shutdown_coordinator


def set_shutdown_coordinator(
    coordinator: ShutdownCoordinator | None,
) -> None:
    """Set the global shutdown coordinator."""
    global _shutdown_coordinator
    _shutdown_coordinator = coordinator


def is_shutting_down() -> bool:
    """Check if the application is shutting down."""
    coordinator = get_shutdown_coordinator()
    return coordinator.is_shutting_down if coordinator else False
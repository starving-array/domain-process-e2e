import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from domain_processing_service.config import AppSettings
from domain_processing_service.database import DatabaseLifecycle, SqlAlchemyDatabase
from domain_processing_service.logging import configure_logging, log_event
from domain_processing_service.manager import TaskManager
from domain_processing_service.metrics import (
    MetricsCollector,
    MetricsMiddleware,
    set_metrics,
)
from domain_processing_service.middleware import RequestIDMiddleware
from domain_processing_service.routes import router as jobs_router
from domain_processing_service.scheduler import RefreshScheduler
from domain_processing_service.shutdown import ShutdownCoordinator
from domain_processing_service.worker import WorkerPool
from domain_processing_service.worker.worker import create_phase9_task_handler

logger = logging.getLogger(__name__)


def create_app(
    settings: AppSettings | None = None,
    database: DatabaseLifecycle | None = None,
    *,
    enable_worker_pool: bool = True,
    enable_scheduler: bool = True,
    task_handler: Callable[[Any, Any], Any] | None = None,
) -> FastAPI:
    app_settings = settings or AppSettings()
    app_database = database or SqlAlchemyDatabase(app_settings)
    configure_logging(app_settings.log_level)

    # Create session maker for the Task Manager, Worker Pool, and Scheduler
    session_maker = app_database.session_maker

    # Create Worker Pool, Task Manager, and Scheduler only if enabled
    worker_pool: WorkerPool | None = None
    task_manager: TaskManager | None = None
    scheduler: RefreshScheduler | None = None

    # Create singleton DomainLockManager for Redis locking across all workers
    from domain_processing_service.domain_lock import DomainLockManager
    domain_lock_manager = DomainLockManager(app_settings)

    # Create metrics collector
    metrics = MetricsCollector()
    set_metrics(metrics)

    # Create shutdown coordinator
    shutdown_coordinator = ShutdownCoordinator(
        settings=app_settings,
        task_manager=None,  # Will be set below
        scheduler=None,  # Will be set below
        worker_pool=worker_pool,
    )
    # Update references after creation
    shutdown_coordinator._task_manager = None  # Will be set after task_manager creation
    shutdown_coordinator._scheduler = None  # Will be set after scheduler creation

    if enable_worker_pool:
        # Use Phase 9 task handler with shared domain_lock_manager if no custom handler provided
        actual_task_handler = task_handler or create_phase9_task_handler(
            app_settings, domain_lock_manager=domain_lock_manager
        )

        worker_pool = WorkerPool(
            session_maker=session_maker,
            settings=app_settings,
            task_handler=actual_task_handler,
        )

        task_manager = TaskManager(
            session_maker=session_maker,
            settings=app_settings,
            worker_pool=worker_pool,
        )

        # Update shutdown coordinator references
        shutdown_coordinator._task_manager = task_manager
        shutdown_coordinator._worker_pool = worker_pool

        # Create scheduler with worker pool reference for backpressure
        if enable_scheduler:
            scheduler = RefreshScheduler(
                session_maker=session_maker,
                settings=app_settings,
                worker_pool=worker_pool,
            )
            shutdown_coordinator._scheduler = scheduler

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        log_event(logger, "app.startup.started")
        await app_database.connect()
        try:
            await domain_lock_manager.connect()
        except Exception as e:
            log_event(
                logger,
                "redis.connection_failed",
                level=logging.WARNING,
                error=str(e),
                error_type=type(e).__name__,
            )

        if worker_pool is not None:
            await worker_pool.start()

        if task_manager is not None:
            await task_manager.start()

        if scheduler is not None:
            await scheduler.start()

        # Install signal handlers after startup
        shutdown_coordinator.install_signal_handlers()

        log_event(logger, "app.startup.completed", dependency="postgresql")
        try:
            yield
        finally:
            log_event(logger, "app.shutdown.started")
            await shutdown_coordinator.shutdown(database=app_database)
            try:
                await domain_lock_manager.close()
            except Exception as e:
                log_event(
                    logger,
                    "redis.close_failed",
                    level=logging.WARNING,
                    error=str(e),
                    error_type=type(e).__name__,
                )

    app = FastAPI(title=app_settings.app_name, lifespan=lifespan)

    # Add metrics middleware (must be before RequestIDMiddleware to capture all requests)
    app.add_middleware(MetricsMiddleware)
    app.add_middleware(RequestIDMiddleware)

    app.state.settings = app_settings
    app.state.database = app_database
    app.state.domain_lock_manager = domain_lock_manager
    app.state.shutdown_coordinator = shutdown_coordinator
    if worker_pool is not None:
        app.state.worker_pool = worker_pool
    if task_manager is not None:
        app.state.task_manager = task_manager
    if scheduler is not None:
        app.state.scheduler = scheduler
    app.include_router(jobs_router)

    @app.get("/health/live")
    async def health_live(request: Request) -> dict[str, str]:
        log_event(
            logger,
            "health.live.checked",
            request_id=request.state.request_id,
            result="live",
            reason="process_running",
        )
        return {
            "status": "live",
            "service": app_settings.app_name,
            "request_id": request.state.request_id,
        }

    @app.get("/health/ready")
    async def health_ready(request: Request) -> JSONResponse:
        ready = await app_database.is_ready()
        status_code = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
        log_event(
            logger,
            "health.ready.checked",
            request_id=request.state.request_id,
            result="ready" if ready else "not_ready",
            reason="postgresql_available" if ready else "postgresql_unavailable",
            dependency="postgresql",
        )
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "ready" if ready else "not_ready",
                "service": app_settings.app_name,
                "dependencies": {"postgresql": "ok" if ready else "unavailable"},
                "request_id": request.state.request_id,
            },
        )

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        """Prometheus metrics endpoint."""
        metrics_data = generate_latest()
        return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)

    return app

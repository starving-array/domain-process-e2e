import json
import logging
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any, Literal

from domain_processing_service.config import LogLevel

request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)
LogEventName = Literal[
    "app.startup.started",
    "app.startup.completed",
    "app.shutdown.started",
    "app.shutdown.completed",
    "request.started",
    "request.completed",
    "request.failed",
    "health.live.checked",
    "health.ready.checked",
    "database.connection.started",
    "database.connection.ready",
    "database.connection.failed",
    "database.migration.started",
    "database.migration.completed",
    "database.migration.failed",
    "request.received",
    "request.validated",
    "request.completed",
    "domain.normalization.started",
    "domain.normalized",
    "domain.validation.failed",
    "domain.deduplicated",
    "job.creation.started",
    "job.creation.committed",
    "job.created",
    "tasks.created",
    "task_manager.started",
    "task_manager.stopped",
    "task_manager.cycle_failed",
    "task_manager.claim_attempt",
    "task_manager.tasks_claimed",
    "task_manager.no_tasks_available",
    "worker_pool.started",
    "worker_pool.stopped",
    "worker.started",
    "worker.stopped",
    "worker.task_received",
    "worker.task_completed",
    "worker.task_failed",
    "worker_pool.queue_full",
    "worker_pool.queue_space_available",
    "task_manager.no_worker_pool",
    "worker_pool.task_dispatched",
    "worker_pool.queue_full_on_dispatch",
    "task_manager.recovery_cycle_failed",
    "task_manager.recovery_no_worker_pool",
    "task_manager.recovery_attempt",
    "task_manager.tasks_recovered",
    "task_manager.task_failed_max_attempts",
    "task_manager.no_expired_tasks",
    "domain_processing.started",
    "domain_processing.domain_resolved",
    "domain_processing.fresh_detail_reused",
    "domain_processing.needs_processing",
    "domain_processing.lock_contention",
    "domain_processing.fresh_after_lock",
    "domain_processing.lock_acquired",
    "domain_processing.dns_resolved",
    "domain_processing.dns_permanent_failure",
    "domain_processing.dns_transient_failure",
    "domain_processing.ssrf_rejected",
    "domain_processing.ip_validated",
    "domain_processing.http_completed",
    "domain_processing.http_transient_failure",
    "domain_processing.http_permanent_failure",
    "domain_processing.http_completed",
    "domain_processing.completed",
    "scheduler.started",
    "scheduler.stopped",
    "scheduler.tick_started",
    "scheduler.tick_completed",
    "scheduler.refresh_candidates_discovered",
    "scheduler.refresh_candidate_skipped",
    "scheduler.refresh_task_created",
    "scheduler.refresh_task_skipped_duplicate",
    "scheduler.backpressure",
    "scheduler.error",
    "scheduler.shutdown",
    "idempotency.request_received",
    "idempotency.hit",
    "idempotency.conflict",
    "idempotency.record_created",
    "occ.update_attempted",
    "occ.conflict",
    "occ.update_succeeded",
    "domain.deactivated",
    "domain.reactivated",
    "domain.inactive_skipped",
    "metrics.exported",
    "metrics.collection_failed",
    "shutdown.signal_received",
    "shutdown.initiated",
    "shutdown.api_closed",
    "shutdown.task_manager_stopped",
    "shutdown.scheduler_stopped",
    "shutdown.worker_pool_draining",
    "shutdown.worker_pool_stopped",
    "shutdown.database_closed",
    "shutdown.completed",
    "shutdown.timeout",
]

_LOG_RECORD_RESERVED = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", record.getMessage()),
            "request_id": getattr(record, "request_id", None),
        }

        for key, value in record.__dict__.items():
            if key not in _LOG_RECORD_RESERVED and key not in payload:
                payload[key] = value

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, separators=(",", ":"))


class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = request_id_context.get()
        return True


def configure_logging(log_level: LogLevel) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestContextFilter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)


def log_event(
    logger: logging.Logger,
    event: LogEventName,
    *,
    level: int = logging.INFO,
    **fields: object,
) -> None:
    """Emit one stable structured lifecycle event."""
    logger.log(level, event, extra={"event": event, **fields})

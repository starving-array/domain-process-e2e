"""Metrics collection and export for Phase 13."""

import logging
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from domain_processing_service.logging import log_event


@dataclass
class MetricsCollector:
    """
    Collects and exports metrics for the domain processing service.

    All metrics use low-cardinality labels per architecture requirements.
    High-cardinality labels (domain, job_id, task_id, request_id, url) are FORBIDDEN.
    """

    # Allow custom registry for testing
    _registry: CollectorRegistry = field(default_factory=CollectorRegistry, init=False, repr=False)

    # API Metrics
    api_requests_total: Counter = field(init=False)
    api_latency_seconds: Histogram = field(init=False)
    api_errors_total: Counter = field(init=False)

    # Task Metrics
    tasks_pending_total: Gauge = field(init=False)
    tasks_processing_total: Gauge = field(init=False)
    tasks_completed_total: Counter = field(init=False)
    task_retry_total: Counter = field(init=False)

    # Worker Metrics
    worker_queue_depth: Gauge = field(init=False)
    worker_active_count: Gauge = field(init=False)

    # Domain Metrics
    domain_dns_latency: Histogram = field(init=False)
    domain_http_latency: Histogram = field(init=False)
    domain_ssrf_rejections_total: Counter = field(init=False)

    # Infrastructure Metrics
    db_pool_utilization: Gauge = field(init=False)
    redis_lock_contention_total: Counter = field(init=False)

    def __post_init__(self) -> None:
        # API Metrics
        self.api_requests_total = Counter(
            "api_requests_total",
            "Total number of API requests",
            ["method", "endpoint", "status_code"],
            registry=self._registry,
        )

        self.api_latency_seconds = Histogram(
            "api_latency_seconds",
            "API request latency in seconds",
            ["method", "endpoint"],
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
            registry=self._registry,
        )

        self.api_errors_total = Counter(
            "api_errors_total",
            "Total number of API errors",
            ["method", "endpoint", "error_type"],
            registry=self._registry,
        )

        # Task Metrics
        self.tasks_pending_total = Gauge(
            "tasks_pending_total",
            "Number of pending tasks",
            ["task_type"],
            registry=self._registry,
        )

        self.tasks_processing_total = Gauge(
            "tasks_processing_total",
            "Number of tasks currently being processed",
            ["task_type"],
            registry=self._registry,
        )

        self.tasks_completed_total = Counter(
            "tasks_completed_total",
            "Total number of completed tasks",
            ["task_type", "status"],
            registry=self._registry,
        )

        self.task_retry_total = Counter(
            "task_retry_total",
            "Total number of task retries",
            ["task_type", "retry_reason"],
            registry=self._registry,
        )

        # Worker Metrics
        self.worker_queue_depth = Gauge(
            "worker_queue_depth",
            "Current number of tasks in the worker queue",
            registry=self._registry,
        )

        self.worker_active_count = Gauge(
            "worker_active_count",
            "Number of workers currently processing tasks",
            registry=self._registry,
        )

        # Domain Metrics
        self.domain_dns_latency = Histogram(
            "domain_dns_latency",
            "DNS resolution latency in seconds",
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
            registry=self._registry,
        )

        self.domain_http_latency = Histogram(
            "domain_http_latency",
            "HTTP probe latency in seconds",
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0),
            registry=self._registry,
        )

        self.domain_ssrf_rejections_total = Counter(
            "domain_ssrf_rejections_total",
            "Total number of SSRF rejections",
            ["rejection_reason"],
            registry=self._registry,
        )

        # Infrastructure Metrics
        self.db_pool_utilization = Gauge(
            "db_pool_utilization",
            "Database connection pool utilization (0.0 to 1.0)",
            registry=self._registry,
        )

        self.redis_lock_contention_total = Counter(
            "redis_lock_contention_total",
            "Total number of Redis lock contentions",
            registry=self._registry,
        )

    @property
    def registry(self) -> CollectorRegistry:
        """Get the collector registry for metric export."""
        return self._registry

    def record_api_request(
        self, method: str, endpoint: str, status_code: int, latency_seconds: float
    ) -> None:
        """Record an API request."""
        self.api_requests_total.labels(
            method=method, endpoint=endpoint, status_code=str(status_code)
        ).inc()
        self.api_latency_seconds.labels(method=method, endpoint=endpoint).observe(
            latency_seconds
        )

    def record_api_error(self, method: str, endpoint: str, error_type: str) -> None:
        """Record an API error."""
        self.api_errors_total.labels(
            method=method, endpoint=endpoint, error_type=error_type
        ).inc()


# Global metrics instance (uses default registry for production)
_metrics: MetricsCollector | None = None
_metrics_lock = Lock()


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _metrics
    with _metrics_lock:
        if _metrics is None:
            _metrics = MetricsCollector()
        return _metrics


def set_metrics(metrics: MetricsCollector | None) -> None:
    """Set the global metrics collector (for testing)."""
    global _metrics
    with _metrics_lock:
        _metrics = metrics


async def export_metrics() -> bytes:
    """Export metrics in Prometheus format."""
    metrics = get_metrics()
    logger = logging.getLogger(__name__)
    log_event(
        logger,
        "metrics.exported",
        level=20,  # INFO
    )
    return generate_latest(metrics.registry)


class MetricsMiddleware:
    """ASGI middleware for collecting API metrics."""

    def __init__(self, app: Any) -> None:
        self.app = app
        self.metrics = get_metrics()

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.perf_counter()
        method = scope.get("method", "UNKNOWN")
        path = scope.get("path", "/")

        async def send_wrapper(message: Any) -> None:
            if message["type"] == "http.response.start":
                status_code = message.get("status", 500)
                latency = time.perf_counter() - start_time
                self.metrics.record_api_request(method, path, status_code, latency)
                if 400 <= status_code < 600:
                    error_type = "client_error" if status_code < 500 else "server_error"
                    self.metrics.record_api_error(method, path, error_type)
            await send(message)

        await self.app(scope, receive, send_wrapper)
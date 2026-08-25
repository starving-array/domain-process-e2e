import json
import logging

import pytest

from domain_processing_service.logging import JsonFormatter, log_event, request_id_context


def test_json_formatter_includes_structured_event_and_correlation_fields() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request.completed",
        args=(),
        exc_info=None,
    )
    record.event = "request.completed"
    record.request_id = "req-test"
    record.job_id = "job-test"

    payload = json.loads(formatter.format(record))

    assert payload["event"] == "request.completed"
    assert payload["request_id"] == "req-test"
    assert payload["job_id"] == "job-test"
    assert "timestamp" in payload
    assert payload["level"] == "INFO"


def test_json_formatter_omits_task_name_and_null_fields() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test.worker",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="worker.task_received",
        args=(),
        exc_info=None,
    )
    record.event = "worker.task_received"
    record.taskName = "Task-54"  # Leaked asyncio task name
    record.job_id = None  # Null field should be omitted
    record.task_id = "task-123"
    record.attempt = 1

    payload = json.loads(formatter.format(record))

    assert "taskName" not in payload
    assert "job_id" not in payload
    assert "request_id" not in payload
    assert payload["task_id"] == "task-123"
    assert payload["attempt"] == 1


def test_dns_record_lookup_failed_has_clean_structure() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="domain_processing_service.dns",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="dns.record_lookup_failed",
        args=(),
        exc_info=None,
    )
    record.event = "dns.record_lookup_failed"
    record.record_type = "AAAA"
    record.domain = "github.com"
    record.error_code = 11
    record.error = "Could not contact DNS servers"
    record.error_type = "DNSError"

    payload = json.loads(formatter.format(record))

    assert payload["event"] == "dns.record_lookup_failed"
    assert payload["record_type"] == "AAAA"
    assert payload["domain"] == "github.com"
    assert payload["error_code"] == 11
    assert payload["error"] == "Could not contact DNS servers"
    assert payload["error_type"] == "DNSError"
    assert "taskName" not in payload


def test_domain_processing_rescheduled_and_max_attempts_schema() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="domain_processing_service.processor",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="domain_processing.rescheduled",
        args=(),
        exc_info=None,
    )
    record.event = "domain_processing.rescheduled"
    record.task_id = "task-abc"
    record.job_id = "job-xyz"
    record.domain = "example.com"
    record.attempt = 1
    record.retry_delay_seconds = 73
    record.error_code = "TRANSIENT_ERROR"

    payload = json.loads(formatter.format(record))

    assert payload["event"] == "domain_processing.rescheduled"
    assert payload["task_id"] == "task-abc"
    assert payload["job_id"] == "job-xyz"
    assert payload["domain"] == "example.com"
    assert payload["attempt"] == 1
    assert payload["retry_delay_seconds"] == 73


def test_log_event_uses_request_context_when_request_id_not_explicit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = request_id_context.set("req-context")
    logger = logging.getLogger("domain-processing-service-test")
    logger.addFilter(logging.Filter())
    try:
        with caplog.at_level(logging.INFO):
            log_event(logger, "request.started", http_method="GET", path="/health/live")
    finally:
        request_id_context.reset(token)

    assert caplog.records[0].__dict__["event"] == "request.started"


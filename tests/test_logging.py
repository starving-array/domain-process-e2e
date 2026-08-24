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

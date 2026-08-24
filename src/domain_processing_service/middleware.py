import logging
from collections.abc import Awaitable, Callable
from time import perf_counter
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from domain_processing_service.logging import log_event, request_id_context

REQUEST_ID_HEADER = "X-Request-ID"
logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or f"req-{uuid4()}"
        token = request_id_context.set(request_id)
        request.state.request_id = request_id
        started_at = perf_counter()
        log_event(
            logger,
            "request.started",
            request_id=request_id,
            http_method=request.method,
            path=request.url.path,
        )
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((perf_counter() - started_at) * 1000, 3)
            log_event(
                logger,
                "request.failed",
                level=logging.ERROR,
                request_id=request_id,
                http_method=request.method,
                path=request.url.path,
                duration_ms=duration_ms,
                reason="unhandled_exception",
            )
            raise
        else:
            duration_ms = round((perf_counter() - started_at) * 1000, 3)
            log_event(
                logger,
                "request.completed",
                request_id=request_id,
                http_method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=duration_ms,
            )
            response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            request_id_context.reset(token)

"""Request ID assignment, request/response logging, and latency recording.

One middleware covers all three because they share the same before/after
shape around `call_next` (I-018 later binds this same `request_id` into
structlog's context and threads it into the vote queue payload — see
I-018's design for the full cross-process correlation story).

The incoming-request log line deliberately omits the token itself, even
though the spec's middleware stack describes logging "method, path,
token" — logging a live bearer token is exactly the credential-leak risk
I-018 calls out explicitly for the same codebase, so only its presence is
noted here, never its value.
"""

import json
import logging
import random
import time
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from src.metrics.latency import record_latency

logger = logging.getLogger("poll_app.api")

_LOG_SAMPLE_RATE = 1 / 1000


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or f"req-{uuid4().hex[:12]}"
        request.state.request_id = request_id

        sampled = random.random() < _LOG_SAMPLE_RATE
        if sampled:
            logger.info(
                json.dumps(
                    {
                        "event": "request_received",
                        "request_id": request_id,
                        "method": request.method,
                        "path": request.url.path,
                        "has_auth_header": "authorization" in request.headers,
                    }
                )
            )

        start = time.monotonic()
        response = await call_next(request)
        latency_ms = (time.monotonic() - start) * 1000

        route = request.scope.get("route")
        route_path = route.path if route else request.url.path
        record_latency(route_path, latency_ms)

        response.headers["X-Request-ID"] = request_id

        is_error = response.status_code >= 400
        if is_error or sampled:
            logger.info(
                json.dumps(
                    {
                        "event": "request_completed",
                        "request_id": request_id,
                        "method": request.method,
                        "route": route_path,
                        "status_code": response.status_code,
                        "latency_ms": round(latency_ms, 2),
                    }
                )
            )

        return response

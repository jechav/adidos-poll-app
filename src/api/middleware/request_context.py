"""Request ID assignment, request/response logging, and latency recording.

One middleware covers all three because they share the same before/after
shape around `call_next`.

I-018 binds `request_id` into `structlog.contextvars` here, for the
lifetime of the request: every log call made anywhere downstream (route
handlers, services) during that request automatically carries it via the
shared `merge_contextvars` processor (`src/logging/config.py`), without
having to pass `request_id` explicitly at each call site. The same value
is also threaded into the `VotePayload` pushed onto `queue:votes`
(`src/api/routes/user.py`) so the worker (I-008) can re-bind it and
continue the same trace — see `docs/architecture/logging.md`.

This middleware's own `request_received`/`request_completed` lines are a
generic, route-agnostic 1-in-1000 sample (there's no `vote_id` at this
layer to sample deterministically by); the vote-specific lifecycle events
(`vote_accepted`, `vote_dequeued`, `vote_written`, ...) use
`src.logging.sampling.is_sampled` instead.

The incoming-request log line deliberately omits the token itself, even
though the spec's middleware stack describes logging "method, path,
token" — logging a live bearer token is exactly the credential-leak risk
I-018 calls out explicitly for the same codebase, so only its presence is
noted here, never its value.
"""

import random
import time
from uuid import uuid4

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from src.metrics.registry import record_request

logger = structlog.get_logger("poll_app.api")

_LOG_SAMPLE_RATE = 1 / 1000


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or f"req-{uuid4().hex[:12]}"
        request.state.request_id = request_id

        # Cleared in `finally` so this contextvar can never leak into the
        # next request handled by the same worker process/task.
        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            sampled = random.random() < _LOG_SAMPLE_RATE
            if sampled:
                logger.info(
                    "request_received",
                    method=request.method,
                    path=request.url.path,
                    has_auth_header="authorization" in request.headers,
                )

            start = time.monotonic()
            response = await call_next(request)
            latency_ms = (time.monotonic() - start) * 1000

            route = request.scope.get("route")
            # Labeled by the matched route, not the raw path, to avoid
            # unbounded cardinality from path parameters (I-017) — a request
            # that never matched a route (404) is labeled "unmatched" rather
            # than leaking the arbitrary requested path into a metric label.
            route_path = route.path if route else request.url.path
            record_request(
                route=route.path if route else "unmatched",
                method=request.method,
                status_code=response.status_code,
                latency_s=latency_ms / 1000,
            )

            response.headers["X-Request-ID"] = request_id

            is_error = response.status_code >= 400
            if is_error or sampled:
                logger.info(
                    "request_completed",
                    method=request.method,
                    route=route_path,
                    status_code=response.status_code,
                    latency_ms=round(latency_ms, 2),
                )

            return response
        finally:
            structlog.contextvars.unbind_contextvars("request_id")

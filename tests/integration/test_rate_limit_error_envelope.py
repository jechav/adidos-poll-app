"""Integration test: I-007's exceptions through I-003's error envelope.

I-005 (the `POST /v1/vote` handler that will actually call
`check_rate_limit`) doesn't exist yet, so this exercises the same
exception-handling wiring (`register_exception_handlers`) a real vote
route runs through, against a minimal throwaway route that raises the
same exceptions -- confirming the 429/503 envelope and `Retry-After`
header I-007's acceptance criteria require, independent of I-005.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.middleware.error_handler import register_exception_handlers
from src.services.rate_limit import (
    RateLimitExceededError,
    RateLimitServiceUnavailableError,
)


@pytest.fixture
def client():
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/probe/rate-limited")
    async def rate_limited():
        raise RateLimitExceededError("User rate limit exceeded", retry_after=60)

    @app.get("/probe/redis-down")
    async def redis_down():
        raise RateLimitServiceUnavailableError()

    return TestClient(app, raise_server_exceptions=False)


def test_rate_limit_exceeded_returns_429_with_envelope_and_retry_after(client):
    response = client.get("/probe/rate-limited")

    assert response.status_code == 429
    assert response.headers["retry-after"] == "60"

    body = response.json()
    assert body["success"] is False
    assert body["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert body["error"]["message"] == "User rate limit exceeded"
    assert body["error"]["details"] == {"retry_after_seconds": 60}
    assert "timestamp" in body["meta"]


def test_redis_unavailable_returns_503_not_a_bypass(client):
    response = client.get("/probe/redis-down")

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["error"]["code"] == "SERVICE_UNAVAILABLE"

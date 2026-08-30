"""Unit tests for I-018's structlog wiring into I-003's request-ID
middleware: binding `request_id` into `structlog.contextvars` so every
log call made during a request automatically carries it, rendered as
single-line JSON via the shared `configure_logging()` pipeline, and
cleared afterward so it never leaks into the next request handled by the
same worker process.
"""

import json

import pytest
import structlog
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.api.middleware.error_handler import register_exception_handlers
from src.api.middleware.request_context import RequestContextMiddleware
from src.logging.config import configure_logging

_HANDLER_LOGGER = structlog.get_logger("test.handler")


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    register_exception_handlers(app)

    @app.get("/ok")
    async def ok():
        _HANDLER_LOGGER.info("inside_handler")
        return {"status": "ok"}

    @app.get("/boom")
    async def boom():
        raise ValueError("boom")

    return app


@pytest.fixture
async def client():
    configure_logging()
    transport = ASGITransport(app=_build_app())
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def _json_lines(capsys):
    out = capsys.readouterr().out.strip()
    return [json.loads(line) for line in out.splitlines() if line.strip()]


@pytest.mark.asyncio
async def test_downstream_log_calls_automatically_carry_the_request_id(client, capsys):
    resp = await client.get("/ok", headers={"x-request-id": "req-fixed-abc"})

    assert resp.status_code == 200
    lines = _json_lines(capsys)
    handler_lines = [line for line in lines if line["event"] == "inside_handler"]
    assert len(handler_lines) == 1
    assert handler_lines[0]["request_id"] == "req-fixed-abc"


@pytest.mark.asyncio
async def test_contextvars_do_not_leak_across_requests(client, capsys):
    await client.get("/ok", headers={"x-request-id": "req-first"})
    capsys.readouterr()  # discard first request's output

    await client.get("/ok", headers={"x-request-id": "req-second"})
    lines = _json_lines(capsys)
    handler_lines = [line for line in lines if line["event"] == "inside_handler"]
    assert len(handler_lines) == 1
    assert handler_lines[0]["request_id"] == "req-second"


@pytest.mark.asyncio
async def test_unhandled_exception_is_logged_at_100_percent_with_the_request_id(client, capsys):
    """Starlette routes a bare `Exception` handler onto `ServerErrorMiddleware`,
    which sits *outside* user-added middleware — so an unhandled exception
    never reaches `RequestContextMiddleware`'s own post-`call_next`
    logging. `error_handler.py`'s catch-all handler is what actually
    guarantees this case is logged at 100%, independent of sampling.
    """
    # httpx's ASGITransport re-raises the server-side exception after the
    # response has already been sent (matching a real ASGI server logging
    # the exception while still returning 500 to the client) — the log
    # line under test is emitted before that re-raise.
    with pytest.raises(ValueError):
        await client.get("/boom", headers={"x-request-id": "req-error-case"})

    lines = _json_lines(capsys)
    error_lines = [line for line in lines if line["event"] == "unhandled_exception"]
    assert len(error_lines) == 1
    assert error_lines[0]["request_id"] == "req-error-case"
    assert error_lines[0]["level"] == "error"

"""Integration tests for I-006's exception-handler wiring into I-003's
error envelope. No live route raises these yet (I-005 doesn't exist), so
this exercises a minimal standalone app with the same
`register_exception_handlers` used by the real app, mirroring
tests/integration/test_api_routes.py's ASGI-request style.
"""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.api.middleware.error_handler import register_exception_handlers
from src.api.middleware.request_context import RequestContextMiddleware
from src.services.uniqueness import DuplicateVoteError, UniquenessCheckUnavailableError


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    register_exception_handlers(app)

    @app.get("/raise-duplicate")
    async def raise_duplicate():
        raise DuplicateVoteError("user-1", "poll-1")

    @app.get("/raise-unavailable")
    async def raise_unavailable():
        raise UniquenessCheckUnavailableError("user-1", "poll-1", ConnectionError("down"))

    return app


@pytest.fixture
async def client():
    transport = ASGITransport(app=_build_app())
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_duplicate_vote_error_maps_to_409(client):
    resp = await client.get("/raise-duplicate")
    assert resp.status_code == 409
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "DUPLICATE_VOTE"


@pytest.mark.asyncio
async def test_uniqueness_unavailable_error_maps_to_503(client):
    resp = await client.get("/raise-unavailable")
    assert resp.status_code == 503
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "SERVICE_UNAVAILABLE"

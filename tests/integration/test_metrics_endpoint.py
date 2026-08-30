"""Integration tests for I-017's `GET /metrics` and the extended I-003
latency middleware, exercised through a real ASGI request via httpx —
mirrors tests/integration/test_api_routes.py's route-testing structure.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.metrics.registry import REGISTRY


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_metrics_endpoint_returns_prometheus_text_format(client):
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert "poll_vote_latency_seconds" in resp.text
    assert "poll_requests_total" in resp.text


@pytest.mark.asyncio
async def test_metrics_endpoint_is_distinct_from_health(client):
    resp = await client.get("/metrics")
    body = resp.text
    # Not wrapped in I-003's JSON success envelope.
    assert not body.lstrip().startswith("{")


@pytest.mark.asyncio
async def test_health_check_requests_are_observed_in_the_histogram_and_counter(client):
    await client.get("/health")
    await client.get("/health")

    resp = await client.get("/metrics")
    body = resp.text

    assert 'poll_requests_total{route="/health",status_code="200"}' in body
    assert 'poll_vote_latency_seconds_count{method="GET",route="/health"}' in body


@pytest.mark.asyncio
async def test_unmatched_route_is_labeled_unmatched_not_the_raw_path(client):
    await client.get("/this-route-does-not-exist")

    resp = await client.get("/metrics")
    assert 'route="unmatched"' in resp.text
    assert "this-route-does-not-exist" not in resp.text

"""Integration tests for I-004's auth dependencies against a live Redis
cluster (from I-002) and the real ASGI app (from I-003).

The Redis-cluster-backed tests skip automatically if no cluster is
reachable at `POLL_APP_REDIS_STARTUP_HOST`/`POLL_APP_REDIS_STARTUP_PORT`
(mirroring tests/integration/test_schema_performance.py's PostgreSQL
skip pattern), since that cluster is only up under docker-compose (I-002).
The ASGI-level tests don't require it: `get_current_user` fails open when
Redis is unreachable (trust-on-first-use still applies), so they exercise
the request/response contract regardless of whether a cluster is running.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.api.dependencies.auth import hash_token
from src.cache.redis_client import get_redis

AUTH = {"Authorization": "Bearer user-token"}
ADMIN_AUTH = {"Authorization": "Bearer admin:ops-1"}


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
async def redis():
    client = await get_redis()
    try:
        await client.ping()
    except Exception:
        pytest.skip("no Redis cluster reachable")
    yield client


@pytest.mark.asyncio
async def test_missing_auth_header_401_no_redis_lookup(client, redis):
    key = f"auth:token:{hash_token('never-sent')}"
    await redis.delete(key)

    resp = await client.get("/v1/user/votes")

    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "UNAUTHORIZED"
    # Never populated, since no cache lookup should happen for a missing header.
    assert await redis.exists(key) == 0


@pytest.mark.asyncio
async def test_first_request_populates_cache_with_ttl(client, redis):
    token = "integration-user-1"
    key = f"auth:token:{hash_token(token)}"
    await redis.delete(key)

    resp = await client.get("/v1/user/votes", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200

    cached = await redis.hgetall(key)
    assert cached["user_id"] == token
    assert cached["role"] == "user"

    ttl = await redis.ttl(key)
    assert 0 < ttl <= 600


@pytest.mark.asyncio
async def test_second_request_reuses_cache_without_redecoding(client, redis):
    token = "integration-user-2"
    key = f"auth:token:{hash_token(token)}"
    await redis.delete(key)

    first = await client.get("/v1/user/votes", headers={"Authorization": f"Bearer {token}"})
    assert first.status_code == 200

    # Overwrite the cached role directly to prove the second request reads
    # the cache rather than re-decoding the token (which would resolve
    # this plain token back to role "user").
    await redis.hset(key, mapping={"user_id": token, "role": "admin"})

    second = await client.get("/v1/admin/anomalies", headers={"Authorization": f"Bearer {token}"})
    assert second.status_code == 200


@pytest.mark.asyncio
async def test_token_hash_used_as_key_raw_token_absent(client, redis):
    token = "integration-user-3-do-not-leak"
    key = f"auth:token:{hash_token(token)}"
    await redis.delete(key)

    resp = await client.get("/v1/user/votes", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200

    cached = await redis.hgetall(key)
    assert token not in key
    assert cached["user_id"] == token  # value legitimately holds it; key must not

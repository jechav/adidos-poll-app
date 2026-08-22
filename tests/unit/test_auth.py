from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.api.dependencies.auth import (
    decode_adidos_token,
    extract_bearer_token,
    get_current_user,
    hash_token,
    require_admin,
)


def _redis_mock(hgetall_return=None, hgetall_side_effect=None):
    redis = AsyncMock()
    if hgetall_side_effect is not None:
        redis.hgetall.side_effect = hgetall_side_effect
    else:
        redis.hgetall.return_value = hgetall_return or {}
    return redis


# --- Token extraction -------------------------------------------------


def test_extract_bearer_token_missing_header():
    with pytest.raises(HTTPException) as exc_info:
        extract_bearer_token(None)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail["code"] == "UNAUTHORIZED"


def test_extract_bearer_token_malformed_header():
    with pytest.raises(HTTPException) as exc_info:
        extract_bearer_token("Token abc123")
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail["code"] == "UNAUTHORIZED"


def test_extract_bearer_token_empty_bearer():
    with pytest.raises(HTTPException) as exc_info:
        extract_bearer_token("Bearer    ")
    assert exc_info.value.status_code == 401


def test_extract_bearer_token_valid_header():
    assert extract_bearer_token("Bearer abc123") == "abc123"


# --- Token decoding -----------------------------------------------------


def test_decode_adidos_token_regular_user():
    claims = decode_adidos_token("user-token")
    assert claims == {"user_id": "user-token", "role": "user"}


def test_decode_adidos_token_admin():
    claims = decode_adidos_token("admin:ops-1")
    assert claims == {"user_id": "ops-1", "role": "admin"}


def test_hash_token_is_sha256_hex_and_never_contains_raw_token():
    digest = hash_token("super-secret-token")
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)
    assert "super-secret-token" not in digest


def test_hash_token_is_deterministic():
    assert hash_token("abc") == hash_token("abc")
    assert hash_token("abc") != hash_token("abd")


# --- get_current_user: cache miss (trust-on-first-use) ------------------


@pytest.mark.asyncio
async def test_get_current_user_cache_miss_decodes_and_returns_regular_user():
    redis = _redis_mock(hgetall_return={})
    user = await get_current_user(token="user-token", redis=redis)
    assert user.user_id == "user-token"
    assert user.role == "user"


@pytest.mark.asyncio
async def test_get_current_user_cache_miss_decodes_and_returns_admin():
    redis = _redis_mock(hgetall_return={})
    user = await get_current_user(token="admin:ops-1", redis=redis)
    assert user.user_id == "ops-1"
    assert user.role == "admin"


@pytest.mark.asyncio
async def test_get_current_user_cache_miss_populates_cache_with_hash_key():
    redis = _redis_mock(hgetall_return={})
    await get_current_user(token="user-token", redis=redis)

    expected_key = f"auth:token:{hash_token('user-token')}"
    redis.hset.assert_awaited_once()
    args, kwargs = redis.hset.call_args
    assert args[0] == expected_key
    assert kwargs["mapping"] == {"user_id": "user-token", "role": "user"}
    # The raw token must never appear in what's written to Redis.
    assert "user-token" not in expected_key


@pytest.mark.asyncio
async def test_get_current_user_cache_miss_sets_ttl_within_5_to_10_minutes():
    redis = _redis_mock(hgetall_return={})
    await get_current_user(token="user-token", redis=redis)

    redis.expire.assert_awaited_once()
    args, _ = redis.expire.call_args
    ttl = args[1]
    assert 300 <= ttl <= 600


# --- get_current_user: cache hit -----------------------------------------


@pytest.mark.asyncio
async def test_get_current_user_cache_hit_uses_cached_values():
    redis = _redis_mock(hgetall_return={"user_id": "cached-user", "role": "admin"})
    user = await get_current_user(token="some-token", redis=redis)
    assert user.user_id == "cached-user"
    assert user.role == "admin"


@pytest.mark.asyncio
async def test_get_current_user_cache_hit_does_not_rewrite_cache():
    redis = _redis_mock(hgetall_return={"user_id": "cached-user", "role": "user"})
    await get_current_user(token="some-token", redis=redis)
    redis.hset.assert_not_awaited()
    redis.expire.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_current_user_cache_hit_looks_up_correct_key():
    redis = _redis_mock(hgetall_return={"user_id": "cached-user", "role": "user"})
    await get_current_user(token="some-token", redis=redis)
    expected_key = f"auth:token:{hash_token('some-token')}"
    redis.hgetall.assert_awaited_once_with(expected_key)


# --- get_current_user: Redis unavailable (fail open) ---------------------


@pytest.mark.asyncio
async def test_get_current_user_falls_back_when_redis_read_fails():
    redis = _redis_mock(hgetall_side_effect=ConnectionError("cluster unreachable"))
    user = await get_current_user(token="user-token", redis=redis)
    assert user.user_id == "user-token"
    assert user.role == "user"


@pytest.mark.asyncio
async def test_get_current_user_does_not_raise_when_redis_write_fails():
    redis = _redis_mock(hgetall_return={})
    redis.hset.side_effect = ConnectionError("cluster unreachable")
    user = await get_current_user(token="user-token", redis=redis)
    assert user.user_id == "user-token"


# --- require_admin --------------------------------------------------------


@pytest.mark.asyncio
async def test_require_admin_accepts_admin():
    redis = _redis_mock(hgetall_return={})
    user = await get_current_user(token="admin:ops-1", redis=redis)
    result = await require_admin(user=user)
    assert result.role == "admin"


@pytest.mark.asyncio
async def test_require_admin_rejects_regular_user():
    redis = _redis_mock(hgetall_return={})
    user = await get_current_user(token="user-token", redis=redis)
    with pytest.raises(HTTPException) as exc_info:
        await require_admin(user=user)
    assert exc_info.value.status_code == 403
    assert exc_info.value.detail["code"] == "FORBIDDEN"

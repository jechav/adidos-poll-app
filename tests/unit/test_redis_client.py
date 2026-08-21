import pytest
import pytest_asyncio

import src.cache.redis_client as redis_client_module
from src.cache.redis_client import get_redis, close_redis, redis_dependency
from src.config import Settings


@pytest_asyncio.fixture(autouse=True)
async def reset_singleton():
    redis_client_module._redis = None
    yield
    await close_redis()


@pytest.mark.asyncio
async def test_get_redis_returns_singleton():
    first = await get_redis()
    second = await get_redis()
    assert first is second


@pytest.mark.asyncio
async def test_redis_dependency_returns_same_singleton():
    direct = await get_redis()
    via_dependency = await redis_dependency()
    assert direct is via_dependency


@pytest.mark.asyncio
async def test_close_redis_resets_singleton():
    first = await get_redis()
    await close_redis()
    assert redis_client_module._redis is None

    second = await get_redis()
    assert second is not first


def test_settings_default_startup_node():
    settings = Settings(_env_file=None)
    assert settings.redis_startup_host == "localhost"
    assert settings.redis_startup_port == 7001


def test_settings_reads_env_prefix(monkeypatch):
    monkeypatch.setenv("POLL_APP_REDIS_STARTUP_HOST", "redis-node-1")
    monkeypatch.setenv("POLL_APP_REDIS_STARTUP_PORT", "7002")
    settings = Settings()
    assert settings.redis_startup_host == "redis-node-1"
    assert settings.redis_startup_port == 7002

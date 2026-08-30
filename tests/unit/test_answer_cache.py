"""Unit tests for I-009's answer-metadata cache read path, plus I-017's
hit/miss counters on that same path (`poll_redis_cache_hits_total` /
`poll_redis_cache_misses_total`, labeled `cache="answers"`).
"""

import json
from uuid import uuid4

import pytest
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

import src.cache.answer_cache as answer_cache_module
from src.cache.answer_cache import answers_cache_key, get_cached_answers
from src.metrics.registry import REGISTRY


def _counter_value(sample_name: str, cache: str) -> float:
    text = generate_latest(REGISTRY).decode("utf-8")
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == sample_name and s.labels.get("cache") == cache:
                return s.value
    return 0.0


class FakeRedis:
    def __init__(self, store: dict | None = None):
        self.store = store or {}
        self.set_calls: list[tuple] = []

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.store[key] = value
        self.set_calls.append((key, value, ex))


@pytest.fixture(autouse=True)
def fake_db_load(monkeypatch):
    """No real Postgres for the unit suite — a cache miss loads from this
    fake instead of `_load_answers_from_db`.
    """

    async def fake_load(poll_id):
        return [{"answer_id": "a-1", "text": "Yes", "order": 0}]

    monkeypatch.setattr(answer_cache_module, "_load_answers_from_db", fake_load)


@pytest.mark.asyncio
async def test_cache_hit_returns_deserialized_answers_and_increments_hit_counter():
    poll_id = uuid4()
    redis = FakeRedis({answers_cache_key(poll_id): json.dumps([{"answer_id": "a-1"}])})
    before = _counter_value("poll_redis_cache_hits_total", "answers")

    result = await get_cached_answers(poll_id, redis)

    assert result == [{"answer_id": "a-1"}]
    assert _counter_value("poll_redis_cache_hits_total", "answers") == before + 1


@pytest.mark.asyncio
async def test_cache_miss_loads_from_db_and_increments_miss_counter():
    poll_id = uuid4()
    redis = FakeRedis()
    before = _counter_value("poll_redis_cache_misses_total", "answers")

    result = await get_cached_answers(poll_id, redis)

    assert result == [{"answer_id": "a-1", "text": "Yes", "order": 0}]
    assert _counter_value("poll_redis_cache_misses_total", "answers") == before + 1
    # Miss path also populates the cache — unrelated to the counter, but
    # regressing it silently would still be a real bug worth catching here.
    assert redis.set_calls

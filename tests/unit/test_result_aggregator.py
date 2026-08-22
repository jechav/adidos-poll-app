"""Unit tests for I-009's `compute_poll_results`/`compute_poll_results_batch`:
percentage rounding, the zero-vote edge case, and missing counter keys —
all against a fake Redis + monkeypatched answer cache, no real
Redis/Postgres needed.

Mirrors tests/unit/test_worker_processor.py's fake-Redis style.
"""

import uuid

import pytest

import src.services.result_aggregator as aggregator_module
from src.services.result_aggregator import (
    _percentage,
    compute_poll_results,
    compute_poll_results_batch,
)


class FakePipeline:
    def __init__(self, store: dict):
        self._store = store
        self._keys: list[str] = []

    def get(self, key: str):
        self._keys.append(key)
        return self

    async def execute(self):
        return [self._store.get(key) for key in self._keys]


class FakeRedis:
    """In-memory stand-in exposing just enough of the redis-py cluster
    surface for `mget_pipelined` (`.pipeline()` + buffered `.get()`s).
    """

    def __init__(self, store: dict | None = None):
        self.store = store or {}

    def pipeline(self, transaction: bool = False):
        return FakePipeline(self.store)


def _answers(poll_id, *, a_id=None, b_id=None):
    a_id = a_id or uuid.uuid4()
    b_id = b_id or uuid.uuid4()
    return [
        {"answer_id": str(a_id), "text": "Yes", "order": 0},
        {"answer_id": str(b_id), "text": "No", "order": 1},
    ]


def _counter_key(poll_id, answer_id) -> str:
    return f"cache:poll:{poll_id}:answer:{answer_id}"


# --- _percentage ---------------------------------------------------------


def test_percentage_zero_votes_is_zero():
    assert _percentage(0, 0) == 0.0


def test_percentage_rounds_to_two_decimal_places():
    # spec's prior-art example: 1 vote / 2 votes -> 33.33% / 66.67%
    assert _percentage(1, 3) == 33.33
    assert _percentage(2, 3) == 66.67


def test_percentage_ten_votes_six_four_split():
    assert _percentage(6, 10) == 60.0
    assert _percentage(4, 10) == 40.0


def test_percentage_single_answer_has_all_votes():
    assert _percentage(5, 5) == 100.0


# --- compute_poll_results --------------------------------------------------


@pytest.mark.asyncio
async def test_compute_poll_results_returns_counts_and_percentages(monkeypatch):
    poll_id = uuid.uuid4()
    answers = _answers(poll_id)
    monkeypatch.setattr(
        aggregator_module, "get_cached_answers", lambda pid, redis: _fut(answers)
    )

    store = {
        _counter_key(poll_id, answers[0]["answer_id"]): "6",
        _counter_key(poll_id, answers[1]["answer_id"]): "4",
    }
    redis = FakeRedis(store)

    result = await compute_poll_results(poll_id, redis)

    assert result.poll_id == poll_id
    assert result.total_votes == 10
    assert result.answers[0].vote_count == 6
    assert result.answers[0].percentage == 60.0
    assert result.answers[1].vote_count == 4
    assert result.answers[1].percentage == 40.0


@pytest.mark.asyncio
async def test_compute_poll_results_zero_votes_no_exception(monkeypatch):
    poll_id = uuid.uuid4()
    answers = _answers(poll_id)
    monkeypatch.setattr(
        aggregator_module, "get_cached_answers", lambda pid, redis: _fut(answers)
    )

    redis = FakeRedis({})  # no counters written yet

    result = await compute_poll_results(poll_id, redis)

    assert result.total_votes == 0
    assert all(a.vote_count == 0 for a in result.answers)
    assert all(a.percentage == 0.0 for a in result.answers)


@pytest.mark.asyncio
async def test_compute_poll_results_missing_counter_key_treated_as_zero(monkeypatch):
    poll_id = uuid.uuid4()
    answers = _answers(poll_id)
    monkeypatch.setattr(
        aggregator_module, "get_cached_answers", lambda pid, redis: _fut(answers)
    )

    # Only one of the two counters has ever been INCRed.
    store = {_counter_key(poll_id, answers[0]["answer_id"]): "3"}
    redis = FakeRedis(store)

    result = await compute_poll_results(poll_id, redis)

    assert result.total_votes == 3
    assert result.answers[0].vote_count == 3
    assert result.answers[1].vote_count == 0
    assert result.answers[1].percentage == 0.0


# --- compute_poll_results_batch -------------------------------------------


@pytest.mark.asyncio
async def test_batch_matches_per_poll_results(monkeypatch):
    poll_a, poll_b = uuid.uuid4(), uuid.uuid4()
    answers_a = _answers(poll_a)
    answers_b = _answers(poll_b)

    answers_map = {poll_a: answers_a, poll_b: answers_b}
    monkeypatch.setattr(
        aggregator_module,
        "get_cached_answers",
        lambda pid, redis: _fut(answers_map[pid]),
    )

    store = {
        _counter_key(poll_a, answers_a[0]["answer_id"]): "6",
        _counter_key(poll_a, answers_a[1]["answer_id"]): "4",
        _counter_key(poll_b, answers_b[0]["answer_id"]): "1",
        _counter_key(poll_b, answers_b[1]["answer_id"]): "2",
    }
    redis = FakeRedis(store)

    batch = await compute_poll_results_batch([poll_a, poll_b], redis)

    assert set(batch.keys()) == {poll_a, poll_b}
    assert batch[poll_a].total_votes == 10
    assert batch[poll_a].answers[0].percentage == 60.0
    assert batch[poll_b].total_votes == 3
    assert batch[poll_b].answers[0].percentage == 33.33
    assert batch[poll_b].answers[1].percentage == 66.67


@pytest.mark.asyncio
async def test_batch_empty_input_returns_empty_dict():
    redis = FakeRedis({})
    assert await compute_poll_results_batch([], redis) == {}


@pytest.mark.asyncio
async def test_batch_single_mget_round_trip_for_all_polls(monkeypatch):
    """The batch variant must flatten every poll's counter keys into one
    `mget_pipelined` call, not one call per poll (I-009 acceptance
    criterion: "computes results for N polls in a single MGET round
    trip").
    """
    poll_a, poll_b = uuid.uuid4(), uuid.uuid4()
    answers_a = _answers(poll_a)
    answers_b = _answers(poll_b)
    answers_map = {poll_a: answers_a, poll_b: answers_b}
    monkeypatch.setattr(
        aggregator_module,
        "get_cached_answers",
        lambda pid, redis: _fut(answers_map[pid]),
    )

    calls = []
    original_mget = aggregator_module.mget_pipelined

    async def counting_mget(redis, keys):
        calls.append(list(keys))
        return await original_mget(redis, keys)

    monkeypatch.setattr(aggregator_module, "mget_pipelined", counting_mget)

    redis = FakeRedis({})
    await compute_poll_results_batch([poll_a, poll_b], redis)

    assert len(calls) == 1
    assert len(calls[0]) == 4  # 2 polls x 2 answers each


def _fut(value):
    """Wrap a plain value as an awaitable, for monkeypatching an async
    function with a plain lambda.
    """

    async def _coro():
        return value

    return _coro()

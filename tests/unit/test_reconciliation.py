"""Unit tests for I-017's hourly reconciliation job: the pure drift
calculation (`compute_drift`) and `run_reconciliation`'s orchestration
against monkeypatched module functions — mirrors
tests/unit/test_report_anomalies.py's fake-injection style (no real
Redis/Postgres needed).
"""

from uuid import uuid4

import pytest
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

import src.jobs.reconciliation as job
from src.jobs.reconciliation import DRIFT_ALERT_THRESHOLD, ReconciliationPoll, compute_drift
from src.metrics.registry import REGISTRY


def _drift_gauge_value() -> float | None:
    text = generate_latest(REGISTRY).decode("utf-8")
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == "poll_reconciliation_drift_max_ratio":
                return s.value
    return None


class TestComputeDrift:
    def test_zero_drift_when_counts_match(self):
        assert compute_drift(redis_total=100, db_total=100) == 0.0

    def test_returns_none_when_db_total_is_zero(self):
        # Nothing to compare against — a brand-new poll with no durable
        # votes yet must not divide by zero or report a false 100% drift.
        assert compute_drift(redis_total=5, db_total=0) is None

    def test_drift_ratio_is_computed_against_db_total(self):
        # 5 votes ahead of a DB total of 100 -> 5% drift.
        assert compute_drift(redis_total=105, db_total=100) == pytest.approx(0.05)

    def test_drift_is_symmetric_regardless_of_direction(self):
        assert compute_drift(redis_total=95, db_total=100) == pytest.approx(0.05)

    def test_exact_one_percent_boundary_is_not_greater_than_the_threshold(self):
        drift = compute_drift(redis_total=101, db_total=100)
        assert drift == pytest.approx(0.01)
        assert not (drift > DRIFT_ALERT_THRESHOLD)


class TestRunReconciliation:
    @pytest.mark.asyncio
    async def test_publishes_the_worst_drift_across_polls(self, monkeypatch):
        poll_a, poll_b = ReconciliationPoll(poll_id=uuid4()), ReconciliationPoll(poll_id=uuid4())

        async def fake_polls():
            return [poll_a, poll_b]

        async def fake_redis_counts(poll_id, redis):
            return {"a1": 100} if poll_id == poll_a.poll_id else {"a1": 50}

        async def fake_db_counts(poll_id, num_shards=None):
            # poll_a: 100 vs 100 -> 0 drift. poll_b: 50 vs 40 -> 25% drift.
            return {"a1": 100} if poll_id == poll_a.poll_id else {"a1": 40}

        monkeypatch.setattr(job, "get_active_and_recently_closed_polls", fake_polls)
        monkeypatch.setattr(job, "get_redis_vote_counts", fake_redis_counts)
        monkeypatch.setattr(job, "get_materialized_vote_counts", fake_db_counts)
        monkeypatch.setattr(job, "get_redis", lambda: _fake_redis())

        worst = await job.run_reconciliation()

        assert worst == pytest.approx(0.25)
        assert _drift_gauge_value() == pytest.approx(0.25)

    @pytest.mark.asyncio
    async def test_polls_with_zero_db_total_are_skipped_not_counted_as_drift(self, monkeypatch):
        poll = ReconciliationPoll(poll_id=uuid4())

        async def fake_polls():
            return [poll]

        async def fake_redis_counts(poll_id, redis):
            return {"a1": 10}

        async def fake_db_counts(poll_id, num_shards=None):
            return {}

        monkeypatch.setattr(job, "get_active_and_recently_closed_polls", fake_polls)
        monkeypatch.setattr(job, "get_redis_vote_counts", fake_redis_counts)
        monkeypatch.setattr(job, "get_materialized_vote_counts", fake_db_counts)
        monkeypatch.setattr(job, "get_redis", lambda: _fake_redis())

        worst = await job.run_reconciliation()

        assert worst == 0.0

    @pytest.mark.asyncio
    async def test_no_active_polls_publishes_zero_drift(self, monkeypatch):
        async def fake_polls():
            return []

        monkeypatch.setattr(job, "get_active_and_recently_closed_polls", fake_polls)
        monkeypatch.setattr(job, "get_redis", lambda: _fake_redis())

        worst = await job.run_reconciliation()

        assert worst == 0.0
        assert _drift_gauge_value() == 0.0


async def _fake_redis():
    return object()

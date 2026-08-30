"""Integration tests for I-017's reconciliation job against a real
PostgreSQL instance — verifies the actual SQL (`polls` state filter,
`vote_counts` freshness check, raw `votes` fan-out fallback) that the
fake-based unit tests in tests/unit/test_reconciliation.py can't exercise.

Mirrors tests/integration/test_refresh_vote_counts_db.py's fixture/skip
pattern.
"""

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from src.cache.answer_cache import answers_cache_key
from src.cache.redis_client import get_redis
from src.db.queries.reconciliation import (
    get_active_and_recently_closed_polls,
    get_materialized_vote_counts,
)
from src.jobs.reconciliation import run_reconciliation
from src.metrics.registry import REGISTRY


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app")


def _drift_gauge_value() -> float | None:
    text = generate_latest(REGISTRY).decode("utf-8")
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == "poll_reconciliation_drift_max_ratio":
                return s.value
    return None


@pytest.fixture
def sync_conn():
    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=2)
    except psycopg.OperationalError:
        pytest.skip(f"no PostgreSQL reachable at {DATABASE_URL}")
    yield conn
    conn.rollback()
    conn.close()


def _make_poll(sync_conn, *, state: str, closed_at: datetime | None = None):
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    with sync_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state, closed_at) VALUES (%s, %s, %s, %s)",
            (poll_id, "reconciliation test poll", state, closed_at),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_id, poll_id, "yes"),
        )
    sync_conn.commit()
    return poll_id, answer_id


@pytest.mark.asyncio
async def test_active_poll_is_included(sync_conn):
    poll_id, _ = _make_poll(sync_conn, state="active")

    polls = await get_active_and_recently_closed_polls()

    assert poll_id in {p.poll_id for p in polls}


@pytest.mark.asyncio
async def test_poll_closed_within_the_last_hour_is_included(sync_conn):
    poll_id, _ = _make_poll(
        sync_conn, state="closed", closed_at=datetime.now(timezone.utc) - timedelta(minutes=10)
    )

    polls = await get_active_and_recently_closed_polls()

    assert poll_id in {p.poll_id for p in polls}


@pytest.mark.asyncio
async def test_poll_closed_over_an_hour_ago_is_excluded(sync_conn):
    poll_id, _ = _make_poll(
        sync_conn, state="closed", closed_at=datetime.now(timezone.utc) - timedelta(hours=3)
    )

    polls = await get_active_and_recently_closed_polls()

    assert poll_id not in {p.poll_id for p in polls}


@pytest.mark.asyncio
async def test_draft_poll_is_excluded(sync_conn):
    poll_id, _ = _make_poll(sync_conn, state="draft")

    polls = await get_active_and_recently_closed_polls()

    assert poll_id not in {p.poll_id for p in polls}


@pytest.mark.asyncio
async def test_fresh_vote_counts_are_read_from_the_materialized_view(sync_conn):
    poll_id, answer_id = _make_poll(sync_conn, state="active")
    with sync_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO vote_counts (poll_id, answer_id, count, percentage, last_updated_at) "
            "VALUES (%s, %s, %s, %s, now())",
            (poll_id, answer_id, 7, 100.0),
        )
    sync_conn.commit()

    counts = await get_materialized_vote_counts(poll_id)

    assert counts == {answer_id: 7}


@pytest.mark.asyncio
async def test_stale_vote_counts_fall_back_to_a_raw_count(sync_conn):
    poll_id, answer_id = _make_poll(sync_conn, state="active")
    with sync_conn.cursor() as cur:
        # A materialized row that says 999 but is well outside the 5-minute
        # freshness window, plus 3 real votes -- the raw fallback must
        # report 3, not the stale materialized value.
        cur.execute(
            "INSERT INTO vote_counts (poll_id, answer_id, count, percentage, last_updated_at) "
            "VALUES (%s, %s, %s, %s, now() - interval '1 hour')",
            (poll_id, answer_id, 999, 100.0),
        )
        for i in range(3):
            cur.execute(
                "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
                (uuid.uuid4(), f"reconciliation-stale-user-{i}", poll_id, answer_id),
            )
    sync_conn.commit()

    # Local dev/tests stand in one Postgres instance for every shard (see
    # test_refresh_vote_counts_db.py's identical `num_shards=1` pattern),
    # so the raw fallback's fan-out is pinned to 1 shard here — fanning
    # out to the real `settings.num_shards` against one shared instance
    # would sum this poll's 3 votes once per (identical) shard.
    counts = await get_materialized_vote_counts(poll_id, num_shards=1)

    assert counts == {answer_id: 3}


@pytest.fixture
async def redis():
    client = await get_redis()
    try:
        await asyncio.wait_for(client.ping(), timeout=3)
    except Exception:
        pytest.skip("no Redis cluster reachable")
    yield client


@pytest.mark.asyncio
async def test_run_reconciliation_publishes_drift_for_a_seeded_mismatch(sync_conn, redis):
    """End-to-end: 5 durable votes but only 2 counted live in Redis is a
    60% drift, well past the 1% alert threshold.

    `run_reconciliation` walks *every* active/recently-closed poll in the
    (shared, multi-suite) test database, so this only asserts a floor —
    the seeded poll's own 60% drift must surface in the published gauge —
    rather than an exact value another suite's concurrently-active poll
    could push higher.
    """
    poll_id, answer_id = _make_poll(sync_conn, state="active")
    with sync_conn.cursor() as cur:
        for i in range(5):
            cur.execute(
                "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
                (uuid.uuid4(), f"reconciliation-drift-user-{i}", poll_id, answer_id),
            )
    sync_conn.commit()

    await redis.delete(answers_cache_key(poll_id))
    counter_key = f"cache:poll:{poll_id}:answer:{answer_id}"
    await redis.delete(counter_key)
    await redis.set(counter_key, 2)

    # See test_stale_vote_counts_fall_back_to_a_raw_count's comment on
    # `num_shards=1`: local dev/tests share one Postgres instance across
    # every shard.
    worst_drift = await run_reconciliation(num_shards=1)

    assert worst_drift >= 0.6 - 1e-9
    assert _drift_gauge_value() == pytest.approx(worst_drift)

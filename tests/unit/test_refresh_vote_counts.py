"""Unit tests for I-010's `scripts/jobs/refresh_vote_counts.py`: the
cross-shard merge, percentage rounding parity with I-009, and
partial-shard-failure handling — all against fake shard rows, no real
Postgres needed (see tests/integration/test_refresh_vote_counts_db.py for
the real-DB round-trip tests).
"""

import uuid

import pytest

from scripts.jobs.refresh_vote_counts import (
    compute_percentages,
    merge_shard_results,
    refresh_vote_counts,
    upsert_vote_counts,
)


def _ids():
    return uuid.uuid4(), uuid.uuid4(), uuid.uuid4()


# --- merge_shard_results --------------------------------------------------


def test_merge_sums_counts_across_shards():
    poll_id, answer_a, answer_b = _ids()
    per_shard_results = [
        [(poll_id, answer_a, 3), (poll_id, answer_b, 1)],
        [(poll_id, answer_a, 2), (poll_id, answer_b, 0)],
    ]

    merged = merge_shard_results(per_shard_results)

    assert merged == {(poll_id, answer_a): 5, (poll_id, answer_b): 1}


def test_merge_skips_a_failed_shard_but_keeps_the_others():
    poll_id, answer_a, answer_b = _ids()
    per_shard_results = [
        [(poll_id, answer_a, 3), (poll_id, answer_b, 1)],
        Exception("shard 1 unreachable"),
        [(poll_id, answer_a, 2), (poll_id, answer_b, 0)],
    ]

    merged = merge_shard_results(per_shard_results)

    # Only shards 0 and 2 contributed; shard 1's exception is skipped, not raised.
    assert merged == {(poll_id, answer_a): 5, (poll_id, answer_b): 1}


def test_merge_of_all_failed_shards_is_empty():
    merged = merge_shard_results([Exception("down"), Exception("also down")])
    assert merged == {}


def test_merge_includes_zero_vote_answers_present_in_shard_rows():
    poll_id, answer_a, answer_b = _ids()
    # A shard's LEFT JOIN row for a zero-vote answer still shows up as (id, id, 0).
    merged = merge_shard_results([[(poll_id, answer_a, 0), (poll_id, answer_b, 0)]])
    assert merged == {(poll_id, answer_a): 0, (poll_id, answer_b): 0}


# --- compute_percentages (rounding parity with I-009) ---------------------


def test_percentages_split_evenly():
    poll_id, answer_a, answer_b = _ids()
    merged = {(poll_id, answer_a): 5, (poll_id, answer_b): 5}

    rows = compute_percentages(merged)

    assert rows[(poll_id, answer_a)] == (5, 50.0)
    assert rows[(poll_id, answer_b)] == (5, 50.0)


def test_percentages_round_independently_without_sum_to_100_correction():
    poll_id, answer_a, answer_b = _ids()
    merged = {(poll_id, answer_a): 1, (poll_id, answer_b): 2}

    rows = compute_percentages(merged)

    assert rows[(poll_id, answer_a)] == (1, 33.33)
    assert rows[(poll_id, answer_b)] == (2, 66.67)


def test_percentages_are_zero_when_poll_has_no_votes():
    poll_id, answer_a, answer_b = _ids()
    merged = {(poll_id, answer_a): 0, (poll_id, answer_b): 0}

    rows = compute_percentages(merged)

    assert rows[(poll_id, answer_a)] == (0, 0.0)
    assert rows[(poll_id, answer_b)] == (0, 0.0)


def test_percentages_are_computed_independently_per_poll():
    poll_1, answer_1a, answer_1b = _ids()
    poll_2, answer_2a, answer_2b = _ids()
    merged = {
        (poll_1, answer_1a): 10,
        (poll_1, answer_1b): 0,
        (poll_2, answer_2a): 1,
        (poll_2, answer_2b): 1,
    }

    rows = compute_percentages(merged)

    assert rows[(poll_1, answer_1a)] == (10, 100.0)
    assert rows[(poll_1, answer_1b)] == (0, 0.0)
    assert rows[(poll_2, answer_2a)] == (1, 50.0)
    assert rows[(poll_2, answer_2b)] == (1, 50.0)


# --- upsert_vote_counts (broadcast to every shard) ------------------------


class FakeCursor:
    def __init__(self, log: list):
        self._log = log

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params=None):
        if params is not None:
            self._log.append(params)


class FakeConn:
    def __init__(self, log: list):
        self._log = log
        self.committed = False

    def cursor(self):
        return FakeCursor(self._log)

    async def commit(self):
        self.committed = True


class FakePool:
    def __init__(self, num_shards: int, unavailable_shards: set[int] = frozenset()):
        self.writes: dict[int, list] = {i: [] for i in range(num_shards)}
        self._unavailable = unavailable_shards
        self.conns = {i: FakeConn(self.writes[i]) for i in range(num_shards)}

    async def get_connection(self, shard_id: int):
        if shard_id in self._unavailable:
            from src.worker.db import ShardUnavailableError

            raise ShardUnavailableError(f"shard {shard_id} unreachable")
        return self.conns[shard_id]


@pytest.mark.asyncio
async def test_upsert_broadcasts_every_row_to_every_shard():
    poll_id, answer_a, answer_b = _ids()
    rows = {(poll_id, answer_a): (5, 50.0), (poll_id, answer_b): (5, 50.0)}
    pool = FakePool(num_shards=3)

    await upsert_vote_counts(pool, num_shards=3, rows=rows)

    for shard_id in range(3):
        assert pool.conns[shard_id].committed is True
        assert len(pool.writes[shard_id]) == 2
        written_answer_ids = {params[1] for params in pool.writes[shard_id]}
        assert written_answer_ids == {str(answer_a), str(answer_b)}


@pytest.mark.asyncio
async def test_upsert_skips_an_unavailable_shard_without_failing_the_others():
    poll_id, answer_a, _ = _ids()
    rows = {(poll_id, answer_a): (5, 100.0)}
    pool = FakePool(num_shards=3, unavailable_shards={1})

    await upsert_vote_counts(pool, num_shards=3, rows=rows)

    assert len(pool.writes[0]) == 1
    assert len(pool.writes[1]) == 0
    assert len(pool.writes[2]) == 1


@pytest.mark.asyncio
async def test_upsert_of_no_rows_writes_nothing():
    pool = FakePool(num_shards=2)

    await upsert_vote_counts(pool, num_shards=2, rows={})

    assert pool.writes[0] == []
    assert pool.writes[1] == []


class FailingCursor(FakeCursor):
    """Connects fine, but the write itself fails mid-`INSERT` — the
    connection-drops-mid-write failure mode, distinct from
    `ShardUnavailableError` (which only covers the connect step)."""

    async def execute(self, sql, params=None):
        raise RuntimeError("connection dropped mid-INSERT")


class FailingConn(FakeConn):
    def cursor(self):
        return FailingCursor(self._log)


@pytest.mark.asyncio
async def test_upsert_skips_a_shard_whose_write_fails_after_connecting():
    """A shard that connects successfully but whose INSERT/commit then
    raises (not a ShardUnavailableError) must not abort the other
    shards' writes — regression test for the code-review finding that
    upsert_vote_counts's write_to_shard only caught ShardUnavailableError,
    letting any other exception propagate out of asyncio.gather and abort
    every other shard's broadcast write.
    """
    poll_id, answer_a, _ = _ids()
    rows = {(poll_id, answer_a): (5, 100.0)}
    pool = FakePool(num_shards=3)
    pool.conns[1] = FailingConn(pool.writes[1])

    await upsert_vote_counts(pool, num_shards=3, rows=rows)

    assert len(pool.writes[0]) == 1
    assert pool.conns[0].committed is True
    assert len(pool.writes[1]) == 0
    assert len(pool.writes[2]) == 1
    assert pool.conns[2].committed is True


# --- refresh_vote_counts (end-to-end orchestration over fakes) -----------


class FakeReadPool(FakePool):
    """Extends FakePool with fake per-shard read results for
    `refresh_vote_counts`'s fetch step."""

    def __init__(self, shard_rows: dict[int, list | Exception]):
        super().__init__(num_shards=len(shard_rows))
        self._shard_rows = shard_rows

    async def get_connection(self, shard_id: int):
        result = self._shard_rows[shard_id]
        if isinstance(result, Exception):
            raise result
        conn = self.conns[shard_id]
        conn._rows = result
        return conn


class FakeReadCursor(FakeCursor):
    def __init__(self, log, rows):
        super().__init__(log)
        self._rows = rows

    async def fetchall(self):
        return self._rows


class FakeReadConn(FakeConn):
    def __init__(self, log, rows=None):
        super().__init__(log)
        self._rows = rows or []

    def cursor(self):
        return FakeReadCursor(self._log, self._rows)


@pytest.mark.asyncio
async def test_refresh_vote_counts_end_to_end_over_two_shards():
    poll_id, answer_a, answer_b = _ids()

    class Pool:
        def __init__(self):
            self.writes: dict[int, list] = {0: [], 1: []}

        async def get_connection(self, shard_id: int):
            if shard_id == 0:
                return FakeReadConn(self.writes[0], [(poll_id, answer_a, 3), (poll_id, answer_b, 1)])
            return FakeReadConn(self.writes[1], [(poll_id, answer_a, 1), (poll_id, answer_b, 0)])

    pool = Pool()
    await refresh_vote_counts(pool, num_shards=2)

    # Merged totals: answer_a=4, answer_b=1 -> 80.0% / 20.0%, broadcast to both shards.
    for shard_id in (0, 1):
        written = {params[1]: (params[2], params[3]) for params in pool.writes[shard_id]}
        assert written[str(answer_a)] == (4, 80.0)
        assert written[str(answer_b)] == (1, 20.0)


@pytest.mark.asyncio
async def test_refresh_vote_counts_tolerates_one_unreachable_shard():
    poll_id, answer_a, _ = _ids()
    from src.worker.db import ShardUnavailableError

    class Pool:
        def __init__(self):
            self.writes: dict[int, list] = {0: [], 1: []}

        async def get_connection(self, shard_id: int):
            if shard_id == 0:
                raise ShardUnavailableError("shard 0 down")
            return FakeReadConn(self.writes[1], [(poll_id, answer_a, 7)])

    pool = Pool()
    # Must not raise, even though shard 0's read failed.
    await refresh_vote_counts(pool, num_shards=2)

    written = {params[1]: (params[2], params[3]) for params in pool.writes[1]}
    assert written[str(answer_a)] == (7, 100.0)

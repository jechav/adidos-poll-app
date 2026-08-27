"""State-filtered, paginated poll listing for `GET /v1/polls` (I-011).

Polls/answers are replicated to every shard (I-001), so — mirroring
`src.services.polls`'s and `src.cache.answer_cache`'s precedent — a
single shared connection is enough here too; no shard routing is needed
for this metadata read, unlike `votes` itself.

This module owns only the read-replica metadata query
(`fetch_polls_page`) and the I-010 fallback read (`fetch_vote_counts_fallback`).
Live vote counts on the primary path come from I-009's
`compute_poll_results_batch`, not from here.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import psycopg

from src.config import settings
from src.schemas.results import AggregatedResult, AnswerResult

_conn: psycopg.AsyncConnection | None = None
_conn_lock = asyncio.Lock()


@dataclass(frozen=True)
class PollRow:
    poll_id: UUID
    question: str
    state: str
    created_at: datetime
    activated_at: datetime | None


COUNT_QUERY = "SELECT count(*) FROM polls WHERE state = %s"

# Creation order (newest first, `created_at DESC`) is the only sort this
# endpoint supports — see I-011's "Out of Scope": no other sort order is
# called for by any in-scope user story.
POLLS_PAGE_QUERY = """
    SELECT poll_id, question, state, created_at, activated_at
    FROM polls
    WHERE state = %s
    ORDER BY created_at DESC
    LIMIT %s OFFSET %s
"""

VOTE_COUNTS_FALLBACK_QUERY = """
    SELECT vc.poll_id, vc.answer_id, a.answer_text, a."order", vc.count, vc.percentage
    FROM vote_counts vc
    JOIN answers a ON a.answer_id = vc.answer_id
    WHERE vc.poll_id = ANY(%s)
    ORDER BY vc.poll_id, a."order"
"""


async def _get_connection() -> psycopg.AsyncConnection:
    """Assumes the caller already holds `_conn_lock`."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = await psycopg.AsyncConnection.connect(
            settings.database_url, connect_timeout=2
        )
    return _conn


async def fetch_polls_page(
    state: str, limit: int, offset: int
) -> tuple[list[PollRow], int]:
    """One page of `polls` filtered by `state`, plus the full matching count
    (`pagination.total`, not just the page size).
    """
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(POLLS_PAGE_QUERY, (state, limit, offset))
            rows = await cur.fetchall()
            await cur.execute(COUNT_QUERY, (state,))
            (total,) = await cur.fetchone()

    polls = [
        PollRow(
            poll_id=row[0],
            question=row[1],
            state=row[2],
            created_at=row[3],
            activated_at=row[4],
        )
        for row in rows
    ]
    return polls, total


async def fetch_vote_counts_fallback(
    poll_ids: list[UUID],
) -> dict[UUID, AggregatedResult]:
    """I-010 fallback: read counts/percentages already stored in
    `vote_counts` rather than recomputing them — the refresh job stores
    `percentage` directly, so this is a plain read plus an `answers` join
    for the answer text `compute_poll_results_batch` would otherwise
    supply from I-009's answer cache.
    """
    if not poll_ids:
        return {}

    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                VOTE_COUNTS_FALLBACK_QUERY, ([str(p) for p in poll_ids],)
            )
            rows = await cur.fetchall()

    answers_by_poll: dict[UUID, list[AnswerResult]] = defaultdict(list)
    for poll_id, answer_id, answer_text, _order, count, percentage in rows:
        answers_by_poll[poll_id].append(
            AnswerResult(
                answer_id=answer_id,
                text=answer_text,
                vote_count=count,
                percentage=float(percentage) if percentage is not None else 0.0,
            )
        )

    return {
        poll_id: AggregatedResult(
            poll_id=poll_id,
            total_votes=sum(a.vote_count for a in answers),
            answers=answers,
        )
        for poll_id, answers in answers_by_poll.items()
    }

"""Single-shard voting history query for `GET /v1/user/votes` (I-012).

This endpoint deliberately bypasses I-009's Redis aggregation path — a
user's own vote history is a low-volume, single-user lookup, not the hot
aggregate path that endpoint exists for. It reuses I-008's shard-routing
function (`shard_for_user`) and connection-pool shape (`ShardConnectionPool`)
rather than reimplementing either: a user's votes always live on exactly
the shard their `user_id` hashes to, so this never fans out across shards
the way I-010's aggregation job does.

The `WHERE v.user_id = %s` clause is satisfied by the implicit unique
index Postgres creates for `UNIQUE(user_id, poll_id)` on `votes` (I-001's
`polls.sql`) — `user_id` is the leading column, so a lookup by `user_id`
alone is an index scan, not a sequential one. No new index is needed.

Anonymization interaction (documented behavior, not a bug): I-001's 90-day
anonymization migration nulls `votes.user_id` for old votes. Once nulled,
`WHERE v.user_id = $1` no longer matches that row, so the vote silently
drops out of the user's visible history — consistent with the spec's
retention policy, not special-cased here.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from src.config import settings
from src.worker.db import ShardConnectionPool
from src.worker.sharding import shard_for_user


@dataclass(frozen=True)
class UserVoteRow:
    """One row of `USER_VOTES_QUERY`, named rather than a positional tuple
    — mirrors `src.services.polls.Poll`/`Answer`'s precedent for DB rows
    the API layer consumes, so callers aren't coupled to the query's
    column order.
    """

    poll_id: UUID
    question: str
    poll_state: str
    answer_id: UUID
    answer_text: str
    voted_at: datetime

USER_VOTES_QUERY = """
    SELECT
      v.poll_id,
      p.question,
      p.state AS poll_state,
      v.answer_id,
      a.answer_text,
      v.created_at AS voted_at
    FROM votes v
    JOIN polls p ON p.poll_id = v.poll_id
    JOIN answers a ON a.answer_id = v.answer_id
    WHERE v.user_id = %s
    ORDER BY v.created_at DESC
    LIMIT %s OFFSET %s
"""

USER_VOTES_COUNT_QUERY = "SELECT count(*) FROM votes WHERE user_id = %s"

# Module-level pool, mirroring I-008's `ShardConnectionPool` usage: lazily
# connects per shard and reuses connections across requests rather than
# opening one per call.
_pool = ShardConnectionPool()


async def get_user_votes(
    user_id: str, limit: int, offset: int
) -> tuple[list[UserVoteRow], int]:
    """Fetch one page of `user_id`'s vote history plus the total count.

    Routes to the single shard `user_id` hashes to (I-008's
    `shard_for_user`) — never a cross-shard fan-out. Returns `UserVoteRow`
    values (not raw tuples) alongside the total number of matching votes,
    for the caller to shape into `UserVotesData`.
    """
    shard_id = shard_for_user(user_id, settings.num_shards)
    conn = await _pool.get_connection(shard_id)
    async with conn.cursor() as cur:
        await cur.execute(USER_VOTES_QUERY, (user_id, limit, offset))
        rows = await cur.fetchall()
        await cur.execute(USER_VOTES_COUNT_QUERY, (user_id,))
        (total,) = await cur.fetchone()
    return [UserVoteRow(*row) for row in rows], total

"""Dual-layer vote uniqueness enforcement (I-006).

The system guarantees a user votes on a poll **at most once**. Because
votes are accepted synchronously but written to PostgreSQL asynchronously
(I-005 queues, I-008 processes), a single check at write time isn't
enough — two requests for the same (user_id, poll_id) can both pass a
naive check before either reaches the database. Two layers close that
gap:

- **Layer 1 — Redis `SET NX`** (`check_and_reserve_uniqueness`): an
  atomic, sub-millisecond reservation made *before* a vote is queued.
  Fast, but not durable — a Redis restart, failover, or cache flush can
  lose the key.
- **Layer 2 — PostgreSQL `UNIQUE(user_id, poll_id)`**
  (`insert_vote_or_log_duplicate`): the durable backstop, exercised by
  I-008's insert path. If Layer 1 ever fails to catch a duplicate, the DB
  constraint is what actually prevents two rows from existing — the
  second insert is logged and silently skipped, never surfaced as an
  error to a client that already received its 202.

Neither layer is "the" uniqueness system; they exist together because
Redis is fast-but-not-durable and PostgreSQL is durable-but-too-slow to
check synchronously on every request. See docs/issues/I-006-uniqueness.md
and SPECIFICATION.md decision #4.

This module is intentionally standalone: I-005 (vote acceptance) and
I-008 (vote processor) do not exist yet, so nothing here is wired into a
live endpoint. It exposes a small interface for those issues to call into
once built:

    await check_and_reserve_uniqueness(redis, user_id, poll_id)
    inserted = await insert_vote_or_log_duplicate(insert_fn, vote)

Redis key format (must match docs/architecture/redis-keys.md and I-002's
namespace table exactly): ``vote:user:{user_id}:poll:{poll_id}``, value
``"1"``, no TTL — the key persists for the life of the poll.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Protocol
from uuid import UUID

from redis.exceptions import RedisError

logger = logging.getLogger("poll_app.uniqueness")

VOTE_KEY_TEMPLATE = "vote:user:{user_id}:poll:{poll_id}"


def vote_reservation_key(user_id: str, poll_id: UUID | str) -> str:
    """The Layer 1 Redis key for a given (user_id, poll_id) pair.

    Matches the namespace documented in docs/architecture/redis-keys.md.
    """
    return VOTE_KEY_TEMPLATE.format(user_id=user_id, poll_id=poll_id)


class DuplicateVoteError(Exception):
    """Layer 1 rejected the request: the (user_id, poll_id) reservation
    key already existed in Redis. Callers (I-005) should translate this
    into ``409 DUPLICATE_VOTE`` and must not queue the vote.
    """

    def __init__(self, user_id: str, poll_id: UUID | str):
        self.user_id = user_id
        self.poll_id = poll_id
        super().__init__(f"user {user_id!r} already voted on poll {poll_id!r}")


class UniquenessCheckUnavailableError(Exception):
    """The Layer 1 reservation attempt itself failed (Redis connection
    error, cluster down, etc.) — the check could not be performed at all.

    This must fail *safe*, not open: callers (I-005) should translate
    this into ``503 SERVICE_UNAVAILABLE`` and reject the request, rather
    than treating "couldn't check" as "no duplicate found" and letting an
    unverified vote through to the queue.
    """

    def __init__(self, user_id: str, poll_id: UUID | str, cause: Exception):
        self.user_id = user_id
        self.poll_id = poll_id
        self.__cause__ = cause
        super().__init__(
            f"uniqueness check unavailable for user {user_id!r} "
            f"poll {poll_id!r}: {cause}"
        )


class SupportsSetNX(Protocol):
    async def set(self, key: str, value: str, nx: bool = ...) -> Any: ...
    async def delete(self, key: str) -> Any: ...


async def check_and_reserve_uniqueness(
    redis: SupportsSetNX, user_id: str, poll_id: UUID | str
) -> None:
    """Layer 1: atomically reserve (user_id, poll_id) via Redis `SET NX`.

    Returns normally if this call won the reservation (first vote).
    Raises ``DuplicateVoteError`` if the key already existed — the
    reservation was already held, so this request must be rejected
    before it is ever queued.

    Raises ``UniquenessCheckUnavailableError`` if the Redis call itself
    fails. This is deliberately a *different* exception from
    ``DuplicateVoteError``: a Redis outage must never be interpreted as
    "not a duplicate."

    `SET NX` is atomic at the Redis level, so two concurrent callers for
    the same key cannot both receive a truthy result — Redis serializes
    the command server-side and exactly one caller wins the reservation.
    """
    key = vote_reservation_key(user_id, poll_id)
    try:
        reserved = await redis.set(key, "1", nx=True)
    except RedisError as exc:
        raise UniquenessCheckUnavailableError(user_id, poll_id, exc) from exc

    if not reserved:
        raise DuplicateVoteError(user_id, poll_id)


async def release_reservation(
    redis: SupportsSetNX, user_id: str, poll_id: UUID | str
) -> None:
    """Best-effort release of a Layer 1 reservation that was won but whose
    vote never made it onto `queue:votes` (I-005: the `LPUSH` itself
    failed after `check_and_reserve_uniqueness` already succeeded).

    Without this, a transient Redis hiccup during enqueue would
    permanently lock the user out of a vote that was never actually
    accepted. Failures here are swallowed by the caller (I-005) rather
    than raised — releasing the reservation is a nice-to-have, not a
    correctness requirement: I-006's Layer 2 DB constraint is what
    actually prevents a duplicate row if the release fails and a retry
    later succeeds.

    Accepted trade-off: `SET NX` and `LPUSH` are not one atomic operation.
    If the `LPUSH` actually landed server-side but the client only saw a
    connection error (e.g. a timeout after the write, not before it), this
    release clears a *valid* reservation, and a client retry would then
    push a second entry onto the queue for the same logical vote. This is
    considered an acceptable, rare failure mode rather than something
    worth a distributed-transaction mechanism to close — see I-006 and
    I-008 for how a duplicate row is still caught downstream.
    """
    key = vote_reservation_key(user_id, poll_id)
    try:
        await redis.delete(key)
    except RedisError:
        pass


InsertFn = Callable[[dict], Awaitable[None]]


async def insert_vote_or_log_duplicate(
    insert_fn: InsertFn, vote: dict, *, unique_violation: type[Exception]
) -> bool:
    """Layer 2: insert a vote row, treating a `UNIQUE(user_id, poll_id)`
    violation as an expected, silent no-op rather than an error.

    ``insert_fn`` performs the actual insert (e.g. against a psycopg
    connection/cursor) and is expected to raise ``unique_violation``
    (e.g. ``psycopg.errors.UniqueViolation``) when the constraint from
    I-001 rejects the row. Any other exception propagates unchanged.

    This is the backstop the spec relies on when Layer 1 didn't catch a
    duplicate (Redis was down, the key was evicted, or a race let two
    reservations through). Per I-006's acceptance criteria:
    - no retry — the DB is correct, the row must not exist twice
    - no error surfaced to any client — the original request already
      received its 202 at queue time (I-005); the caller here is I-008's
      background worker, which has no client waiting on it
    - the violation is logged for audit/anomaly purposes (feeds I-016)

    Returns True if the row was inserted, False if it was skipped as a
    duplicate.
    """
    try:
        await insert_fn(vote)
    except unique_violation:
        logger.warning(
            "duplicate_vote_at_db_layer",
            extra={
                "vote_id": vote.get("vote_id"),
                "user_id": vote.get("user_id"),
                "poll_id": vote.get("poll_id"),
            },
        )
        return False
    return True

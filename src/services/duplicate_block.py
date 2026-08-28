"""3-strike duplicate-vote block (I-015, spec Implementation Decision #5).

A single duplicate rejection (I-006's 409) doesn't discourage a client
from retrying the same duplicate repeatedly, and repeated duplicate
attempts from the same source are themselves a bot signal (spec decision
#13). This module adds two check points I-005's `POST /v1/vote` handler
calls into:

- `enforce_not_blocked` — the cheapest possible early-exit for a
  known-bad `(user_id, ip)` pair; no poll lookup, no uniqueness check.
- `record_duplicate_attempt` — called only from I-006's `DuplicateVoteError`
  (409) path; increments a per-`(user_id, ip, poll_id)` strike counter,
  and on the 3rd strike, sets the block flag and writes an `anomalies`
  row.

This is deliberately a narrow, mechanical rule (a counter and a
threshold) — see I-014's bot-detection module for the broader, heuristic
sibling that looks across many users/IPs at once.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from src.services import anomalies as anomalies_repo
from src.services.rate_limit import RateLimitExceededError

STRIKE_THRESHOLD = 3
BLOCK_TTL_SECONDS = 3600


def _block_key(user_id: str, ip: str) -> str:
    return f"blocked:{user_id}:{ip}"


def _strike_key(user_id: str, ip: str, poll_id: UUID | str) -> str:
    return f"dup_attempts:{user_id}:{ip}:{poll_id}"


async def enforce_not_blocked(user_id: str, ip: str, redis) -> None:
    """Raise `RateLimitExceededError` (429, reusing I-003's existing
    `RATE_LIMIT_EXCEEDED` error code — a block and a rate-limit rejection
    present identically to the client) if `(user_id, ip)` is currently
    blocked. Returns normally otherwise.
    """
    if await redis.exists(_block_key(user_id, ip)):
        raise RateLimitExceededError("Blocked after repeated duplicate vote attempts")


async def record_duplicate_attempt(
    user_id: str, ip: str, poll_id: UUID, redis
) -> None:
    """Increment the per-`(user_id, ip, poll_id)` strike counter. On the
    3rd strike, sets the cross-poll block flag and writes a `critical`
    `anomalies` row.

    Only ever called from I-006's `DuplicateVoteError` path (the 409
    branch), so a legitimate first-time vote never touches this counter.
    Because `enforce_not_blocked` short-circuits every request once the
    block key is set, this function's 3rd-strike branch is reached at
    most once per block cycle — attempts 4, 5, 6... during the block
    window never reach I-006's uniqueness check at all, so no duplicate
    `anomalies` rows are written for the same block.
    """
    key = _strike_key(user_id, ip, poll_id)
    strikes = await redis.incr(key)
    if strikes == 1:
        await redis.expire(key, BLOCK_TTL_SECONDS)

    if strikes >= STRIKE_THRESHOLD:
        await redis.set(_block_key(user_id, ip), "1", ex=BLOCK_TTL_SECONDS)
        await anomalies_repo.create(
            alert_id=uuid4(),
            user_id=user_id,
            ip_address=ip,
            alert_type="duplicate_attempts_blocked",
            poll_id=poll_id,
            severity="critical",
            description=(
                f"User {user_id} blocked for 1h after {strikes} duplicate vote "
                f"attempts on poll {poll_id} from {ip}"
            ),
            created_at=datetime.now(timezone.utc),
            action_taken=f"blocked:{user_id}:{ip} set for {BLOCK_TTL_SECONDS}s",
        )

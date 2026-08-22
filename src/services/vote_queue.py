"""`queue:votes` enqueue step (I-005).

`enqueue_vote` is the last thing `POST /v1/vote` does before returning
202: LPUSH a JSON-encoded `VotePayload` onto `queue:votes`, per
docs/architecture/redis-keys.md's canonical key table. I-008's worker
(`src/worker/queue_consumer.py`) BRPOPs from the same list and parses
each entry with `VotePayload.model_validate_json` — this module's
`VotePayload` carries one extra field (`requested_at`) that the worker's
own `VotePayload` doesn't declare; Pydantic ignores unknown fields by
default, so the extra field round-trips harmlessly.
"""

from uuid import UUID

from pydantic import BaseModel
from redis.exceptions import RedisError

QUEUE_KEY = "queue:votes"


class VotePayload(BaseModel):
    vote_id: UUID
    user_id: str
    poll_id: UUID
    answer_id: UUID
    requested_at: str


class VoteQueueUnavailableError(Exception):
    """Raised when the `LPUSH` onto `queue:votes` itself fails (Redis
    unreachable). Callers (I-005's route) should translate this into
    `503 SERVICE_UNAVAILABLE` — a vote that can't be queued must never be
    reported as accepted.
    """

    def __init__(self, message: str = "Vote queue is temporarily unavailable"):
        self.message = message
        super().__init__(message)


async def enqueue_vote(redis, payload: VotePayload, *, queue_key: str = QUEUE_KEY) -> None:
    try:
        await redis.lpush(queue_key, payload.model_dump_json())
    except RedisError as exc:
        raise VoteQueueUnavailableError() from exc

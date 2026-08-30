"""Wire shape for an entry on `queue:votes` (I-008).

Matches the columns I-001 defined on the sharded `votes` table
(`vote_id`, `user_id`, `poll_id`, `answer_id`) — this is what I-005 is
expected to push (as JSON) and what this worker parses back out.

`request_id` (I-018) is `Optional` here, unlike the API-side
`src.services.vote_queue.VotePayload` where it's required: production
traffic always carries one (the route always sets it before enqueueing),
but this is the model many worker unit tests construct directly for
concerns unrelated to logging (sharding, batching, requeue), so a
default keeps those call sites unchanged. When present, it's what lets
the worker re-bind the same correlation ID the API bound for this vote's
request and continue the same trace — see `docs/architecture/logging.md`.
"""

from uuid import UUID

from pydantic import BaseModel


class VotePayload(BaseModel):
    vote_id: UUID
    user_id: str
    poll_id: UUID
    answer_id: UUID
    request_id: str | None = None

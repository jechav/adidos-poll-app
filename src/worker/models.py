"""Wire shape for an entry on `queue:votes` (I-008).

Matches the columns I-001 defined on the sharded `votes` table
(`vote_id`, `user_id`, `poll_id`, `answer_id`) — this is what I-005 is
expected to push (as JSON) and what this worker parses back out.
"""

from uuid import UUID

from pydantic import BaseModel


class VotePayload(BaseModel):
    vote_id: UUID
    user_id: str
    poll_id: UUID
    answer_id: UUID

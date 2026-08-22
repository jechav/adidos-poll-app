"""Request schema for `POST /v1/vote` (I-005).

`user_id` is deliberately absent: it comes exclusively from the
authenticated token via `Depends(get_current_user)` (I-004), never from
the request body, so a client can't vote on another user's behalf.
"""

from uuid import UUID

from pydantic import BaseModel


class VoteRequest(BaseModel):
    poll_id: UUID
    answer_id: UUID

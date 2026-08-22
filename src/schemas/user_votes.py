"""Response schemas for `GET /v1/user/votes` (I-012).

`voted_at` is a formatted string (not a `datetime` field) so its wire
format matches the rest of the app's envelope convention
(`src.schemas.responses.now_iso`'s `%Y-%m-%dT%H:%M:%SZ`), rather than
Pydantic's default ISO 8601 rendering of a naive datetime, which omits the
`Z` suffix.
"""

from uuid import UUID

from pydantic import BaseModel


class UserVoteEntry(BaseModel):
    poll_id: UUID
    question: str
    poll_state: str
    answer_id: UUID
    answer_text: str
    voted_at: str


class Pagination(BaseModel):
    limit: int
    offset: int
    total: int


class UserVotesData(BaseModel):
    votes: list[UserVoteEntry]
    pagination: Pagination

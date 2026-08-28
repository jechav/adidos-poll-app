"""Response schemas for `GET /v1/polls` (I-011).

`PollSummary.answers` reuses `src.schemas.results.AnswerResult` directly
(rather than redefining an equivalent shape here) so this endpoint's
answer entries and I-009's `compute_poll_results_batch` output never
drift apart — both the primary (Redis) and fallback (I-010) paths
populate the same field set.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from src.schemas.results import AnswerResult


class PollSummary(BaseModel):
    poll_id: UUID
    question: str
    state: str
    answers: list[AnswerResult]
    total_votes: int
    created_at: str
    activated_at: str | None = None


class Pagination(BaseModel):
    limit: int
    offset: int
    total: int


class PollListData(BaseModel):
    polls: list[PollSummary]
    pagination: Pagination


def _format_ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")

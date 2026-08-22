"""Response schemas for poll result aggregation (I-009).

`AggregatedResult`/`AnswerResult` are the shared shape `result_aggregator`
returns and I-011's `GET /v1/polls` serializes directly — keeping the
schema here (rather than duplicating fields in the route module) is what
prevents the route and this module's rounding rule from drifting apart.
"""

from uuid import UUID

from pydantic import BaseModel


class AnswerResult(BaseModel):
    answer_id: UUID
    text: str
    vote_count: int
    percentage: float


class AggregatedResult(BaseModel):
    poll_id: UUID
    total_votes: int
    answers: list[AnswerResult]

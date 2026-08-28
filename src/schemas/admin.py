"""Request/response schemas for the admin routes (I-013).

`CreatePollRequest.answers` enforces "exactly 2 answers" at the schema
level (`min_length=2, max_length=2`) so an invalid payload (0, 1, or 3+
answers) is rejected by FastAPI/Pydantic before the handler ever runs —
per I-013's acceptance criteria for `POST /v1/admin/polls`.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class AnswerInput(BaseModel):
    text: str = Field(..., min_length=1, max_length=255)


class CreatePollRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=255)
    answers: list[AnswerInput] = Field(..., min_length=2, max_length=2)


class UpdatePollStateRequest(BaseModel):
    state: Literal["active", "closed", "archived"]


class AnswerOut(BaseModel):
    answer_id: UUID
    text: str
    order: int


class PollOut(BaseModel):
    poll_id: UUID
    question: str
    state: str
    answers: list[AnswerOut] = []
    created_at: str
    activated_at: str | None = None
    closed_at: str | None = None
    archived_at: str | None = None


class AnomalySummary(BaseModel):
    alert_id: UUID
    alert_type: Literal[
        "rate_limit_exceeded", "duplicate_attempts_blocked", "bot_pattern_detected"
    ]
    severity: Literal["warning", "critical"]
    user_id: str | None
    ip_address: str | None
    poll_id: UUID | None
    description: str
    created_at: datetime
    acknowledged_at: datetime | None
    action_taken: str | None

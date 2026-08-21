"""User-facing v1 routes (I-003).

Routes are reserved here with request/response shapes and auth wiring in
place; the business logic behind each is a separate issue:
`GET /v1/polls` (I-011), `POST /v1/vote` (I-005), `GET /v1/user/votes`
(I-012).
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel

from src.api.dependencies.auth import get_current_user
from src.schemas.responses import success_envelope
from src.schemas.user_context import UserContext

router = APIRouter(prefix="/v1", tags=["user"])


class VoteRequest(BaseModel):
    poll_id: UUID
    answer_id: UUID


@router.get("/polls")
async def list_polls(
    request: Request, user: UserContext = Depends(get_current_user)
):
    """Placeholder — result aggregation lands in I-011."""
    return success_envelope({"polls": []}, request)


@router.post("/vote", status_code=status.HTTP_202_ACCEPTED)
async def cast_vote(
    request: Request,
    vote: VoteRequest,
    user: UserContext = Depends(get_current_user),
):
    """Placeholder — queueing, rate limiting, and uniqueness land in I-005."""
    return success_envelope(
        {
            "status": "queued",
            "poll_id": str(vote.poll_id),
            "answer_id": str(vote.answer_id),
        },
        request,
    )


@router.get("/user/votes")
async def list_user_votes(
    request: Request, user: UserContext = Depends(get_current_user)
):
    """Placeholder — voting history lands in I-012."""
    return success_envelope({"votes": []}, request)

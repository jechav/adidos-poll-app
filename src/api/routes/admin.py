"""Admin v1 routes (I-003).

Routes are reserved here with request/response shapes and `require_admin`
wiring in place; the business logic behind each lands in I-013.
"""

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, Field

from src.api.dependencies.auth import require_admin
from src.schemas.responses import success_envelope
from src.schemas.user_context import UserContext

router = APIRouter(prefix="/v1/admin", tags=["admin"])


class PollCreateRequest(BaseModel):
    question: str
    answers: list[str] = Field(min_length=2, max_length=2)


class PollStateUpdateRequest(BaseModel):
    state: Literal["active", "closed", "archived"]


@router.post("/polls", status_code=status.HTTP_201_CREATED)
async def create_poll(
    request: Request,
    payload: PollCreateRequest,
    admin: UserContext = Depends(require_admin),
):
    """Placeholder — poll creation lands in I-013."""
    return success_envelope(
        {"poll_id": None, "question": payload.question, "state": "draft"}, request
    )


@router.put("/polls/{poll_id}/state")
async def update_poll_state(
    request: Request,
    poll_id: UUID,
    payload: PollStateUpdateRequest,
    admin: UserContext = Depends(require_admin),
):
    """Placeholder — state transition validation lands in I-013."""
    return success_envelope(
        {"poll_id": str(poll_id), "state": payload.state}, request
    )


@router.get("/anomalies")
async def list_anomalies(
    request: Request, admin: UserContext = Depends(require_admin)
):
    """Placeholder — bot alert listing lands in I-013/I-014."""
    return success_envelope({"anomalies": []}, request)

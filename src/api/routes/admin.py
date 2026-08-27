"""Admin v1 routes (I-003, handlers implemented in I-013).

All three routes require `Depends(require_admin)` (I-004) — a non-admin
token gets `403 FORBIDDEN` before any handler logic runs. Poll creation
and state transitions are owned entirely here; the `anomalies` data
surfaced by the third route is produced elsewhere (I-014, I-015) — this
router is strictly a read-only consumer of that table (spec decision
#38: admins see aggregates and alerts, never raw vote logs).
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from src.api.dependencies.auth import require_admin
from src.schemas.admin import (
    AnswerOut,
    CreatePollRequest,
    PollOut,
    UpdatePollStateRequest,
)
from src.schemas.responses import success_envelope
from src.schemas.user_context import UserContext
from src.services import anomalies as anomalies_repo
from src.services.polls import (
    AdminPoll,
    create_admin_poll,
    get_admin_poll,
    transition_poll,
    validate_transition,
)

router = APIRouter(prefix="/v1/admin", tags=["admin"])


def _format_ts(value) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _poll_response(poll: AdminPoll) -> dict:
    return PollOut(
        poll_id=poll.poll_id,
        question=poll.question,
        state=poll.state,
        answers=[
            AnswerOut(answer_id=a.answer_id, text=a.text, order=a.order)
            for a in poll.answers
        ],
        created_at=_format_ts(poll.created_at),
        activated_at=_format_ts(poll.activated_at),
        closed_at=_format_ts(poll.closed_at),
        archived_at=_format_ts(poll.archived_at),
    ).model_dump(mode="json")


@router.post("/polls", status_code=status.HTTP_201_CREATED)
async def create_poll(
    request: Request,
    payload: CreatePollRequest,
    admin: UserContext = Depends(require_admin),
):
    """Create a poll + its 2 answers, always landing in `draft` state
    (I-013, spec stories #11/#12). `order` is derived from list position,
    not accepted from the client.
    """
    poll = await create_admin_poll(
        question=payload.question, answer_texts=[a.text for a in payload.answers]
    )
    return success_envelope(_poll_response(poll), request)


@router.put("/polls/{poll_id}/state")
async def update_poll_state(
    request: Request,
    poll_id: UUID,
    payload: UpdatePollStateRequest,
    admin: UserContext = Depends(require_admin),
):
    """Transition a poll's state, enforcing the one-way
    `draft -> active -> closed -> archived` sequence (I-013, spec
    stories #12/#13/#14). Requesting the poll's current state is an
    idempotent no-op; anything else that isn't exactly one step forward
    is rejected with 400 `INVALID_REQUEST`.
    """
    poll = await get_admin_poll(poll_id)
    if poll is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Poll {poll_id} not found"},
        )
    if payload.state == poll.state:
        return success_envelope(_poll_response(poll), request)

    validate_transition(poll.state, payload.state)
    updated = await transition_poll(poll_id, payload.state)
    return success_envelope(_poll_response(updated), request)


@router.get("/anomalies")
async def list_anomalies(
    request: Request,
    severity: Literal["warning", "critical"] | None = None,
    alert_type: str | None = None,
    since: datetime | None = None,
    limit: int = Query(default=50, le=500),
    admin: UserContext = Depends(require_admin),
):
    """Read-only view of detected anomalies: alert rows plus a
    `counts` summary, filterable by `severity`, `alert_type`, and
    `since` (I-013, spec stories #16/#17). Never reads `votes` or any
    per-vote detail (spec decision #38) — only `anomalies_repo` is
    queried here.
    """
    rows = await anomalies_repo.list_anomalies(
        severity=severity, alert_type=alert_type, since=since, limit=limit
    )
    counts = await anomalies_repo.counts_by_type(since=since)
    data = {
        "anomalies": [
            {
                "alert_id": str(row.alert_id),
                "alert_type": row.alert_type,
                "severity": row.severity,
                "user_id": row.user_id,
                "ip_address": row.ip_address,
                "poll_id": str(row.poll_id) if row.poll_id else None,
                "description": row.description,
                "created_at": _format_ts(row.created_at),
                "acknowledged_at": _format_ts(row.acknowledged_at),
                "action_taken": row.action_taken,
            }
            for row in rows
        ],
        "counts": counts,
    }
    return success_envelope(data, request)

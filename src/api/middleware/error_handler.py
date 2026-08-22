"""Exception handlers that translate any raised error into the standard
error envelope (I-003) — callers never see FastAPI/Starlette's default
error shapes.
"""

import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from src.schemas.responses import error_envelope
from src.services.polls import InvalidRequestError, PollClosedError, PollNotFoundError
from src.services.rate_limit import (
    RateLimitExceededError,
    RateLimitServiceUnavailableError,
)
from src.services.uniqueness import DuplicateVoteError, UniquenessCheckUnavailableError
from src.services.vote_queue import VoteQueueUnavailableError
from src.worker.db import ShardUnavailableError

logger = logging.getLogger("poll_app.api")

_CODE_BY_STATUS = {
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    409: "DUPLICATE_VOTE",
    429: "RATE_LIMIT_EXCEEDED",
    503: "SERVICE_UNAVAILABLE",
}


def _code_for_status(status_code: int) -> str:
    if status_code in _CODE_BY_STATUS:
        return _CODE_BY_STATUS[status_code]
    return "INVALID_REQUEST" if status_code < 500 else "INTERNAL_ERROR"


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        detail = exc.detail
        if isinstance(detail, dict) and "code" in detail:
            code = detail["code"]
            message = detail.get("message", "")
            details = detail.get("details")
        else:
            code = _code_for_status(exc.status_code)
            message = str(detail)
            details = None
        return JSONResponse(
            status_code=exc.status_code,
            content=error_envelope(code, message, request, details),
        )

    @app.exception_handler(DuplicateVoteError)
    async def duplicate_vote_exception_handler(
        request: Request, exc: DuplicateVoteError
    ):
        # I-006 Layer 1 (Redis SET NX) rejected an obvious duplicate
        # before it was ever queued. Not wired into a live route yet —
        # I-005 will call check_and_reserve_uniqueness() and let this
        # propagate once it exists.
        return JSONResponse(
            status_code=409,
            content=error_envelope(
                "DUPLICATE_VOTE", str(exc), request
            ),
        )

    @app.exception_handler(UniquenessCheckUnavailableError)
    async def uniqueness_check_unavailable_exception_handler(
        request: Request, exc: UniquenessCheckUnavailableError
    ):
        # I-006's Redis reservation check itself failed (connection
        # error, cluster down, etc.) — fail safe with 503, never fail
        # open by treating an unchecked request as "not a duplicate."
        logger.warning("uniqueness_check_unavailable", exc_info=exc)
        return JSONResponse(
            status_code=503,
            content=error_envelope(
                "SERVICE_UNAVAILABLE", str(exc), request
            ),
        )

    @app.exception_handler(RateLimitExceededError)
    async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceededError):
        return JSONResponse(
            status_code=429,
            content=error_envelope(
                "RATE_LIMIT_EXCEEDED",
                exc.message,
                request,
                details={"retry_after_seconds": exc.retry_after},
            ),
            headers={"Retry-After": str(exc.retry_after)},
        )

    @app.exception_handler(RateLimitServiceUnavailableError)
    async def rate_limit_unavailable_handler(
        request: Request, exc: RateLimitServiceUnavailableError
    ):
        return JSONResponse(
            status_code=503,
            content=error_envelope("SERVICE_UNAVAILABLE", exc.message, request),
        )

    @app.exception_handler(PollNotFoundError)
    async def poll_not_found_handler(request: Request, exc: PollNotFoundError):
        return JSONResponse(
            status_code=404,
            content=error_envelope("NOT_FOUND", str(exc), request),
        )

    @app.exception_handler(PollClosedError)
    async def poll_closed_handler(request: Request, exc: PollClosedError):
        return JSONResponse(
            status_code=400,
            content=error_envelope("POLL_CLOSED", str(exc), request),
        )

    @app.exception_handler(InvalidRequestError)
    async def invalid_request_handler(request: Request, exc: InvalidRequestError):
        return JSONResponse(
            status_code=400,
            content=error_envelope("INVALID_REQUEST", str(exc), request),
        )

    @app.exception_handler(VoteQueueUnavailableError)
    async def vote_queue_unavailable_handler(request: Request, exc: VoteQueueUnavailableError):
        logger.warning("vote_queue_unavailable", exc_info=exc)
        return JSONResponse(
            status_code=503,
            content=error_envelope("SERVICE_UNAVAILABLE", exc.message, request),
        )

    @app.exception_handler(ShardUnavailableError)
    async def shard_unavailable_handler(request: Request, exc: ShardUnavailableError):
        # I-012's read path hit the same per-shard outage I-008's writes
        # already handle with backoff — surfaced as 503, not a 500, since
        # it's an infra availability issue, not a bug.
        logger.warning("shard_unavailable", exc_info=exc)
        return JSONResponse(
            status_code=503,
            content=error_envelope("SERVICE_UNAVAILABLE", str(exc), request),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        return JSONResponse(
            status_code=400,
            content=error_envelope(
                "INVALID_REQUEST",
                "Request validation failed",
                request,
                details=exc.errors(),
            ),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.exception("unhandled_exception", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content=error_envelope(
                "INTERNAL_ERROR", "Internal server error", request
            ),
        )

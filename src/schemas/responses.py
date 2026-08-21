"""Response envelope shapes shared by every route (I-003).

Every response — success or error — carries the same `meta` block
(`timestamp`, `request_id`) so clients have one place to look regardless of
which endpoint they called.
"""

from datetime import datetime, timezone
from typing import Any

from fastapi import Request


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def meta_for(request: Request) -> dict:
    return {
        "timestamp": now_iso(),
        "request_id": getattr(request.state, "request_id", None),
    }


def success_envelope(data: Any, request: Request) -> dict:
    return {"success": True, "data": data, "meta": meta_for(request)}


def error_envelope(
    code: str, message: str, request: Request, details: Any = None
) -> dict:
    error: dict = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
    return {"success": False, "error": error, "meta": meta_for(request)}

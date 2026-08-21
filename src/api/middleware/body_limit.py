"""Request body size cap (I-003 acceptance criterion: 1MB max body).

Written as raw ASGI rather than `BaseHTTPMiddleware` so the limit is
enforced against the bytes actually received, not just a declared
`Content-Length` header — a request with a missing, chunked, or
understated length would otherwise slip through uncapped.
"""

import json
from uuid import uuid4

from src.schemas.responses import now_iso

MAX_BODY_BYTES = 1_000_000


class BodySizeLimitMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        buffered = []
        total = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                buffered.append(message)
                break
            total += len(message.get("body", b""))
            buffered.append(message)
            if total > MAX_BODY_BYTES:
                await self._reject(send)
                return
            if not message.get("more_body", False):
                break

        async def replay_receive():
            if buffered:
                return buffered.pop(0)
            return {"type": "http.request", "body": b"", "more_body": False}

        await self.app(scope, replay_receive, send)

    @staticmethod
    async def _reject(send) -> None:
        body = json.dumps(
            {
                "success": False,
                "error": {
                    "code": "INVALID_REQUEST",
                    "message": "Request body exceeds 1MB limit",
                },
                "meta": {"timestamp": now_iso(), "request_id": f"req-{uuid4().hex[:12]}"},
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})

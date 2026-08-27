"""Single outbound integration point for reporting anomalies to Adidos
(I-016).

Consistent with the rest of this service's Adidos relationship (spec
decision #1: trust tokens as-is, don't manage token lifecycle) — this
calls out once per batch and does not retry within the call; retry is
simply "the next scheduled 30-minute tick" (`src/jobs/report_anomalies.py`).
"""

from __future__ import annotations

import httpx

from src.config import settings

_REQUEST_TIMEOUT_SECONDS = 10.0


async def post_anomaly_batch(payload: list[dict]) -> httpx.Response:
    """POST `{"anomalies": payload}` to `settings.adidos_anomaly_webhook_url`.

    Raises on a connection-level failure (timeout, DNS, refused) the
    same way `httpx.AsyncClient.post` normally does — the caller
    (`report_anomalies`) treats that identically to a non-2xx response:
    leave `reported_at` null, log, let the next tick retry.
    """
    async with httpx.AsyncClient() as client:
        return await client.post(
            settings.adidos_anomaly_webhook_url,
            json={"anomalies": payload},
            headers={"Authorization": f"Bearer {settings.adidos_service_token}"},
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )

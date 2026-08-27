"""Adidos anomaly-reporting job (I-016), run as a Kubernetes CronJob every
30 minutes (`*/30 * * * *`, see deploy/cronjobs/report-anomalies.yaml).

Batches `anomalies` rows that haven't been sent yet (`reported_at IS
NULL`), POSTs them to Adidos in the domain model's `BotSuspected`-shaped
payload, and marks what it successfully sent so the next run doesn't
resend it. Fails safe: on any failure (non-2xx, network error, or a
crash mid-batch before the mark-reported write), the same rows are
simply picked up again on the next tick — no retry-with-backoff inside a
single run, per I-016's "Out of Scope: Retrying within a single run".

This is a reporting job only — see I-016's Problem Statement: this
service does local, short-term defense (I-007/I-014/I-015); Adidos owns
the longer-term escalation decision (IP blacklist, account ban) because
only Adidos has visibility across every service a bad actor might be
hitting.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from src.cache.redis_client import get_redis
from src.clients.adidos_anomaly_client import post_anomaly_batch
from src.services import anomalies as anomalies_repo
from src.services.anomalies import AnomalyRow

logger = logging.getLogger("poll_app.report_anomalies")

BATCH_SIZE = 500
LOCK_KEY = "report_anomalies:lock"
LOCK_TTL_SECONDS = 300


def _to_bot_suspected_payload(row: AnomalyRow) -> dict:
    """Map an `anomalies` row onto DOMAIN_MODEL.md's `BotSuspected` event
    shape rather than inventing a new wire format. `action` falls back to
    `alert_type` when `action_taken` is null (e.g. a `rate_limit_exceeded`
    row I-007 writes without an explicit action string).
    """
    return {
        "alert_id": str(row.alert_id),
        "user_id": row.user_id,
        "ip_address": row.ip_address,
        "reason": row.description,
        "action": row.action_taken or row.alert_type,
        "timestamp": row.created_at.isoformat(),
    }


async def report_anomalies() -> None:
    """One full run: acquire the lock, batch + send unreported rows, mark
    what succeeded, always release the lock. A run with zero unreported
    rows is a no-op that never calls Adidos.
    """
    redis = await get_redis()

    acquired = await redis.set(LOCK_KEY, "1", nx=True, ex=LOCK_TTL_SECONDS)
    if not acquired:
        logger.info("report_anomalies_skipped_lock_held")
        return

    try:
        rows = await anomalies_repo.list_unreported(limit=BATCH_SIZE)
        if not rows:
            return

        payload = [_to_bot_suspected_payload(row) for row in rows]

        try:
            response = await post_anomaly_batch(payload)
        except httpx.HTTPError:
            logger.warning("report_anomalies_request_failed", exc_info=True)
            return

        if response.status_code // 100 == 2:
            await anomalies_repo.mark_reported(
                [row.alert_id for row in rows], reported_at=datetime.now(timezone.utc)
            )
        else:
            logger.warning(
                "report_anomalies_non_2xx_response",
                extra={"status_code": response.status_code},
            )
    finally:
        await redis.delete(LOCK_KEY)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    await report_anomalies()


if __name__ == "__main__":
    asyncio.run(main())

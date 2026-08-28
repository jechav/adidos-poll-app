"""Admin-notification hook for critical anomalies (I-014, story #20).

`notify_admins()` is the call site this issue owns; the actual delivery
mechanism (Slack/PagerDuty/email) is I-019's concern (Phase 5, Alerting
& Dashboards). This stub's obligation is that every `critical` anomaly
reliably reaches it — see `src.jobs.bot_detection.raise_anomaly`.
"""

from __future__ import annotations

import logging

from src.services.anomalies import AnomalyRow

logger = logging.getLogger("poll_app.notifications")


async def notify_admins(alert: AnomalyRow) -> None:
    """Stub delivery: logs at a level an on-call alerting pipeline would
    scrape. Real paging/Slack/webhook wiring lands in I-019 behind this
    same interface, so callers never need to change.
    """
    logger.critical(
        "critical_anomaly_alert",
        extra={
            "alert_id": str(alert.alert_id),
            "alert_type": alert.alert_type,
            "user_id": alert.user_id,
            "ip_address": alert.ip_address,
            "description": alert.description,
        },
    )

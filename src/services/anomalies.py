"""Read/write access to I-001's `anomalies` table.

This is the single module every anomaly producer and consumer goes
through, so the table's shape is only known in one place:
- I-013 (admin) reads it read-only — `list()`, `counts_by_type()` — and
  never touches `votes`, per spec decision #38.
- I-014 (bot detection) and I-015 (duplicate detection) write to it via
  `create()`.
- I-016 (Adidos reporting) reads the unreported tail (`list_unreported()`)
  and marks rows sent (`mark_reported()`).

`anomalies` is replicated to every shard (I-001's schema comment), so —
mirroring `src.services.polls` and `src.cache.answer_cache` — a single
shared connection is enough; no shard routing is needed here.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import psycopg

from src.config import settings

_conn: psycopg.AsyncConnection | None = None
_conn_lock = asyncio.Lock()

# The three values of I-001's `anomaly_alert_type` enum. Kept here (not
# just implied by the DB) so `counts_by_type()` can report a `0` count
# for a type with no rows yet, rather than omitting it from the response.
ALERT_TYPES = ("rate_limit_exceeded", "duplicate_attempts_blocked", "bot_pattern_detected")
SEVERITIES = ("warning", "critical")


@dataclass(frozen=True)
class AnomalyRow:
    alert_id: UUID
    alert_type: str
    severity: str
    user_id: str | None
    ip_address: str | None
    poll_id: UUID | None
    description: str
    created_at: datetime
    acknowledged_at: datetime | None
    action_taken: str | None


async def _get_connection() -> psycopg.AsyncConnection:
    """Assumes the caller already holds `_conn_lock`."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = await psycopg.AsyncConnection.connect(
            settings.database_url, connect_timeout=2
        )
    return _conn


def _row_to_anomaly(row) -> AnomalyRow:
    return AnomalyRow(
        alert_id=row[0],
        alert_type=row[1],
        severity=row[2],
        user_id=row[3],
        ip_address=row[4],
        poll_id=row[5],
        description=row[6],
        created_at=row[7],
        acknowledged_at=row[8],
        action_taken=row[9],
    )


_SELECT_COLUMNS = (
    "alert_id, alert_type, severity, user_id, ip_address, poll_id, "
    "description, created_at, acknowledged_at, action_taken"
)


async def create(
    *,
    alert_id: UUID,
    alert_type: str,
    severity: str,
    description: str,
    created_at: datetime,
    user_id: str | None = None,
    ip_address: str | None = None,
    poll_id: UUID | None = None,
    action_taken: str | None = None,
) -> AnomalyRow:
    """Insert one `anomalies` row. Callers are responsible for satisfying
    I-001's `CHECK (user_id IS NOT NULL OR ip_address IS NOT NULL)` —
    this function does not itself default either field.
    """
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO anomalies "
                "(alert_id, user_id, ip_address, alert_type, poll_id, "
                "description, severity, created_at, action_taken) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    str(alert_id),
                    user_id,
                    ip_address,
                    alert_type,
                    str(poll_id) if poll_id else None,
                    description,
                    severity,
                    created_at,
                    action_taken,
                ),
            )
        await conn.commit()

    return AnomalyRow(
        alert_id=alert_id,
        alert_type=alert_type,
        severity=severity,
        user_id=user_id,
        ip_address=ip_address,
        poll_id=poll_id,
        description=description,
        created_at=created_at,
        acknowledged_at=None,
        action_taken=action_taken,
    )


async def list_anomalies(
    *,
    severity: str | None = None,
    alert_type: str | None = None,
    since: datetime | None = None,
    limit: int = 50,
) -> list[AnomalyRow]:
    """Alert rows, newest first, optionally filtered. Never touches
    `votes` — this is I-013's read-only view (spec decision #38).
    """
    clauses = []
    params: list = []
    if severity is not None:
        clauses.append("severity = %s")
        params.append(severity)
    if alert_type is not None:
        clauses.append("alert_type = %s")
        params.append(alert_type)
    if since is not None:
        clauses.append("created_at >= %s")
        params.append(since)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        f"SELECT {_SELECT_COLUMNS} FROM anomalies {where} "
        "ORDER BY created_at DESC LIMIT %s"
    )
    params.append(limit)

    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()

    return [_row_to_anomaly(row) for row in rows]


async def counts_by_type(*, since: datetime | None = None) -> dict[str, dict[str, int]]:
    """`{alert_type: {"warning": n, "critical": n}}` for every alert type,
    including types with zero rows (so admin clients don't need to
    special-case a missing key).
    """
    counts = {t: {s: 0 for s in SEVERITIES} for t in ALERT_TYPES}

    where = "WHERE created_at >= %s" if since is not None else ""
    params = [since] if since is not None else []
    query = (
        f"SELECT alert_type, severity, count(*) FROM anomalies {where} "
        "GROUP BY alert_type, severity"
    )

    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()

    for alert_type, severity, n in rows:
        counts.setdefault(alert_type, {s: 0 for s in SEVERITIES})[severity] = n
    return counts


async def list_unreported(*, limit: int = 500) -> list[AnomalyRow]:
    """Rows I-016's Adidos-reporting job hasn't successfully forwarded
    yet (`reported_at IS NULL`), oldest first — so a backlog drains in
    creation order across successive 30-minute ticks rather than newest
    rows starving older ones.
    """
    query = (
        f"SELECT {_SELECT_COLUMNS} FROM anomalies "
        "WHERE reported_at IS NULL ORDER BY created_at ASC LIMIT %s"
    )
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(query, (limit,))
            rows = await cur.fetchall()

    return [_row_to_anomaly(row) for row in rows]


async def mark_reported(alert_ids: list[UUID], *, reported_at: datetime) -> None:
    """Set `reported_at` for every id in `alert_ids` — called only after
    Adidos's endpoint returns 2xx for the batch containing them.
    """
    if not alert_ids:
        return
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE anomalies SET reported_at = %s WHERE alert_id = ANY(%s)",
                (reported_at, [str(a) for a in alert_ids]),
            )
        await conn.commit()

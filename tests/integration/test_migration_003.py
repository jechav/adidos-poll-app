"""Verifies migration 003 (`scripts/migrations/003_anomalies_reported_at.sql`)
against a DB already seeded via I-001's schema: `reported_at` exists,
defaults to NULL for existing rows, and is tracked independently of
`acknowledged_at` in `schema_migrations`.

Skips automatically if DATABASE_URL isn't reachable.
"""

import os
import uuid

import psycopg
import pytest

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app"
)


@pytest.fixture
def db_conn():
    try:
        connection = psycopg.connect(DATABASE_URL, connect_timeout=2)
    except psycopg.OperationalError:
        pytest.skip(f"no PostgreSQL reachable at {DATABASE_URL}")
    yield connection
    connection.rollback()
    connection.close()


def test_migration_003_recorded_as_applied(db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM schema_migrations WHERE version = '003_anomalies_reported_at'"
        )
        assert cur.fetchone() is not None


def test_reported_at_column_exists_and_defaults_to_null(db_conn):
    alert_id = uuid.uuid4()
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomalies (alert_id, user_id, alert_type, description, severity) "
            "VALUES (%s, %s, 'rate_limit_exceeded', 'migration test row', 'warning')",
            (alert_id, f"migration-test-{alert_id}"),
        )
        cur.execute("SELECT reported_at FROM anomalies WHERE alert_id = %s", (alert_id,))
        (reported_at,) = cur.fetchone()
    db_conn.commit()
    assert reported_at is None


def test_existing_rows_and_columns_are_unaffected(db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'anomalies'"
        )
        columns = {row[0] for row in cur.fetchall()}
    assert {
        "alert_id",
        "user_id",
        "ip_address",
        "alert_type",
        "poll_id",
        "description",
        "severity",
        "created_at",
        "acknowledged_at",
        "action_taken",
        "reported_at",
    } <= columns


def test_partial_unreported_index_exists(db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_anomalies_unreported'"
        )
        row = cur.fetchone()
    assert row is not None
    assert "reported_at IS NULL" in row[0]

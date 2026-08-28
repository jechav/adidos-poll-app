"""Unit tests for I-016: payload mapping from an `anomalies` row to the
`BotSuspected` shape, and the job's lock acquire/release behavior — all
against fakes, no live Postgres/Redis/Adidos required.
"""

from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

import src.jobs.report_anomalies as job
import src.services.anomalies as anomalies_module
from src.services.anomalies import AnomalyRow


def _row(**overrides) -> AnomalyRow:
    defaults = dict(
        alert_id=uuid4(),
        alert_type="rate_limit_exceeded",
        severity="warning",
        user_id="user-1",
        ip_address="1.2.3.4",
        poll_id=None,
        description="rate limit exceeded",
        created_at=datetime(2026, 8, 21, 9, 12, 3, tzinfo=timezone.utc),
        acknowledged_at=None,
        action_taken=None,
    )
    defaults.update(overrides)
    return AnomalyRow(**defaults)


class TestPayloadMapping:
    def test_maps_columns_onto_bot_suspected_shape(self):
        row = _row(action_taken="blocked:user-1:1.2.3.4 set for 3600s")

        payload = job._to_bot_suspected_payload(row)

        assert payload == {
            "alert_id": str(row.alert_id),
            "user_id": "user-1",
            "ip_address": "1.2.3.4",
            "reason": "rate limit exceeded",
            "action": "blocked:user-1:1.2.3.4 set for 3600s",
            "timestamp": "2026-08-21T09:12:03+00:00",
        }

    def test_action_falls_back_to_alert_type_when_action_taken_is_null(self):
        row = _row(alert_type="rate_limit_exceeded", action_taken=None)

        payload = job._to_bot_suspected_payload(row)

        assert payload["action"] == "rate_limit_exceeded"


class FakeRedis:
    def __init__(self, *, lock_held: bool = False):
        self._locked = lock_held
        self.deleted: list[str] = []

    async def set(self, key: str, value: str, nx: bool = False, ex: int | None = None):
        if nx and self._locked:
            return None
        self._locked = True
        return True

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        self._locked = False
        return 1


@pytest.fixture
def fake_redis(monkeypatch):
    fake = FakeRedis()

    async def _get_redis():
        return fake

    monkeypatch.setattr(job, "get_redis", _get_redis)
    return fake


@pytest.fixture
def no_unreported_rows(monkeypatch):
    async def fake_list_unreported(*, limit):
        return []

    monkeypatch.setattr(anomalies_module, "list_unreported", fake_list_unreported)


class TestLockBehavior:
    @pytest.mark.asyncio
    async def test_skips_run_when_lock_already_held(self, monkeypatch):
        fake = FakeRedis(lock_held=True)

        async def _get_redis():
            return fake

        monkeypatch.setattr(job, "get_redis", _get_redis)

        called = {"list": False}

        async def fake_list_unreported(*, limit):
            called["list"] = True
            return []

        monkeypatch.setattr(anomalies_module, "list_unreported", fake_list_unreported)

        await job.report_anomalies()

        assert called["list"] is False  # never even queried; lock lost immediately

    @pytest.mark.asyncio
    async def test_lock_released_after_a_successful_run(
        self, fake_redis, no_unreported_rows
    ):
        await job.report_anomalies()
        assert fake_redis.deleted == [job.LOCK_KEY]

    @pytest.mark.asyncio
    async def test_lock_released_even_when_the_adidos_call_raises(self, fake_redis, monkeypatch):
        async def fake_list_unreported(*, limit):
            return [_row()]

        async def fake_post_that_raises(payload):
            raise httpx.ConnectError("connection refused")

        monkeypatch.setattr(anomalies_module, "list_unreported", fake_list_unreported)
        monkeypatch.setattr(job, "post_anomaly_batch", fake_post_that_raises)

        await job.report_anomalies()

        assert fake_redis.deleted == [job.LOCK_KEY]


class TestReportRun:
    @pytest.mark.asyncio
    async def test_zero_unreported_rows_makes_no_http_call(
        self, fake_redis, no_unreported_rows, monkeypatch
    ):
        called = {"post": False}

        async def fake_post(payload):
            called["post"] = True

        monkeypatch.setattr(job, "post_anomaly_batch", fake_post)

        await job.report_anomalies()

        assert called["post"] is False

    @pytest.mark.asyncio
    async def test_2xx_response_marks_rows_reported(self, fake_redis, monkeypatch):
        rows = [_row(), _row()]

        async def fake_list_unreported(*, limit):
            return rows

        marked = {}

        async def fake_mark_reported(alert_ids, *, reported_at):
            marked["ids"] = alert_ids
            marked["reported_at"] = reported_at

        async def fake_post(payload):
            return httpx.Response(200, request=httpx.Request("POST", "https://x"))

        monkeypatch.setattr(anomalies_module, "list_unreported", fake_list_unreported)
        monkeypatch.setattr(anomalies_module, "mark_reported", fake_mark_reported)
        monkeypatch.setattr(job, "post_anomaly_batch", fake_post)

        await job.report_anomalies()

        assert set(marked["ids"]) == {r.alert_id for r in rows}

    @pytest.mark.asyncio
    async def test_non_2xx_response_leaves_rows_unmarked(self, fake_redis, monkeypatch):
        rows = [_row()]

        async def fake_list_unreported(*, limit):
            return rows

        called = {"mark": False}

        async def fake_mark_reported(alert_ids, *, reported_at):
            called["mark"] = True

        async def fake_post(payload):
            return httpx.Response(500, request=httpx.Request("POST", "https://x"))

        monkeypatch.setattr(anomalies_module, "list_unreported", fake_list_unreported)
        monkeypatch.setattr(anomalies_module, "mark_reported", fake_mark_reported)
        monkeypatch.setattr(job, "post_anomaly_batch", fake_post)

        await job.report_anomalies()

        assert called["mark"] is False

    @pytest.mark.asyncio
    async def test_network_failure_leaves_rows_unmarked(self, fake_redis, monkeypatch):
        rows = [_row()]

        async def fake_list_unreported(*, limit):
            return rows

        called = {"mark": False}

        async def fake_mark_reported(alert_ids, *, reported_at):
            called["mark"] = True

        async def fake_post_that_raises(payload):
            raise httpx.ConnectError("connection refused")

        monkeypatch.setattr(anomalies_module, "list_unreported", fake_list_unreported)
        monkeypatch.setattr(anomalies_module, "mark_reported", fake_mark_reported)
        monkeypatch.setattr(job, "post_anomaly_batch", fake_post_that_raises)

        await job.report_anomalies()

        assert called["mark"] is False

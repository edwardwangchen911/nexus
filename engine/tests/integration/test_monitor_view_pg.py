"""Persisted monitoring traverses authenticated research API without business writes."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET
from helpers.paper_batch import CALENDAR_ID
from helpers.portfolio_api import app, headers
from integration.test_monitor_increment_pg import ready_trigger
from nexus.ops.research_monitor import parser
from nexus.ops.research_monitor import run as monitor_entry
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorCoverageReceipt,
    MonitorDailyTriggerReceipt,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import (
    AuditEvent,
    Job,
    JobLease,
    OutboxDeliveryAttempt,
    OutboxEvent,
)
from nexus.worker import run_agent_worker_once
from sqlalchemy import select


async def business_facts(factory):
    facts = {}
    async with factory() as session:
        for model in (
            AgentResearchRunRecord,
            AgentDailyBudgetReservationRecord,
            MonitorCheck,
            MonitorCoverageReceipt,
            MonitorDailyTriggerReceipt,
            MonitorPlan,
            MonitorTrigger,
            Job,
            JobLease,
            OutboxEvent,
            OutboxDeliveryAttempt,
            AuditEvent,
        ):
            rows = (
                (await session.execute(select(model.__table__).order_by(model.id))).mappings().all()
            )
            facts[model.__tablename__] = sha256(
                json.dumps([dict(r) for r in rows], sort_keys=True, default=str).encode()
            ).hexdigest()
    return facts


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True], ids=["pending", "unknown"])
async def test_monitoring_readback_is_exact_and_never_changes_work(
    migrated_database_url, monkeypatch, unknown
):
    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    if unknown:
        from nexus.agents import codex_staged

        native = codex_staged.invoke_codex

        async def invoke(payload, **kwargs):
            if payload["stage"] == "critique":
                raise TimeoutError("controlled unknown monitoring readback")
            return await native(payload, **kwargs)

        monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
        admitted = await monitor_entry(
            settings,
            parser().parse_args(
                [
                    "admit",
                    "--trigger-id",
                    str(trigger.id),
                    "--calendar-authority-id",
                    str(CALENDAR_ID),
                ]
            ),
        )
        assert admitted["status"] == "admitted"
        assert await run_agent_worker_once(
            settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
        )
        await monitor_entry(
            settings, parser().parse_args(["finish", "--trigger-id", str(trigger.id)])
        )
        await monitor_entry(settings, parser().parse_args(["reconcile"]))
        async with factory() as session:
            latest = await session.scalar(
                select(MonitorCoverageReceipt)
                .where(
                    MonitorCoverageReceipt.account_id == trigger.account_id,
                    MonitorCoverageReceipt.instrument_id == parent.instrument_id,
                )
                .order_by(MonitorCoverageReceipt.research_as_of.desc())
                .limit(1)
            )
            assert latest.status == "unknown" and latest.active_plan_id is not None
            original = await session.get(MonitorPlan, latest.active_plan_id)
            assert original.source_run_id == parent.id
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == UUID(admitted["run_id"])
                )
            )
            assert budget.state == "reserved" and budget.settled_usd is None

    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)
    engine_app = app(migrated_database_url)
    engine_app.state.utc_clock = lambda: datetime.now(UTC)
    path = f"/internal/v1/research-runs/{parent.id}"
    before = await business_facts(factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://test"
    ) as client:
        for _ in range(2):
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, response.text
            view = response.json()["data"]["monitoring"]
            # This fixture retains the prior plan because factual inputs are
            # unchanged; unknown usage blocks new work independently of coverage.
            assert view["state"] == "valid", view
            assert view["trigger_count"] == 1 and view["latest_check"]["result"] == "true"
            item = view["triggers"][0]
            assert item["trigger_id"] == str(trigger.id)
            assert item["status"] == ("unknown" if unknown else "pending")
            assert item["usage_status"] == ("unknown" if unknown else "not_started")
            assert item["notification_status"] == "queued"
            assert item["blocked_reason"] is None
    assert await business_facts(factory) == before
    capture_dir = os.environ.get("NEXUS_MONITOR_VIEW_E2E_DIR")
    if capture_dir and unknown:
        # Optional actual BFF/browser acceptance uses this still-live isolated DB.
        # Ordinary CI never starts a server or waits for this explicit harness.
        import uvicorn

        destination = Path(capture_dir)
        destination.mkdir(exist_ok=True, parents=True)
        (destination / "ready.json").write_text(
            json.dumps(
                {
                    "run_id": str(parent.id),
                    "instrument_id": str(parent.instrument_id),
                    "database_url": migrated_database_url,
                    "hmac_key_id": TEST_HMAC_KEY_ID,
                    "hmac_secret": TEST_HMAC_SECRET,
                    "response": response.json(),
                },
                default=str,
            )
        )
        server = uvicorn.Server(
            uvicorn.Config(engine_app, host="127.0.0.1", port=8129, log_level="warning")
        )
        task = asyncio.create_task(server.serve())
        try:
            for _ in range(180):
                if (destination / "release").exists():
                    break
                await asyncio.sleep(1)
        finally:
            server.should_exit = True
            await task
        assert await business_facts(factory) == before
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["child", "settlement", "published", "dead_letter"])
async def test_price_api_does_not_read_later_run_usage_or_notification_facts(
    migrated_database_url, monkeypatch, boundary
):
    """Removing a timestamp fence must expose a later fact through the normal GET."""
    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    clock = [now]
    expected_run = None
    expected_usage = "not_started"
    expected_status = "pending"
    terminal_at = now + timedelta(seconds=10)
    if boundary in {"child", "settlement"}:
        admitted = await monitor_entry(
            settings,
            parser().parse_args(
                [
                    "admit",
                    "--trigger-id",
                    str(trigger.id),
                    "--calendar-authority-id",
                    str(CALENDAR_ID),
                ]
            ),
        )
        assert admitted["status"] == "admitted", admitted
        expected_run, expected_status = admitted["run_id"], "admitted"
        if boundary == "child":
            early = now - timedelta(seconds=1)
            terminal_at = now
        else:
            from sqlalchemy.ext.asyncio import AsyncSession

            original_scalar = AsyncSession.scalar

            async def completion_clock(self, statement, *args, **kwargs):
                if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                    return terminal_at
                return await original_scalar(self, statement, *args, **kwargs)

            class WorkerClock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return terminal_at if tz is None else terminal_at.astimezone(tz)

            monkeypatch.setattr(AsyncSession, "scalar", completion_clock)
            monkeypatch.setattr("nexus.worker.datetime", WorkerClock)
            assert await run_agent_worker_once(
                settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
            )
            finished = await monitor_entry(
                settings, parser().parse_args(["finish", "--trigger-id", str(trigger.id)])
            )
            assert finished["status"] == "completed", finished
            expected_usage, expected_status = "known", "completed"
            early = now + timedelta(seconds=5)
    else:
        from nexus.outbox.dispatcher import dispatch_outbox_once
        from nexus.outbox.models import PublisherSpec
        from nexus.outbox.registry import PublisherRegistry

        async with factory() as session:
            notification = await session.get(OutboxEvent, trigger.outbox_id)

        async def synthetic_publisher(message):
            assert message.event_id == trigger.outbox_id
            if boundary == "dead_letter":
                raise ValueError("controlled notification failure; no external transport")

        registry = PublisherRegistry(
            (
                PublisherSpec(
                    event_type=notification.event_type,
                    schema_version=notification.schema_version,
                    consumer_idempotent=True,
                    publisher=synthetic_publisher,
                ),
            )
        )
        for attempt in range(1 if boundary == "published" else notification.max_attempts):
            terminal_at = now + timedelta(seconds=10 * (attempt + 1))
            assert await dispatch_outbox_once(
                factory,
                registry=registry,
                worker_id="price-asof-synthetic-notification",
                clock=lambda: terminal_at,
                claim_registered_only=True,
            )
        early = now + timedelta(seconds=1)
        async with factory() as session:
            saved = await session.get(OutboxEvent, trigger.outbox_id)
            assert saved.status == boundary

    application = app(migrated_database_url)
    application.state.utc_clock = lambda: datetime.now(UTC)
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: clock[0])
    path = f"/internal/v1/research-runs/{parent.id}"
    before = await business_facts(factory)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test"
        ) as client:
            for read_at, later in ((early, False), (terminal_at, True)):
                clock[0] = read_at
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200, response.text
                view = response.json()["data"]["monitoring"]
                assert view["execution_locked"] and view["trigger_count"] == 1
                item = view["triggers"][0]
                assert item["trigger_id"] == str(trigger.id)
                assert item["run_id"] == (
                    None if boundary == "child" and not later else expected_run
                )
                assert item["usage_status"] == (
                    "unknown" if boundary == "settlement" and not later else expected_usage
                )
                assert item["status"] == (
                    "unknown"
                    if boundary in {"child", "settlement"} and not later
                    else expected_status
                )
                expected_notification = (
                    (boundary if later else "unknown")
                    if boundary in {"published", "dead_letter"}
                    else "queued"
                )
                assert item["notification_status"] == expected_notification
        assert await business_facts(factory) == before
    finally:
        await application.state.session_factory.kw["bind"].dispose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["observed_at", "created_at"])
async def test_later_daily_receipt_cannot_supply_usage_for_an_original_pending_trigger(
    migrated_database_url, monkeypatch, boundary
):
    from integration.test_monitor_daily_receipts_pg import receipt_source

    factory, receipt, trigger = await receipt_source(migrated_database_url, monkeypatch)
    async with factory() as session, session.begin():
        if boundary == "created_at":
            receipt.created_at = receipt.observed_at + timedelta(seconds=2)
        session.add(receipt)
        checked = await session.get(MonitorCheck, trigger.check_id)
        plan = await session.get(MonitorPlan, checked.plan_id)
    visible_at = max(receipt.observed_at, receipt.created_at)
    clock = [visible_at - timedelta(microseconds=1)]
    application = app(migrated_database_url)
    application.state.utc_clock = lambda: datetime.now(UTC)
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: clock[0])
    path = f"/internal/v1/research-runs/{plan.source_run_id}"
    before = await business_facts(factory)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test"
        ) as client:
            for visible in (False, True):
                if visible:
                    clock[0] = visible_at
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200, response.text
                item = response.json()["data"]["monitoring"]["triggers"][0]
                assert item["trigger_id"] == str(trigger.id)
                assert (item["run_id"], item["usage_status"], item["status"]) == (
                    (str(receipt.run_id), "known", "covered")
                    if visible
                    else (None, "not_started", "pending")
                )
        assert await business_facts(factory) == before
    finally:
        await application.state.session_factory.kw["bind"].dispose()
        await factory.kw["bind"].dispose()

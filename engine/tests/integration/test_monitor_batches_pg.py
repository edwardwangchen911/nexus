"""Normal audited plan installation to sealed stock facts and one outbox."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.disclosure_observations import append_capture, reader_source
from helpers.paper_batch import CALENDAR_ID
from integration.test_bark_outbox_pg import _decrypted_bark_notification
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_kinds_pg import clone
from integration.test_research_monitor_notification_pg import settings as bark_settings
from nexus.monitoring.admission import admit_trigger
from nexus.monitoring.batches import scan_stock_batch
from nexus.monitoring.pending import pending_stock
from nexus.notifications.worker import run_bark_worker
from nexus.ops.research_monitor import parser, run
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorObservationBatch,
    MonitorTrigger,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, OutboxEvent
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError


async def installed(
    database_url,
    monkeypatch,
    root,
    *,
    added=2,
    kinds=("price", "disclosure"),
    monitoring_lifetime_days=None,
    independent=False,
):
    factory, settings, store, captured, pointer = await reader_source(
        database_url,
        monkeypatch,
        root,
        monitoring_lifetime_days=monitoring_lifetime_days,
        independent=independent,
    )
    monkeypatch.setattr(
        "nexus.monitoring.disclosure_observations.configured_exhibit_store", lambda session: store
    )
    await append_capture(settings, store, captured, factory, root, "current", added=added)
    async with factory() as session:
        snapshot = await session.get(DataSnapshot, pointer.data_snapshot_id)
        if independent:
            from types import SimpleNamespace

            cycle = SimpleNamespace(
                candidate_batch_id=UUID(
                    snapshot.payload["independent_monitor_context"]["candidate_batch_id"]
                )
            )
        else:
            cycle = await session.get(
                AgentPaperCycleRecord, UUID(snapshot.payload["agent_paper_context"]["cycle_id"])
            )
    settings = settings.model_copy(
        update={
            "agent_paper_account_id": captured["account_id"],
            "agent_paper_candidate_batch_id": cycle.candidate_batch_id,
        }
    )
    captured["clock"][0] = max(captured["clock"][0], AT + timedelta(seconds=6))
    await _set_test_insert_clock(factory, captured["clock"][0])
    for kind in kinds:
        result = await run(
            settings,
            parser().parse_args(
                ["install", "--kind", kind, "--output-id", str(pointer.source_output_id)]
            ),
        )
        assert result["plan_id"]
        repeated = await run(
            settings,
            parser().parse_args(
                ["install", "--kind", kind, "--output-id", str(pointer.source_output_id)]
            ),
        )
        assert repeated["plan_id"] == result["plan_id"]
    captured["clock"][0] = max(captured["clock"][0], AT + timedelta(seconds=7))
    await _set_test_insert_clock(factory, captured["clock"][0])
    return factory, settings, store, captured, pointer


@pytest.mark.asyncio
@pytest.mark.parametrize("added", [2, 33])
async def test_full_stock_batch_preserves_all_events_and_one_notification(
    migrated_database_url, monkeypatch, tmp_path, added
):
    factory, settings, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path, added=added
    )
    async with factory() as session:
        counts_before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job)
            ]
        )
    async with factory() as session, session.begin():
        result = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
        assert result["new_triggers"] == added + 1, result
        assert result["notifications"] == 1
    async with factory() as session, session.begin():
        repeated = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
        assert repeated["batch_id"] == result["batch_id"] and repeated["new_triggers"] == 0
        assert repeated["notifications"] == 0
        assert result["automatic_review_capacity"] == ("blocked" if added > 32 else "within_limit")
    async with factory() as session:
        batch = await session.get(MonitorObservationBatch, UUID(result["batch_id"]))
        triggers = tuple(
            await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch.id))
        )
        assert len(triggers) == added + 1 and sum(t.kind == "price" for t in triggers) == 1
        assert len({t.outbox_id for t in triggers}) == 1
        event = await session.get(OutboxEvent, batch.outbox_id)
        assert event.payload["trigger_ids"] == sorted(str(t.id) for t in triggers)
        assert len(event.payload["event_ids"]) == added + 1
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job)
                ]
            )
            == counts_before
        )
        assert result["model_requests"] == result["provider_requests"] == 0
        run_row = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        model_id = run_row.model_config_id
        price_trigger = next(t for t in triggers if t.kind == "price")
        assert not await pending_stock(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
            allow_empty=True,
        )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="MONITOR_TYPED_CONTEXT_REQUIRED"):
            await admit_trigger(
                session,
                account_id=pointer.account_id,
                trigger_id=price_trigger.id,
                candidate_batch_id=settings.agent_paper_candidate_batch_id,
                calendar_authority_id=CALENDAR_ID,
                model_config_id=model_id,
                daily_limit_usd=Decimal("10"),
                daily_sdk_call_limit=9,
                now=captured["clock"][0],
            )
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job)
                ]
            )
            == counts_before
        )
        unchanged = await session.get(MonitorTrigger, price_trigger.id)
        assert unchanged.status == "pending" and unchanged.research_run_id is None
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_first", [False, True])
async def test_normal_scan_to_worker_keeps_batch_and_research_unchanged(
    migrated_database_url, monkeypatch, tmp_path, fail_first
):
    factory, config, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path
    )
    async with factory() as session, session.begin():
        later = AT + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=later.date(),
                decision_at=later.replace(hour=21),
                market_open_at=later.replace(hour=13, minute=30),
                market_close_at=later.replace(hour=20),
                created_at=AT - timedelta(hours=1),
            )
        )
    scanned = await run(
        config, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    )
    assert scanned["new_triggers"] == 3 and scanned["notifications"] == 1
    async with factory() as session:
        batch = await session.scalar(
            select(MonitorObservationBatch).where(MonitorObservationBatch.outbox_id.is_not(None))
        )
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job)
            ]
        )
    seen = []
    entered, release = asyncio.Event(), asyncio.Event()

    async def transport(request):
        item = _decrypted_bark_notification(request)
        seen.append(item)
        is_batch = item["id"] == f"research-monitor-batch:{batch.id}"
        if is_batch:
            entered.set()
            if not fail_first:
                await release.wait()
        batch_attempts = sum(row["id"] == f"research-monitor-batch:{batch.id}" for row in seen)
        return httpx.Response(
            503 if is_batch and fail_first and batch_attempts == 1 else 200,
            json={"code": 200},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        options = dict(
            settings=bark_settings(migrated_database_url).model_copy(
                update={"activated_at": batch.sealed_at}
            ),
            run_forever=False,
            client=client,
            clock=lambda: batch.sealed_at + timedelta(seconds=1),
        )
        if fail_first:
            assert await run_bark_worker(**options) == 1
            options["clock"] = lambda: batch.sealed_at + timedelta(seconds=4)
            assert await run_bark_worker(**options) == 1
        else:
            first = asyncio.create_task(run_bark_worker(**options))
            try:
                await asyncio.wait_for(entered.wait(), timeout=10)
                # The other two candidate stocks have independent legacy price notifications.
                assert await run_bark_worker(**options) == 1
            finally:
                release.set()
            assert await first == 1
        delivered = []
        for _ in range(4):
            delivered.append(await run_bark_worker(**options))
        assert delivered[-1] == 0
    batch_seen = [item for item in seen if item["id"] == f"research-monitor-batch:{batch.id}"]
    assert len(batch_seen) == (2 if fail_first else 1)
    assert all(item == batch_seen[0] for item in batch_seen)
    assert len(seen) == len(batch_seen) + 2
    assert len({item["id"] for item in seen}) == 3
    assert batch_seen[0]["message_id"] == str(batch.outbox_id)
    assert len(batch_seen[0]["body"]) < 250
    async with factory() as session:
        event = await session.get(OutboxEvent, batch.outbox_id)
        assert event.status == "published"
        assert event.attempt_count == (2 if fail_first else 1)
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job)
                ]
            )
            == before
        )
        members = tuple(
            await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch.id))
        )
        assert len(members) == 3
        assert all(t.status == "pending" and t.research_run_id is None for t in members)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_late_capture_is_separate_and_legacy_outbox_reuse_is_rejected(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, config, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path
    )
    async with factory() as session, session.begin():
        first = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
    async with factory() as session:
        original = await session.get(MonitorObservationBatch, UUID(first["batch_id"]))
        frozen = list(original.trigger_ids)
        trigger = await session.scalar(
            select(MonitorTrigger).where(
                MonitorTrigger.batch_id == original.id, MonitorTrigger.kind == "price"
            )
        )
    from nexus.persistence.models.monitoring import MonitorCheck

    started = asyncio.Event()
    backend = []

    async def forbidden_legacy_member():
        async with factory() as session, session.begin():
            backend.append(await session.scalar(select(func.pg_backend_pid())))
            old = await session.get(MonitorCheck, trigger.check_id)
            checked = clone(
                old,
                scheduled_at=old.scheduled_at + timedelta(minutes=5),
                checked_at=old.checked_at + timedelta(minutes=5),
            )
            session.add(checked)
            await session.flush()
            session.add(
                clone(trigger, batch_id=None, check_id=checked.id, episode=trigger.episode + 1)
            )
            started.set()

    async with factory() as locker, locker.begin():
        await locker.scalar(
            select(OutboxEvent).where(OutboxEvent.id == original.outbox_id).with_for_update()
        )
        attempt = asyncio.create_task(forbidden_legacy_member())
        await asyncio.wait_for(started.wait(), timeout=10)
        # Verify actual database blocking, rather than relying on a short sleep
        # to infer that the two deferred guards share an ownership lock.
        for _ in range(100):
            blockers = await locker.scalar(select(func.pg_blocking_pids(backend[0])))
            if blockers:
                break
            await asyncio.sleep(0.05)
        assert blockers, "late legacy insertion must wait for the outbox ownership lock"
    with pytest.raises(DBAPIError, match="MONITOR_BATCH_MEMBERS_INVALID"):
        await attempt
    await append_capture(
        config,
        store,
        captured,
        factory,
        tmp_path,
        "late",
        added=3,
        received=AT + timedelta(seconds=8),
        imported_at=AT + timedelta(seconds=9),
    )
    captured["clock"][0] = AT + timedelta(seconds=10)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory() as session, session.begin():
        later = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
        assert later["batch_id"] != first["batch_id"]
        assert later["new_triggers"] == later["notifications"] == 1
    async with factory() as session:
        unchanged = await session.get(MonitorObservationBatch, original.id)
        assert unchanged.trigger_ids == frozen
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 4
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version",
    ["local-portfolio-synthesis-v8-price-monitor", "local-portfolio-synthesis-v9-daily-pending"],
)
async def test_newer_legacy_monitoring_run_stops_old_sec_plan(
    migrated_database_url, monkeypatch, tmp_path, version
):
    from nexus.monitoring.batches import disclosure_source_current
    from nexus.persistence.models.monitoring import MonitorPlan

    factory, config, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path
    )
    async with factory() as session, session.begin():
        plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
        assert await disclosure_source_current(session, plan, captured["clock"][0])
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        # Structural successor: its mere presence cannot prove SEC retention,
        # regardless of whether its result later completes or vetoes adoption.
        manifest = dict(parent.prompt_manifest)
        manifest["portfolio"] = {**manifest["portfolio"], "version": version}
        session.add(
            clone(
                parent,
                idempotency_key=f"successor-{version}",
                decision_time=AT + timedelta(seconds=8),
                prompt_manifest=manifest,
                status="failed",
            )
        )
    async with factory() as session:
        plan = await session.get(MonitorPlan, plan.id)
        assert not await disclosure_source_current(session, plan, AT + timedelta(seconds=9))
    await factory.kw["bind"].dispose()

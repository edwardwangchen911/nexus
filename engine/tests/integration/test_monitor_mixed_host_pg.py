"""The ordinary host selects one sealed batch and consumes the original native job."""

import argparse
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest
from helpers.mixed_stage import add_mixed_fields
from helpers.monitor_materials import prepared_materials
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_increment_pg import append_fresh_inputs
from nexus.ops import agent_paper_loop as loop
from nexus.ops.research_monitor import run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorIncrement,
    MonitorMixedEventReceipt,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import Job, JobLease
from nexus.settings import AgentRuntimeMode
from sqlalchemy import func, select


async def ready_host_batch(url, monkeypatch, root, *, sec_only=False):
    factory, settings, store, captured, pointer, batch_id, material_id = await prepared_materials(
        url, monkeypatch, root, sec_only=sec_only
    )
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        now = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
    captured["clock"][0] = now
    await _set_test_insert_clock(factory, now)
    await append_fresh_inputs(factory, pointer.instrument_id, now)
    async with factory() as session, session.begin():
        monday = now + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
            )
        )
    settings = settings.model_copy(
        update={
            "agent_paper_formal_model_config_id": parent.model_config_id,
            "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
            "agent_paper_cycle_enabled": True,
        }
    )
    normal = loop.AgentPaperLoopSnapshot(
        now=now,
        calendar_authority_id=CALENDAR_ID,
        market_open_at=now - timedelta(hours=1),
        decision_window_start_at=now - timedelta(minutes=30),
        decision_window_end_at=now + timedelta(hours=4),
        next_market_open_at=now + timedelta(days=3),
        next_decision_window_start_at=now + timedelta(days=3),
    )
    return factory, settings, captured, pointer, batch_id, normal


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["success", "unknown", "sec_only"])
async def test_normal_host_selects_prepared_typed_batch_and_never_replays_it(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from nexus.agents import codex_staged
    from nexus.ops import manual_paper_host
    from nexus.ops.monitor_dispatch import (
        confirm_monitor_shutdown,
        dispatch_configured_once,
        inspect_monitor_queue,
    )
    from unit.agents.test_codex_sdk import _restore_view

    factory, settings, captured, pointer, batch_id, normal = await ready_host_batch(
        migrated_database_url, monkeypatch, tmp_path, sec_only=mode == "sec_only"
    )
    async with factory() as session:
        before_jobs = await session.scalar(select(func.count()).select_from(Job))
        members = tuple(
            await session.scalars(
                select(MonitorTrigger)
                .where(MonitorTrigger.batch_id == batch_id)
                .order_by(MonitorTrigger.id)
            )
        )
    selected = await inspect_monitor_queue(settings, account_id=pointer.account_id, snapshot=normal)
    assert selected.monitor_trigger_id == members[0].id
    assert loop.choose_loop_action(selected) == loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT
    daily = replace(normal, cycle_status="researching", research_job_status="queued")
    assert (
        await inspect_monitor_queue(settings, account_id=pointer.account_id, snapshot=daily)
    ).monitor_trigger_id is None
    calls = []
    native = codex_staged.invoke_codex

    async def invoke(payload, **kwargs):
        wire = _restore_view(payload)
        calls.append(wire["stage"])
        if mode == "unknown" and wire["stage"] == "critique":
            raise TimeoutError("controlled typed unknown")
        raw, usage = await native(payload, **kwargs)
        body = json.loads(raw)
        add_mixed_fields(body, wire)
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)

    async def environment(*args, **kwargs):
        return settings.model_dump(mode="json", by_alias=True), str(CALENDAR_ID)

    async def normal_read(*args, **kwargs):
        return normal

    async def no_eval(*args, **kwargs):
        return None

    async def no_manual(*args, **kwargs):
        return kwargs["snapshot"]

    monkeypatch.setattr(loop, "_load_bound_loop_environment", environment)
    monkeypatch.setattr(loop, "inspect_agent_paper_loop", normal_read)
    monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", no_eval)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_close", no_manual)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_queue", no_manual)
    result = await loop.execute_agent_paper_loop_action(
        settings,
        account_id=pointer.account_id,
        repo_root=Path.cwd(),
        env_file=Path("synthetic.env"),
        action=loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT,
        snapshot=selected,
    )
    assert result == (loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN if mode == "unknown" else None)
    assert calls == (
        ["research", "critique"] if mode == "unknown" else ["research", "critique", "synthesis"]
    )
    if mode == "unknown":
        captured["clock"][0] += timedelta(minutes=2)
        await _set_test_insert_clock(factory, captured["clock"][0])
        await run(settings, argparse.Namespace(action="finish", trigger_id=members[0].id))
    async with factory() as session:
        member = await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.batch_id == batch_id)
        )
        assert member is not None and member.reserved_sdk_calls == 3
        child = await session.get(AgentResearchRunRecord, member.run_id)
        assert child.instrument_id == pointer.instrument_id
        assert await session.scalar(select(func.count()).select_from(Job)) == before_jobs + 1
        job = await session.get(Job, member.job_id)
        assert job.attempt_count == 1 and job.manual_retry_count == 0
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == child.id
            )
        )
        assert reservation.state == ("reserved" if mode == "unknown" else "settled")
        assert (child.settled_cost_usd is None) == (mode == "unknown")
        receipts = tuple(
            await session.scalars(
                select(MonitorMixedEventReceipt).where(MonitorMixedEventReceipt.run_id == child.id)
            )
        )
        coverage = tuple(
            await session.scalars(
                select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == child.id)
            )
        )
        assert len(receipts) == (0 if mode == "unknown" else 2 if mode == "sec_only" else 3)
        assert len(coverage) == (0 if mode == "unknown" else 2)
    again = await dispatch_configured_once(
        settings, trigger_id=members[0].id, calendar_authority_id=CALENDAR_ID
    )
    assert again["status"] == ("unknown" if mode == "unknown" else "completed")
    assert len(calls) == (2 if mode == "unknown" else 3)
    assert await confirm_monitor_shutdown(
        settings, account_id=pointer.account_id, trigger_id=members[0].id
    ) == (mode != "unknown")
    after = await inspect_monitor_queue(
        settings, account_id=pointer.account_id, snapshot=replace(normal, now=captured["clock"][0])
    )
    assert after.monitor_trigger_id == (members[0].id if mode == "unknown" else None)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_typed_admission_stale_inputs_preserve_whole_batch_and_report_block(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, captured, pointer, batch_id, normal = await ready_host_batch(
        migrated_database_url, monkeypatch, tmp_path
    )
    from nexus.monitoring.batch_inputs import SOURCE
    from nexus.persistence.models.data import DataSnapshot

    async with factory() as session:
        material = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE, DataSnapshot.provider_object_id == str(batch_id)
            )
        )
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
            ]
        )
    captured["clock"][0] += timedelta(minutes=2)
    await _set_test_insert_clock(factory, captured["clock"][0])
    result = await run(
        settings,
        argparse.Namespace(
            action="admit-batch",
            batch_id=batch_id,
            materials_snapshot_id=material.id,
            calendar_authority_id=CALENDAR_ID,
        ),
    )
    assert result["status"] == "blocked" and result["reason"] == "MONITOR_INPUTS_UNAVAILABLE"
    assert result["model_requests"] == 0 and result["provider_requests"] == 0
    async with factory() as session:
        rows = tuple(
            await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
        )
        assert len(rows) == 3 and all(
            row.status == "blocked" and row.research_run_id is None for row in rows
        )
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
                ]
            )
            == before
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_readonly_typed_selector_consumes_one_stock_bucket_without_kind_spin(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.audit.service import append_audit_event
    from nexus.ops.monitor_dispatch import DISPATCH_ACTION, inspect_monitor_queue

    factory, settings, captured, pointer, batch_id, normal = await ready_host_batch(
        migrated_database_url, monkeypatch, tmp_path
    )
    first = await inspect_monitor_queue(settings, account_id=pointer.account_id, snapshot=normal)
    assert first.monitor_trigger_id is not None and first.monitor_batch_id == batch_id
    async with factory() as session, session.begin():
        await append_audit_event(
            session,
            actor_id="research-monitor",
            action=DISPATCH_ACTION,
            schema_version="monitor_dispatch_v1",
            occurred_at=normal.now,
            payload={"trigger_id": str(first.monitor_trigger_id)},
        )
    repeated = await inspect_monitor_queue(settings, account_id=pointer.account_id, snapshot=normal)
    assert repeated.monitor_trigger_id is None
    # Different kind members cannot bypass the canonical stock-level dispatch check.
    later = replace(normal, now=normal.now + timedelta(minutes=5))
    assert (
        await inspect_monitor_queue(settings, account_id=pointer.account_id, snapshot=later)
    ).monitor_trigger_id == first.monitor_trigger_id
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_dispatch_prepares_one_exact_filing_then_can_close_without_research(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_monitor_batch_prepare_pg import mocked_collector, setup
    from nexus.ops.monitor_dispatch import confirm_monitor_shutdown, dispatch_configured_once

    factory, settings, store, captured, batch_id, before = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    calls = mocked_collector(monkeypatch, store, captured)
    async with factory() as session:
        first = await session.scalar(
            select(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch_id)
            .order_by(MonitorTrigger.id)
            .limit(1)
        )
    result = await dispatch_configured_once(
        settings,
        trigger_id=first.id,
        calendar_authority_id=CALENDAR_ID,
        repo_root=tmp_path,
        sec_contact="nexus-test@example.test",
    )
    assert result["status"] == "preparing" and result["model_requests"] == 0 and len(calls) == 2
    assert calls[0].endswith("0000123456-26-000002-index.html")
    assert calls[1].endswith("new2.htm")
    assert result["provider_requests"] == 2
    assert await confirm_monitor_shutdown(
        settings, account_id=settings.agent_paper_account_id, trigger_id=first.id
    )
    repeated = await dispatch_configured_once(
        settings,
        trigger_id=first.id,
        calendar_authority_id=CALENDAR_ID,
        repo_root=tmp_path,
        sec_contact="nexus-test@example.test",
    )
    assert repeated["status"] == "checked" and len(calls) == 2
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == before
        assert not await session.scalar(
            select(MonitorIncrement.id).where(MonitorIncrement.batch_id == batch_id)
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_typed_disclosure_check_also_fences_legacy_price_in_same_stock_bucket(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.audit.service import append_audit_event
    from nexus.monitoring.installation import install_plan_from_output
    from nexus.monitoring.observations import read_latest_price
    from nexus.monitoring.storage import record_check
    from nexus.ops.monitor_dispatch import DISPATCH_ACTION, unchecked

    factory, settings, captured, pointer, batch_id, normal = await ready_host_batch(
        migrated_database_url, monkeypatch, tmp_path, sec_only=True
    )
    now = normal.now + timedelta(seconds=1)
    captured["clock"][0] = now
    await _set_test_insert_clock(factory, now)
    async with factory() as session, session.begin():
        plan = await install_plan_from_output(
            session,
            account_id=pointer.account_id,
            output_id=pointer.source_output_id,
            expected_predecessor_id=None,
            now=now,
        )
        threshold = plan.condition["threshold"]
    now += timedelta(seconds=1)
    captured["clock"][0] = now
    await _set_test_insert_clock(factory, now)
    await append_fresh_inputs(factory, pointer.instrument_id, now, midpoint=threshold)
    async with factory() as session, session.begin():
        checked = await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
            checked_at=now,
            observation=await read_latest_price(
                session, instrument_id=pointer.instrument_id, now=now
            ),
        )
        legacy = await session.scalar(
            select(MonitorTrigger).where(MonitorTrigger.check_id == checked.id)
        )
        assert legacy is not None and legacy.batch_id is None
        typed = await session.scalar(
            select(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch_id)
            .order_by(MonitorTrigger.id)
            .limit(1)
        )
        assert typed.kind == "disclosure"
        await append_audit_event(
            session,
            actor_id="research-monitor",
            action=DISPATCH_ACTION,
            schema_version="monitor_dispatch_v1",
            occurred_at=now,
            payload={"trigger_id": str(typed.id)},
        )
    async with factory() as session:
        assert (
            await session.scalar(
                select(MonitorTrigger.id).where(MonitorTrigger.id == legacy.id, unchecked(now))
            )
            is None
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_two_small_batches_cannot_implicitly_split_stock_overflow(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.disclosure_observations import append_capture
    from integration.test_monitor_batches_pg import installed
    from nexus.monitoring.batches import scan_stock_batch
    from nexus.ops.monitor_dispatch import inspect_monitor_queue
    from nexus.persistence.models.monitoring import MonitorObservationBatch

    factory, settings, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path, added=16
    )
    async with factory() as session, session.begin():
        first = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
    later = captured["clock"][0] + timedelta(minutes=6)
    captured["clock"][0] = later
    await _set_test_insert_clock(factory, later)
    await append_fresh_inputs(factory, pointer.instrument_id, later)
    await append_capture(
        settings,
        store,
        captured,
        factory,
        tmp_path,
        "more",
        added=33,
        received=later,
        imported_at=later + timedelta(seconds=1),
    )
    async with factory() as session, session.begin():
        second = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
        batches = tuple(
            await session.scalars(
                select(MonitorObservationBatch).where(
                    MonitorObservationBatch.id.in_(
                        (UUID(first["batch_id"]), UUID(second["batch_id"]))
                    )
                )
            )
        )
        assert sorted(len(row.trigger_ids) for row in batches) == [17, 17]
        assert second["automatic_review_capacity"] == "blocked" and second["pending_triggers"] == 34
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
    settings = settings.model_copy(
        update={
            "agent_paper_cycle_enabled": True,
            "agent_paper_formal_model_config_id": parent.model_config_id,
            "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
        }
    )
    normal = loop.AgentPaperLoopSnapshot(
        now=captured["clock"][0],
        calendar_authority_id=CALENDAR_ID,
        market_open_at=later - timedelta(hours=1),
        decision_window_start_at=later - timedelta(minutes=30),
        decision_window_end_at=later + timedelta(hours=4),
        next_market_open_at=later + timedelta(days=3),
        next_decision_window_start_at=later + timedelta(days=3),
    )
    assert (
        await inspect_monitor_queue(settings, account_id=pointer.account_id, snapshot=normal)
    ).monitor_trigger_id is None
    from decimal import Decimal

    from nexus.monitoring.observations import read_latest_price
    from nexus.monitoring.storage import record_check
    from nexus.persistence.models.monitoring import MonitorPlan

    async with factory() as session:
        plan = await session.scalar(
            select(MonitorPlan).where(
                MonitorPlan.kind == "price",
                MonitorPlan.account_id == pointer.account_id,
                MonitorPlan.instrument_id == pointer.instrument_id,
            )
        )
        threshold = Decimal(plan.condition["threshold"])
        false_value = threshold / 2 if plan.condition["operator"] == "gte" else threshold * 2
    for minute, midpoint in ((5, str(false_value)), (10, str(threshold))):
        now = later + timedelta(minutes=minute)
        captured["clock"][0] = now
        await _set_test_insert_clock(factory, now)
        await append_fresh_inputs(factory, pointer.instrument_id, now, midpoint=midpoint)
        async with factory() as session, session.begin():
            checked = await record_check(
                session,
                plan_id=plan.id,
                scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
                checked_at=now,
                observation=await read_latest_price(
                    session, instrument_id=pointer.instrument_id, now=now
                ),
            )
    async with factory() as session:
        legacy = await session.scalar(
            select(MonitorTrigger).where(MonitorTrigger.check_id == checked.id)
        )
        assert legacy is not None and legacy.batch_id is None
    normal = replace(
        normal,
        now=now,
        market_open_at=now.replace(hour=13, minute=30),
        decision_window_start_at=now.replace(hour=13, minute=30),
    )
    assert (
        await inspect_monitor_queue(settings, account_id=pointer.account_id, snapshot=normal)
    ).monitor_trigger_id is None
    blocked = await run(
        settings,
        argparse.Namespace(action="admit", trigger_id=legacy.id, calendar_authority_id=CALENDAR_ID),
    )
    assert blocked["status"] == "blocked" and blocked["error_code"] == "MONITOR_PENDING_OVERFLOW"
    await factory.kw["bind"].dispose()

"""One sealed stock batch reaches one native job, never three event jobs."""

import argparse
from datetime import timedelta
from uuid import UUID

import pytest
from helpers.monitor_materials import prepared_materials
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_increment_pg import append_fresh_inputs
from nexus.ops.research_monitor import run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import Job, JobLease
from nexus.settings import AgentRuntimeMode
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True, "capacity"])
async def test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
    migrated_database_url, monkeypatch, tmp_path, unknown
):
    factory, settings, store, captured, pointer, batch_id, materials_id = await prepared_materials(
        migrated_database_url, monkeypatch, tmp_path, sec_only=unknown == "capacity"
    )
    expected_count = 2 if unknown == "capacity" else 3
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
            ]
        )
    captured["clock"][0] = later
    await _set_test_insert_clock(factory, later)
    await append_fresh_inputs(factory, pointer.instrument_id, later)
    async with factory() as session, session.begin():
        monday = later + timedelta(days=3)
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
        }
    )
    args = argparse.Namespace(
        action="admit-batch",
        batch_id=batch_id,
        materials_snapshot_id=materials_id,
        calendar_authority_id=CALENDAR_ID,
    )
    admitted = await run(settings, args)
    assert admitted["status"] == "admitted" and admitted["reserved_sdk_calls"] == 3, admitted
    repeated = await run(settings, args)
    assert repeated["run_id"] == admitted["run_id"] and repeated["job_id"] == admitted["job_id"]
    from nexus.monitoring.context import read_monitor_context

    async with factory() as session:
        research = await session.get(AgentResearchRunRecord, UUID(admitted["run_id"]))
        frozen = await read_monitor_context(
            session,
            snapshot_id=research.data_snapshot_id,
            instrument_id=research.instrument_id,
            as_of=research.decision_time,
        )
        assert frozen is not None and frozen[0].purpose == "monitor_increment_v2"
        assert (
            len(frozen[0].trigger_checks) == expected_count
            and len(frozen[0].materials.contents) == 2
        )
        assert {item.kind for item in frozen[0].trigger_checks} == (
            {"disclosure"} if unknown == "capacity" else {"price", "disclosure"}
        )
        assert research.instrument_id == pointer.instrument_id
        assert tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
            ]
        ) == tuple(count + 1 for count in before)
        rows = tuple(
            await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
        )
        assert len(rows) == expected_count and all(
            row.research_run_id == research.id and row.status == "admitted" for row in rows
        )
        member = await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.run_id == research.id)
        )
        assert member.batch_id == batch_id and member.reserved_sdk_calls == 3
    if unknown == "capacity":
        from nexus.audit.service import append_audit_event
        from nexus.monitoring.preclaim import (
            close_capacity_rejected_increment_once,
            preclaim_rejection_qualified,
        )

        assert rows[0].kind == "disclosure"
        async with factory() as session, session.begin():
            await append_audit_event(
                session,
                actor_id="research-monitor",
                action="research.monitor.worker_dispatched",
                schema_version="monitor_dispatch_v1",
                occurred_at=later,
                payload={"trigger_id": str(rows[0].id), "job_id": str(member.job_id)},
            )
        closed = await close_capacity_rejected_increment_once(
            settings,
            account_id=pointer.account_id,
            trigger_id=rows[0].id,
            job_id=member.job_id,
            code="CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
        )
        assert closed["status"] == "failed" and closed["reason"] == "MONITOR_CAPACITY_REJECTED"
        assert (
            await close_capacity_rejected_increment_once(
                settings,
                account_id=pointer.account_id,
                trigger_id=rows[0].id,
                job_id=member.job_id,
                code="CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
            )
            == closed
        )
        async with factory() as session:
            child = await session.get(AgentResearchRunRecord, member.run_id)
            assert (child.input_tokens, child.output_tokens, child.settled_cost_usd) == (0, 0, 0)
            assert await preclaim_rejection_qualified(session, run=child, now=later)
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == child.id
                )
            )
            assert (
                hold.state == "released"
                and (await session.get(MonitorIncrement, member.id)).reserved_sdk_calls == 3
            )
        await factory.kw["bind"].dispose()
        return
    from nexus.agents import codex_staged
    from nexus.worker import run_agent_worker_once
    from unit.agents.test_codex_sdk import _restore_view

    native = codex_staged.invoke_codex
    stages = []

    async def tracked(payload, **kwargs):
        wire = _restore_view(payload)
        stages.append(wire["stage"])
        security = wire["frozen_inputs"]["security_snapshot"]
        assert security["purpose"] == "monitor_increment_v2"
        context = next(
            item["monitor_context"]
            for item in security["items"]
            if item["source"] == "nexus_monitor_increment_v2"
        )
        assert len(context["materials"]["contents"]) == 2
        assert len(context["trigger_checks"]) == 3
        assert any(
            "Additional GAAP reconciliation" in document["text"]
            for content in context["materials"]["contents"]
            for document in content["archive"]["documents"]
        )
        if unknown and wire["stage"] == "critique":
            raise TimeoutError("controlled mixed unknown usage")
        return await native(payload, **kwargs)

    monkeypatch.setattr(codex_staged, "invoke_codex", tracked)
    assert await run_agent_worker_once(
        settings, job_ids=(member.job_id,), formal_stage_capacity=True
    )
    assert stages == (
        ["research", "critique"] if unknown else ["research", "critique", "synthesis"]
    )
    assert not await run_agent_worker_once(
        settings, job_ids=(member.job_id,), formal_stage_capacity=True
    )
    if unknown:
        captured["clock"][0] += timedelta(minutes=2)
    finished = await run(settings, argparse.Namespace(action="finish", trigger_id=rows[0].id))
    assert finished["status"] == ("unknown" if unknown else "completed"), finished
    assert finished["impact_review"] == "not_verified" and finished["coverage"] == "unresolved"
    assert finished["unresolved_event_count"] == 3
    assert await run(settings, args) == {**admitted, "status": finished["status"]}
    async with factory() as session:
        child = await session.get(AgentResearchRunRecord, member.run_id)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == child.id
            )
        )
        assert (child.settled_cost_usd is None) == unknown
        assert reservation.state == ("reserved" if unknown else "settled")
        members = tuple(
            await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
        )
        assert all(t.status == ("unknown" if unknown else "completed") for t in members)
        assert (await session.get(MonitorIncrement, member.id)).reserved_sdk_calls == 3
    from nexus.monitoring.admission import require_monitor_account_available

    async with factory() as session, session.begin():
        if unknown:
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=pointer.account_id, now=captured["clock"][0]
                )
        else:
            await require_monitor_account_available(
                session, account_id=pointer.account_id, now=captured["clock"][0]
            )
    reconciled = await run(settings, argparse.Namespace(action="reconcile"))
    assert reconciled["awaiting_mixed_review"] == 1
    assert len(stages) == (2 if unknown else 3)
    await factory.kw["bind"].dispose()

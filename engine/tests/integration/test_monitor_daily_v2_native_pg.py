"""Known unresolved typed events are consumed only by a new normal daily study."""

import json
from datetime import timedelta

import pytest
from helpers.mixed_stage import add_mixed_fields
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_increment_pg import append_fresh_inputs
from integration.test_monitor_mixed_host_pg import ready_host_batch
from nexus.agents import codex_staged
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorDailyEventReceipt,
    MonitorMixedEventReceipt,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import Job, JobLease
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select
from unit.agents.test_codex_sdk import _restore_view


async def unresolved_then_daily(url, monkeypatch, root):
    from nexus.ops.monitor_dispatch import dispatch_configured_once
    from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
    from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

    factory, settings, captured, pointer, batch_id, normal = await ready_host_batch(
        url, monkeypatch, root
    )
    native = codex_staged.invoke_codex
    old_calls = []

    async def old_invoke(payload, **kwargs):
        wire = _restore_view(payload)
        old_calls.append(wire["stage"])
        raw, usage = await native(payload, **kwargs)
        body = json.loads(raw)
        add_mixed_fields(body, wire)
        if wire["stage"] == "research":
            for role in body["roles"]:
                if role["role"] == "market_state":
                    for item in role["mixed_event_proposal"]["items"]:
                        item["status"] = "unresolved"
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", old_invoke)
    async with factory() as session:
        members = tuple(
            await session.scalars(
                select(MonitorTrigger)
                .where(MonitorTrigger.batch_id == batch_id)
                .order_by(MonitorTrigger.id)
            )
        )
    dispatched = await dispatch_configured_once(
        settings, trigger_id=members[0].id, calendar_authority_id=CALENDAR_ID
    )
    assert old_calls == ["research", "critique", "synthesis"]
    assert dispatched["status"] == "completed", dispatched
    async with factory() as session:
        old = await session.get(MonitorTrigger, members[0].id)
        old_run_id = old.research_run_id
        old_output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == old_run_id,
                AgentResearchOutputRecord.role.is_(None),
            )
        )
        old_hash = old_output.content_hash
        now = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
    captured["clock"][0] = now
    await _set_test_insert_clock(factory, now)
    async with factory() as session:
        instruments = tuple(await session.scalars(select(LatestQuote.instrument_id).distinct()))
        batch = await load_agent_paper_candidate_batch(
            session, batch_id=settings.agent_paper_candidate_batch_id
        )
    for instrument_id in instruments:
        await append_fresh_inputs(factory, instrument_id, now)
    monkeypatch.setattr(codex_staged, "invoke_codex", native)
    async with factory() as session, session.begin():
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=pointer.account_id,
            now=now,
            expected_calendar_authority_id=CALENDAR_ID,
            expected_universe_snapshot_id=batch.universe_snapshot_id,
        )
        result = await start_schwab_agent_paper_cycle(
            session,
            authority=authority,
            decision_slot_at=now.replace(second=0, microsecond=0),
            idempotency_key="new-daily-typed-consumption",
            requested_instrument_ids=batch.instrument_ids,
            candidate_batch_id=batch.batch_id,
            formal_stage_capacity=True,
            formal_model_config_id=settings.agent_paper_formal_model_config_id,
        )
        assert result.result_code == "RESEARCH_JOBS_PREPARED", result
    return factory, settings, pointer, batch_id, members, old_run_id, old_hash, now, result, native


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["addressed", "unresolved", "veto", "unknown", "replacement_fail"])
async def test_new_daily_native_consumes_only_qualified_historical_events(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from nexus.monitoring.mixed_readback import mixed_batch_readback
    from nexus.monitoring.reconciliation import reconcile_account_results

    (
        factory,
        settings,
        pointer,
        batch_id,
        members,
        old_run_id,
        old_hash,
        now,
        result,
        native,
    ) = await unresolved_then_daily(migrated_database_url, monkeypatch, tmp_path)
    async with factory() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids)
                )
            )
        )
        target = next(row for row in runs if row.instrument_id == pointer.instrument_id)
        snapshot = await session.get(DataSnapshot, target.data_snapshot_id)
        frozen = snapshot.payload["agent_paper_context"]["pending_monitoring_v2"]
        assert {item["trigger_id"] for item in frozen["trigger_checks"]} == {
            str(row.id) for row in members
        }
        assert len(frozen["materials"]) == 1
        jobs = tuple(
            await session.scalars(
                select(Job).where(Job.payload["run_id"].astext.in_([str(row.id) for row in runs]))
            )
        )
        frozen_before = json.dumps(snapshot.payload, sort_keys=True)
        plans_before = set(await session.scalars(select(MonitorPlan.id)))
    seen = []

    async def daily_invoke(payload, **kwargs):
        wire = _restore_view(payload)
        seen.append(wire["stage"])
        if wire.get("daily_pending_context_hash") is None:
            return await native(payload, **kwargs)
        if mode == "unknown" and wire["stage"] == "critique":
            raise TimeoutError("controlled new daily unknown")
        body_raw, usage = await native(payload, **kwargs)
        body = json.loads(body_raw)
        citation = next(
            row["index"] for row in wire["evidence"] if row["source"] == "nexus_agent_paper_context"
        )
        if wire["stage"] == "research":
            for role in body["roles"]:
                if role["role"] == "orchestrator":
                    role["daily_pending_proposal"] = {
                        "context_hash": wire["daily_pending_context_hash"],
                        "items": [
                            {
                                "trigger_id": item["trigger_id"],
                                "status": "unresolved" if mode == "unresolved" else "addressed",
                                "rationale": "逐项核对精确原文和原论点，维持观察，不授执行权限。",
                                "citations": [citation],
                            }
                            for item in frozen["trigger_checks"]
                        ],
                    }
        elif wire["stage"] == "critique":
            for role in body["roles"]:
                role["daily_pending_review"] = {
                    "proposal_hash": wire["daily_pending_proposal"]["content_hash"],
                    "decision": "rejected"
                    if mode == "veto" and role["role"] == "evidence_audit"
                    else "accepted",
                    "rationale": "核对每一原事件、正文及新结论的依据。",
                    "citations": [citation],
                }
                if mode == "veto" and role["role"] == "evidence_audit":
                    role["vote"] = "oppose"
        else:
            body["daily_pending_adoption_hash"] = wire["daily_pending_proposal"]["content_hash"]
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", daily_invoke)
    selected = (
        tuple(job for job in jobs if job.payload["run_id"] == str(target.id))
        if mode == "unknown"
        else jobs
    )
    for job in selected:
        assert await run_agent_worker_once(settings, job_ids=(job.id,), formal_stage_capacity=True)
        assert not await run_agent_worker_once(
            settings, job_ids=(job.id,), formal_stage_capacity=True
        )
    if mode == "replacement_fail":
        from nexus.monitoring import disclosure_installation

        async def reject_second_kind(*_args, **_kwargs):
            raise ValueError("MONITOR_PLAN_CONFLICT")

        monkeypatch.setattr(
            disclosure_installation, "_install_mixed_or_legacy_disclosure", reject_second_kind
        )
    async with factory() as session, session.begin():
        report = await reconcile_account_results(
            session, account_id=pointer.account_id, instrument_ids=(pointer.instrument_id,), now=now
        )
        assert report["unverified"] == 0, report
    # Reconciliation is a readback on an already persisted terminal result,
    # never another Worker dispatch or a second replacement/consumption attempt.
    from nexus.persistence.models.runtime import AuditEvent, OutboxEvent

    guarded = (
        MonitorPlan,
        MonitorCoverageReceipt,
        MonitorDailyEventReceipt,
        Job,
        OutboxEvent,
        AuditEvent,
    )
    async with factory() as session:
        saved_counts = {
            model: await session.scalar(select(func.count()).select_from(model))
            for model in guarded
        }
    async with factory() as session, session.begin():
        repeated = await reconcile_account_results(
            session, account_id=pointer.account_id, instrument_ids=(pointer.instrument_id,), now=now
        )
        assert repeated["unverified"] == 0, repeated
    async with factory() as session:
        assert {
            model: await session.scalar(select(func.count()).select_from(model))
            for model in guarded
        } == saved_counts
        proofs = tuple(await session.scalars(select(MonitorDailyEventReceipt)))
        coverage = tuple(
            await session.scalars(
                select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == target.id)
            )
        )
        assert {row.kind for row in coverage} == {"price", "disclosure"}
        if mode in {"unresolved", "veto"}:
            assert all(row.status == "gap" and row.active_plan_id is None for row in coverage)
            assert {row.reason for row in coverage} == {
                "MONITOR_KIND_EVENTS_UNRESOLVED"
                if mode == "unresolved"
                else "MONITOR_DAILY_EVENT_REVIEW_UNVERIFIED"
            }
            assert set(await session.scalars(select(MonitorPlan.id))) == plans_before
        if mode == "replacement_fail":
            assert all(row.status == "gap" for row in coverage)
            assert set(await session.scalars(select(MonitorPlan.id))) == plans_before
        original = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == old_run_id,
                AgentResearchOutputRecord.role.is_(None),
            )
        )
        assert original.content_hash == old_hash
        for item in members:
            row = await session.get(MonitorTrigger, item.id)
            assert row.research_run_id == old_run_id and row.status == "completed"
        assert await session.scalar(select(func.count()).select_from(MonitorMixedEventReceipt)) == 0
        assert (
            json.dumps(
                (await session.get(DataSnapshot, target.data_snapshot_id)).payload, sort_keys=True
            )
            == frozen_before
        )
        readback = await mixed_batch_readback(session, batch_id)
        if mode == "addressed":
            assert len(proofs) == 3
            assert all(
                row.run_id == target.id and row.original_run_id == old_run_id for row in proofs
            )
            assert all(
                row.status == "installed" and row.active_plan_id is not None for row in coverage
            )
            assert all(row.active_plan_id not in plans_before for row in coverage)
            assert readback["impact_review"] == "verified"
            assert readback["unresolved_event_count"] == 0
            from nexus.api.research_monitoring import read_monitoring_view

            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            api = await read_monitoring_view(session, parent, now=now)
            assert len(api.triggers) == 1
            assert api.triggers[0].status == "covered"
            assert api.triggers[0].run_id == target.id
            assert api.triggers[0].usage_status == "known"
        else:
            assert not proofs and readback["unresolved_event_count"] == 3
        if mode in {"unresolved", "veto"}:
            from nexus.api.research_monitoring import read_monitoring_view

            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            api = await read_monitoring_view(session, parent, now=now)
            assert api.execution_locked and api.state == "withdrawn"
            assert len(api.triggers) == 1
            assert api.triggers[0].status == "completed"
            assert api.triggers[0].run_id == old_run_id
            assert api.triggers[0].usage_status == "known"
        target_budget = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == target.id
            )
        )
        if mode == "unknown":
            assert target_budget.state == "reserved" and target_budget.settled_usd is None
            assert all(row.status == "unknown" for row in coverage)
        else:
            assert target_budget.state == "settled" and target_budget.settled_usd is not None
        if mode == "unknown":
            assert seen == ["research", "critique"]
        else:
            assert len(seen) == 9
            assert all(seen.count(stage) == 3 for stage in ("research", "critique", "synthesis"))
    await factory.kw["bind"].dispose()

"""Daily normal-cycle inputs consume only exact, reviewed pending evidence."""

import json
from datetime import timedelta

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_monitor_increment_pg import append_fresh_inputs, ready_trigger
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorDailyTriggerReceipt, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job, OutboxEvent
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select


async def prepare_daily(
    database_url,
    monkeypatch,
    *,
    late_before_freeze=False,
    pending_count=1,
    freeze_fault=None,
    expected_code="RESEARCH_JOBS_PREPARED",
):
    from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
    from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

    factory, settings, trigger, parent, now = await ready_trigger(database_url, monkeypatch)
    if pending_count > 1:
        from integration.test_monitor_pending_pg import pending_members

        await pending_members(factory, trigger, pending_count)
    if late_before_freeze:
        from integration.test_candidate_multistock_path import _set_test_insert_clock
        from integration.test_monitor_pending_pg import pending_members

        await _set_test_insert_clock(factory, now + timedelta(seconds=1))
        await pending_members(factory, trigger, 2)
        await _set_test_insert_clock(factory, now)
    async with factory() as session:
        batch = await load_agent_paper_candidate_batch(
            session, batch_id=settings.agent_paper_candidate_batch_id
        )
    from nexus.persistence.models.market import LatestQuote

    async with factory() as session:
        quote_scope = tuple(await session.scalars(select(LatestQuote.instrument_id).distinct()))
    for instrument_id in quote_scope:
        if instrument_id != parent.instrument_id:
            await append_fresh_inputs(factory, instrument_id, now)
    if freeze_fault is not None:
        from nexus.monitoring import daily_context

        async def invalid_context(*_args, **_kwargs):
            raise ValueError(freeze_fault)

        monkeypatch.setattr(daily_context, "freeze_daily_pending", invalid_context)
    async with factory() as session, session.begin():
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=trigger.account_id,
            now=now,
            expected_calendar_authority_id=CALENDAR_ID,
            expected_universe_snapshot_id=batch.universe_snapshot_id,
        )
        result = await start_schwab_agent_paper_cycle(
            session,
            authority=authority,
            decision_slot_at=now.replace(second=0, microsecond=0),
            idempotency_key="new-daily-pending-cycle",
            requested_instrument_ids=batch.instrument_ids,
            candidate_batch_id=batch.batch_id,
            formal_stage_capacity=True,
            formal_model_config_id=parent.model_config_id,
        )
        assert result.result_code == expected_code, result
    return factory, settings, trigger, parent, now, result


@pytest.mark.asyncio
async def test_normal_daily_does_not_swallow_invalid_pending_evidence(
    migrated_database_url, monkeypatch
):
    from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
    from sqlalchemy import func

    factory, _settings, trigger, _parent, _now, result = await prepare_daily(
        migrated_database_url,
        monkeypatch,
        freeze_fault="DAILY_MONITOR_CONTEXT_INVALID",
        expected_code="RESEARCH_ADMISSION_UNAVAILABLE",
    )
    assert result.status == "unavailable" and not result.research_run_ids
    async with factory() as session:
        cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
        assert cycle.execution_locked is True
        latest = await session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == result.cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        assert latest.to_status == "unavailable"
        assert latest.result_code == "RESEARCH_ADMISSION_UNAVAILABLE"
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AgentResearchRunRecord)
                .where(
                    AgentResearchRunRecord.idempotency_key.like(f"agent-paper:{result.cycle_id}:%")
                )
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(MonitorDailyTriggerReceipt)
                .where(MonitorDailyTriggerReceipt.cycle_id == result.cycle_id)
            )
            == 0
        )
        original = await session.get(MonitorTrigger, trigger.id)
        assert original.research_run_id is None and original.status == "pending"
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["known", "unknown"])
async def test_normal_daily_keeps_overflow_as_frozen_gap_without_consumption(
    migrated_database_url, monkeypatch, mode
):
    from nexus.agents import codex_staged
    from nexus.agents.paper_context import read_agent_paper_research_context
    from nexus.monitoring.pending import pending_stock
    from sqlalchemy import func
    from unit.agents.test_codex_sdk import _restore_view

    factory, settings, trigger, parent, now, result = await prepare_daily(
        migrated_database_url, monkeypatch, pending_count=33
    )
    async with factory() as session:
        target = await session.scalar(
            select(AgentResearchRunRecord).where(
                AgentResearchRunRecord.id.in_(result.research_run_ids),
                AgentResearchRunRecord.instrument_id == parent.instrument_id,
            )
        )
        assert target is not None
        bound = await read_agent_paper_research_context(
            session,
            snapshot_id=target.data_snapshot_id,
            instrument_id=parent.instrument_id,
            as_of=now,
        )
        assert bound is not None
        frozen = bound[0].model_dump(mode="json")
        assert "pending_monitoring" not in frozen
        assert frozen["pending_monitoring_gap"] == {
            "purpose": "daily_monitor_gap_v1",
            "scope": "unassigned_price_v1",
            "reason": "MONITOR_PENDING_OVERFLOW",
            "observed_at": now.isoformat().replace("+00:00", "Z"),
            "pending_count": 33,
            "max_pending": 32,
        }
        assert bound[0].execution_locked is True
        originals = tuple(
            await session.scalars(
                select(MonitorTrigger).where(
                    MonitorTrigger.account_id == trigger.account_id,
                    MonitorTrigger.instrument_id == parent.instrument_id,
                )
            )
        )
        assert len(originals) == 33
        assert all(row.status == "pending" and row.research_run_id is None for row in originals)
        assert (
            await session.scalar(select(func.count()).select_from(MonitorDailyTriggerReceipt)) == 0
        )
        with pytest.raises(ValueError, match="^MONITOR_PENDING_OVERFLOW$"):
            await pending_stock(
                session,
                account_id=trigger.account_id,
                instrument_id=parent.instrument_id,
                now=now,
            )
        # All three normal jobs retain their original reservations and admission.
        holds = tuple(
            await session.scalars(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id.in_(result.research_run_ids)
                )
            )
        )
        assert len(result.research_run_ids) == len(holds) == 3
        assert all(row.state == "reserved" and row.settled_usd is None for row in holds)
        snapshot = await session.get(DataSnapshot, target.data_snapshot_id)
        frozen_before = json.dumps(snapshot.payload, sort_keys=True)
        jobs = tuple(
            await session.scalars(
                select(Job).where(Job.payload["run_id"].astext.in_([str(target.id)]))
            )
        )

    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_monitor_pending_pg import pending_members

    await _set_test_insert_clock(factory, now + timedelta(seconds=1))
    late_id = (await pending_members(factory, trigger, 34, start=33))[-1]
    await _set_test_insert_clock(factory, now)

    # A genuinely new daily job still uses the original Worker and settlement.
    # Timeout is a synthetic new unknown; it must not clear the old backlog.
    native = codex_staged.invoke_codex
    seen = []

    async def invoke(payload, **kwargs):
        payload = _restore_view(payload)
        seen.append(payload["stage"])
        assert payload["daily_pending_context_hash"] is None
        assert "not reviewed or consumed" in payload["instructions"]
        if mode == "unknown" and payload["stage"] == "critique":
            raise TimeoutError("controlled daily capacity-gap unknown")
        return await native(payload, **kwargs)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    assert len(jobs) == 1
    assert await run_agent_worker_once(settings, job_ids=(jobs[0].id,), formal_stage_capacity=True)
    assert not await run_agent_worker_once(
        settings, job_ids=(jobs[0].id,), formal_stage_capacity=True
    )
    from nexus.monitoring.reconciliation import reconcile_account_results

    async with factory() as session, session.begin():
        await reconcile_account_results(
            session, account_id=trigger.account_id, instrument_ids=(parent.instrument_id,), now=now
        )
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(MonitorDailyTriggerReceipt)) == 0
        )
        old_rows = tuple(
            await session.scalars(
                select(MonitorTrigger).where(MonitorTrigger.instrument_id == parent.instrument_id)
            )
        )
        assert len(old_rows) == 34 and late_id in {row.id for row in old_rows}
        assert all(row.status == "pending" and row.research_run_id is None for row in old_rows)
        persisted = await session.get(AgentResearchRunRecord, target.id)
        hold = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == target.id
            )
        )
        if mode == "known":
            assert seen == ["research", "critique", "synthesis"]
            assert persisted.status == "completed" and hold.state == "settled"
            assert hold.settled_usd == persisted.settled_cost_usd
        else:
            assert seen == ["research", "critique"]
            assert persisted.status == "failed" and hold.state == "reserved"
            assert persisted.settled_cost_usd is None and hold.settled_usd is None
        snapshot = await session.get(DataSnapshot, target.data_snapshot_id)
        assert json.dumps(snapshot.payload, sort_keys=True) == frozen_before
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pending_count", "late_before_freeze"), [(1, False), (1, True), (32, False)]
)
async def test_normal_daily_freezes_pending_without_rewriting_trigger(
    migrated_database_url, monkeypatch, late_before_freeze, pending_count
):
    from nexus.agents.paper_context import read_agent_paper_research_context

    factory, settings, trigger, parent, now, result = await prepare_daily(
        migrated_database_url,
        monkeypatch,
        late_before_freeze=late_before_freeze,
        pending_count=pending_count,
    )
    async with factory() as session:
        run = await session.scalar(
            select(AgentResearchRunRecord).where(
                AgentResearchRunRecord.id.in_(result.research_run_ids),
                AgentResearchRunRecord.instrument_id == parent.instrument_id,
            )
        )
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        frozen = snapshot.payload["agent_paper_context"].get("pending_monitoring")
        assert frozen is not None
        assert len(frozen["trigger_checks"]) == pending_count
        assert str(trigger.id) in {item["trigger_id"] for item in frozen["trigger_checks"]}
        assert "pending_monitoring_gap" not in snapshot.payload["agent_paper_context"]
        assert frozen["parents"][0]["parent_run_id"] == str(parent.id)
        bound = await read_agent_paper_research_context(
            session, snapshot_id=run.data_snapshot_id, instrument_id=parent.instrument_id, as_of=now
        )
        assert bound is not None and bound[0].pending_monitoring is not None
        old = await session.get(MonitorTrigger, trigger.id)
        assert old.status == "pending" and old.research_run_id is None
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["addressed", "mixed", "unresolved", "rejected", "unknown"])
async def test_daily_native_worker_consumes_only_reviewed_known_frozen_triggers(
    migrated_database_url, monkeypatch, mode
):
    from nexus.agents import codex_staged
    from nexus.monitoring.pending import pending_stock
    from nexus.monitoring.reconciliation import reconcile_account_results
    from nexus.ops.research_monitor import parser
    from nexus.ops.research_monitor import run as monitor_entry
    from unit.agents.test_codex_sdk import _restore_view

    factory, settings, trigger, parent, now, result = await prepare_daily(
        migrated_database_url, monkeypatch, pending_count=2 if mode == "mixed" else 1
    )
    late_id = None
    if mode == "addressed":
        from integration.test_monitor_pending_pg import pending_members

        late_id = (await pending_members(factory, trigger, 2))[-1]
        from nexus.monitoring.storage import mark_trigger_blocked

        async with factory() as session, session.begin():
            await mark_trigger_blocked(
                session, trigger_id=trigger.id, reason="MONITOR_BUDGET_UNAVAILABLE", now=now
            )
    async with factory() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids)
                )
            )
        )
        target = next(run for run in runs if run.instrument_id == parent.instrument_id)
        snapshot = await session.get(DataSnapshot, target.data_snapshot_id)
        frozen_before = json.dumps(snapshot.payload, sort_keys=True)
        frozen_ids = [
            item["trigger_id"]
            for item in snapshot.payload["agent_paper_context"]["pending_monitoring"][
                "trigger_checks"
            ]
        ]
        jobs = tuple(
            await session.scalars(
                select(Job).where(Job.payload["run_id"].astext.in_([str(run.id) for run in runs]))
            )
        )
        outbox_before = set(await session.scalars(select(OutboxEvent.id)))
    native = codex_staged.invoke_codex
    seen = []
    all_seen = []

    async def invoke(payload, **kwargs):
        payload = _restore_view(payload)
        all_seen.append(payload["stage"])
        context_hash = payload["daily_pending_context_hash"]
        body_text, usage = await native(payload, **kwargs)
        if context_hash is None:
            return body_text, usage
        stage = payload["stage"]
        seen.append(stage)
        if mode == "unknown" and stage == "critique":
            raise TimeoutError("controlled unknown daily result")
        body = json.loads(body_text)
        citation = next(
            item["index"]
            for item in payload["evidence"]
            if item["source"] == "nexus_agent_paper_context"
        )
        if stage == "research":
            for row in body["roles"]:
                if row["role"] == "orchestrator":
                    row["daily_pending_proposal"] = {
                        "context_hash": context_hash,
                        "items": [
                            {
                                "trigger_id": trigger_id,
                                "status": "unresolved"
                                if mode == "unresolved" or mode == "mixed" and index
                                else "addressed",
                                "rationale": "已核对冻结父论点和触发证据，保持观察。",
                                "citations": [citation],
                            }
                            for index, trigger_id in enumerate(frozen_ids)
                        ],
                    }
        elif stage == "critique":
            for row in body["roles"]:
                row["daily_pending_review"] = {
                    "proposal_hash": payload["daily_pending_proposal"]["content_hash"],
                    "decision": "rejected"
                    if mode == "rejected" and row["role"] == "evidence_audit"
                    else "accepted",
                    "rationale": "逐项核对原触发、父论点和冻结证据。",
                    "citations": [citation],
                }
        else:
            body["daily_pending_adoption_hash"] = payload["daily_pending_proposal"]["content_hash"]
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    selected_jobs = (
        tuple(job for job in jobs if job.payload["run_id"] == str(target.id))
        if mode == "unknown"
        else jobs
    )
    for job in selected_jobs:
        assert await run_agent_worker_once(settings, job_ids=(job.id,), formal_stage_capacity=True)
        assert not await run_agent_worker_once(
            settings, job_ids=(job.id,), formal_stage_capacity=True
        )
    async with factory() as session, session.begin():
        now = await session.scalar(select(func.clock_timestamp()))
        observed = await reconcile_account_results(
            session,
            account_id=trigger.account_id,
            instrument_ids=tuple(run.instrument_id for run in runs),
            now=now,
        )
        assert observed["unverified"] == 0, observed
    async with factory() as session:
        receipts = tuple(await session.scalars(select(MonitorDailyTriggerReceipt)))
        old = await session.get(MonitorTrigger, trigger.id)
        persisted = await session.get(AgentResearchRunRecord, target.id)
        budget = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == target.id
            )
        )
        assert old.status == ("blocked" if mode == "addressed" else "pending")
        assert old.research_run_id is None
        if mode == "addressed":
            from nexus.api.research_monitoring import read_monitoring_view

            assert old.reason == "MONITOR_BUDGET_UNAVAILABLE"
            view = await read_monitoring_view(session, run=parent, now=now)
            covered = next(item for item in view.triggers if item.trigger_id == trigger.id)
            assert covered.status == "covered" and covered.blocked_reason is None
        assert old.outbox_id == trigger.outbox_id
        assert set(await session.scalars(select(OutboxEvent.id))) == outbox_before
        assert (
            json.dumps(
                (await session.get(DataSnapshot, target.data_snapshot_id)).payload, sort_keys=True
            )
            == frozen_before
        )
        pending = await pending_stock(
            session,
            account_id=trigger.account_id,
            instrument_id=parent.instrument_id,
            now=now,
            allow_empty=True,
        )
        if mode in {"addressed", "mixed"}:
            assert len(receipts) == 1
            assert receipts[0].trigger_id == trigger.id and receipts[0].run_id == target.id
            remaining_id = (
                late_id
                if mode == "addressed"
                else next(row.id for row, _ in pending if str(row.id) == frozen_ids[1])
            )
            assert [row.id for row, _ in pending] == [remaining_id]
            late = await session.get(MonitorTrigger, remaining_id)
            assert late.status == "pending" and late.research_run_id is None
            audits = tuple(
                await session.scalars(
                    select(AuditEvent).where(
                        AuditEvent.action == "research.monitor.daily_trigger_covered"
                    )
                )
            )
            assert len(audits) == 1 and audits[0].payload["receipt_id"] == str(receipts[0].id)
        else:
            assert not receipts and [row.id for row, _ in pending] == [trigger.id]
        if mode == "unknown":
            assert persisted.status == "failed" and persisted.settled_cost_usd is None
            assert budget.state == "reserved" and budget.settled_usd is None
            assert seen == ["research", "critique"]
            queued = tuple(
                await session.scalars(
                    select(Job).where(
                        Job.id.in_([job.id for job in jobs if job not in selected_jobs])
                    )
                )
            )
            assert len(queued) == 2 and all(job.status == "queued" for job in queued)
        else:
            assert persisted.status == "completed" and budget.state == "settled"
            assert seen == ["research", "critique", "synthesis"]
    if mode == "addressed":
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
        assert admitted["status"] == "covered" and admitted["run_id"] == str(target.id)
        assert admitted["model_requests"] == admitted["provider_requests"] == 0
        from nexus.ops.agent_paper_loop import AgentPaperLoopSnapshot
        from nexus.ops.monitor_dispatch import dispatch_configured_once, inspect_monitor_queue

        enabled = settings.model_copy(update={"agent_paper_cycle_enabled": True})
        selected = await inspect_monitor_queue(
            enabled,
            account_id=trigger.account_id,
            snapshot=AgentPaperLoopSnapshot(
                now=now,
                calendar_authority_id=CALENDAR_ID,
                market_open_at=now - timedelta(hours=1),
                decision_window_start_at=now - timedelta(minutes=30),
                decision_window_end_at=now + timedelta(hours=4),
                next_decision_window_start_at=now + timedelta(days=3),
            ),
        )
        assert selected.monitor_trigger_id == late_id
        stale_selection = await dispatch_configured_once(
            enabled, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
        )
        assert stale_selection["status"] == "covered"
        assert stale_selection["run_id"] == str(target.id)
        assert stale_selection["model_requests"] == stale_selection["provider_requests"] == 0
    if mode == "unknown":
        from nexus.paper.manual_request import require_manual_account_available

        async with factory() as session, session.begin():
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=trigger.account_id, now=now
                )
    async with factory() as session, session.begin():
        repeat = await reconcile_account_results(
            session,
            account_id=trigger.account_id,
            instrument_ids=tuple(run.instrument_id for run in runs),
            now=now,
        )
        assert repeat["recorded"] == 0
    assert all_seen == (
        ["research", "critique"] if mode == "unknown" else ["research", "critique", "synthesis"] * 3
    )
    await factory.kw["bind"].dispose()

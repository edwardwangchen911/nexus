"""Real internal fills precede a new hold/watch study and one affected-stock review."""

import asyncio
import json
import os
import subprocess
import sys
import sysconfig
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import app, headers
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as normal_cycle,
)
from integration.test_monitor_increment_pg import append_fresh_inputs
from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
from nexus.agents import codex_staged
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops.openai_local import prepare_database
from nexus.ops.research_monitor import parser, run
from nexus.paper.agent_cycle import (
    resume_schwab_agent_paper_cycle,
    start_schwab_agent_paper_cycle,
)
from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorIncrement,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderRecord,
    PaperPositionAdjustmentEventRecord,
)
from nexus.persistence.models.runtime import Job, JobLease, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once, run_portfolio_synthesis_worker_once
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.agents.test_monitoring_producer import proposal_wire
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


async def ledger_facts(factory):
    result = {}
    async with factory() as session:
        for model in (
            PaperOrderRecord,
            PaperFillRecord,
            PaperLedgerTransactionRecord,
            PaperLedgerEntryRecord,
            PaperPositionAdjustmentEventRecord,
        ):
            rows = (await session.execute(select(model.__table__).order_by(model.id))).mappings()
            result[model.__tablename__] = sha256(
                json.dumps([dict(row) for row in rows], sort_keys=True, default=str).encode()
            ).hexdigest()
    return result


@pytest.mark.asyncio
async def test_filled_hold_and_complete_watch_follow_normal_monitoring_without_new_orders(
    migrated_database_url, monkeypatch, tmp_path
):
    # Normal deterministic order/risk/fill/ledger services, never a broker or a
    # manually inserted position. The existing three-stock fixture fills two.
    await normal_cycle(migrated_database_url, monkeypatch, 3, True, "formal_v11")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        prior = await session.scalar(select(AgentPaperCycleRecord))
        seed_runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
        old_outputs = {
            row.id: row.content_hash
            for row in await session.scalars(select(AgentResearchOutputRecord))
        }
        first_at = max(
            AT + timedelta(hours=1, seconds=1),
            await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1),
        )
        projection = await load_paper_projection(
            session, account_id=prior.account_id, as_of=first_at
        )
        held = {row.instrument_id: row.quantity for row in projection.positions if row.quantity > 0}
        assert len(held) == 2
        assert projection.nav > projection.available_cash > 0
        assert projection.reserved_cash == 0 and not projection.open_orders
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 2
        assert await session.scalar(select(func.sum(PaperLedgerEntryRecord.debit_amount))) == (
            await session.scalar(select(func.sum(PaperLedgerEntryRecord.credit_amount)))
        )
    protected_ledger = await ledger_facts(factory)
    async with factory() as session, session.begin():
        monday = AT + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
            )
        )
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_runtime_mode="codex_sdk",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=prior.account_id,
        agent_paper_candidate_batch_id=prior.candidate_batch_id,
        agent_paper_formal_model_config_id=seed_runs[0].model_config_id,
    )
    clock = [first_at]
    original_scalar = AsyncSession.scalar

    async def database_clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return clock[0]
        return await original_scalar(self, statement, *args, **kwargs)

    class WorkerClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0] if tz is None else clock[0].astimezone(tz)

    monkeypatch.setattr(AsyncSession, "scalar", database_clock)
    monkeypatch.setattr("nexus.worker.datetime", WorkerClock)
    await _set_test_insert_clock(factory, first_at - timedelta(seconds=1))
    await prepare_database(
        settings, **V9_OPTIONS, formal_bounded_stage_budget=True, price_monitoring=True
    )
    await _set_test_insert_clock(factory, first_at)
    for instrument_id, close, *_ in INSTRUMENTS:
        await append_fresh_inputs(factory, instrument_id, clock[0], midpoint=str(close))

    native = codex_staged.invoke_codex
    calls = []

    async def synthetic_hold(payload, **kwargs):
        instrument_id = UUID(payload["request"]["instrument_id"])
        calls.append((instrument_id, payload["stage"]))
        raw, usage = await native(payload, **kwargs)
        body = json.loads(raw)
        if payload["stage"] == "research":
            for row in body["roles"]:
                row["daily_pending_proposal"] = None
                row["monitoring_proposal"] = (
                    proposal_wire(clock[0]) if row["role"] == "market_state" else None
                )
                if row["monitoring_proposal"] is not None:
                    row["monitoring_proposal"]["condition"]["operator"] = "gte"
        elif payload["stage"] == "critique":
            for row in body["roles"]:
                row["daily_pending_review"] = None
                row["monitoring_review"] = {
                    "proposal_hash": payload["monitoring_proposal"]["content_hash"],
                    "decision": "accepted",
                    "rationale": "Check the frozen thesis and price review condition.",
                    "citations": [0],
                }
        else:
            body["synthesis_assessment"]["stance"] = "hold" if instrument_id in held else "watch"
            body["synthesis_assessment"]["claims"][0]["text"] = (
                "Frozen evidence supports maintaining the existing position."
                if instrument_id in held
                else "Keep the unheld security under observation."
            )
            body["daily_pending_adoption_hash"] = None
            body["monitoring_adoption_hash"] = payload["monitoring_proposal"]["content_hash"]
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", synthetic_hold)
    async with factory() as session, session.begin():
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=prior.account_id,
            now=clock[0],
            expected_calendar_authority_id=CALENDAR_ID,
        )
        started = await start_schwab_agent_paper_cycle(
            session,
            authority=authority,
            decision_slot_at=clock[0].replace(second=0, microsecond=0),
            idempotency_key="new-filled-hold-study",
            requested_instrument_ids=tuple(row[0] for row in INSTRUMENTS),
            candidate_batch_id=prior.candidate_batch_id,
            formal_stage_capacity=True,
            formal_model_config_id=settings.agent_paper_formal_model_config_id,
        )
        assert started.result_code == "RESEARCH_JOBS_PREPARED", started
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord)
                .where(AgentResearchRunRecord.id.in_(started.research_run_ids))
                .order_by(AgentResearchRunRecord.instrument_id)
            )
        )
        for research in runs:
            bound = await read_agent_paper_research_context(
                session,
                snapshot_id=research.data_snapshot_id,
                instrument_id=research.instrument_id,
                as_of=research.decision_time,
            )
            assert bound is not None
            assert (Decimal(bound[0].paper_portfolio.candidate_position_weight) > 0) == (
                research.instrument_id in held
            )
        jobs = tuple(
            await session.scalars(
                select(Job).where(
                    Job.payload["run_id"].astext.in_(
                        [str(value) for value in started.research_run_ids]
                    )
                )
            )
        )
    for job in jobs:
        assert await run_agent_worker_once(settings, job_ids=(job.id,), formal_stage_capacity=True)
    assert len(calls) == 9
    async with factory() as session:
        for research in runs:
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == research.id,
                    AgentResearchOutputRecord.role.is_(None),
                )
            )
            assert output.intent["quality_status"] == "complete", output.payload
            assert output.intent["stance"] == (
                "hold" if research.instrument_id in held else "watch"
            )
            assert output.intent["exposure_action"] == (
                "maintain" if research.instrument_id in held else "none"
            )

    clock[0] += timedelta(seconds=2)
    await _set_test_insert_clock(factory, clock[0])
    async with factory() as session, session.begin():
        pending = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=started.cycle_id,
            idempotency_key="hold-synthesis",
            now=clock[0],
            daily_limit_usd=Decimal(3),
            formal_stage_capacity=True,
        )
        assert pending.result_code == "PORTFOLIO_SYNTHESIS_PENDING", pending
        synthesis = await session.scalar(
            select(AgentPortfolioSynthesisRunRecord).where(
                AgentPortfolioSynthesisRunRecord.cycle_id == started.cycle_id
            )
        )
        intent = synthesis_intent(synthesis)
        intent = intent.model_copy(
            update={
                "choices": tuple(
                    choice.model_copy(update={"action": "maintain", "role": "tactical"})
                    if choice.instrument_id in held
                    else choice
                    for choice in intent.choices
                )
            }
        )
        synthesis_job = await session.scalar(
            select(Job).where(Job.payload == {"run_id": str(synthesis.id)})
        )
    portfolio_calls = []

    async def synthetic_portfolio(payload, **kwargs):
        portfolio_calls.append(payload["input_hash"])
        assert payload["input_hash"] == synthesis.input_hash
        return intent.model_dump_json(), UsageSnapshot(
            input_tokens=100, output_tokens=100, provider_calls=1
        )

    monkeypatch.setattr(
        "nexus.agents.portfolio_synthesis_worker.invoke_codex_portfolio", synthetic_portfolio
    )
    assert await run_portfolio_synthesis_worker_once(
        settings, job_ids=(synthesis_job.id,), formal_stage_capacity=True
    )
    clock[0] += timedelta(seconds=1)
    await _set_test_insert_clock(factory, clock[0])
    async with factory() as session, session.begin():
        finished = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=started.cycle_id,
            idempotency_key="hold-closeout",
            now=clock[0],
            formal_stage_capacity=True,
        )
        assert finished.result_code == "NO_ORDER_REQUIRED", finished
        assert not finished.order_ids
        assert (await session.get(AgentPaperCycleRecord, started.cycle_id)).execution_locked
    assert portfolio_calls == [synthesis.input_hash]
    assert await ledger_facts(factory) == protected_ledger
    reconciled = await run(settings, parser().parse_args(["reconcile"]))
    assert reconciled["recorded"] == 3, reconciled
    async with factory() as session:
        plans = tuple(await session.scalars(select(MonitorPlan)))
        assert len(plans) == 3
        assert {plan.source_run_id for plan in plans} == set(started.research_run_ids)
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
    scanned = await run(
        settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    )
    assert scanned["false"] == 3 and scanned["unknown"] == 0, scanned
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
    assert len(calls) == 9

    held_run = next(research for research in runs if research.instrument_id in held)
    async with factory() as session:
        original_plan = next(plan for plan in plans if plan.source_run_id == held_run.id)
        parent_output = await session.get(AgentResearchOutputRecord, original_plan.source_output_id)
        workload_before = {
            model: set(await session.scalars(select(model.id)))
            for model in (AgentResearchRunRecord, Job, OutboxEvent)
        }
    # A genuinely later five-minute bucket remains quiet when fresh prices do
    # not match. Stale/unknown cases are independently covered by the tick tests.
    clock[0] = first_at + timedelta(minutes=5)
    await _set_test_insert_clock(factory, clock[0])
    for instrument_id, close, *_ in INSTRUMENTS:
        await append_fresh_inputs(factory, instrument_id, clock[0], midpoint=str(close))
    quiet = await run(
        settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    )
    assert quiet["false"] == 3 and quiet["unknown"] == 0, quiet
    async with factory() as session:
        assert {
            model: set(await session.scalars(select(model.id))) for model in workload_before
        } == workload_before
    assert len(calls) == 9

    clock[0] = first_at + timedelta(minutes=10)
    await _set_test_insert_clock(factory, clock[0])
    for instrument_id, close, *_ in INSTRUMENTS:
        await append_fresh_inputs(
            factory,
            instrument_id,
            clock[0],
            midpoint="123.45" if instrument_id == held_run.instrument_id else str(close),
        )
    # The typed entry must use the same qualified scheduled history. Missing
    # materials remain a hard rejection, with no admitted Job or budget.
    from nexus.monitoring.batches import scan_stock_batch
    from nexus.monitoring.mixed_admission import admit_batch

    async with factory() as session:
        transaction = await session.begin()
        try:
            batch = await scan_stock_batch(
                session,
                account_id=prior.account_id,
                instrument_id=held_run.instrument_id,
                now=clock[0],
            )
            assert batch["new_triggers"] == 1
            with pytest.raises(ValueError, match="^MONITOR_CONTEXT_INVALID$"):
                await admit_batch(
                    session,
                    account_id=prior.account_id,
                    batch_id=UUID(batch["batch_id"]),
                    materials_snapshot_id=UUID(int=0),
                    candidate_batch_id=prior.candidate_batch_id,
                    calendar_authority_id=CALENDAR_ID,
                    model_config_id=settings.agent_paper_formal_model_config_id,
                    daily_limit_usd=settings.agent_daily_budget_usd,
                    daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                    now=clock[0],
                )
        finally:
            await transaction.rollback()
    hit = await run(
        settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    )
    async with factory() as session:
        observed = [
            {
                "instrument": str(row.instrument_id),
                "at": str(row.checked_at),
                "result": row.result,
                "reason": row.reason,
                "observation": row.observation,
            }
            for row in await session.scalars(select(MonitorCheck).order_by(MonitorCheck.checked_at))
        ]
    assert hit["true"] == 1 and hit["false"] == 2 and hit["unknown"] == 0, json.dumps(
        {"scan": hit, "checks": observed, "plan": original_plan.condition}, default=str
    )
    await run(settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)]))
    async with factory() as session:
        triggers = tuple(await session.scalars(select(MonitorTrigger)))
        assert len(triggers) == 1 and triggers[0].instrument_id == held_run.instrument_id
        trigger = triggers[0]
        assert trigger.research_run_id is None
        event = await session.get(OutboxEvent, trigger.outbox_id)
        assert (
            set(await session.scalars(select(AgentResearchRunRecord.id)))
            == workload_before[AgentResearchRunRecord]
        )
        assert set(await session.scalars(select(OutboxEvent.id))) == workload_before[
            OutboxEvent
        ] | {event.id}

    from nexus.outbox.dispatcher import dispatch_outbox_once
    from nexus.outbox.models import PublisherSpec
    from nexus.outbox.registry import PublisherRegistry

    deliveries = []

    async def synthetic_notification(message):
        deliveries.append(message.event_id)

    registry = PublisherRegistry(
        (
            PublisherSpec(
                event_type=event.event_type,
                schema_version=event.schema_version,
                consumer_idempotent=True,
                publisher=synthetic_notification,
            ),
        )
    )
    assert await dispatch_outbox_once(
        factory,
        registry=registry,
        worker_id="held-monitor-notification",
        clock=lambda: clock[0],
        claim_registered_only=True,
    )
    assert not await dispatch_outbox_once(
        factory,
        registry=registry,
        worker_id="held-monitor-notification",
        clock=lambda: clock[0],
        claim_registered_only=True,
    )
    assert deliveries == [event.id]

    async with factory() as session:
        clock[0] = max(
            clock[0] + timedelta(seconds=1),
            await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1),
        )
    await _set_test_insert_clock(factory, clock[0])
    await append_fresh_inputs(factory, held_run.instrument_id, clock[0], midpoint="123.45")
    from nexus.paper.manual_request import require_manual_account_available

    # The manual policy is deliberately unchanged; only the scheduled monitor
    # uses the existing, fully qualified scheduled-completion branch.
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
            await require_manual_account_available(
                session, account_id=prior.account_id, now=clock[0]
            )
    args = parser().parse_args(
        ["admit", "--trigger-id", str(trigger.id), "--calendar-authority-id", str(CALENDAR_ID)]
    )
    admitted = await run(settings, args)
    assert admitted["status"] == "admitted", admitted
    assert (await run(settings, args))["run_id"] == admitted["run_id"]
    child_id = UUID(admitted["run_id"])
    from nexus.monitoring.context import read_monitor_context

    async with factory() as session:
        child = await session.get(AgentResearchRunRecord, child_id)
        bound = await read_monitor_context(
            session,
            snapshot_id=child.data_snapshot_id,
            instrument_id=held_run.instrument_id,
            as_of=child.decision_time,
        )
        assert bound is not None
        assert bound[0].parent_run_id == held_run.id
        assert bound[0].parent_output_id == parent_output.id
        assert bound[0].parent_output_hash == parent_output.content_hash
        assert bound[0].parent_intent.stance == "hold"
        assert bound[0].parent_intent.exposure_action == "maintain"
        assert Decimal(bound[0].paper_portfolio.candidate_position_weight) > 0
        assert [row.trigger_id for row in bound[0].trigger_checks] == [trigger.id]
        assert bound[0].decision_quote.quote_id != bound[0].trigger_checks[0].observation.quote_id
        assert set(await session.scalars(select(AgentResearchRunRecord.id))) == workload_before[
            AgentResearchRunRecord
        ] | {child_id}
        member = await session.scalar(select(MonitorIncrement))
        assert member.run_id == child_id and member.reserved_sdk_calls == 3
    assert await run_agent_worker_once(
        settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
    )
    assert len(calls) == 12 and all(row[0] == held_run.instrument_id for row in calls[-3:])
    finished_increment = await run(
        settings, parser().parse_args(["finish", "--trigger-id", str(trigger.id)])
    )
    assert finished_increment["status"] == "completed", finished_increment
    replacement = await run(settings, parser().parse_args(["reconcile"]))
    assert replacement["recorded"] == 1, replacement
    assert (await run(settings, parser().parse_args(["reconcile"])))["recorded"] == 0
    async with factory() as session:
        latest = await session.scalar(
            select(MonitorPlan)
            .where(
                MonitorPlan.instrument_id == held_run.instrument_id,
            )
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        assert latest.generation == 2 and latest.predecessor_id == original_plan.id
        assert (
            latest.source_run_id == child_id
            and latest.condition_hash == original_plan.condition_hash
        )
        child = await session.get(AgentResearchRunRecord, child_id)
        assert child.status == "completed" and child.settled_cost_usd is not None
        child_output = await session.get(AgentResearchOutputRecord, latest.source_output_id)
        assert (
            child_output.intent["stance"] == "hold"
            and child_output.intent["quality_status"] == "complete"
        )
        budgets = tuple(await session.scalars(select(AgentDailyBudgetReservationRecord)))
        assert len(budgets) == 7 and all(
            row.state == "settled" and row.settled_usd is not None for row in budgets
        )
        assert {
            row.id: row.content_hash
            for row in await session.scalars(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.id.in_(old_outputs)
                )
            )
        } == old_outputs
    assert await ledger_facts(factory) == protected_ledger
    await run(settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)]))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 1
    assert len(calls) == 12 and deliveries == [event.id]

    from nexus.monitoring.admission import require_monitor_account_available
    from nexus.paper.manual_request import require_settled_account_cycle_history

    async with factory() as session:
        clock[0] = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(
            seconds=1
        )
    await _set_test_insert_clock(factory, clock[0])
    for instrument_id, close, *_ in INSTRUMENTS:
        await append_fresh_inputs(factory, instrument_id, clock[0], midpoint=str(close))
    async with factory() as session:
        async with session.begin():
            await require_settled_account_cycle_history(
                session, account_id=prior.account_id, now=clock[0], allow_scheduled_completion=True
            )
            await require_monitor_account_available(
                session, account_id=prior.account_id, now=clock[0]
            )
        transaction = await session.begin()
        try:
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=prior.account_id,
                now=clock[0],
                expected_calendar_authority_id=CALENDAR_ID,
            )
            next_cycle = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=clock[0].replace(second=0, microsecond=0),
                idempotency_key="held-next-admission-rollback-probe",
                requested_instrument_ids=tuple(row[0] for row in INSTRUMENTS),
                candidate_batch_id=prior.candidate_batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=settings.agent_paper_formal_model_config_id,
            )
            assert (
                next_cycle.result_code == "RESEARCH_JOBS_PREPARED"
                and len(next_cycle.research_run_ids) == 3
            ), next_cycle
        finally:
            await transaction.rollback()
    assert len(calls) == 12
    assert await ledger_facts(factory) == protected_ledger

    from integration.test_monitor_view_pg import business_facts

    before_reads = await business_facts(factory)
    application = app(migrated_database_url, now=clock[0])
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: clock[0])
    path = f"/internal/v1/research-runs/{held_run.id}"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test"
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["intent"]["stance"] == "hold" and data["intent"]["quality_status"] == "complete"
        assert data["monitoring"]["state"] == "superseded" and data["monitoring"][
            "replacement_run_id"
        ] == str(child_id)
        item = data["monitoring"]["triggers"][0]
        assert (item["run_id"], item["usage_status"], item["notification_status"]) == (
            str(child_id),
            "known",
            "published",
        )
        cli = await read_registered_cli(settings, latest.id, tmp_path)
        assert cli["plan_id"] == str(latest.id) and cli["source_output_id"] == str(child_output.id)
        assert cli["research_quality"] == "complete" and cli["coverage"] == "expired"
        assert cli["execution_locked"] and cli["model_requests"] == cli["provider_requests"] == 0
        browser_root = os.environ.get("NEXUS_TEST_MONITOR_BROWSER_DIR")
        if browser_root:
            from helpers.monitoring_readview_browser import verify_monitoring_browser

            destination = Path(browser_root).resolve() / "hold"
            destination.mkdir(parents=True, exist_ok=False)
            (destination / "ready.json").write_text(
                json.dumps(
                    {
                        "run_id": str(held_run.id),
                        "instrument_id": str(held_run.instrument_id),
                        "child_id": str(child_id),
                        "response": response.json(),
                        "daily_stages": len(calls[:-3]),
                        "increment_stages": len(calls[-3:]),
                        "held_count": len(held),
                        "cli": cli,
                    }
                )
            )
            await verify_monitoring_browser(
                application,
                directory=destination,
                spec_path="tests/e2e/monitoring-hold-live.spec.ts",
            )
        assert await business_facts(factory) == before_reads
        assert await ledger_facts(factory) == protected_ledger
    finally:
        await application.state.session_factory.kw["bind"].dispose()
    await factory.kw["bind"].dispose()


async def read_registered_cli(settings, plan_id, root):
    """Real installed CLI and current DB time; a dated plan must remain expired."""
    repo = root / "cli-project"
    repo.mkdir()
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    (repo / ".gitignore").write_text(".private/\n")
    private = repo / ".private"
    private.mkdir(mode=0o700)
    env_file = private / "test.env"
    env_file.write_text(
        f"NEXUS_ENV=test\nDATABASE_URL={settings.database_url}\n"
        f"AGENT_PAPER_ACCOUNT_ID={settings.agent_paper_account_id}\n"
        f"AGENT_PAPER_CANDIDATE_BATCH_ID={settings.agent_paper_candidate_batch_id}\n"
    )
    env_file.chmod(0o600)
    engine_root = Path(__file__).resolve().parents[2]
    cli_env = root / "cli-venv"
    # Install this candidate's registered commands without changing the shared
    # workstation environment. Runtime dependencies are the already-used ones.
    subprocess.run(["uv", "venv", "--quiet", "--python", sys.executable, str(cli_env)], check=True)
    installed = await asyncio.create_subprocess_exec(
        "uv",
        "pip",
        "install",
        "--quiet",
        "--offline",
        "--no-deps",
        "--python",
        str(cli_env / "bin/python"),
        "--editable",
        str(engine_root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    install_stdout, install_stderr = await asyncio.wait_for(installed.communicate(), timeout=120)
    assert installed.returncode == 0, (install_stdout.decode(), install_stderr.decode())
    entry = cli_env / "bin/nexus-engine-research-monitor"
    assert entry.is_file()
    environment = {
        "PATH": os.environ["PATH"],
        "NEXUS_ENV": "test",
        "PYTHONPATH": os.pathsep.join((str(engine_root / "src"), sysconfig.get_path("purelib"))),
    }
    process = await asyncio.create_subprocess_exec(
        str(entry),
        "--repo-root",
        str(repo),
        "--env-file",
        str(env_file),
        "inspect",
        "--plan-id",
        str(plan_id),
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
    assert process.returncode == 0, (stdout.decode(), stderr.decode())
    return json.loads(stdout)

"""Normal completion to automatic coverage, using original frozen Worker outputs."""

import asyncio
from datetime import timedelta

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import AT
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as normal_cycle,
)
from nexus.ops.research_monitor import parser, run
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["approved", "audit-veto", "invalid-one", "current-scope"])
async def test_normal_scan_reconciles_daily_outputs_once_without_model_calls(
    migrated_database_url, monkeypatch, case
):
    rejected = case == "audit-veto"
    expected = 2 if case in {"invalid-one", "current-scope"} else 3
    await normal_cycle(
        migrated_database_url,
        monkeypatch,
        3,
        False,
        "formal_v11",
        monitoring=True,
        monitoring_rejected=rejected,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        cycle = await session.scalar(select(AgentPaperCycleRecord))
        original_runs = tuple(await session.scalars(select(AgentResearchRunRecord.id)))
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord

    async with factory() as session, session.begin():
        monday = AT + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
                created_at=AT - timedelta(days=1),
            )
        )
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_paper_account_id=cycle.account_id,
        agent_paper_candidate_batch_id=cycle.candidate_batch_id,
    )
    from nexus.monitoring import reconciliation
    from nexus.paper.candidate_batch import create_agent_paper_candidate_batch

    async with factory() as session:
        securities = tuple(
            await session.scalars(
                select(AgentResearchRunRecord.instrument_id).order_by(
                    AgentResearchRunRecord.instrument_id
                )
            )
        )
    if case == "invalid-one":
        native_scope = reconciliation._scope_evidence

        async def one_invalid(session, *, run, account_id):
            if run.instrument_id == securities[0]:
                raise ValueError("MONITOR_SOURCE_INVALID")
            return await native_scope(session, run=run, account_id=account_id)

        monkeypatch.setattr(reconciliation, "_scope_evidence", one_invalid)
    if case == "current-scope":
        async with factory() as session, session.begin():
            current = await create_agent_paper_candidate_batch(
                session,
                account_id=cycle.account_id,
                universe_snapshot_id=cycle.universe_snapshot_id,
                instrument_ids=securities[:2],
                idempotency_key="new-current-monitor-scope",
                created_at=AT + timedelta(seconds=10),
            )
        settings = settings.model_copy(update={"agent_paper_candidate_batch_id": current.batch_id})
    old_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return AT + timedelta(seconds=10)
        return await old_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    args = parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    first = await run(settings, args)
    assert first["reconciled_results"] == (3 if case == "invalid-one" else expected)
    assert first["model_requests"] == first["provider_requests"] == 0
    assert first["checks"] == (0 if rejected else expected)
    assert first["coverage_gap"] == (3 if rejected else 1 if case == "invalid-one" else 0)
    async with factory() as session:
        plans = tuple(await session.scalars(select(MonitorPlan)))
        receipts = tuple(await session.scalars(select(MonitorCoverageReceipt)))
        assert len(plans) == (0 if rejected else expected)
        if case == "invalid-one":
            unavailable = [r for r in receipts if r.status == "unverified"]
            assert len(unavailable) == 1 and unavailable[0].active_plan_id is None
            assert unavailable[0].quality_status is None and unavailable[0].output_id is None
        assert len(receipts) == (3 if case == "invalid-one" else expected)
        verified = [r for r in receipts if r.status != "unverified"]
        assert {r.status for r in verified} == ({"gap"} if rejected else {"installed"})
        assert {r.reason for r in verified} == (
            {"MONITOR_AUDIT_REJECTED"} if rejected else {"MONITOR_INSTALLED"}
        )
        assert {r.quality_status for r in verified} == {"complete"}
        ids = {r.id for r in receipts}
        assert set(await session.scalars(select(AgentResearchRunRecord.id))) == set(original_runs)
    repeated = await asyncio.gather(
        *(run(settings, parser().parse_args(["reconcile"])) for _ in range(2))
    )
    assert all(r["recorded"] == 0 for r in repeated)
    async with factory() as session:
        assert set(await session.scalars(select(MonitorCoverageReceipt.id))) == ids
        assert await session.scalar(
            select(func.count())
            .select_from(AuditEvent)
            .where(AuditEvent.action == "research.monitor.coverage_recorded")
        ) == (3 if case == "invalid-one" else expected)

    if case == "approved":
        from pathlib import Path

        from alembic import command
        from alembic.config import Config
        from sqlalchemy import update
        from sqlalchemy.exc import DBAPIError

        async with factory() as session, session.begin():
            with pytest.raises(DBAPIError, match="MONITOR_FACT_IMMUTABLE"):
                async with session.begin_nested():
                    await session.execute(update(MonitorCoverageReceipt).values(reason="rewritten"))
        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
        with pytest.raises(Exception, match="MONITOR_COVERAGE_HISTORY_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0089_monitor_evidence")
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["complete", "partial", "veto", "no-adoption", "unknown", "withdrawn-unknown"]
)
async def test_increment_completion_replaces_or_preserves_explicit_coverage(
    migrated_database_url, monkeypatch, mode
):
    import json
    from uuid import UUID, uuid4

    from integration.test_monitor_increment_pg import ready_trigger
    from nexus.agents import codex_staged
    from nexus.monitoring.reconciliation import plan_covered
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.monitoring import MonitorTrigger
    from nexus.worker import run_agent_worker_once

    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    first = await run(settings, parser().parse_args(["reconcile"]))
    assert first["installed"] == 3
    async with factory() as session:
        old = await session.scalar(
            select(MonitorPlan).where(MonitorPlan.source_run_id == parent.id)
        )
    admitted = await run(
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
    unknown = mode in {"unknown", "withdrawn-unknown"}
    if mode == "withdrawn-unknown":
        # Controlled earlier withdrawal boundary: native current work is already
        # in flight when the prior refusal receipt becomes visible. Its source
        # validation is covered by the normal veto case below, not this seed.
        async with factory() as session, session.begin():
            values = {
                c.name: getattr(parent, c.name)
                for c in parent.__table__.columns
                if c.name not in {"id", "created_at", "decision_time", "idempotency_key"}
            }
            earlier = AgentResearchRunRecord(
                **values,
                id=uuid4(),
                decision_time=parent.decision_time + timedelta(minutes=1),
                idempotency_key="controlled-prior-withdrawal",
            )
            session.add(earlier)
            await session.flush()
            session.add(
                MonitorCoverageReceipt(
                    account_id=settings.agent_paper_account_id,
                    instrument_id=parent.instrument_id,
                    run_id=earlier.id,
                    output_id=None,
                    prior_plan_id=old.id,
                    active_plan_id=None,
                    research_as_of=earlier.decision_time,
                    observed_at=now,
                    status="gap",
                    reason="MONITOR_AUDIT_REJECTED",
                    quality_status="partial",
                )
            )
    native = codex_staged.invoke_codex
    calls = []

    async def model(payload, **kwargs):
        calls.append(payload["stage"])
        if unknown and payload["stage"] == "critique":
            raise TimeoutError("controlled unknown stage")
        raw, usage = await native(payload, **kwargs)
        value = json.loads(raw)
        if mode == "partial" and payload["stage"] == "synthesis":
            value["synthesis_assessment"]["review_conclusion"] = "limited"
            value["synthesis_assessment"]["uncertainties"] = [
                "Independent evidence remains incomplete."
            ]
        if mode == "veto" and payload["stage"] == "critique":
            for role in value["roles"]:
                if role["role"] == "bear":
                    role["monitoring_review"]["decision"] = "rejected"
        if mode == "no-adoption" and payload["stage"] == "synthesis":
            value["monitoring_adoption_hash"] = None
        return json.dumps(value), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", model)
    assert await run_agent_worker_once(
        settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
    )
    if mode == "veto":
        async with factory() as session:
            # A newly finished rejection must block old-parent admission even
            # before the next automatic reconciliation has written its receipt.
            assert not await plan_covered(session, plan=old, now=now)
    final = await run(settings, parser().parse_args(["reconcile"]))
    assert final["recorded"] == 1
    expected = (
        "unknown"
        if unknown
        else "gap"
        if mode == "veto"
        else "retained"
        if mode == "no-adoption"
        else "installed"
    )
    async with factory() as session:
        receipt = await session.scalar(
            select(MonitorCoverageReceipt).where(
                MonitorCoverageReceipt.run_id == UUID(admitted["run_id"])
            )
        )
        assert receipt.status == expected, (receipt.status, receipt.reason)
        assert receipt.quality_status == (
            None if unknown else "partial" if mode == "partial" else "complete"
        )
        current = await session.scalar(
            select(MonitorPlan)
            .where(MonitorPlan.instrument_id == parent.instrument_id)
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        assert current.generation == (2 if mode in {"complete", "partial"} else 1)
        if mode in {"veto", "withdrawn-unknown"}:
            assert receipt.active_plan_id is None
            assert not await plan_covered(session, plan=old, now=now)
        else:
            assert receipt.active_plan_id == current.id
            assert await plan_covered(session, plan=current, now=now)
        original_trigger = await session.get(MonitorTrigger, trigger.id)
        assert original_trigger.status == ("unknown" if unknown else "completed")
        budget = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == UUID(admitted["run_id"])
            )
        )
        assert budget.state == ("reserved" if unknown else "settled")
        assert (budget.settled_usd is None) is unknown
        trigger_count = await session.scalar(select(func.count()).select_from(MonitorTrigger))
    inspected = await run(settings, parser().parse_args(["inspect", "--plan-id", str(current.id)]))
    assert inspected["coverage"] == (
        "unavailable" if mode in {"veto", "withdrawn-unknown"} else "active"
    )
    assert inspected["research_quality"] == receipt.quality_status
    assert inspected["latest_result_state"] == receipt.status
    scanned = await run(
        settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    )
    assert scanned["reconciled_results"] == 0
    assert scanned["coverage_gap"] == (1 if mode in {"veto", "withdrawn-unknown"} else 0)
    async with factory() as session:
        # Same matched condition is not a new episode merely because a new result installed.
        assert (
            await session.scalar(select(func.count()).select_from(MonitorTrigger)) == trigger_count
        )
    assert calls == (["research", "critique"] if unknown else ["research", "critique", "synthesis"])

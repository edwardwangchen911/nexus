"""Native scheduled resume must close only unused research without waiting for Workers."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from integration.test_terminal_cycle_cancel_pg import AT
from nexus.agents.execution import _terminal_without_usage
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job, fail_job
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, PaperOrderRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition", ["plain", "budget_busy", "job_busy", "guard_fault", "claimed", "legacy"]
)
async def test_native_scheduled_failure_cleanup(migrated_database_url, monkeypatch, condition):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS, decision_time=AT
    )
    locked = None
    try:
        await _set_test_insert_clock(sessions, AT)
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(x.instrument_id for x in manifest),
                idempotency_key="scheduled-cleanup",
                created_at=AT,
            )
            result = await agent_cycle.start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key="scheduled-cleanup",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=condition != "legacy",
                formal_model_config_id=model.id if condition != "legacy" else None,
            )
            jobs = list(await session.scalars(select(Job).order_by(Job.id)))

            async def guard(_session, selected):
                return selected.id in {job.id for job in jobs} and selected.attempt_count == 0

            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="controlled-failed-worker",
                now=AT,
                job_ids=(jobs[0].id,),
                job_types=("agent.paper.research",),
                lease_seconds=210 if condition != "legacy" else 30,
                research_budget_profile="codex-sdk-chatgpt-v8-stage-capacity"
                if condition != "legacy"
                else None,
                locked_job_guard=guard,
            )
            assert claim is not None
            run = await session.get(AgentResearchRunRecord, UUID(jobs[0].payload["run_id"]))
            await _terminal_without_usage(
                session,
                run,
                output_sequence=1,
                event_sequence=2,
                status="failed",
                reason="controlled_pre_call_failure",
            )
            await fail_job(session, claim=claim, now=AT, error_code="AGENT_RESEARCH_RESULT_UNKNOWN")
            if condition == "claimed":
                other = await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id="controlled-live-worker",
                    now=AT,
                    job_ids=(jobs[1].id,),
                    job_types=("agent.paper.research",),
                    lease_seconds=210 if condition != "legacy" else 30,
                    research_budget_profile="codex-sdk-chatgpt-v8-stage-capacity"
                    if condition != "legacy"
                    else None,
                    locked_job_guard=guard,
                )
                assert other is not None

        async def research_facts():
            from nexus.paper.history_seal import _row_hash

            async with sessions() as session:
                return {
                    table.__tablename__: sorted(
                        _row_hash(row) for row in await session.scalars(select(table))
                    )
                    for table in (
                        Job,
                        JobAttempt,
                        JobLease,
                        AgentResearchRunRecord,
                        AgentResearchEventRecord,
                        AgentResearchOutputRecord,
                        AgentDailyBudgetReservationRecord,
                    )
                }

        before_research = await research_facts()
        if condition in {"budget_busy", "job_busy"}:
            locked = sessions()
            await locked.begin()
            if condition == "budget_busy":
                await locked.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                    {"key": "agent-budget:2026-10-09"},
                )
            else:
                await locked.scalar(select(Job).where(Job.id == jobs[1].id).with_for_update())
        if condition == "guard_fault":
            from nexus.paper import manual_cancel

            original = manual_cancel.require_reserved_daily_budget
            calls = []

            async def fault(session, *, run_id):
                calls.append(run_id)
                if len(calls) == 2:
                    raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
                return await original(session, run_id=run_id)

            monkeypatch.setattr(manual_cancel, "require_reserved_daily_budget", fault)

        async def resume():
            async with sessions.begin() as session:
                return await agent_cycle.resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=result.cycle_id,
                    idempotency_key="scheduled-cleanup-resume",
                    now=AT + timedelta(seconds=2),
                    daily_limit_usd=Decimal("3"),
                    formal_stage_capacity=condition != "legacy",
                )

        stopped = await asyncio.wait_for(resume(), timeout=30)
        assert stopped.status == "unavailable"
        assert stopped.result_code == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
        expected = {
            "plain": 2,
            "budget_busy": 0,
            "job_busy": 1,
            "guard_fault": 0,
            "claimed": 1,
            "legacy": 0,
        }[condition]
        async with sessions() as session:
            counts = list(await session.scalars(select(Job.status).order_by(Job.id)))
            assert counts.count("skipped") == expected
            last = await session.scalar(
                select(AgentPaperCycleEventRecord)
                .where(AgentPaperCycleEventRecord.cycle_id == result.cycle_id)
                .order_by(AgentPaperCycleEventRecord.sequence.desc())
                .limit(1)
            )
            if condition == "legacy":
                assert "unused_research_cancelled" not in last.fact_ids
            else:
                assert last.fact_ids["unused_research_cancelled"] == expected
            if condition == "guard_fault":
                assert (
                    last.fact_ids["unused_research_cleanup_code"]
                    == "UNUSED_RESEARCH_CLEANUP_REJECTED"
                )
            if condition == "claimed":
                remaining = await session.get(
                    AgentResearchRunRecord, UUID(jobs[1].payload["run_id"])
                )
                assert remaining.settled_cost_usd is None and remaining.input_tokens is None
                assert counts.count("running") == 1
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            attempts = await session.scalar(select(func.count()).select_from(JobAttempt))
        if condition in {"guard_fault", "budget_busy", "legacy"}:
            assert await research_facts() == before_research
        if locked is not None:
            await locked.rollback()
            await locked.close()
            locked = None
        replay = await resume()
        assert replay.replayed
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(JobAttempt)) == attempts
    finally:
        if locked is not None:
            await locked.rollback()
            await locked.close()
        await engine.dispose()

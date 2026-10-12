import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import func, select

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)


def event_values(events):
    return tuple(
        (
            e.cycle_id,
            e.sequence,
            e.from_status,
            e.to_status,
            e.result_code,
            json.dumps(e.fact_ids, sort_keys=True),
            e.occurred_at,
            e.event_hash,
        )
        for e in events
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_version", ["v4", "v5", "v6"])
@pytest.mark.parametrize("prior_claimed", [False, True])
@pytest.mark.parametrize("guard_fault", [False, True])
async def test_legacy_terminal_recovery_skips_only_unused_jobs_and_is_idempotent(
    migrated_database_url, monkeypatch, legacy_version, prior_claimed, guard_fault
):
    from nexus.agents.codex_versions import (
        ACTIONABLE_CONFIG_VERSION,
        DEADLINE_BUDGET_CONFIG_VERSION,
        LEGACY_ACTIONABLE_CONFIG_VERSION,
    )
    from nexus.paper.manual_cancel import close_terminal_cycle_unused_research

    stage_budget = legacy_version == "v6"
    expected_version = {
        "v4": LEGACY_ACTIONABLE_CONFIG_VERSION,
        "v5": ACTIONABLE_CONFIG_VERSION,
        "v6": DEADLINE_BUDGET_CONFIG_VERSION,
    }[legacy_version]
    if legacy_version == "v4":
        # Seed the old immutable registry identity; cancellation never runs its model.
        monkeypatch.setattr("nexus.ops.openai_local.ACTIONABLE_CONFIG_VERSION", expected_version)

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS, decision_time=AT
    )
    try:
        from integration.test_candidate_multistock_path import _set_test_insert_clock

        await _set_test_insert_clock(sessions, AT - timedelta(minutes=1))
        await prepare_database(
            settings,
            structured_synthesis=True,
            aggregate_input_tokens=96000,
            max_wire_bytes=64000,
            inline_citations=True,
            actionable_paper=True,
            stage_time_budget=stage_budget,
            allow_stage_time_budget_registration=stage_budget,
        )
        async with sessions.begin() as session:
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="legacy-cancel",
                created_at=AT,
            )
            result = await agent_cycle.start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key="legacy-cancel",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                daily_limit_usd=Decimal("3"),
            )
            assert result.status == "researching"
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            assert {run.pricing_version for run in runs} == {expected_version}
            with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
                await close_terminal_cycle_unused_research(
                    session, account_id=authority.account_id, cycle_id=result.cycle_id, now=AT
                )
            if prior_claimed:
                from nexus.jobs.models import QueueName
                from nexus.jobs.repository import claim_next_job, fail_job

                job = await session.scalar(select(Job).order_by(Job.id).limit(1))
                claim = await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id="controlled-old-attempt",
                    now=AT,
                    job_ids=(job.id,),
                    job_types=("agent.paper.research",),
                )
                assert claim is not None
                await fail_job(
                    session, claim=claim, now=AT, error_code="AGENT_RESEARCH_RESULT_UNKNOWN"
                )
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            await agent_cycle._stop_failed(
                session,
                cycle,
                await agent_cycle._events(session, cycle.id),
                "RESEARCH_OUTPUT_NOT_ACTIONABLE",
            )
            original_events = event_values(await agent_cycle._events(session, cycle.id))
        with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
            async with sessions.begin() as session:
                await close_terminal_cycle_unused_research(
                    session,
                    account_id=UUID(int=999),
                    cycle_id=result.cycle_id,
                    now=AT + timedelta(minutes=1),
                )
        if guard_fault:
            async with sessions.begin() as session:
                jobs = tuple(
                    await session.scalars(
                        select(Job).where(Job.status == "queued").order_by(Job.id)
                    )
                )
                damaged = await session.get(
                    AgentResearchRunRecord, UUID(jobs[-1].payload["run_id"])
                )
                damaged.input_tokens = 1
                before_counts = [
                    await session.scalar(select(func.count()).select_from(t))
                    for t in (
                        JobAttempt,
                        JobLease,
                        AgentResearchOutputRecord,
                        AgentResearchEventRecord,
                    )
                ]
                before_budget = [
                    (row.id, row.state, row.settled_usd)
                    for row in await session.scalars(
                        select(AgentDailyBudgetReservationRecord).order_by(
                            AgentDailyBudgetReservationRecord.id
                        )
                    )
                ]
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                async with sessions.begin() as session:
                    await close_terminal_cycle_unused_research(
                        session,
                        account_id=authority.account_id,
                        cycle_id=result.cycle_id,
                        now=AT + timedelta(minutes=1),
                    )
            async with sessions() as session:
                jobs = tuple(await session.scalars(select(Job).where(Job.status == "queued")))
                assert len(jobs) == (2 if prior_claimed else 3)
                assert all(job.attempt_count == 0 for job in jobs)
                runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
                assert all(
                    run.status == "prepared" and run.settled_cost_usd is None for run in runs
                )
                assert [
                    await session.scalar(select(func.count()).select_from(t))
                    for t in (
                        JobAttempt,
                        JobLease,
                        AgentResearchOutputRecord,
                        AgentResearchEventRecord,
                    )
                ] == before_counts
                assert [
                    (row.id, row.state, row.settled_usd)
                    for row in await session.scalars(
                        select(AgentDailyBudgetReservationRecord).order_by(
                            AgentDailyBudgetReservationRecord.id
                        )
                    )
                ] == before_budget
                assert all(
                    state == "reserved" and value is None for _, state, value in before_budget
                )
                assert (
                    event_values(await agent_cycle._events(session, result.cycle_id))
                    == original_events
                )
            return
        # Exercise the normal CLI transaction and host gate against this isolated database.
        from argparse import Namespace

        from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
        from nexus.ops import manual_paper
        from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL, manual_host_scope
        from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
        from nexus.persistence.models.agents import AgentModelConfigRecord
        from sqlalchemy.ext.asyncio import AsyncSession
        from unit.ops.test_formal_stage_registration import OPTIONS

        if legacy_version == "v4":
            monkeypatch.setattr(
                "nexus.ops.openai_local.ACTIONABLE_CONFIG_VERSION", ACTIONABLE_CONFIG_VERSION
            )
        await prepare_database(settings, **OPTIONS)
        async with sessions.begin() as session:
            formal = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
                )
            )
            runtime = settings.model_copy(
                update={
                    "agent_paper_account_id": authority.account_id,
                    "agent_paper_candidate_batch_id": batch.batch_id,
                    "agent_paper_formal_model_config_id": formal.id,
                    "agent_paper_manual_enabled": True,
                    "agent_paper_cycle_enabled": True,
                    "release_revision": "a" * 40,
                }
            )
            session.add(
                WorkerHeartbeat(
                    worker_id=f"agent-paper-loop-{UUID(int=555)}:manual:{manual_host_scope(runtime)}",
                    queue="agent_paper",
                    seen_at=AT + timedelta(minutes=1),
                    started_at=AT,
                    process_version=f"{MANUAL_HOST_PROTOCOL}+{'a' * 40}",
                    migration_head=CURRENT_MIGRATION_HEAD,
                )
            )
        original_scalar = AsyncSession.scalar

        async def controlled_clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return AT + timedelta(minutes=1)
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", controlled_clock)
        for expected in (2 if prior_claimed else 3, 0):
            closed = await manual_paper.run(
                runtime,
                Namespace(
                    action="cancel-unused-research",
                    cycle_id=result.cycle_id,
                ),
            )
            assert closed.cancelled_research == expected
        async with sessions() as session:
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            assert sum(run.status == "abstained" for run in runs) == (2 if prior_claimed else 3)
            assert all(
                run.settled_cost_usd == 0 and run.input_tokens == run.output_tokens == 0
                for run in runs
                if run.status == "abstained"
            )
            assert all(
                run.settled_cost_usd is None
                and run.input_tokens is None
                and run.output_tokens is None
                for run in runs
                if run.status == "prepared"
            )
            jobs = tuple(await session.scalars(select(Job)))
            assert sum(job.status == "skipped" for job in jobs) == (2 if prior_claimed else 3)
            assert all(job.attempt_count == 1 for job in jobs)
            assert (
                event_values(await agent_cycle._events(session, result.cycle_id)) == original_events
            )
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
    finally:
        await engine.dispose()

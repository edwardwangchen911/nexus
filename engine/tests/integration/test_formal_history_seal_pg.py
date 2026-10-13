"""Formal failed research can be explicitly disposed without settling unknown usage."""

from argparse import Namespace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from integration.test_terminal_cycle_cancel_pg import AT, event_values
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.codex_versions import (
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
)
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job, fail_job
from nexus.ops import manual_paper
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL, manual_host_scope
from nexus.paper.manual_request import require_manual_account_available
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_formal_stage_registration import OPTIONS
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v8", "v9"])
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "failed-run",
        "live-lease",
        "missing-confirmation",
        "cycle-binding-hash",
        "cycle-formal-flag",
    ],
)
async def test_formal_failure_seal_keeps_unknown_usage_and_unlocks_only_settled_account(
    migrated_database_url, monkeypatch, version, fault
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT
    )
    try:
        await _set_test_insert_clock(sessions, AT - timedelta(minutes=1))
        await prepare_database(settings, **(OPTIONS if version == "v8" else V9_OPTIONS))
        expected = (
            FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            if version == "v8"
            else FULL_INPUT_CAPACITY_CONFIG_VERSION
        )
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(AgentModelConfigRecord.version == expected)
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="formal-history-seal",
                created_at=AT,
            )
            result = await agent_cycle.start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key="formal-history-seal",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                daily_limit_usd=Decimal("3"),
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            job = await session.scalar(select(Job).order_by(Job.id).limit(1))
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="controlled-formal-failure",
                now=AT,
                job_ids=(job.id,),
                job_types=("agent.paper.research",),
                research_budget_profile=expected,
                locked_job_guard=formal_stage_job_qualified,
                lease_seconds=210,
            )
            assert claim is not None
            await fail_job(session, claim=claim, now=AT, error_code="AGENT_RESEARCH_RESULT_UNKNOWN")
            run_id = UUID(job.payload["run_id"])
            if fault == "failed-run":
                from nexus.agents.execution import _persist_terminal

                run = await session.get(AgentResearchRunRecord, run_id)
                await _persist_terminal(
                    session,
                    run,
                    output_sequence=1,
                    event_sequence=1,
                    status="failed",
                    reason="model_usage_uncertain",
                )
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            await agent_cycle._stop_failed(
                session,
                cycle,
                await agent_cycle._events(session, cycle.id),
                "RESEARCH_OUTPUT_NOT_ACTIONABLE",
            )
            original_events = event_values(await agent_cycle._events(session, cycle.id))
            runtime = settings.model_copy(
                update={
                    "agent_paper_account_id": authority.account_id,
                    "agent_paper_candidate_batch_id": batch.batch_id,
                    "agent_paper_formal_model_config_id": model.id,
                    "agent_paper_manual_enabled": True,
                    "agent_paper_cycle_enabled": True,
                    "release_revision": "a" * 40,
                }
            )
            seal_now = AT + timedelta(days=3)
            session.add(
                WorkerHeartbeat(
                    worker_id=f"agent-paper-loop-{UUID(int=555)}:manual:{manual_host_scope(runtime)}",
                    queue="agent_paper",
                    seen_at=seal_now,
                    started_at=AT,
                    process_version=f"{MANUAL_HOST_PROTOCOL}+{'a' * 40}",
                    migration_head=CURRENT_MIGRATION_HEAD,
                )
            )
            if fault == "live-lease":
                lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
                lease.expires_at = seal_now + timedelta(minutes=1)
        original_scalar = AsyncSession.scalar

        async def clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return seal_now
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", clock)
        monkeypatch.setattr(
            "nexus.paper.history_seal.require_research_processes_quiescent", lambda: None
        )
        args = Namespace(
            action="seal-failed-research",
            cycle_id=result.cycle_id,
            run_ids=(run_id,),
            accept_unknown_usage=fault != "missing-confirmation",
        )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=authority.account_id, now=seal_now
                )
        rejected = fault is not None and fault != "failed-run"
        if rejected:
            with monkeypatch.context() as binding_read:
                if fault in {"cycle-binding-hash", "cycle-formal-flag"}:
                    original_reader = agent_cycle._events

                    async def incompatible_binding(session, cycle_id):
                        events = await original_reader(session, cycle_id)
                        facts = dict(events[0].fact_ids)
                        if fault == "cycle-binding-hash":
                            facts["formal_model_hash"] = "0" * 64
                        else:
                            facts.pop("formal_stage_capacity")
                        # Fault the binding read, never rewrite append-only database facts.
                        return [
                            SimpleNamespace(fact_ids=facts, result_code=events[0].result_code),
                            *events[1:],
                        ]

                    binding_read.setattr(agent_cycle, "_events", incompatible_binding)
                with pytest.raises(
                    ValueError, match="MANUAL_(ACCOUNT_BUSY_OR_UNKNOWN|REQUEST_INVALID)"
                ):
                    await manual_paper.run(runtime, args)
        else:
            receipt = await manual_paper.run(runtime, args)
            assert receipt.sealed_research == 1 and not receipt.replayed
            assert (await manual_paper.run(runtime, args)).replayed
            closed = await manual_paper.run(
                runtime,
                Namespace(
                    action="cancel-unused-research",
                    cycle_id=result.cycle_id,
                ),
            )
            assert closed.cancelled_research == len(manifest) - 1
            async with sessions.begin() as session:
                await require_manual_account_available(
                    session, account_id=authority.account_id, now=seal_now
                )
        async with sessions() as session:
            run = await session.get(AgentResearchRunRecord, run_id)
            assert run.status == ("failed" if fault == "failed-run" else "prepared")
            assert run.settled_cost_usd is run.input_tokens is run.output_tokens is None
            assert (
                event_values(await agent_cycle._events(session, result.cycle_id)) == original_events
            )
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            seals = await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(
                    AuditEvent.action == "paper.legacy_research.sealed",
                )
            )
            assert seals == (0 if rejected else 1)
    finally:
        await engine.dispose()

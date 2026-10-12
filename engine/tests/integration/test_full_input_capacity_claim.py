"""Native first claim of isolated v9 Jobs; generic consumers remain closed."""

import pytest
from integration.test_candidate_research_authority import NOW
from integration.test_full_input_capacity_admission import admit, prepare
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.repository import claim_next_job
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_generic_worker_cannot_claim_v9_even_with_explicit_job_id(migrated_database_url):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        await admit(session, model_id, full_input_capacity=True)
        job = await session.scalar(select(Job))
        result = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="generic",
            now=NOW,
            lease_seconds=150,
            job_ids=(job.id,),
        )
        assert result is None
        await session.refresh(job)
        assert job.status == "queued" and job.attempt_count == 0
        for record in (JobAttempt, JobLease):
            assert await session.scalar(select(func.count()).select_from(record)) == 0


@pytest.mark.asyncio
async def test_isolated_v9_qualified_first_claim_keeps_existing_lease_and_one_attempt(
    migrated_database_url,
):
    from nexus.agents.codex_capacity import capacity_job_qualified

    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        admission = await admit(session, model_id, full_input_capacity=True)
        job = await session.scalar(select(Job))
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="v9-isolated",
            now=NOW,
            lease_seconds=210,
            job_ids=(job.id,),
            job_types=(JobType.AGENT_PAPER_RESEARCH.value,),
            locked_job_guard=capacity_job_qualified,
            research_budget_profile="codex-sdk-chatgpt-v9-full-input-capacity",
        )
        assert claim is not None and claim.payload == {"run_id": str(admission.run_id)}
        assert await capacity_job_qualified(session, job, claimed=True)
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="v9-isolated",
                now=NOW,
                lease_seconds=210,
                job_ids=(job.id,),
                locked_job_guard=capacity_job_qualified,
                research_budget_profile="codex-sdk-chatgpt-v9-full-input-capacity",
            )
            is None
        )
        for record in (JobAttempt, JobLease):
            assert await session.scalar(select(func.count()).select_from(record)) == 1


@pytest.mark.asyncio
async def test_native_isolated_job_persists_three_stages_and_full_assessment(
    migrated_database_url, monkeypatch
):
    import json

    from nexus import worker
    from nexus.agents.codex_capacity import (
        CapacityPoolMember,
        capacity_job_qualified,
        preflight_capacity_pool,
    )
    from nexus.agents.execution import execute_research_run
    from nexus.agents.execution_authority import load_frozen_evidence
    from nexus.agents.models import ResearchRequest
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.agents.synthesis import domain_evidence
    from nexus.api.research_stages import read_stage_trace
    from nexus.jobs.models import RetryPolicy
    from nexus.jobs.registry import JobHandlerResult, JobHandlerSpec, JobRegistry
    from nexus.jobs.worker import run_job_worker_once
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchOutputRecord,
        AgentResearchRunRecord,
    )
    from nexus.settings import EngineSettings
    from unit.agents.test_codex_sdk import _assessment_payload, _staged_batch

    factory, model_id = await prepare(migrated_database_url, real_sources=True)
    async with factory.begin() as session:
        admission = await admit(session, model_id, full_input_capacity=True)
        job_id = await session.scalar(select(Job.id))
    calls = []

    async def invoke(payload, **options):
        calls.append(payload["stage"])
        assert options["max_wire_bytes"] == 100479
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")

    async def handle(context):
        run = await context.session.get(AgentResearchRunRecord, admission.run_id)
        assert await capacity_job_qualified(
            context.session, await context.session.get(Job, job_id), claimed=True
        )
        committee = await worker._committee(
            context.session,
            settings,
            run,
            responses_client=None,
            tools=None,
            deadline=context.monotonic_deadline(),
            full_input_capacity=True,
        )
        evidence = await load_frozen_evidence(context.session, run, allow_fixture_evidence=False)
        request = ResearchRequest(
            instrument_id=run.instrument_id,
            data_snapshot_id=run.data_snapshot_id,
            decision_time=run.decision_time,
        )
        await preflight_capacity_pool(
            [CapacityPoolMember(committee, request, tuple(domain_evidence(e) for e in evidence))],
            expected_instrument_ids=(run.instrument_id,),
        )
        result = await execute_research_run(
            context.session, run_id=run.id, committee=committee, allow_fixture_evidence=False
        )
        assert result.status == "completed"
        return JobHandlerResult(outcome="succeeded", result_code="AGENT_COMPLETED")

    registry = JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.AGENT_PAPER_RESEARCH,
                schema_version="agent_research_job_v1",
                payload_model=worker._AgentResearchJobPayload,
                queue=QueueName.GENERAL,
                timeout_seconds=195,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=1,
                idempotency_namespace="agent.research",
                handler=handle,
                research_budget_profile="codex-sdk-chatgpt-v9-full-input-capacity",
            ),
        )
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="isolated-v9",
        worker_started_at=NOW,
        process_version=worker._PROCESS_VERSION,
        migration_head=worker._MIGRATION_HEAD,
        clock=lambda: NOW,
        job_ids=(job_id,),
        locked_job_guard=capacity_job_qualified,
        claim_registered_only=True,
    )
    assert calls == ["research", "critique", "synthesis"]
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
        job = await session.get(Job, job_id)
        assert run.status == "completed" and job.status == "succeeded"
        assert run.retry_count == 0 and job.attempt_count == 1
        trace = await read_stage_trace(session, run)
        assert trace.status == "verified" and all(
            stage.status == "complete" for stage in trace.stages
        )
        assert sum(len(stage.roles) for stage in trace.stages) == 8
        assert (run.input_tokens, run.output_tokens) == (900, 240)
        terminal = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == run.id, AgentResearchOutputRecord.role.is_(None)
            )
        )
        assert terminal.intent["synthesis_assessment"] is not None
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run.id
            )
        )
        assert reservation.state == "settled"

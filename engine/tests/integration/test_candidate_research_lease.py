"""Candidate leases require an exact locked first claim, with no retry path."""

from datetime import timedelta

import pytest
from integration.test_job_scope import _NOW, _state
from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.registry import JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.jobs.repository import claim_next_job, enqueue_job
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.runtime import JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: str


async def enqueue(
    factory,
    *,
    job_type=JobType.AGENT_PAPER_RESEARCH,
    schema_version="agent_research_job_v1",
    max_attempts=1,
    retry_policy=RetryPolicy.AUTOMATIC_SAFE,
):
    payload = {"run_id": "00000000-0000-0000-0000-000000000001"}
    async with factory() as session, session.begin():
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=job_type,
                scope_kind="global",
                scope_id="candidate.lease",
                effective_at=_NOW,
                input_hash=job_input_hash(payload),
            ),
            schema_version=schema_version,
            payload=payload,
            queue=QueueName.GENERAL,
            retry_policy=retry_policy,
            max_attempts=max_attempts,
        )
        return job.id


@pytest.mark.asyncio
@pytest.mark.parametrize("qualified", [False, True])
async def test_normal_candidate_worker_requires_lock_and_uses_210_second_lease(
    migrated_database_url,
    qualified,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    target = await enqueue(factory)
    before = await _state(factory, target)
    calls = []

    async def guard(_session, job):
        assert job.id == target and job.attempt_count == 0 and job.status == "queued"
        return qualified

    async def handler(context):
        lease = await context.session.scalar(select(JobLease).where(JobLease.job_id == target))
        assert lease.expires_at == _NOW + timedelta(seconds=210)
        assert context.monotonic_deadline() is not None
        calls.append(context.claim.job_id)
        return JobHandlerResult(outcome="succeeded", result_code="TEST")

    registry = JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.AGENT_PAPER_RESEARCH,
                schema_version="agent_research_job_v1",
                payload_model=Payload,
                queue=QueueName.GENERAL,
                timeout_seconds=195,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=1,
                idempotency_namespace="agent.research",
                handler=handler,
                research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
            ),
        )
    )
    args = dict(
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="candidate-test",
        worker_started_at=_NOW,
        process_version="candidate-test",
        migration_head="0066",
        clock=lambda: _NOW,
        job_ids=(target,),
    )
    with pytest.raises(ValueError, match="RESEARCH_BUDGET_PROFILE_REQUIRES_LOCKED_SCOPE"):
        await run_job_worker_once(factory, **args)
    assert await _state(factory, target) == before
    assert await run_job_worker_once(factory, locked_job_guard=guard, **args) is qualified
    if not qualified:
        assert calls == [] and await _state(factory, target) == before
    else:
        assert calls == [target]
        assert (await _state(factory, target))[0] == "succeeded"
        assert await run_job_worker_once(factory, locked_job_guard=guard, **args) is False
        assert calls == [target]


@pytest.mark.asyncio
async def test_long_lease_cannot_claim_legacy_or_wrong_job(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    target = await enqueue(factory, job_type=JobType.AGENT_RESEARCH)
    before = await _state(factory, target)
    calls = []

    async def guard(_session, _job):
        calls.append(True)
        return True

    async with factory() as session, session.begin():
        with pytest.raises(ValueError):
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="test",
                now=_NOW,
                lease_seconds=210,
                job_ids=(target,),
                locked_job_guard=guard,
            )
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="test",
                now=_NOW,
                lease_seconds=210,
                job_ids=(target,),
                locked_job_guard=guard,
                research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
            )
            is None
        )
    assert calls == [] and await _state(factory, target) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"lease_seconds": 211},
        {"lease_seconds": 150},
        {"research_budget_profile": "unknown"},
        {"job_ids": ()},
        {"job_ids": None},
        {"locked_job_guard": None},
    ],
)
async def test_candidate_long_lease_rejects_open_scope_or_unknown_profile(
    migrated_database_url,
    change,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    target = await enqueue(factory)
    before = await _state(factory, target)
    calls = []

    async def guard(_session, _job):
        calls.append(True)
        return True

    args = dict(
        queue=QueueName.GENERAL,
        worker_id="test",
        now=_NOW,
        lease_seconds=210,
        job_ids=(target,),
        locked_job_guard=guard,
        research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
    )
    args.update(change)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError):
            await claim_next_job(session, **args)
    assert calls == [] and await _state(factory, target) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["old_schema", "more_attempts", "attempted", "manual_retry"])
async def test_candidate_claim_refuses_changed_persisted_contract_before_guard(
    migrated_database_url,
    case,
):
    from nexus.jobs.manual_retry import retry_manual_safe_job
    from nexus.jobs.repository import fail_job
    from nexus.persistence.models.runtime import Job

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    target = await enqueue(
        factory,
        schema_version="old_schema_v1" if case == "old_schema" else "agent_research_job_v1",
        max_attempts=2 if case in {"more_attempts", "attempted"} else 1,
        retry_policy=RetryPolicy.MANUAL_SAFE
        if case == "manual_retry"
        else RetryPolicy.AUTOMATIC_SAFE,
    )
    if case in {"attempted", "manual_retry"}:
        async with factory() as session, session.begin():
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="previous-test",
                now=_NOW,
                job_ids=(target,),
            )
            assert claim is not None
            await fail_job(session, claim=claim, now=_NOW, error_code="TEST_FAILURE")
        if case == "manual_retry":
            async with factory() as session, session.begin():
                job = await session.get(Job, target)
                await retry_manual_safe_job(
                    session,
                    job_id=target,
                    actor_id="isolated-test",
                    expected_version=job.version,
                    idempotency_key="candidate-retry-test",
                    now=_NOW,
                )
    before = await _state(factory, target)
    assert before[0] == "queued"
    if case in {"attempted", "manual_retry"}:
        assert before[1] == 1
    calls = []

    async def guard(_session, _job):
        calls.append(True)
        return True

    async with factory() as session, session.begin():
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="test",
                now=_NOW,
                lease_seconds=210,
                job_ids=(target,),
                locked_job_guard=guard,
                research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
            )
            is None
        )
    assert calls == [] and await _state(factory, target) == before

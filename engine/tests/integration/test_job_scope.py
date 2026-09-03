"""Explicit once workers must not claim or reclaim another workflow's jobs."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.registry import JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.jobs.repository import (
    JobDependencyInput,
    claim_next_job,
    enqueue_job,
    reclaim_expired_jobs,
)
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

_NOW = datetime(2026, 9, 4, tzinfo=UTC)


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: str


async def _enqueue(factory, value, job_type=JobType.DIAGNOSTICS_RUN, **kwargs):
    payload = {"value": value}
    async with factory() as session, session.begin():
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=job_type,
                scope_kind="global",
                scope_id="synthetic.manual.scope",
                effective_at=_NOW,
                input_hash=job_input_hash(payload),
            ),
            schema_version="job_scope_test_v1",
            payload=payload,
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=3,
            **kwargs,
        )
        return job.id


async def _state(factory, job_id):
    async with factory() as session:
        job = await session.get(Job, job_id)
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == job_id))
        attempts = tuple(
            (
                await session.execute(
                    select(JobAttempt.attempt_no, JobAttempt.outcome, JobAttempt.finished_at)
                    .where(JobAttempt.job_id == job_id)
                    .order_by(JobAttempt.attempt_no)
                )
            ).all()
        )
        assert job is not None
        return (
            job.status,
            job.attempt_count,
            job.version,
            job.last_error_code,
            None if lease is None else (lease.lease_token, lease.expires_at),
            attempts,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_scope", [False, True])
async def test_scoped_worker_ignores_higher_priority_and_expired_other_jobs(
    migrated_database_url,
    empty_scope,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    expired = await _enqueue(factory, "other-expired", JobType.QUALITY_FREEZE)
    async with factory() as session, session.begin():
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="other-owner",
            now=_NOW,
        )
        assert claim is not None and claim.job_id == expired
    higher = await _enqueue(factory, "other-high", JobType.QUALITY_FREEZE)
    target = await _enqueue(factory, "target")
    before = await _state(factory, expired), await _state(factory, higher)
    executed = []

    async def handler(context):
        executed.append(context.claim.job_id)
        return JobHandlerResult(outcome="succeeded", result_code="SCOPE_TEST_OK")

    registry = JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.DIAGNOSTICS_RUN,
                schema_version="job_scope_test_v1",
                payload_model=_Payload,
                queue=QueueName.GENERAL,
                timeout_seconds=1,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace="scope-test",
                handler=handler,
            ),
        )
    )
    processed = await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="manual-owner",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0036",
        clock=lambda: _NOW + timedelta(seconds=61),
        job_ids=() if empty_scope else (target,),
    )
    assert processed is not empty_scope
    assert executed == ([] if empty_scope else [target])
    assert (await _state(factory, target))[0] == ("queued" if empty_scope else "succeeded")
    assert before == (await _state(factory, expired), await _state(factory, higher))


@pytest.mark.asyncio
async def test_reclaim_scope_is_applied_before_limit_and_empty_means_none(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    other = await _enqueue(factory, "earlier-other")
    target = await _enqueue(factory, "later-target")
    for job_id in (other, target):
        async with factory() as session, session.begin():
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="old-owner",
                now=_NOW,
                job_ids=(job_id,),
            )
            assert claim is not None and claim.job_id == job_id
    before = await _state(factory, other)
    async with factory() as session, session.begin():
        assert (
            await reclaim_expired_jobs(
                session,
                queue=QueueName.GENERAL,
                now=_NOW + timedelta(seconds=31),
                job_ids=(),
            )
            == 0
        )
        assert (
            await reclaim_expired_jobs(
                session,
                queue=QueueName.GENERAL,
                now=_NOW + timedelta(seconds=31),
                job_ids=(target,),
                limit=1,
            )
            == 1
        )
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="none",
                now=_NOW,
                job_ids=(),
            )
            is None
        )
    assert await _state(factory, other) == before
    assert (await _state(factory, target))[:2] == ("queued", 1)
    async with factory() as session, session.begin():
        assert (
            await reclaim_expired_jobs(
                session,
                queue=QueueName.GENERAL,
                now=_NOW + timedelta(seconds=31),
            )
            == 1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("restriction", ["queue", "type", "future", "missing"])
async def test_id_scope_never_bypasses_other_claim_conditions(
    migrated_database_url,
    restriction,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(factory, "scoped")
    async with factory() as session, session.begin():
        assert (
            await claim_next_job(
                session,
                queue=QueueName.PROVIDER if restriction == "queue" else QueueName.GENERAL,
                worker_id="scoped-owner",
                now=_NOW - timedelta(seconds=1) if restriction == "future" else _NOW,
                job_types=(JobType.AGENT_RESEARCH.value,) if restriction == "type" else None,
                job_ids=(UUID(int=1),) if restriction == "missing" else (job_id,),
            )
            is None
        )
    assert (await _state(factory, job_id))[:2] == ("queued", 0)


@pytest.mark.asyncio
async def test_scope_does_not_claim_queued_dependency_outside_scope(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    parent_id = await _enqueue(factory, "pending-parent", JobType.QUALITY_FREEZE)
    child_id = await _enqueue(
        factory,
        "pending-child",
        dependencies=(JobDependencyInput(depends_on_job_id=parent_id),),
    )
    async with factory() as session, session.begin():
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="child-owner",
                now=_NOW,
                job_ids=(child_id,),
            )
            is None
        )
    assert (await _state(factory, parent_id))[:2] == ("queued", 0)
    assert (await _state(factory, child_id))[:2] == ("queued", 0)

"""Real-PostgreSQL contracts for durable Job claims and fencing."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.audit.service import append_audit_event
from nexus.jobs.models import (
    JOB_PRIORITIES,
    JobKeyV1,
    JobType,
    QueueName,
    RetryPolicy,
    job_input_hash,
)
from nexus.jobs.repository import (
    JobConflictError,
    JobDependencyInput,
    LeaseLostError,
    claim_next_job,
    enqueue_job,
    fail_job,
    finalize_job,
    heartbeat_job,
    reclaim_expired_jobs,
    upsert_worker_heartbeat,
)
from nexus.persistence.models.runtime import (
    AuditEvent,
    IntegrationStatus,
    Job,
    JobAttempt,
    OutboxEvent,
    WorkerHeartbeat,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

_NOW = datetime(2026, 8, 27, 1, 0, tzinfo=UTC)
_SCOPE_ID = "0198d500-1000-7000-8000-000000002001"


def _key(job_type: JobType, payload: dict[str, object], *, second: int = 0) -> JobKeyV1:
    return JobKeyV1(
        job_type=job_type,
        scope_kind="portfolio",
        scope_id=_SCOPE_ID,
        effective_at=_NOW + timedelta(seconds=second),
        input_hash=job_input_hash(payload),
    )


async def _enqueue(
    session: AsyncSession,
    *,
    job_type: JobType,
    payload: dict[str, object],
    second: int = 0,
    retry_policy: RetryPolicy = RetryPolicy.AUTOMATIC_SAFE,
    max_attempts: int = 3,
    dependencies: tuple[JobDependencyInput, ...] = (),
) -> Job:
    return await enqueue_job(
        session,
        key=_key(job_type, payload, second=second),
        schema_version="job_test_v1",
        payload=payload,
        queue=QueueName.GENERAL,
        retry_policy=retry_policy,
        max_attempts=max_attempts,
        dependencies=dependencies,
    )


@pytest.mark.asyncio
async def test_concurrent_enqueue_is_idempotent_and_claim_prefers_priority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    payload = {"authority": "same"}

    async def enqueue_same() -> UUID:
        async with factory() as session, session.begin():
            return (await _enqueue(session, job_type=JobType.AGENT_RESEARCH, payload=payload)).id

    first_id, second_id = await asyncio.gather(enqueue_same(), enqueue_same())
    assert first_id == second_id
    async with factory() as session, session.begin():
        high = await _enqueue(
            session,
            job_type=JobType.QUALITY_FREEZE,
            payload={"authority": "quality"},
            second=1,
        )
    async with factory() as session, session.begin():
        claimed = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-priority",
            now=_NOW + timedelta(seconds=2),
        )
    assert claimed is not None
    assert claimed.job_id == high.id
    assert claimed.priority == JOB_PRIORITIES[JobType.QUALITY_FREEZE]
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 2


@pytest.mark.asyncio
async def test_dependencies_block_claim_until_required_parent_outcome(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        parent = await _enqueue(
            session,
            job_type=JobType.QUALITY_FREEZE,
            payload={"stage": "quality"},
        )
        child = await _enqueue(
            session,
            job_type=JobType.DIAGNOSTICS_RUN,
            payload={"stage": "diagnostics"},
            second=1,
            dependencies=(JobDependencyInput(depends_on_job_id=parent.id),),
        )
    with pytest.raises(JobConflictError, match="dependencies"):
        async with factory() as session, session.begin():
            await _enqueue(
                session,
                job_type=JobType.DIAGNOSTICS_RUN,
                payload={"stage": "diagnostics"},
                second=1,
            )
    async with factory() as session, session.begin():
        parent_claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-parent",
            now=_NOW + timedelta(seconds=2),
        )
    assert parent_claim is not None and parent_claim.job_id == parent.id
    async with factory() as session, session.begin():
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="worker-blocked",
                now=_NOW + timedelta(seconds=3),
            )
            is None
        )
    async with factory() as session, session.begin():
        await finalize_job(
            session,
            claim=parent_claim,
            now=_NOW + timedelta(seconds=4),
            outcome="succeeded",
            result_code="QUALITY_PASS",
        )
    async with factory() as session, session.begin():
        child_claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-child",
            now=_NOW + timedelta(seconds=5),
        )
    assert child_claim is not None and child_claim.job_id == child.id


@pytest.mark.asyncio
async def test_concurrent_skip_locked_claims_never_duplicate_job(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _enqueue(
            session,
            job_type=JobType.AGENT_RESEARCH,
            payload={"job": 1},
        )
        await _enqueue(
            session,
            job_type=JobType.AGENT_RESEARCH,
            payload={"job": 2},
            second=1,
        )

    async def claim(worker_id: str) -> UUID | None:
        async with factory() as session, session.begin():
            result = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id=worker_id,
                now=_NOW + timedelta(seconds=2),
            )
            return None if result is None else result.job_id

    claimed = await asyncio.gather(claim("worker-a"), claim("worker-b"))
    assert None not in claimed
    assert len(set(claimed)) == 2


@pytest.mark.asyncio
async def test_reclaim_fences_old_worker_and_finalize_is_one_transaction(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await _enqueue(
            session,
            job_type=JobType.PAPER_PERFORMANCE,
            payload={"session": "2026-08-26"},
        )
    async with factory() as session, session.begin():
        old_claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-old",
            now=_NOW,
            lease_seconds=30,
        )
    assert old_claim is not None
    async with factory() as session, session.begin():
        expires_at = await heartbeat_job(
            session,
            claim=old_claim,
            now=_NOW + timedelta(seconds=10),
            extend_seconds=30,
        )
    assert expires_at == _NOW + timedelta(seconds=40)

    async def add_then_fail(session: AsyncSession) -> None:
        session.add(
            IntegrationStatus(
                provider="rollback-job-test",
                status="READY",
                last_success_at=_NOW,
                mode_changed_at=_NOW,
            )
        )
        await append_audit_event(
            session,
            actor_id="system:job-test",
            action="job.test_finalize",
            schema_version="job_test_v1",
            payload={"outcome": "should_rollback"},
            occurred_at=_NOW + timedelta(seconds=11),
        )
        session.add(
            OutboxEvent(
                event_type="job.test_finalize",
                aggregate_id=job.id,
                schema_version="job_test_v1",
                payload={"outcome": "should_rollback"},
                published_at=None,
            )
        )
        await session.flush()
        raise RuntimeError("injected finalize failure")

    with pytest.raises(RuntimeError, match="injected finalize failure"):
        async with factory() as session, session.begin():
            await finalize_job(
                session,
                claim=old_claim,
                now=_NOW + timedelta(seconds=11),
                outcome="succeeded",
                result_code="SHOULD_ROLL_BACK",
                apply=add_then_fail,
            )
    async with factory() as session:
        still_running = await session.get(Job, job.id)
        rollback_fact_count = await session.scalar(
            select(func.count())
            .select_from(IntegrationStatus)
            .where(IntegrationStatus.provider == "rollback-job-test")
        )
        audit_count = await session.scalar(
            select(func.count()).select_from(AuditEvent)
        )
        outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxEvent)
        )
    assert still_running is not None and still_running.status == "running"
    assert (rollback_fact_count, audit_count, outbox_count) == (0, 0, 0)
    async with factory() as session, session.begin():
        assert (
            await reclaim_expired_jobs(
                session,
                queue=QueueName.GENERAL,
                now=_NOW + timedelta(seconds=41),
            )
            == 1
        )
    async with factory() as session, session.begin():
        new_claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-new",
            now=_NOW + timedelta(seconds=42),
        )
    assert new_claim is not None
    assert new_claim.fence_token == old_claim.fence_token + 1
    assert new_claim.lease_token != old_claim.lease_token

    async def add_business_fact(session: AsyncSession) -> None:
        session.add(
            IntegrationStatus(
                provider="fenced-job-test",
                status="READY",
                last_success_at=_NOW,
                mode_changed_at=_NOW,
            )
        )

    with pytest.raises(LeaseLostError, match="lease"):
        async with factory() as session, session.begin():
            await finalize_job(
                session,
                claim=old_claim,
                now=_NOW + timedelta(seconds=43),
                outcome="succeeded",
                result_code="STALE_RESULT",
                apply=add_business_fact,
            )
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(IntegrationStatus)
                .where(IntegrationStatus.provider == "fenced-job-test")
            )
            == 0
        )
    async with factory() as session, session.begin():
        await finalize_job(
            session,
            claim=new_claim,
            now=_NOW + timedelta(seconds=44),
            outcome="succeeded",
            result_code="PERSISTED",
            apply=add_business_fact,
        )
    async with factory() as session:
        persisted = await session.get(Job, job.id)
        attempts = tuple(
            await session.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job.id)
                .order_by(JobAttempt.attempt_no)
            )
        )
        side_effects = await session.scalar(
            select(func.count())
            .select_from(IntegrationStatus)
            .where(IntegrationStatus.provider == "fenced-job-test")
        )
    assert persisted is not None and persisted.status == "succeeded"
    assert [(item.attempt_no, item.outcome) for item in attempts] == [
        (1, "lost"),
        (2, "succeeded"),
    ]
    assert side_effects == 1


@pytest.mark.asyncio
async def test_automatic_failures_exhaust_and_worker_heartbeat_is_monotonic(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await _enqueue(
            session,
            job_type=JobType.AGENT_RESEARCH,
            payload={"run_id": "fixture"},
            max_attempts=2,
        )
        first_heartbeat = await upsert_worker_heartbeat(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-heartbeat",
            process_version="v1",
            migration_head="0021_manual_job_retry_allowance",
            started_at=_NOW,
            seen_at=_NOW,
        )
    assert first_heartbeat.seen_at == _NOW
    async with factory() as session, session.begin():
        latest_heartbeat = await upsert_worker_heartbeat(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-heartbeat",
            process_version="v2",
            migration_head="0021_manual_job_retry_allowance",
            started_at=_NOW,
            seen_at=_NOW + timedelta(seconds=5),
        )
    assert latest_heartbeat.process_version == "v2"
    async with factory() as session, session.begin():
        first_claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-fail-one",
            now=_NOW,
        )
    assert first_claim is not None
    async with factory() as session, session.begin():
        first_status = await fail_job(
            session,
            claim=first_claim,
            now=_NOW + timedelta(seconds=1),
            error_code="TRANSIENT_FIXTURE",
            retry_delay_seconds=1,
        )
    assert first_status == "queued"
    async with factory() as session, session.begin():
        second_claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-fail-two",
            now=_NOW + timedelta(seconds=2),
        )
    assert second_claim is not None
    async with factory() as session, session.begin():
        second_status = await fail_job(
            session,
            claim=second_claim,
            now=_NOW + timedelta(seconds=3),
            error_code="TRANSIENT_FIXTURE",
        )
    assert second_status == "dead_letter"
    async with factory() as session, session.begin():
        replayed = await _enqueue(
            session,
            job_type=JobType.AGENT_RESEARCH,
            payload={"run_id": "fixture"},
            max_attempts=2,
        )
    assert replayed.id == job.id
    async with factory() as session:
        persisted = await session.get(Job, job.id)
        heartbeat_count = await session.scalar(
            select(func.count()).select_from(WorkerHeartbeat)
        )
    assert persisted is not None
    assert (persisted.status, persisted.attempt_count, persisted.last_error_code) == (
        "dead_letter",
        2,
        "TRANSIENT_FIXTURE",
    )
    assert heartbeat_count == 1

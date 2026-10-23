"""Real-PostgreSQL coverage for fenced short Job checkpoints."""

from datetime import UTC, datetime, timedelta

import pytest
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import (
    LeaseLostError,
    checkpoint_job,
    claim_next_job,
    enqueue_job,
)
from nexus.persistence.models.runtime import IntegrationStatus, Job, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

NOW = datetime(2026, 9, 10, 9, tzinfo=UTC)


@pytest.mark.asyncio
async def test_checkpoint_applies_business_fact_and_extends_current_lease_atomically(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    claim = await _claim(factory, lease_seconds=30)

    async def apply(session: AsyncSession) -> None:
        session.add(
            IntegrationStatus(
                provider="checkpoint-atomic",
                status="READY",
                last_success_at=NOW,
                mode_changed_at=NOW,
            )
        )

    async with factory() as session, session.begin():
        expires_at = await checkpoint_job(
            session,
            claim=claim,
            now=NOW + timedelta(seconds=10),
            apply=apply,
            extend_seconds=60,
        )

    async with factory() as session:
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
        fact_count = await session.scalar(
            select(func.count())
            .select_from(IntegrationStatus)
            .where(IntegrationStatus.provider == "checkpoint-atomic")
        )
    assert expires_at == NOW + timedelta(seconds=70)
    assert lease is not None and lease.expires_at == expires_at
    assert fact_count == 1


@pytest.mark.asyncio
async def test_checkpoint_rejects_expired_claim_before_business_apply(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    claim = await _claim(factory, lease_seconds=5)
    applied = False

    async def apply(session: AsyncSession) -> None:
        nonlocal applied
        applied = True
        session.add(
            IntegrationStatus(
                provider="checkpoint-stale",
                status="READY",
                last_success_at=NOW,
                mode_changed_at=NOW,
            )
        )

    with pytest.raises(LeaseLostError, match="lease"):
        async with factory() as session, session.begin():
            await checkpoint_job(
                session,
                claim=claim,
                now=NOW + timedelta(seconds=5),
                apply=apply,
            )

    async with factory() as session:
        fact_count = await session.scalar(
            select(func.count())
            .select_from(IntegrationStatus)
            .where(IntegrationStatus.provider == "checkpoint-stale")
        )
    assert applied is False
    assert fact_count == 0


@pytest.mark.asyncio
async def test_checkpoint_rolls_back_flushed_business_fact_and_lease_on_failure(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    claim = await _claim(factory, lease_seconds=30)

    async def apply(session: AsyncSession) -> None:
        session.add(
            IntegrationStatus(
                provider="checkpoint-rollback",
                status="READY",
                last_success_at=NOW,
                mode_changed_at=NOW,
            )
        )
        await session.flush()
        raise RuntimeError("injected checkpoint failure")

    with pytest.raises(RuntimeError, match="injected checkpoint failure"):
        async with factory() as session, session.begin():
            await checkpoint_job(
                session,
                claim=claim,
                now=NOW + timedelta(seconds=10),
                apply=apply,
                extend_seconds=60,
            )

    async with factory() as session:
        job = await session.get(Job, claim.job_id)
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
        fact_count = await session.scalar(
            select(func.count())
            .select_from(IntegrationStatus)
            .where(IntegrationStatus.provider == "checkpoint-rollback")
        )
    assert job is not None and job.version == claim.version
    assert lease is not None and lease.expires_at == claim.expires_at
    assert lease.heartbeat_at == NOW
    assert fact_count == 0


async def _claim(factory: object, *, lease_seconds: int):
    async with factory() as session, session.begin():
        await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.AGENT_RESEARCH,
                scope_kind="portfolio",
                scope_id="01990000-0000-7000-8000-000000000001",
                effective_at=NOW,
                input_hash=job_input_hash({"checkpoint": lease_seconds}),
            ),
            schema_version="checkpoint_test_v1",
            payload={"checkpoint": lease_seconds},
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.NEVER,
            max_attempts=1,
        )
    async with factory() as session, session.begin():
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id=f"checkpoint-{lease_seconds}",
            now=NOW,
            lease_seconds=lease_seconds,
        )
    assert claim is not None
    return claim


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["succeeded", "failed", "skipped"])
async def test_native_terminal_job_consumes_future_lease(migrated_database_url, outcome):
    from nexus.jobs.repository import JobCompletion, execute_and_finalize_job, job_lease_consumed

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    claim = await _claim(factory, lease_seconds=30)
    finished = NOW + timedelta(seconds=5)

    async def execute(session):
        return JobCompletion(
            outcome=outcome,
            result_code="NATIVE_TERMINAL",
            error_code="NATIVE_FAILURE" if outcome == "failed" else None,
            finished_at=finished,
        )

    async with factory() as session, session.begin():
        job = await session.get(Job, claim.job_id)
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
        assert not await job_lease_consumed(session, job=job, lease=lease, now=finished)
        await execute_and_finalize_job(session, claim=claim, checked_at=finished, execute=execute)
    async with factory() as session:
        job = await session.get(Job, claim.job_id)
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
        assert lease.expires_at > finished
        assert await job_lease_consumed(session, job=job, lease=lease, now=finished)
        assert not await job_lease_consumed(
            session, job=job, lease=lease, now=finished - timedelta(microseconds=1)
        )

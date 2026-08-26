"""Generic worker registry, timeout, retry, and dead-letter contracts."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.jobs.models import (
    JobKeyV1,
    JobType,
    QueueName,
    RetryPolicy,
    job_input_hash,
)
from nexus.jobs.registry import (
    JobHandlerContext,
    JobHandlerResult,
    JobHandlerSpec,
    JobRegistry,
)
from nexus.jobs.repository import (
    JobDependencyInput,
    claim_next_job,
    enqueue_job,
    finalize_job,
)
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.runtime import IntegrationStatus, Job, WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_NOW = datetime(2026, 8, 27, 2, 0, tzinfo=UTC)
_SCOPE_ID = "0198d500-1000-7000-8000-000000002101"


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: str


class _UnsafePayload(BaseModel):
    value: str


async def _enqueue(
    factory: async_sessionmaker[AsyncSession],
    *,
    job_type: JobType,
    payload: dict[str, object],
    retry_policy: RetryPolicy = RetryPolicy.AUTOMATIC_SAFE,
    max_attempts: int = 3,
    second: int = 0,
    dependencies: tuple[JobDependencyInput, ...] = (),
) -> UUID:
    async with factory() as session, session.begin():
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=job_type,
                scope_kind="portfolio",
                scope_id=_SCOPE_ID,
                effective_at=_NOW + timedelta(seconds=second),
                input_hash=job_input_hash(payload),
            ),
            schema_version="job_worker_test_v1",
            payload=payload,
            queue=QueueName.GENERAL,
            retry_policy=retry_policy,
            max_attempts=max_attempts,
            dependencies=dependencies,
        )
    return job.id


@pytest.mark.asyncio
async def test_terminal_dependency_mismatch_skips_child_without_running_handler(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    parent_id = await _enqueue(
        factory,
        job_type=JobType.QUALITY_FREEZE,
        payload={"value": "quality"},
    )
    async with factory() as session, session.begin():
        parent_claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="worker-quality",
            now=_NOW + timedelta(seconds=1),
        )
    assert parent_claim is not None and parent_claim.job_id == parent_id
    async with factory() as session, session.begin():
        await finalize_job(
            session,
            claim=parent_claim,
            now=_NOW + timedelta(seconds=2),
            outcome="skipped",
            result_code="SKIPPED_DATA_QUALITY",
        )
    child_id = await _enqueue(
        factory,
        job_type=JobType.DIAGNOSTICS_RUN,
        payload={"value": "must-not-run"},
        second=3,
        dependencies=(JobDependencyInput(depends_on_job_id=parent_id),),
    )
    executed = False

    async def handler(context: JobHandlerContext) -> JobHandlerResult:
        nonlocal executed
        del context
        executed = True
        return JobHandlerResult(outcome="succeeded", result_code="UNREACHABLE")

    assert await run_job_worker_once(
        factory,
        registry=JobRegistry((_spec(handler, job_type=JobType.DIAGNOSTICS_RUN),)),
        queue=QueueName.GENERAL,
        worker_id="worker-dependent",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: _NOW + timedelta(seconds=4),
        claim_registered_only=True,
    )
    async with factory() as session:
        child = await session.get(Job, child_id)
    assert executed is False
    assert child is not None
    assert (child.status, child.result_code, child.attempt_count) == (
        "skipped",
        "DEPENDENCY_DATA_QUALITY",
        1,
    )


def _spec(
    handler,
    *,
    job_type: JobType = JobType.AGENT_RESEARCH,
    retry_policy: RetryPolicy = RetryPolicy.AUTOMATIC_SAFE,
    max_attempts: int = 3,
    timeout_seconds: float = 1,
    payload_model: type[BaseModel] = _Payload,
) -> JobHandlerSpec:
    return JobHandlerSpec(
        job_type=job_type,
        schema_version="job_worker_test_v1",
        payload_model=payload_model,
        queue=QueueName.GENERAL,
        timeout_seconds=timeout_seconds,
        retry_policy=retry_policy,
        max_attempts=max_attempts,
        idempotency_namespace="job-worker-test",
        handler=handler,
    )


def test_registry_rejects_duplicate_and_non_strict_payload_model() -> None:
    async def handler(context: JobHandlerContext) -> JobHandlerResult:
        del context
        return JobHandlerResult(outcome="succeeded", result_code="OK")

    registry = JobRegistry((_spec(handler),))
    with pytest.raises(ValueError, match="duplicate"):
        registry.register(_spec(handler))
    with pytest.raises(ValueError, match="extra='forbid'"):
        _spec(handler, payload_model=_UnsafePayload)


@pytest.mark.asyncio
async def test_empty_tick_records_compatible_worker_heartbeat(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    worked = await run_job_worker_once(
        factory,
        registry=JobRegistry(),
        queue=QueueName.GENERAL,
        worker_id="worker-empty",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: _NOW,
    )
    assert worked is False
    async with factory() as session:
        heartbeat = await session.scalar(select(WorkerHeartbeat))
    assert heartbeat is not None
    assert (heartbeat.queue, heartbeat.seen_at) == ("general", _NOW)


@pytest.mark.asyncio
async def test_registered_handler_commits_business_fact_and_terminal_job_atomically(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={"value": "persist"},
    )
    unrelated_id = await _enqueue(
        factory,
        job_type=JobType.DIAGNOSTICS_RUN,
        payload={"value": "must-remain-queued"},
        second=1,
    )

    async def handler(context: JobHandlerContext) -> JobHandlerResult:
        context.session.add(
            IntegrationStatus(
                provider="generic-worker-test",
                status="READY",
                last_success_at=context.finished_at(),
                mode_changed_at=context.finished_at(),
            )
        )
        return JobHandlerResult(outcome="succeeded", result_code="HANDLER_OK")

    worked = await run_job_worker_once(
        factory,
        registry=JobRegistry((_spec(handler),)),
        queue=QueueName.GENERAL,
        worker_id="worker-success",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )
    assert worked is True
    async with factory() as session:
        job = await session.get(Job, job_id)
        unrelated = await session.get(Job, unrelated_id)
        facts = await session.scalar(
            select(func.count())
            .select_from(IntegrationStatus)
            .where(IntegrationStatus.provider == "generic-worker-test")
        )
    assert job is not None
    assert unrelated is not None and unrelated.status == "queued"
    assert (job.status, job.result_code, job.attempt_count, facts) == (
        "succeeded",
        "HANDLER_OK",
        1,
        1,
    )


@pytest.mark.asyncio
async def test_unknown_handler_dead_letters_without_interpreting_payload(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(
        factory,
        job_type=JobType.DIAGNOSTICS_RUN,
        payload={"untrusted": {"url": "https://invalid.example"}},
    )
    assert await run_job_worker_once(
        factory,
        registry=JobRegistry(),
        queue=QueueName.GENERAL,
        worker_id="worker-unknown",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: _NOW + timedelta(seconds=1),
    )
    async with factory() as session:
        job = await session.get(Job, job_id)
    assert job is not None
    assert (job.status, job.last_error_code) == (
        "dead_letter",
        "HANDLER_UNREGISTERED",
    )


@pytest.mark.asyncio
async def test_registered_handler_rejects_url_payload_before_execution(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={"value": "https://invalid.example/provider-method"},
    )
    executed = False

    async def handler(context: JobHandlerContext) -> JobHandlerResult:
        nonlocal executed
        del context
        executed = True
        return JobHandlerResult(outcome="succeeded", result_code="UNSAFE")

    assert await run_job_worker_once(
        factory,
        registry=JobRegistry((_spec(handler),)),
        queue=QueueName.GENERAL,
        worker_id="worker-unsafe-payload",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: _NOW + timedelta(seconds=1),
    )
    async with factory() as session:
        job = await session.get(Job, job_id)
    assert executed is False
    assert job is not None
    assert (job.status, job.last_error_code) == (
        "dead_letter",
        "HANDLER_PAYLOAD_UNSAFE",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_policy", "expected_first_status"),
    [
        (RetryPolicy.AUTOMATIC_SAFE, "queued"),
        (RetryPolicy.MANUAL_SAFE, "failed"),
        (RetryPolicy.NEVER, "failed"),
    ],
)
async def test_timeout_obeys_declared_retry_policy_and_attempt_limit(
    migrated_database_url: str,
    retry_policy: RetryPolicy,
    expected_first_status: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={"value": "timeout"},
        retry_policy=retry_policy,
        max_attempts=2,
    )

    async def timeout_handler(context: JobHandlerContext) -> JobHandlerResult:
        del context
        await asyncio.sleep(0.02)
        return JobHandlerResult(outcome="succeeded", result_code="TOO_LATE")

    current = _NOW + timedelta(seconds=1)

    def clock() -> datetime:
        return current

    registry = JobRegistry(
        (
            _spec(
                timeout_handler,
                retry_policy=retry_policy,
                max_attempts=2,
                timeout_seconds=0.001,
            ),
        )
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id=f"worker-timeout-{retry_policy.value}",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=clock,
    )
    async with factory() as session:
        first = await session.get(Job, job_id)
    assert first is not None
    assert (first.status, first.last_error_code) == (
        expected_first_status,
        "HANDLER_TIMEOUT",
    )
    if retry_policy is RetryPolicy.AUTOMATIC_SAFE:
        current = _NOW + timedelta(seconds=4)
        assert await run_job_worker_once(
            factory,
            registry=registry,
            queue=QueueName.GENERAL,
            worker_id="worker-timeout-auto-second",
            worker_started_at=_NOW,
            process_version="test-v1",
            migration_head="0021_manual_job_retry_allowance",
            clock=clock,
        )
        async with factory() as session:
            exhausted = await session.get(Job, job_id)
        assert exhausted is not None
        assert (exhausted.status, exhausted.attempt_count) == ("dead_letter", 2)

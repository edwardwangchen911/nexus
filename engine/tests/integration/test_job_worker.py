"""Generic worker registry, timeout, retry, and dead-letter contracts."""

import asyncio
from datetime import UTC, date, datetime, timedelta
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


class _NestedStrictPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authority_id: UUID
    session_date: date
    observed_at: datetime
    sequence: int


class _StrictJsonPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nested: _NestedStrictPayload


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


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout,attempts,elapsed,lease", [(2, 3, 1, 60), (135, 1, 90, 150)])
async def test_worker_lease_covers_bounded_handler_and_terminal_write(
    migrated_database_url,
    timeout,
    attempts,
    elapsed,
    lease,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={"value": "bounded-stages"},
        max_attempts=attempts,
    )
    now = _NOW

    async def handler(context):
        nonlocal now
        assert (context.claim.expires_at - _NOW).total_seconds() == lease
        now += timedelta(seconds=elapsed)
        return JobHandlerResult(outcome="succeeded", result_code="STAGES_COMPLETED")

    assert await run_job_worker_once(
        factory,
        registry=JobRegistry((_spec(handler, timeout_seconds=timeout, max_attempts=attempts),)),
        queue=QueueName.GENERAL,
        worker_id="bounded-stages",
        worker_started_at=_NOW,
        process_version="test",
        migration_head="0033_cash_flow_business_date",
        clock=lambda: now,
    )
    async with factory() as session:
        job = await session.get(Job, job_id)
        assert job.status == "succeeded" and job.attempt_count == 1


@pytest.mark.parametrize(
    "job_type,attempts",
    [(JobType.AGENT_RESEARCH, 2), (JobType.DAILY_BRIEF, 1)],
)
def test_extended_handler_budget_is_limited_to_single_attempt_research(job_type, attempts):
    with pytest.raises(ValueError, match="timeout_seconds"):
        _spec(lambda _: None, job_type=job_type, timeout_seconds=120, max_attempts=attempts)


@pytest.mark.asyncio
async def test_preclaim_guard_fails_before_heartbeat_reclaim_or_claim(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={"value": "guarded"},
    )

    async def reject(_session: AsyncSession) -> None:
        raise RuntimeError("PRECLAIM_GUARD_REJECTED")

    with pytest.raises(RuntimeError, match="PRECLAIM_GUARD_REJECTED"):
        await run_job_worker_once(
            factory,
            registry=JobRegistry((_spec(lambda _: None),)),
            queue=QueueName.GENERAL,
            worker_id="worker-preclaim-guard",
            worker_started_at=_NOW,
            process_version="test-v1",
            migration_head="0032_ticker_alias_authority",
            clock=lambda: _NOW + timedelta(seconds=1),
            preclaim_guard=reject,
        )

    async with factory() as session:
        job = await session.get(Job, job_id)
        heartbeat_count = await session.scalar(select(func.count()).select_from(WorkerHeartbeat))
    assert job is not None
    assert (job.status, job.attempt_count) == ("queued", 0)
    assert heartbeat_count == 0


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
async def test_worker_passes_one_absolute_deadline_into_handler(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={"value": "deadline"},
    )
    seen_remaining: list[float] = []

    async def handler(context: JobHandlerContext) -> JobHandlerResult:
        deadline = context.monotonic_deadline()
        assert deadline is not None
        seen_remaining.append(deadline - asyncio.get_running_loop().time())
        return JobHandlerResult(outcome="succeeded", result_code="DEADLINE_OBSERVED")

    assert await run_job_worker_once(
        factory,
        registry=JobRegistry((_spec(handler, timeout_seconds=2),)),
        queue=QueueName.GENERAL,
        worker_id="worker-deadline",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0032_ticker_alias_authority",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )

    assert len(seen_remaining) == 1
    assert 0 < seen_remaining[0] <= 2


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
async def test_registered_handler_strictly_parses_nested_jsonb_types(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    authority_id = UUID("0198d500-1000-7000-8000-000000002102")
    job_id = await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={
            "nested": {
                "authority_id": str(authority_id),
                "session_date": "2026-08-27",
                "observed_at": "2026-08-27T02:00:00Z",
                "sequence": 1,
            }
        },
    )
    observed: tuple[UUID, date, datetime, int] | None = None

    async def handler(context: JobHandlerContext) -> JobHandlerResult:
        nonlocal observed
        assert isinstance(context.payload, _StrictJsonPayload)
        nested = context.payload.nested
        observed = (
            nested.authority_id,
            nested.session_date,
            nested.observed_at,
            nested.sequence,
        )
        return JobHandlerResult(outcome="succeeded", result_code="STRICT_JSON_OK")

    assert await run_job_worker_once(
        factory,
        registry=JobRegistry((_spec(handler, payload_model=_StrictJsonPayload),)),
        queue=QueueName.GENERAL,
        worker_id="worker-strict-json",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )
    async with factory() as session:
        job = await session.get(Job, job_id)

    assert observed == (authority_id, date(2026, 8, 27), _NOW, 1)
    assert job is not None
    assert (job.status, job.result_code, job.attempt_count) == (
        "succeeded",
        "STRICT_JSON_OK",
        1,
    )


@pytest.mark.asyncio
async def test_registered_handler_dead_letters_string_for_strict_nested_integer(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await _enqueue(
        factory,
        job_type=JobType.AGENT_RESEARCH,
        payload={
            "nested": {
                "authority_id": "0198d500-1000-7000-8000-000000002102",
                "session_date": "2026-08-27",
                "observed_at": "2026-08-27T02:00:00Z",
                "sequence": "1",
            }
        },
    )
    executed = False

    async def handler(context: JobHandlerContext) -> JobHandlerResult:
        nonlocal executed
        del context
        executed = True
        return JobHandlerResult(outcome="succeeded", result_code="UNREACHABLE")

    assert await run_job_worker_once(
        factory,
        registry=JobRegistry((_spec(handler, payload_model=_StrictJsonPayload),)),
        queue=QueueName.GENERAL,
        worker_id="worker-strict-json-invalid",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )
    async with factory() as session:
        job = await session.get(Job, job_id)

    assert executed is False
    assert job is not None
    assert (job.status, job.last_error_code, job.attempt_count) == (
        "dead_letter",
        "HANDLER_PAYLOAD_INVALID",
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

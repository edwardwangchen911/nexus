"""PostgreSQL-authoritative Job enqueue, claim, lease, and terminalization."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import and_, exists, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.jobs.models import (
    BROKER_JOB_TYPES,
    JOB_PRIORITIES,
    PROVIDER_JOB_TYPES,
    JobKeyV1,
    JobStatus,
    JobType,
    QueueName,
    RetryPolicy,
    canonical_job_key,
)
from nexus.persistence.models.runtime import (
    Job,
    JobAttempt,
    JobDependency,
    JobLease,
    WorkerHeartbeat,
)

RequiredOutcome = Literal["succeeded", "succeeded_or_skipped"]
TerminalOutcome = Literal["succeeded", "skipped"]
ExecutionOutcome = Literal["succeeded", "skipped", "failed"]
JobApply = Callable[[AsyncSession], Awaitable[None]]
JobExecution = Callable[[AsyncSession], Awaitable["JobCompletion"]]

_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,99}$")
class JobConflictError(RuntimeError):
    """A canonical JobKey resolved to different immutable authority fields."""


class LeaseLostError(RuntimeError):
    """The caller no longer owns the current, unexpired fenced lease."""


@dataclass(frozen=True)
class JobDependencyInput:
    depends_on_job_id: UUID
    required_outcome: RequiredOutcome = "succeeded"


@dataclass(frozen=True)
class ClaimedJob:
    job_id: UUID
    job_type: str
    schema_version: str
    payload: dict[str, object]
    queue: str
    priority: int
    retry_policy: str
    max_attempts: int
    attempt_no: int
    effective_at: datetime
    worker_id: str
    lease_token: UUID
    fence_token: int
    expires_at: datetime
    version: int
    dependencies_satisfied: bool
    dependency_result_code: str | None


@dataclass(frozen=True)
class JobCompletion:
    outcome: ExecutionOutcome
    result_code: str | None
    finished_at: datetime
    error_code: str | None = None


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must use UTC")
    return value.astimezone(UTC)


def _worker_id(value: str) -> str:
    if not 1 <= len(value) <= 255 or value.strip() != value:
        raise ValueError("worker_id is invalid")
    return value


def _error_code(value: str) -> str:
    if _ERROR_CODE.fullmatch(value) is None:
        raise ValueError("error_code is invalid")
    return value


def _require_queue(job_type: JobType, queue: QueueName) -> None:
    expected = (
        QueueName.BROKER
        if job_type in BROKER_JOB_TYPES
        else QueueName.PROVIDER
        if job_type in PROVIDER_JOB_TYPES
        else QueueName.STRATEGY_RND
        if job_type is JobType.STRATEGY_RND
        else QueueName.GENERAL
    )
    if queue is not expected:
        raise ValueError(f"{job_type.value} must use the {expected.value} queue")


async def enqueue_job(
    session: AsyncSession,
    *,
    key: JobKeyV1,
    schema_version: str,
    payload: dict[str, object],
    queue: QueueName,
    retry_policy: RetryPolicy,
    max_attempts: int = 3,
    available_at: datetime | None = None,
    dependencies: Sequence[JobDependencyInput] = (),
) -> Job:
    """Insert one canonical intent or return the exact existing Job."""
    _require_queue(key.job_type, queue)
    canonical_json(payload)
    if not 1 <= len(schema_version) <= 32:
        raise ValueError("schema_version is invalid")
    if not 1 <= max_attempts <= 3:
        raise ValueError("max_attempts must be between 1 and 3")
    available = key.effective_at if available_at is None else _utc(
        available_at, field="available_at"
    )
    job_key = canonical_job_key(key)
    priority = JOB_PRIORITIES[key.job_type]
    candidate_id = new_uuid7()
    inserted_id = await session.scalar(
        insert(Job)
        .values(
            id=candidate_id,
            job_key=job_key,
            input_hash=key.input_hash,
            status=JobStatus.QUEUED.value,
            schema_version=schema_version,
            payload=payload,
            job_type=key.job_type.value,
            queue=queue.value,
            scope_kind=key.scope_kind,
            scope_id=key.scope_id,
            effective_at=key.effective_at,
            priority=priority,
            retry_policy=retry_policy.value,
            max_attempts=max_attempts,
            attempt_count=0,
            manual_retry_count=0,
            available_at=available,
            started_at=None,
            finished_at=None,
            last_error_code=None,
            result_code=None,
            version=1,
        )
        .on_conflict_do_nothing(index_elements=[Job.job_key])
        .returning(Job.id)
    )
    created = inserted_id is not None
    job_id = inserted_id
    if job_id is None:
        job_id = await session.scalar(select(Job.id).where(Job.job_key == job_key))
    if job_id is None:
        raise RuntimeError("durable Job conflict could not be resolved")
    job = await session.get(Job, job_id)
    if job is None:
        raise RuntimeError("durable Job is unavailable")
    expected = (
        key.input_hash,
        schema_version,
        payload,
        key.job_type.value,
        queue.value,
        key.scope_kind,
        key.scope_id,
        key.effective_at,
        priority,
        retry_policy.value,
        max_attempts,
    )
    actual = (
        job.input_hash,
        job.schema_version,
        job.payload,
        job.job_type,
        job.queue,
        job.scope_kind,
        job.scope_id,
        job.effective_at,
        job.priority,
        job.retry_policy,
        job.max_attempts,
    )
    if actual != expected:
        raise JobConflictError("canonical Job authority conflicts with the existing row")
    expected_dependencies = {
        (dependency.depends_on_job_id, dependency.required_outcome)
        for dependency in dependencies
    }
    if created:
        for dependency in dependencies:
            await session.execute(
                insert(JobDependency).values(
                    id=new_uuid7(),
                    job_id=job.id,
                    depends_on_job_id=dependency.depends_on_job_id,
                    required_outcome=dependency.required_outcome,
                )
            )
    else:
        existing_dependencies = set(
            (
                await session.execute(
                    select(
                        JobDependency.depends_on_job_id,
                        JobDependency.required_outcome,
                    ).where(JobDependency.job_id == job.id)
                )
            ).tuples()
        )
        if existing_dependencies != expected_dependencies:
            raise JobConflictError("canonical Job dependencies conflict with existing edges")
    await session.flush()
    return job


async def claim_next_job(
    session: AsyncSession,
    *,
    queue: QueueName,
    worker_id: str,
    now: datetime,
    lease_seconds: int = 30,
    job_types: Sequence[str] | None = None,
    job_ids: tuple[UUID, ...] | None = None,
) -> ClaimedJob | None:
    """Claim the highest-priority ready Job without blocking another worker."""
    now = _utc(now, field="now")
    worker_id = _worker_id(worker_id)
    if not 1 <= lease_seconds <= 150:
        raise ValueError("lease_seconds must be between 1 and 150")
    if job_types is not None and not job_types:
        return None
    if job_ids is not None and not job_ids:
        return None
    dependency = aliased(JobDependency)
    parent = aliased(Job)
    nonterminal_dependency = exists(
        select(1)
        .select_from(dependency)
        .join(parent, parent.id == dependency.depends_on_job_id)
        .where(
            dependency.job_id == Job.id,
            parent.status.in_((JobStatus.QUEUED.value, JobStatus.RUNNING.value)),
        )
    )
    conditions = [
        Job.queue == queue.value,
        Job.status == JobStatus.QUEUED.value,
        Job.available_at <= now,
        Job.attempt_count < Job.max_attempts + Job.manual_retry_count,
        ~nonterminal_dependency,
    ]
    if job_types is not None:
        conditions.append(Job.job_type.in_(tuple(job_types)))
    if job_ids is not None:
        conditions.append(Job.id.in_(job_ids))
    job = await session.scalar(
        select(Job)
        .where(*conditions)
        .order_by(
            Job.priority.desc(),
            Job.available_at,
            Job.created_at,
            Job.id,
        )
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    if job is None:
        return None
    dependency_rows = tuple(
        (
            await session.execute(
                select(
                    JobDependency.required_outcome,
                    parent.status,
                    parent.result_code,
                )
                .select_from(JobDependency)
                .join(parent, parent.id == JobDependency.depends_on_job_id)
                .where(JobDependency.job_id == job.id)
            )
        ).tuples()
    )
    dependency_mismatches = tuple(
        row
        for row in dependency_rows
        if not (
            row[1] == JobStatus.SUCCEEDED.value
            or (
                row[0] == "succeeded_or_skipped"
                and row[1] == JobStatus.SKIPPED.value
            )
        )
    )
    dependency_result_code = (
        None
        if not dependency_mismatches
        else "DEPENDENCY_DATA_QUALITY"
        if any(
            row[2] in {"SKIPPED_DATA_QUALITY", "DEPENDENCY_DATA_QUALITY"}
            for row in dependency_mismatches
        )
        else "DEPENDENCY_NOT_SATISFIED"
    )
    lease = await session.scalar(
        select(JobLease).where(JobLease.job_id == job.id).with_for_update()
    )
    lease_token = new_uuid7()
    expires_at = now + timedelta(seconds=lease_seconds)
    if lease is None:
        fence_token = 1
        lease = JobLease(
            id=new_uuid7(),
            job_id=job.id,
            worker_id=worker_id,
            expires_at=expires_at,
            lease_token=lease_token,
            fence_token=fence_token,
            acquired_at=now,
            heartbeat_at=now,
        )
        session.add(lease)
    else:
        if now < lease.heartbeat_at:
            raise ValueError("claim time precedes the prior lease heartbeat")
        lease.worker_id = worker_id
        lease.expires_at = expires_at
        lease.lease_token = lease_token
        lease.fence_token += 1
        lease.acquired_at = now
        lease.heartbeat_at = now
        fence_token = lease.fence_token
    job.status = JobStatus.RUNNING.value
    job.attempt_count += 1
    job.started_at = now
    job.finished_at = None
    job.result_code = None
    job.version += 1
    attempt = JobAttempt(
        id=new_uuid7(),
        job_id=job.id,
        attempt_no=job.attempt_count,
        lease_token=lease_token,
        fence_token=fence_token,
        worker_id=worker_id,
        started_at=now,
        finished_at=None,
        outcome="running",
        error_code=None,
    )
    session.add(attempt)
    await session.flush()
    return ClaimedJob(
        job_id=job.id,
        job_type=job.job_type,
        schema_version=job.schema_version,
        payload=dict(job.payload),
        queue=job.queue,
        priority=job.priority,
        retry_policy=job.retry_policy,
        max_attempts=job.max_attempts,
        attempt_no=job.attempt_count,
        effective_at=job.effective_at,
        worker_id=worker_id,
        lease_token=lease_token,
        fence_token=fence_token,
        expires_at=expires_at,
        version=job.version,
        dependencies_satisfied=not dependency_mismatches,
        dependency_result_code=dependency_result_code,
    )


async def _lock_active_claim(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    now: datetime,
) -> tuple[Job, JobLease, JobAttempt]:
    now = _utc(now, field="now")
    row = (
        await session.execute(
            select(Job, JobLease)
            .join(JobLease, JobLease.job_id == Job.id)
            .where(Job.id == claim.job_id)
            .with_for_update()
        )
    ).one_or_none()
    if row is None:
        raise LeaseLostError("job lease is unavailable")
    job, lease = row
    attempt = await session.scalar(
        select(JobAttempt)
        .where(
            JobAttempt.job_id == claim.job_id,
            JobAttempt.attempt_no == claim.attempt_no,
        )
        .with_for_update()
    )
    if (
        job.status != JobStatus.RUNNING.value
        or lease.lease_token != claim.lease_token
        or lease.fence_token != claim.fence_token
        or lease.worker_id != claim.worker_id
        or now >= lease.expires_at
        or attempt is None
        or attempt.lease_token != claim.lease_token
        or attempt.fence_token != claim.fence_token
        or attempt.outcome != "running"
    ):
        raise LeaseLostError("job lease is stale or expired")
    return job, lease, attempt


async def heartbeat_job(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    now: datetime,
    extend_seconds: int = 30,
) -> datetime:
    """Extend only the currently owned lease, by no more than 60 seconds."""
    now = _utc(now, field="now")
    if not 1 <= extend_seconds <= 60:
        raise ValueError("extend_seconds must be between 1 and 60")
    _, lease, _ = await _lock_active_claim(session, claim=claim, now=now)
    if now < lease.heartbeat_at:
        raise LeaseLostError("job lease heartbeat cannot move backwards")
    lease.heartbeat_at = now
    lease.expires_at = max(
        lease.expires_at,
        now + timedelta(seconds=extend_seconds),
    )
    await session.flush()
    return lease.expires_at


async def checkpoint_job(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    now: datetime,
    apply: JobApply,
    extend_seconds: int = 30,
) -> datetime:
    """Apply one short business checkpoint under the current fenced lease."""
    now = _utc(now, field="now")
    if not 1 <= extend_seconds <= 60:
        raise ValueError("extend_seconds must be between 1 and 60")
    job, lease, attempt = await _lock_active_claim(session, claim=claim, now=now)
    if now < lease.heartbeat_at:
        raise LeaseLostError("job lease heartbeat cannot move backwards")
    await apply(session)
    if (
        job.status != JobStatus.RUNNING.value
        or lease.lease_token != claim.lease_token
        or lease.fence_token != claim.fence_token
        or lease.worker_id != claim.worker_id
        or attempt.outcome != "running"
    ):
        raise LeaseLostError("job lease changed during checkpoint")
    lease.heartbeat_at = now
    lease.expires_at = max(lease.expires_at, now + timedelta(seconds=extend_seconds))
    await session.flush()
    return lease.expires_at


async def finalize_job(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    now: datetime,
    outcome: TerminalOutcome,
    result_code: str,
    apply: JobApply | None = None,
) -> Job:
    """Validate fencing, apply business facts, and terminalize in one transaction."""
    now = _utc(now, field="now")

    async def execute(target_session: AsyncSession) -> JobCompletion:
        if apply is not None:
            await apply(target_session)
        return JobCompletion(
            outcome=outcome,
            result_code=result_code,
            finished_at=now,
        )

    return await execute_and_finalize_job(
        session,
        claim=claim,
        checked_at=now,
        execute=execute,
    )


async def execute_and_finalize_job(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    checked_at: datetime,
    execute: JobExecution,
) -> Job:
    """Run one fenced transactional handler and persist only its safe result."""
    checked_at = _utc(checked_at, field="checked_at")
    job, lease, attempt = await _lock_active_claim(
        session, claim=claim, now=checked_at
    )
    completion = await execute(session)
    finished_at = _utc(completion.finished_at, field="finished_at")
    if finished_at < checked_at:
        raise ValueError("finished_at precedes the fenced execution check")
    if completion.result_code is not None and not 1 <= len(completion.result_code) <= 100:
        raise ValueError("result_code is invalid")
    if completion.outcome == JobStatus.FAILED.value:
        if completion.error_code is None:
            raise ValueError("failed completion requires error_code")
        _error_code(completion.error_code)
    elif completion.error_code is not None:
        raise ValueError("successful completion cannot include error_code")
    if (
        lease.lease_token != claim.lease_token
        or lease.fence_token != claim.fence_token
        or finished_at >= lease.expires_at
        or attempt.outcome != "running"
    ):
        raise LeaseLostError("job lease expired during handler execution")
    attempt.outcome = completion.outcome
    attempt.finished_at = finished_at
    attempt.error_code = completion.error_code
    job.status = completion.outcome
    job.finished_at = finished_at
    job.last_error_code = completion.error_code
    job.result_code = completion.result_code
    job.version += 1
    await session.flush()
    return job


async def fail_job(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    now: datetime,
    error_code: str,
    retry_delay_seconds: int = 0,
) -> str:
    """Record one failed attempt and safely requeue or terminalize its Job."""
    now = _utc(now, field="now")
    error_code = _error_code(error_code)
    if not 0 <= retry_delay_seconds <= 3_600:
        raise ValueError("retry_delay_seconds is invalid")
    job, _, attempt = await _lock_active_claim(session, claim=claim, now=now)
    attempt.outcome = "failed"
    attempt.finished_at = now
    attempt.error_code = error_code
    if (
        job.retry_policy == RetryPolicy.AUTOMATIC_SAFE.value
        and job.attempt_count < job.max_attempts
    ):
        status = JobStatus.QUEUED.value
        job.started_at = None
        job.finished_at = None
        job.available_at = now + timedelta(seconds=retry_delay_seconds)
    elif job.attempt_count >= job.max_attempts:
        status = JobStatus.DEAD_LETTER.value
        job.finished_at = now
    else:
        status = JobStatus.FAILED.value
        job.finished_at = now
    job.status = status
    job.last_error_code = error_code
    job.result_code = None
    job.version += 1
    await session.flush()
    return status


async def dead_letter_job(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    now: datetime,
    error_code: str,
) -> Job:
    """Terminalize an unsafe or unregistered claim without running its payload."""
    now = _utc(now, field="now")
    error_code = _error_code(error_code)
    job, _, attempt = await _lock_active_claim(session, claim=claim, now=now)
    attempt.outcome = "failed"
    attempt.finished_at = now
    attempt.error_code = error_code
    job.status = JobStatus.DEAD_LETTER.value
    job.finished_at = now
    job.last_error_code = error_code
    job.result_code = None
    job.version += 1
    await session.flush()
    return job


async def reclaim_expired_jobs(
    session: AsyncSession,
    *,
    queue: QueueName,
    now: datetime,
    limit: int = 100,
    job_ids: tuple[UUID, ...] | None = None,
) -> int:
    """Fence expired attempts and requeue only jobs with remaining attempts."""
    now = _utc(now, field="now")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    if job_ids is not None and not job_ids:
        return 0
    conditions = [
        Job.queue == queue.value,
        Job.status == JobStatus.RUNNING.value,
        JobLease.expires_at <= now,
    ]
    if job_ids is not None:
        conditions.append(Job.id.in_(job_ids))
    rows = tuple(
        (
            await session.execute(
                select(Job, JobLease)
                .join(JobLease, JobLease.job_id == Job.id)
                .where(*conditions)
                .order_by(JobLease.expires_at, Job.id)
                .with_for_update(skip_locked=True)
                .limit(limit)
            )
        ).all()
    )
    for job, lease in rows:
        attempt = await session.scalar(
            select(JobAttempt)
            .where(
                JobAttempt.job_id == job.id,
                JobAttempt.lease_token == lease.lease_token,
                JobAttempt.outcome == "running",
            )
            .with_for_update()
        )
        if attempt is None:
            raise RuntimeError("running Job has no current attempt authority")
        attempt.outcome = "lost"
        attempt.finished_at = now
        attempt.error_code = "LEASE_EXPIRED"
        if job.attempt_count < job.max_attempts:
            job.status = JobStatus.QUEUED.value
            job.started_at = None
            job.finished_at = None
            job.available_at = now
        else:
            job.status = JobStatus.DEAD_LETTER.value
            job.finished_at = now
        job.last_error_code = "LEASE_EXPIRED"
        job.result_code = None
        job.version += 1
    await session.flush()
    return len(rows)


async def upsert_worker_heartbeat(
    session: AsyncSession,
    *,
    queue: QueueName,
    worker_id: str,
    process_version: str,
    migration_head: str,
    started_at: datetime,
    seen_at: datetime,
) -> WorkerHeartbeat:
    """Persist one monotonic, non-identifying worker liveness projection."""
    worker_id = _worker_id(worker_id)
    started_at = _utc(started_at, field="started_at")
    seen_at = _utc(seen_at, field="seen_at")
    if seen_at < started_at:
        raise ValueError("seen_at precedes started_at")
    if not 1 <= len(process_version) <= 64 or not 1 <= len(migration_head) <= 64:
        raise ValueError("worker version authority is invalid")
    candidate_id = new_uuid7()
    heartbeat_id = await session.scalar(
        insert(WorkerHeartbeat)
        .values(
            id=candidate_id,
            queue=queue.value,
            worker_id=worker_id,
            process_version=process_version,
            migration_head=migration_head,
            started_at=started_at,
            seen_at=seen_at,
        )
        .on_conflict_do_update(
            index_elements=[WorkerHeartbeat.queue, WorkerHeartbeat.worker_id],
            set_={
                "process_version": process_version,
                "migration_head": migration_head,
                "seen_at": seen_at,
            },
            where=and_(
                WorkerHeartbeat.started_at == started_at,
                WorkerHeartbeat.seen_at <= seen_at,
            ),
        )
        .returning(WorkerHeartbeat.id)
    )
    if heartbeat_id is None:
        raise ValueError("worker heartbeat cannot move backwards")
    heartbeat = await session.get(WorkerHeartbeat, heartbeat_id)
    if heartbeat is None or heartbeat.started_at != started_at:
        raise JobConflictError("worker identity was reused with another start time")
    await session.flush()
    return heartbeat

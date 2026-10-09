"""One-tick generic worker with strict registry and fenced finalization."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from math import ceil
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.jobs.errors import RetryableJobError, UnsafeJobError
from nexus.jobs.models import QueueName
from nexus.jobs.registry import (
    JobHandlerContext,
    JobHandlerSpec,
    JobRegistry,
    job_payload_is_safe,
)
from nexus.jobs.repository import (
    ClaimedJob,
    JobCompletion,
    LeaseLostError,
    LockedJobGuard,
    claim_next_job,
    dead_letter_job,
    execute_and_finalize_job,
    fail_job,
    reclaim_expired_jobs,
    upsert_worker_heartbeat,
)

Clock = Callable[[], datetime]
PreclaimGuard = Callable[[AsyncSession], Awaitable[None]]


def _authority_matches(claim: ClaimedJob, spec: JobHandlerSpec) -> bool:
    return (
        claim.schema_version == spec.schema_version
        and claim.queue == spec.queue.value
        and claim.priority == spec.priority
        and claim.retry_policy == spec.retry_policy.value
        and claim.max_attempts == spec.max_attempts
    )


async def _dead_letter(
    factory: async_sessionmaker[AsyncSession],
    *,
    claim: ClaimedJob,
    now: datetime,
    error_code: str,
) -> None:
    try:
        async with factory() as session, session.begin():
            await dead_letter_job(
                session,
                claim=claim,
                now=now,
                error_code=error_code,
            )
    except LeaseLostError:
        return


async def _fail(
    factory: async_sessionmaker[AsyncSession],
    *,
    claim: ClaimedJob,
    now: datetime,
    error_code: str,
) -> None:
    try:
        async with factory() as session, session.begin():
            await fail_job(
                session,
                claim=claim,
                now=now,
                error_code=error_code,
                retry_delay_seconds=min(2**claim.attempt_no, 60),
            )
    except LeaseLostError:
        return


async def run_job_worker_once(
    factory: async_sessionmaker[AsyncSession],
    *,
    registry: JobRegistry,
    queue: QueueName,
    worker_id: str,
    worker_started_at: datetime,
    process_version: str,
    migration_head: str,
    clock: Clock,
    claim_registered_only: bool = False,
    preclaim_guard: PreclaimGuard | None = None,
    job_ids: tuple[UUID, ...] | None = None,
    locked_job_guard: LockedJobGuard | None = None,
) -> bool:
    """Heartbeat, reclaim, claim, and terminalize at most one durable Job."""
    # Freeze one scope for both operations, including when an untyped caller
    # supplied a mutable sequence. None retains the existing queue-wide mode.
    job_ids = None if job_ids is None else tuple(job_ids)
    if locked_job_guard is not None and (job_ids is None or len(job_ids) != 1):
        raise ValueError("LOCKED_JOB_GUARD_REQUIRES_EXACT_SCOPE")
    profiles = {spec.research_budget_profile for spec in registry.specs}
    research_budget_profile = next(iter(profiles)) if len(profiles) == 1 else None
    if any(profile is not None for profile in profiles) and (
        len(registry.specs) != 1
        or research_budget_profile is None
        or locked_job_guard is None
        or job_ids is None
        or len(job_ids) != 1
    ):
        raise ValueError("RESEARCH_BUDGET_PROFILE_REQUIRES_LOCKED_SCOPE")
    claim_time = clock()
    # Preserve the normal 60s lease; a bounded research handler needs its full
    # runtime plus the same 15s margin before another worker may reclaim it.
    lease_seconds = max(
        60,
        ceil(
            max(
                (spec.timeout_seconds for spec in registry.specs if spec.queue is queue),
                default=45,
            )
        )
        + 15,
    )
    async with factory() as session, session.begin():
        if preclaim_guard is not None:
            await preclaim_guard(session)
        await upsert_worker_heartbeat(
            session,
            queue=queue,
            worker_id=worker_id,
            process_version=process_version,
            migration_head=migration_head,
            started_at=worker_started_at,
            seen_at=claim_time,
        )
        if locked_job_guard is None:
            await reclaim_expired_jobs(session, queue=queue, now=claim_time, job_ids=job_ids)
        # Qualification is a first-claim path, not a recovery operation. Do not
        # mutate an existing expired attempt before its authority is qualified.
        # Established recovery callers retain the original unguarded behavior.
        claim = await claim_next_job(
            session,
            queue=queue,
            worker_id=worker_id,
            now=claim_time,
            lease_seconds=lease_seconds,
            job_types=(tuple(sorted(registry.job_types)) if claim_registered_only else None),
            job_ids=job_ids,
            locked_job_guard=locked_job_guard,
            research_budget_profile=research_budget_profile,
        )
    if claim is None:
        return False
    if not claim.dependencies_satisfied:
        try:
            async with factory() as session, session.begin():
                await execute_and_finalize_job(
                    session,
                    claim=claim,
                    checked_at=clock(),
                    execute=lambda _: _dependency_skip(claim, clock),
                )
        except LeaseLostError:
            pass
        return True
    spec = registry.resolve(claim.job_type)
    if spec is None:
        await _dead_letter(
            factory,
            claim=claim,
            now=clock(),
            error_code="HANDLER_UNREGISTERED",
        )
        return True
    if not _authority_matches(claim, spec):
        await _dead_letter(
            factory,
            claim=claim,
            now=clock(),
            error_code="HANDLER_AUTHORITY_MISMATCH",
        )
        return True
    if not job_payload_is_safe(claim.payload):
        await _dead_letter(
            factory,
            claim=claim,
            now=clock(),
            error_code="HANDLER_PAYLOAD_UNSAFE",
        )
        return True
    try:
        payload = spec.payload_model.model_validate_json(
            json.dumps(claim.payload, separators=(",", ":")),
            strict=True,
        )
    except ValidationError:
        await _dead_letter(
            factory,
            claim=claim,
            now=clock(),
            error_code="HANDLER_PAYLOAD_INVALID",
        )
        return True

    loop = asyncio.get_running_loop()
    handler_deadline = loop.time() + spec.timeout_seconds
    try:
        async with asyncio.timeout_at(handler_deadline):
            async with factory() as session, session.begin():

                async def execute(target_session: AsyncSession) -> JobCompletion:
                    context = JobHandlerContext(
                        session=target_session,
                        claim=claim,
                        payload=payload,
                        _clock=clock,
                        _monotonic_deadline=handler_deadline,
                    )
                    result = await spec.handler(context)
                    return JobCompletion(
                        outcome=result.outcome,
                        result_code=result.result_code,
                        finished_at=clock(),
                        error_code=result.error_code,
                    )

                await execute_and_finalize_job(
                    session,
                    claim=claim,
                    checked_at=clock(),
                    execute=execute,
                )
    except TimeoutError:
        await _fail(
            factory,
            claim=claim,
            now=clock(),
            error_code="HANDLER_TIMEOUT",
        )
    except RetryableJobError as error:
        await _fail(
            factory,
            claim=claim,
            now=clock(),
            error_code=error.error_code,
        )
    except UnsafeJobError as error:
        await _dead_letter(
            factory,
            claim=claim,
            now=clock(),
            error_code=error.error_code,
        )
    except LeaseLostError:
        pass
    except Exception:
        await _fail(
            factory,
            claim=claim,
            now=clock(),
            error_code="HANDLER_FAILED",
        )
    return True


async def _dependency_skip(claim: ClaimedJob, clock: Clock) -> JobCompletion:
    return JobCompletion(
        outcome="skipped",
        result_code=claim.dependency_result_code or "DEPENDENCY_NOT_SATISFIED",
        finished_at=clock(),
    )

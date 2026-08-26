"""One-tick generic worker with strict registry and fenced finalization."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
    claim_next_job,
    dead_letter_job,
    execute_and_finalize_job,
    fail_job,
    reclaim_expired_jobs,
    upsert_worker_heartbeat,
)

Clock = Callable[[], datetime]


class RetryableJobError(RuntimeError):
    """A handler failed with a sanitized, registry-approved retryable code."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class UnsafeJobError(RuntimeError):
    """A handler detected an input or capability that must never be retried."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


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
) -> bool:
    """Heartbeat, reclaim, claim, and terminalize at most one durable Job."""
    claim_time = clock()
    async with factory() as session, session.begin():
        await upsert_worker_heartbeat(
            session,
            queue=queue,
            worker_id=worker_id,
            process_version=process_version,
            migration_head=migration_head,
            started_at=worker_started_at,
            seen_at=claim_time,
        )
        await reclaim_expired_jobs(session, queue=queue, now=claim_time)
        claim = await claim_next_job(
            session,
            queue=queue,
            worker_id=worker_id,
            now=claim_time,
            lease_seconds=60,
            job_types=(
                tuple(sorted(registry.job_types)) if claim_registered_only else None
            ),
        )
    if claim is None:
        return False
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
        payload = spec.payload_model.model_validate(claim.payload, strict=True)
    except ValidationError:
        await _dead_letter(
            factory,
            claim=claim,
            now=clock(),
            error_code="HANDLER_PAYLOAD_INVALID",
        )
        return True

    try:
        async with asyncio.timeout(spec.timeout_seconds):
            async with factory() as session, session.begin():

                async def execute(target_session: AsyncSession) -> JobCompletion:
                    context = JobHandlerContext(
                        session=target_session,
                        claim=claim,
                        payload=payload,
                        _clock=clock,
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

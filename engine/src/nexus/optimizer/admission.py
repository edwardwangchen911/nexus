"""Transaction-safe queued optimization-job admission."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.jobs.models import JobKeyV1, JobType, canonical_job_key
from nexus.persistence.models.runtime import Job
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

_SCHEMA_VERSION = "optimization_job_v1"


@dataclass(frozen=True)
class QueuedOptimizationJob:
    job_id: UUID
    replayed: bool


async def admit_queued_job(
    session: AsyncSession,
    *,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    payload: dict[str, str],
    portfolio_id: UUID,
    effective_at: datetime,
    after_authority: Callable[[], Awaitable[None]] | None = None,
) -> QueuedOptimizationJob:
    """Claim first, then atomically create one queued job and completed replay response."""
    if after_authority is not None:
        await after_authority()
    record, owner = await claim_or_lock(
        session, actor_id, route, idempotency_key, request_hash, _SCHEMA_VERSION
    )
    if not owner:
        if record.request_hash != request_hash:
            raise IdempotencyConflict("idempotency key body hash conflicts with prior request")
        raw_job_id = record.response_payload.get("job_id")
        if not isinstance(raw_job_id, str):
            raise RuntimeError("optimization idempotency response is incomplete")
        return QueuedOptimizationJob(job_id=UUID(raw_job_id), replayed=True)
    job_type = JobType.OPTIMIZATION_PROPOSE
    job_key = canonical_job_key(
        JobKeyV1(
            job_type=job_type,
            scope_kind="portfolio",
            scope_id=str(portfolio_id),
            effective_at=effective_at,
            input_hash=request_hash,
        )
    )
    candidate_id = new_uuid7()
    job_id = await session.scalar(
        insert(Job)
        .values(
            id=candidate_id,
            job_key=job_key,
            input_hash=request_hash,
            status="queued",
            schema_version=_SCHEMA_VERSION,
            payload=payload,
            job_type=job_type.value,
            queue="general",
            scope_kind="portfolio",
            scope_id=str(portfolio_id),
            effective_at=effective_at,
            priority=180,
            retry_policy="manual_safe",
            max_attempts=3,
            attempt_count=0,
            manual_retry_count=0,
            available_at=effective_at,
            started_at=None,
            finished_at=None,
            last_error_code=None,
            result_code=None,
            version=1,
        )
        .on_conflict_do_nothing(index_elements=[Job.job_key])
        .returning(Job.id)
    )
    replayed_job = job_id is None
    if job_id is None:
        job_id = await session.scalar(select(Job.id).where(Job.job_key == job_key))
    if job_id is None:
        raise RuntimeError("optimization durable job conflict could not be resolved")
    record.response_payload = {"job_id": str(job_id), "status": "QUEUED"}
    await session.flush()
    return QueuedOptimizationJob(job_id=job_id, replayed=replayed_job)

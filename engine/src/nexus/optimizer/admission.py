"""Transaction-safe queued optimization-job admission."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

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
    job = Job(
        job_key=f"optimization-proposal:{actor_id}:{idempotency_key}",
        input_hash=request_hash,
        status="QUEUED",
        schema_version="1",
        payload=payload,
    )
    session.add(job)
    await session.flush()
    record.response_payload = {"job_id": str(job.id), "status": "QUEUED"}
    await session.flush()
    return QueuedOptimizationJob(job_id=job.id, replayed=False)

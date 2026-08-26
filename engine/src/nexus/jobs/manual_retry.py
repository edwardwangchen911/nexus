"""Versioned and idempotent manual-safe Job recovery."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.risk_audit import JobRetryResponse
from nexus.audit.canonical import canonical_json
from nexus.audit.service import append_audit_event
from nexus.persistence.idempotency import IdempotencyClaimError, claim_idempotency
from nexus.persistence.models.runtime import Job


class ManualRetryError(RuntimeError):
    """A stable closed reason for refusing a manual Job retry."""


async def retry_manual_safe_job(
    session: AsyncSession,
    *,
    job_id: UUID,
    actor_id: str,
    expected_version: int,
    idempotency_key: str,
    now: datetime,
) -> JobRetryResponse:
    """Requeue one immutable manual-safe Job using CAS and durable replay."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("manual retry time must use UTC")
    if expected_version < 1 or not 1 <= len(idempotency_key) <= 255:
        raise ValueError("manual retry precondition is invalid")
    route = f"/internal/v1/jobs/{job_id}/retry"
    request_hash = hashlib.sha256(
        canonical_json(
            {
                "expected_version": expected_version,
                "job_id": str(job_id),
                "schema": "job_manual_retry_request_v1",
            }
        ).encode()
    ).hexdigest()
    try:
        claim, created = await claim_idempotency(
            session,
            actor_id=actor_id,
            route=route,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            schema_version="job_manual_retry_response_v1",
        )
    except IdempotencyClaimError as error:
        raise ManualRetryError(str(error)) from error
    if not created:
        if not claim.response_payload:
            raise ManualRetryError("IDEMPOTENCY_INCOMPLETE")
        return JobRetryResponse.model_validate(claim.response_payload).model_copy(
            update={"replayed": True}
        )
    job = await session.scalar(select(Job).where(Job.id == job_id).with_for_update())
    if job is None:
        raise ManualRetryError("NOT_FOUND")
    if job.version != expected_version:
        raise ManualRetryError("VERSION_CONFLICT")
    if job.retry_policy != "manual_safe":
        raise ManualRetryError("RETRY_NOT_PERMITTED")
    if job.status not in {"failed", "dead_letter"}:
        raise ManualRetryError("JOB_NOT_RETRYABLE")
    if job.manual_retry_count >= 2:
        raise ManualRetryError("RETRY_LIMIT_REACHED")
    job.status = "queued"
    job.manual_retry_count += 1
    job.available_at = now
    job.started_at = None
    job.finished_at = None
    job.result_code = None
    job.version += 1
    result = JobRetryResponse(
        job_id=job.id,
        status="queued",
        version=job.version,
        manual_retry_count=job.manual_retry_count,
        replayed=False,
    )
    await append_audit_event(
        session,
        actor_id=f"user:{actor_id}",
        action="job.manual_retry.queued",
        schema_version="job_manual_retry_v1",
        payload={
            "manual_retry_count": job.manual_retry_count,
            "retryable": True,
            "status": "queued",
        },
        occurred_at=now,
    )
    claim.response_payload = result.model_dump(mode="json")
    await session.flush()
    return result

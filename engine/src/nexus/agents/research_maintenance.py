"""Build-fixed maintenance behavior for the schema-compatible rollback image."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.reconciliation.admission_claim import claim_or_lock, validate_request_identity

# Docker fixes this value into R; runtime environment settings cannot disable it.
IMAGE_MAINTENANCE = False


def require_research_open() -> None:
    if IMAGE_MAINTENANCE:
        raise ValueError("RESEARCH_MAINTENANCE")


async def claim_research_identity(
    session: AsyncSession,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    schema_version: str,
) -> tuple[IdempotencyRecord, bool]:
    """Keep exact replay without creating even an uncommitted identity in R."""
    if not IMAGE_MAINTENANCE:
        return await claim_or_lock(
            session, actor_id, route, idempotency_key, request_hash, schema_version
        )
    validate_request_identity(actor_id, route, idempotency_key, request_hash)
    record = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.actor_id == actor_id,
            IdempotencyRecord.route == route,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        .with_for_update()
    )
    if record is None or not record.response_payload:
        raise ValueError("RESEARCH_MAINTENANCE")
    return record, False

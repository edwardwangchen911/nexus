"""Shared transactional claim primitive for internal API idempotency."""

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.runtime import IdempotencyRecord


class IdempotencyClaimError(RuntimeError):
    """The same scoped key names a different request or incomplete result."""


async def claim_idempotency(
    session: AsyncSession,
    *,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    schema_version: str,
) -> tuple[IdempotencyRecord, bool]:
    """Insert or lock one actor/route/key claim and bind it to a request hash."""
    inserted_id = await session.scalar(
        insert(IdempotencyRecord)
        .values(
            actor_id=actor_id,
            route=route,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            schema_version=schema_version,
            response_payload={},
        )
        .on_conflict_do_nothing(
            index_elements=[
                IdempotencyRecord.actor_id,
                IdempotencyRecord.route,
                IdempotencyRecord.idempotency_key,
            ]
        )
        .returning(IdempotencyRecord.id)
    )
    claim = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.actor_id == actor_id,
            IdempotencyRecord.route == route,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        .with_for_update()
    )
    if claim is None or claim.request_hash != request_hash:
        raise IdempotencyClaimError("IDEMPOTENCY_CONFLICT")
    return claim, inserted_id is not None

"""Transaction-level claim for globally idempotent broker sync admission."""

import re
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.portfolio import BrokerSyncRun
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.reconciliation.persistence_errors import (
    BrokerSyncPersistenceError,
    IdempotencyConflict,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDEMPOTENCY_SCHEMA_VERSION = "broker_sync_run_v1"


async def claim_or_replay(
    session: AsyncSession,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
) -> tuple[IdempotencyRecord, BrokerSyncRun | None]:
    record, inserted = await claim_or_lock(
        session, actor_id, route, idempotency_key, request_hash, _IDEMPOTENCY_SCHEMA_VERSION
    )
    if inserted:
        return record, None
    if record.request_hash != request_hash:
        raise IdempotencyConflict("idempotency key body hash conflicts with prior request")
    raw_run_id = record.response_payload.get("broker_sync_run_id")
    if record.schema_version != _IDEMPOTENCY_SCHEMA_VERSION or not isinstance(raw_run_id, str):
        raise BrokerSyncPersistenceError("broker idempotency record is invalid")
    try:
        run_id = UUID(raw_run_id)
    except ValueError:
        raise BrokerSyncPersistenceError("broker idempotency record is invalid") from None
    run = cast(BrokerSyncRun | None, await session.get(BrokerSyncRun, run_id))
    if run is None:
        raise BrokerSyncPersistenceError("broker idempotency record is invalid")
    return record, run


async def claim_or_lock(
    session: AsyncSession,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    schema_version: str,
) -> tuple[IdempotencyRecord, bool]:
    """Claim one idempotency row or lock the committed owner response for replay."""
    validate_request_identity(actor_id, route, idempotency_key, request_hash)
    claim_id = new_uuid7()
    inserted = await session.scalar(
        insert(IdempotencyRecord)
        .values(
            id=claim_id,
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
    record = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.actor_id == actor_id,
            IdempotencyRecord.route == route,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        .with_for_update()
    )
    if record is None:
        raise BrokerSyncPersistenceError("broker idempotency claim failed")
    return record, inserted is not None


def validate_request_identity(
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
) -> None:
    if not actor_id or len(actor_id) > 255:
        raise BrokerSyncPersistenceError("invalid actor identity")
    if not route or len(route) > 255:
        raise BrokerSyncPersistenceError("invalid route identity")
    if not idempotency_key or len(idempotency_key) > 255:
        raise BrokerSyncPersistenceError("invalid idempotency key")
    if _SHA256.fullmatch(request_hash) is None:
        raise BrokerSyncPersistenceError("request hash must be lowercase SHA-256")

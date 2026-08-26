"""Idempotent, non-executable proposal lifecycle admission."""

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.optimizer.repository import TargetStatus, transition_group
from nexus.persistence.models.runtime import OutboxEvent
from nexus.persistence.models.shadow import (
    ShadowCheckpointRecord,
    ShadowGroupRecord,
    ShadowSealRecord,
    ShadowSessionAuthorityRecord,
)
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.shadow.canonical import final_authority_content_hash
from nexus.shadow.repository import lock_proposal_group

_SCHEMA_VERSION = "optimization_lifecycle_v1"


@dataclass(frozen=True)
class LifecycleTransition:
    group_id: UUID
    status: str
    version: int
    replayed: bool


async def transition_group_idempotently(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    group_id: UUID,
    target: TargetStatus,
    expected_version: int,
) -> LifecycleTransition:
    """Claim one group lifecycle key, then CAS only the proposal group status/version."""
    record, owner = await claim_or_lock(
        session,
        actor_id,
        f"optimization-proposal-lifecycle:{group_id}",
        idempotency_key,
        request_hash,
        _SCHEMA_VERSION,
    )
    if not owner:
        if record.request_hash != request_hash:
            raise IdempotencyConflict(
                "idempotency key lifecycle request conflicts with prior request"
            )
        payload = record.response_payload
        raw_group_id = payload.get("proposal_group_id")
        status = payload.get("status")
        version = payload.get("version")
        if (
            raw_group_id != str(group_id)
            or not isinstance(status, str)
            or not isinstance(version, int)
        ):
            raise RuntimeError("optimization lifecycle idempotency response is incomplete")
        return LifecycleTransition(group_id=group_id, status=status, version=version, replayed=True)
    group = await transition_group(session, group_id, target, expected_version=expected_version)
    record.response_payload = {
        "proposal_group_id": str(group.id),
        "status": group.status,
        "version": group.version,
    }
    await session.flush()
    return LifecycleTransition(
        group_id=group.id, status=group.status, version=group.version, replayed=False
    )


async def reject_group_idempotently(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    group_id: UUID,
    expected_version: int,
) -> LifecycleTransition:
    """Seal an existing Shadow before the lifecycle CAS in the same transaction."""
    record, owner = await claim_or_lock(
        session,
        actor_id,
        f"optimization-proposal-lifecycle:{group_id}",
        idempotency_key,
        request_hash,
        _SCHEMA_VERSION,
    )
    if not owner:
        if record.request_hash != request_hash:
            raise IdempotencyConflict(
                "idempotency key lifecycle request conflicts with prior request"
            )
        return _replay_lifecycle(record.response_payload, group_id)

    group = await lock_proposal_group(session, group_id)
    if group is None or group.version != expected_version:
        raise ValueError("VERSION_CONFLICT")
    if group.status not in {"draft", "compared", "shadowing"}:
        raise ValueError("VALIDATION_ERROR")

    shadow_group: ShadowGroupRecord | None = None
    sealed_at: datetime | None = None
    if group.status == "shadowing":
        shadow_group = await session.scalar(
            select(ShadowGroupRecord)
            .where(ShadowGroupRecord.proposal_group_id == group.id)
            .with_for_update()
        )
        if shadow_group is None:
            raise ValueError("PROPOSAL_SHADOW_INCOMPLETE")
        sealed_at = await session.scalar(select(func.transaction_timestamp()))
        if not isinstance(sealed_at, datetime):
            raise RuntimeError("database transaction time is unavailable")
        sealed_at = sealed_at.astimezone(UTC)
        session_hashes = tuple(
            (
                authority.session,
                authority.content_hash,
            )
            for authority in await session.scalars(
                select(ShadowSessionAuthorityRecord)
                .where(
                    ShadowSessionAuthorityRecord.shadow_group_id == shadow_group.id
                )
                .order_by(ShadowSessionAuthorityRecord.session)
            )
        )
        checkpoint_hashes = tuple(
            (checkpoint.ordinal, checkpoint.metrics_hash)
            for checkpoint in await session.scalars(
                select(ShadowCheckpointRecord)
                .where(ShadowCheckpointRecord.shadow_group_id == shadow_group.id)
                .order_by(ShadowCheckpointRecord.ordinal)
            )
        )
        session.add(
            ShadowSealRecord(
                id=new_uuid7(),
                shadow_group_id=shadow_group.id,
                reason="rejected",
                reason_code="USER_REJECTED",
                sealed_at=sealed_at,
                final_coverage=len(session_hashes),
                final_authority_hash=final_authority_content_hash(
                    session_hashes,
                    checkpoint_hashes,
                ),
            )
        )
        await session.flush()

    group.status = "rejected"
    group.version = expected_version + 1
    await session.flush()
    record.response_payload = {
        "proposal_group_id": str(group.id),
        "status": group.status,
        "version": group.version,
    }
    if shadow_group is not None:
        if sealed_at is None:
            raise RuntimeError("database transaction time is unavailable")
        event_payload = {
            "proposal_group_id": str(group.id),
            "shadow_group_id": str(shadow_group.id),
            "version": group.version,
            "reason": "rejected",
        }
        await append_audit_event(
            session,
            actor_id=actor_id,
            action="shadow.rejected",
            schema_version="1",
            payload=event_payload,
            occurred_at=sealed_at,
        )
        session.add(
            OutboxEvent(
                event_type="shadow.rejected",
                aggregate_id=shadow_group.id,
                schema_version="1",
                payload=event_payload,
                published_at=None,
            )
        )
    await session.flush()
    return LifecycleTransition(
        group_id=group.id,
        status=group.status,
        version=group.version,
        replayed=False,
    )


def _replay_lifecycle(payload: dict[str, object], group_id: UUID) -> LifecycleTransition:
    raw_group_id = payload.get("proposal_group_id")
    status = payload.get("status")
    version = payload.get("version")
    if (
        raw_group_id != str(group_id)
        or not isinstance(status, str)
        or not isinstance(version, int)
    ):
        raise RuntimeError("optimization lifecycle idempotency response is incomplete")
    return LifecycleTransition(
        group_id=group_id,
        status=status,
        version=version,
        replayed=True,
    )

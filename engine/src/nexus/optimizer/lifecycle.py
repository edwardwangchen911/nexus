"""Idempotent, non-executable proposal lifecycle admission."""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.optimizer.repository import TargetStatus, transition_group
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

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

"""Idempotent production proposal-group construction without a worker/lease."""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.optimizer.models import OptimizationProposalGroup
from nexus.optimizer.persistence import (
    ProposalComparisonInput,
    ProposalPersistenceAuthority,
    persist_proposal_group,
)
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

_SCHEMA_VERSION = "optimization_group_v1"


@dataclass(frozen=True)
class PersistedProposalGroup:
    group_id: UUID
    replayed: bool


async def create_proposal_group_idempotently(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    group: OptimizationProposalGroup,
    authority: ProposalPersistenceAuthority,
    comparisons: tuple[ProposalComparisonInput, ProposalComparisonInput, ProposalComparisonInput],
) -> PersistedProposalGroup:
    """Write exactly one complete immutable group, or replay its committed identity."""
    record, owner = await claim_or_lock(
        session,
        actor_id,
        "optimization-proposal-group",
        idempotency_key,
        request_hash,
        _SCHEMA_VERSION,
    )
    if not owner:
        if record.request_hash != request_hash:
            raise IdempotencyConflict("idempotency key group request conflicts with prior request")
        raw_group_id = record.response_payload.get("proposal_group_id")
        if not isinstance(raw_group_id, str):
            raise RuntimeError("optimization group idempotency response is incomplete")
        group_id = UUID(raw_group_id)
        if await session.get(OptimizationProposalGroupRecord, group_id) is None:
            raise RuntimeError("optimization group idempotency response is invalid")
        return PersistedProposalGroup(group_id, replayed=True)
    record_group = await persist_proposal_group(session, group, authority, comparisons)
    record.response_payload = {"proposal_group_id": str(record_group.id)}
    await session.flush()
    return PersistedProposalGroup(record_group.id, replayed=False)

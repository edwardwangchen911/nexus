"""Database-enforced lifecycle for immutable optimization proposal children."""

from typing import Literal
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
)

TargetStatus = Literal["compared", "shadowing", "validated", "rejected", "expired"]
_ALLOWED = {
    "draft": {"compared", "rejected", "expired"},
    "compared": {"shadowing", "rejected", "expired"},
    "shadowing": {"validated", "rejected", "expired"},
    "validated": set(),
    "rejected": set(),
    "expired": set(),
}


async def transition_group(
    session: AsyncSession, group_id: UUID, target: TargetStatus, *, expected_version: int
) -> OptimizationProposalGroupRecord:
    """CAS transition a group only; proposal children and trading objects are never written."""
    group = await session.scalar(
        select(OptimizationProposalGroupRecord)
        .where(OptimizationProposalGroupRecord.id == group_id)
        .with_for_update()
    )
    if group is None or group.version != expected_version:
        raise ValueError("VERSION_CONFLICT")
    if target not in _ALLOWED[group.status]:
        raise ValueError("VALIDATION_ERROR")
    if target == "shadowing":
        strategy = await session.scalar(
            select(OptimizationProposalRecord).where(
                OptimizationProposalRecord.proposal_group_id == group_id,
                OptimizationProposalRecord.variant == "strategy_target",
            )
        )
        if (
            strategy is None
            or strategy.analysis_only
            or not strategy.strategy_evidence_complete
            or not group.baseline_ready
            or not group.diagnostic_complete
            or group.source_conflict
            or group.partial
            or group.requires_fx
            or not group.cost_complete
        ):
            raise ValueError("PROPOSAL_NOT_EXECUTABLE")
    result = await session.execute(
        update(OptimizationProposalGroupRecord)
        .where(
            OptimizationProposalGroupRecord.id == group_id,
            OptimizationProposalGroupRecord.version == expected_version,
        )
        .values(status=target, version=expected_version + 1)
        .returning(OptimizationProposalGroupRecord)
    )
    updated = result.scalar_one_or_none()
    if updated is None:
        raise ValueError("VERSION_CONFLICT")
    return updated

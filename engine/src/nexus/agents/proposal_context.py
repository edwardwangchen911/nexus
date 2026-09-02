"""Redacted, point-in-time comparison facts for the existing research snapshot."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.optimizer.compare import ComparableProposal, ComparisonDimensions, compare_proposals
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
)


class FrozenComparisonVariant(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    variant: Literal["hold_current", "minimum_change", "strategy_target"]
    dimensions: ComparisonDimensions
    non_executable: bool
    requires_fx: bool
    cost_complete: bool


class FrozenProposalComparison(BaseModel):
    """A saved comparison is evidence, never a recommendation or execution grant."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    proposal_group_id: UUID
    data_snapshot_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    optimizer_policy_id: UUID
    valuation_time: datetime
    frozen_at: datetime
    partial: bool
    source_conflict: bool
    authority_stale: bool
    diagnostic_complete: bool
    execution_locked: Literal[True] = True
    variants: tuple[FrozenComparisonVariant, ...] = Field(min_length=3, max_length=3)


def assess_frozen_comparison(comparison: FrozenProposalComparison) -> dict[str, object]:
    """Add a current read assessment; never rewrite the saved snapshot or its content hash."""
    result = compare_proposals(tuple(
        ComparableProposal(
            variant=row.variant, dimensions=row.dimensions,
            requires_fx=row.requires_fx, cost_complete=row.cost_complete,
            eligibility_complete=not row.non_executable,
            evidence_complete=comparison.diagnostic_complete,
            partial=comparison.partial, stale=comparison.authority_stale,
            source_conflict=comparison.source_conflict,
        )
        for row in comparison.variants
    ))
    return {
        "status": result.status, "recommendation": result.recommendation,
        "reasons": list(result.reasons),
    }


async def read_comparison_for_freeze(
    session: AsyncSession,
    *,
    portfolio_snapshot_id: UUID,
    diagnostic_report_id: UUID,
    diagnostic_input_hash: str,
    valuation_time: datetime,
    as_of: datetime,
) -> FrozenProposalComparison | None:
    """Read one existing same-case group; never construct or admit a proposal."""
    group = await session.scalar(
        select(OptimizationProposalGroupRecord)
        .join(DataSnapshot, DataSnapshot.id == OptimizationProposalGroupRecord.data_snapshot_id)
        .where(
            OptimizationProposalGroupRecord.baseline_portfolio_snapshot_id == portfolio_snapshot_id,
            OptimizationProposalGroupRecord.diagnostic_report_id == diagnostic_report_id,
            OptimizationProposalGroupRecord.diagnostic_input_hash == diagnostic_input_hash,
            OptimizationProposalGroupRecord.valuation_time == valuation_time,
            OptimizationProposalGroupRecord.portfolio_scope == "real_read_only",
            OptimizationProposalGroupRecord.created_at <= as_of,
            DataSnapshot.created_at <= as_of,
            DataSnapshot.available_at <= as_of,
            DataSnapshot.received_at <= as_of,
            or_(
                and_(DataSnapshot.available_at <= valuation_time,
                     DataSnapshot.received_at <= valuation_time),
                and_(
                    OptimizationProposalGroupRecord.partial.is_(True),
                    DataSnapshot.source == "nexus_optimizer_draft",
                    DataSnapshot.schema_version == "optimization_draft_inputs_v1",
                    DataSnapshot.payload["portfolio_snapshot_id"].astext
                    == str(portfolio_snapshot_id),
                    DataSnapshot.payload["diagnostic_report_id"].astext
                    == str(diagnostic_report_id),
                    DataSnapshot.payload["diagnostic_input_hash"].astext == diagnostic_input_hash,
                ),
            ),
        )
        .order_by(
            OptimizationProposalGroupRecord.created_at.desc(),
            OptimizationProposalGroupRecord.id.desc(),
        )
        .limit(1)
    )
    if group is None:
        return None
    rows = (await session.execute(
        select(OptimizationProposalRecord, ProposalComparisonRecord)
        .join(ProposalComparisonRecord,
              ProposalComparisonRecord.proposal_id == OptimizationProposalRecord.id)
        .where(
            OptimizationProposalRecord.proposal_group_id == group.id,
            OptimizationProposalRecord.baseline_portfolio_snapshot_id == portfolio_snapshot_id,
            OptimizationProposalRecord.portfolio_scope == "real_read_only",
            OptimizationProposalRecord.created_at <= as_of,
            ProposalComparisonRecord.created_at <= as_of,
        )
        .order_by(OptimizationProposalRecord.variant)
        .limit(4)
    )).all()
    if tuple(child.variant for child, _ in rows) != (
        "hold_current", "minimum_change", "strategy_target",
    ):
        return None
    if any(not isinstance(comparison.metrics, dict) for _, comparison in rows):
        return None
    try:
        return FrozenProposalComparison(
            proposal_group_id=group.id,
            data_snapshot_id=group.data_snapshot_id,
            source_policy_id=group.source_policy_id,
            cost_policy_id=group.cost_policy_id,
            optimizer_policy_id=group.policy_id,
            valuation_time=group.valuation_time,
            frozen_at=as_of,
            partial=group.partial,
            source_conflict=group.source_conflict,
            authority_stale=group.authority_stale,
            diagnostic_complete=group.diagnostic_complete,
            variants=tuple(FrozenComparisonVariant.model_validate({
                "variant": child.variant,
                "dimensions": comparison.metrics.get("dimensions"),
                "non_executable": comparison.non_executable or group.partial
                or group.source_conflict or group.authority_stale
                or not group.baseline_ready or not group.diagnostic_complete
                or not group.cost_complete or group.requires_fx,
                "requires_fx": comparison.requires_fx or group.requires_fx,
                "cost_complete": comparison.cost_complete and group.cost_complete,
            }) for child, comparison in rows),
        )
    except (ValidationError, TypeError, ValueError):
        return None

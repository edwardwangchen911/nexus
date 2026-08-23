"""Database-enforced lifecycle for immutable optimization proposal children."""

from typing import Literal
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.compare import ComparisonDimensions
from nexus.optimizer.cost_policy import COST_POLICY_ID, load_cost_policy_v1
from nexus.optimizer.selection_authority import load_selection_authority
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationPolicyRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
)
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.quality.source_policy import SOURCE_POLICY_ID, load_source_policy_v1

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
    if target == "shadowing" and not await _has_executable_shadow_authority(session, group):
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


async def _has_executable_shadow_authority(
    session: AsyncSession, group: OptimizationProposalGroupRecord
) -> bool:
    """Re-read all frozen authority rows before a non-executable shadow transition."""
    if (
        group.diagnostic_report_id is None
        or group.data_snapshot_id is None
        or group.source_policy_id is None
        or group.cost_policy_id is None
        or group.diagnostic_input_hash is None
        or group.valuation_time is None
        or not group.baseline_ready
        or not group.diagnostic_complete
        or group.source_conflict
        or group.partial
        or group.requires_fx
        or not group.cost_complete
        or group.authority_stale
    ):
        return False
    strategy = await session.scalar(
        select(OptimizationProposalRecord).where(
            OptimizationProposalRecord.proposal_group_id == group.id,
            OptimizationProposalRecord.variant == "strategy_target",
        )
    )
    snapshot = await session.get(PortfolioSnapshotRecord, group.baseline_portfolio_snapshot_id)
    report = await session.get(PortfolioDiagnosticReportRecord, group.diagnostic_report_id)
    data_snapshot = await session.get(DataSnapshot, group.data_snapshot_id)
    optimizer_policy = await session.get(OptimizationPolicyRecord, group.policy_id)
    comparisons = tuple(
        await session.scalars(
            select(ProposalComparisonRecord)
            .join(OptimizationProposalRecord)
            .where(OptimizationProposalRecord.proposal_group_id == group.id)
            .with_for_update()
        )
    )
    try:
        source_policy = await load_source_policy_v1(session, instrument_tick_size=None)
        cost_policy = await load_cost_policy_v1(session)
        selection = (
            None
            if group.selection_authority_id is None
            else await load_selection_authority(
                session,
                group.selection_authority_id,
                data_snapshot_id=group.data_snapshot_id,
                valuation_time=group.valuation_time,
            )
        )
    except ValueError:
        return False
    return bool(
        strategy is not None
        and selection is not None
        and strategy.portfolio_scope == group.portfolio_scope
        and (
            group.portfolio_scope == "real_read_only" or not strategy.analysis_only
        )
        and strategy.strategy_evidence_complete
        and snapshot is not None
        and snapshot.status == "READY"
        and report is not None
        and report.portfolio_id == snapshot.portfolio_id
        and report.portfolio_snapshot_id == snapshot.id
        and report.policy_id == REAL_ADVISORY_POLICY_ID
        and report.policy_version == "real_advisory_policy_v1"
        and report.optimization_need == "OPTIMIZATION_CANDIDATE"
        and report.diagnostic_input_hash == group.diagnostic_input_hash
        and report.snapshot_as_of == snapshot.as_of == group.valuation_time
        and data_snapshot is not None
        and data_snapshot.available_at <= group.valuation_time
        and data_snapshot.received_at <= group.valuation_time
        and source_policy.record_id == SOURCE_POLICY_ID == group.source_policy_id
        and source_policy.available_at <= group.valuation_time
        and cost_policy.id == COST_POLICY_ID == group.cost_policy_id
        and cost_policy.available_at <= group.valuation_time
        and optimizer_policy is not None
        and optimizer_policy.key == "optimizer"
        and optimizer_policy.version == "optimizer_v1"
        and len(comparisons) == 3
        and all(_comparison_is_executable(comparison, group) for comparison in comparisons)
    )


def _comparison_is_executable(
    comparison: ProposalComparisonRecord, group: OptimizationProposalGroupRecord
) -> bool:
    """Require every persisted comparison to retain complete executable authority."""
    metrics = comparison.metrics
    try:
        dimensions = ComparisonDimensions.model_validate(metrics.get("dimensions"))
    except (TypeError, ValueError):
        return False
    authority = metrics.get("authority")
    expected_authority = {
        "diagnostic_report_id": str(group.diagnostic_report_id),
        "data_snapshot_id": str(group.data_snapshot_id),
        "source_policy_id": str(group.source_policy_id),
        "optimizer_policy_id": str(group.policy_id),
        "cost_policy_id": str(group.cost_policy_id),
    }
    return bool(
        not comparison.non_executable
        and not comparison.requires_fx
        and comparison.cost_complete
        and metrics.get("ranking_eligible") is True
        and metrics.get("constraint_complete") is True
        and metrics.get("eligibility_complete") is True
        and metrics.get("evidence_complete") is True
        and metrics.get("stale") is False
        and metrics.get("unknown_metrics") == []
        and metrics.get("cost") is not None
        and dimensions.all_ranked_dimensions_known
        and authority == expected_authority
    )

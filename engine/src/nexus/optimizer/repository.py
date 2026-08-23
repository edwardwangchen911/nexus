"""Database-enforced lifecycle for immutable optimization proposal children."""

from typing import Literal
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.persistence.models.data import DataSnapshot, SourcePolicy
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationPolicyRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
)
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord

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
        or group.cost_model_version is None
        or group.diagnostic_input_hash is None
        or group.valuation_time is None
        or not group.baseline_ready
        or not group.diagnostic_complete
        or group.source_conflict
        or group.partial
        or group.requires_fx
        or not group.cost_complete
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
    source_policy = await session.get(SourcePolicy, group.source_policy_id)
    optimizer_policy = await session.get(OptimizationPolicyRecord, group.policy_id)
    return bool(
        strategy is not None
        and not strategy.analysis_only
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
        and source_policy is not None
        and optimizer_policy is not None
        and optimizer_policy.key == "optimizer"
        and optimizer_policy.version == "optimizer_v1"
    )

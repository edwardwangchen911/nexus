"""Load the sole canonical frozen authority for proposal creation."""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID, load_cost_policy_v1
from nexus.optimizer.persistence import ProposalPersistenceAuthority
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationPolicyRecord,
)
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.quality.source_policy import SOURCE_POLICY_ID, load_source_policy_v1


@dataclass(frozen=True)
class LoadedProposalAuthority:
    admission_authority_id: UUID
    portfolio_id: UUID
    persistence: ProposalPersistenceAuthority


async def load_admission_authority(
    session: AsyncSession,
    *,
    portfolio_snapshot_id: UUID,
    data_snapshot_id: UUID,
    cost_policy_id: UUID,
) -> LoadedProposalAuthority | None:
    """Return only an exact Task 11/data/source/cost/selection database binding."""
    snapshot = await session.get(PortfolioSnapshotRecord, portfolio_snapshot_id)
    authority = await session.scalar(
        select(OptimizationAdmissionAuthorityRecord).where(
            OptimizationAdmissionAuthorityRecord.portfolio_snapshot_id
            == portfolio_snapshot_id,
            OptimizationAdmissionAuthorityRecord.data_snapshot_id == data_snapshot_id,
            OptimizationAdmissionAuthorityRecord.cost_policy_id == cost_policy_id,
        )
    )
    if (
        snapshot is None
        or snapshot.status != "READY"
        or snapshot.as_of is None
        or authority is None
    ):
        return None
    report = await session.get(PortfolioDiagnosticReportRecord, authority.diagnostic_report_id)
    data_snapshot = await session.get(DataSnapshot, authority.data_snapshot_id)
    optimizer_policy = await session.get(OptimizationPolicyRecord, authority.optimizer_policy_id)
    try:
        source_policy = await load_source_policy_v1(session, instrument_tick_size=None)
        cost_policy = await load_cost_policy_v1(session)
    except ValueError:
        return None
    if (
        report is None
        or data_snapshot is None
        or optimizer_policy is None
        or report.portfolio_id != snapshot.portfolio_id
        or report.portfolio_snapshot_id != snapshot.id
        or report.policy_id != REAL_ADVISORY_POLICY_ID
        or report.policy_version != "real_advisory_policy_v1"
        or report.optimization_need != "OPTIMIZATION_CANDIDATE"
        or report.diagnostic_input_hash != authority.diagnostic_input_hash
        or report.snapshot_as_of != snapshot.as_of
        or authority.valuation_time != snapshot.as_of
        or data_snapshot.available_at > snapshot.as_of
        or data_snapshot.received_at > snapshot.as_of
        or optimizer_policy.key != "optimizer"
        or optimizer_policy.version != "optimizer_v1"
        or not authority.baseline_ready
        or not authority.diagnostic_complete
        or authority.source_conflict
        or authority.partial
        or authority.requires_fx
        or not authority.cost_complete
        or authority.authority_stale
        or source_policy.record_id != SOURCE_POLICY_ID
        or authority.source_policy_id != SOURCE_POLICY_ID
        or cost_policy.id != COST_POLICY_ID
        or authority.cost_policy_id != COST_POLICY_ID
        or cost_policy.available_at > snapshot.as_of
    ):
        return None
    return LoadedProposalAuthority(
        admission_authority_id=authority.id,
        portfolio_id=snapshot.portfolio_id,
        persistence=ProposalPersistenceAuthority(
            diagnostic_report_id=authority.diagnostic_report_id,
            data_snapshot_id=authority.data_snapshot_id,
            source_policy_id=authority.source_policy_id,
            optimizer_policy_id=authority.optimizer_policy_id,
            cost_policy_id=authority.cost_policy_id,
            diagnostic_input_hash=authority.diagnostic_input_hash,
            valuation_time=authority.valuation_time,
            baseline_ready=authority.baseline_ready,
            diagnostic_complete=authority.diagnostic_complete,
            source_conflict=authority.source_conflict,
            partial=authority.partial,
            requires_fx=authority.requires_fx,
            cost_complete=authority.cost_complete,
            authority_stale=authority.authority_stale,
            portfolio_scope=authority.portfolio_scope,
            selection_authority_id=authority.selection_authority_id,
        ),
    )

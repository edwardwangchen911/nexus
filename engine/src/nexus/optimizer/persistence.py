"""One transaction writer for immutable, complete three-way proposal groups."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.optimizer.models import OptimizationProposalGroup
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)


@dataclass(frozen=True)
class ProposalPersistenceAuthority:
    """Immutable IDs and completeness facts common to all three variants."""

    diagnostic_report_id: UUID
    data_snapshot_id: UUID
    source_policy_id: UUID
    optimizer_policy_id: UUID
    cost_policy_id: UUID
    diagnostic_input_hash: str
    valuation_time: datetime
    baseline_ready: bool
    diagnostic_complete: bool
    source_conflict: bool
    partial: bool
    requires_fx: bool
    cost_complete: bool
    authority_stale: bool = False
    portfolio_scope: str = "real_read_only"
    selection_authority_id: UUID | None = None

    def __post_init__(self) -> None:
        if len(self.diagnostic_input_hash) != 64:
            raise ValueError("diagnostic_input_hash must be a SHA-256 digest")


@dataclass(frozen=True)
class ProposalComparisonInput:
    """Persisted deterministic output; unknown values remain explicit and non-executable."""

    variant: str
    violation_count: int
    turnover: Decimal
    cost: Decimal | None
    unknown_metrics: tuple[str, ...]
    requires_fx: bool
    source_conflict: bool
    partial: bool
    cost_complete: bool
    constraint_complete: bool = True
    eligibility_complete: bool = True
    evidence_complete: bool = True
    stale: bool = False

    def __post_init__(self) -> None:
        if self.variant not in {"hold_current", "minimum_change", "strategy_target"}:
            raise ValueError("comparison variant is invalid")
        if self.violation_count < 0 or self.turnover < 0:
            raise ValueError("comparison values must be nonnegative")
        if any(not item for item in self.unknown_metrics):
            raise ValueError("unknown metrics must be named")

    @property
    def non_executable(self) -> bool:
        return (
            self.cost is None
            or bool(self.unknown_metrics)
            or self.requires_fx
            or self.source_conflict
            or self.partial
            or not self.cost_complete
            or not self.constraint_complete
            or not self.eligibility_complete
            or not self.evidence_complete
            or self.stale
        )

    def metrics_payload(self) -> dict[str, object]:
        return {
            "violation_count": self.violation_count,
            "turnover": str(self.turnover),
            "cost": None if self.cost is None else str(self.cost),
            "unknown_metrics": list(self.unknown_metrics),
            "constraint_complete": self.constraint_complete,
            "eligibility_complete": self.eligibility_complete,
            "evidence_complete": self.evidence_complete,
            "stale": self.stale,
            "ranking_eligible": not self.non_executable,
        }


async def persist_proposal_group(
    session: AsyncSession,
    group: OptimizationProposalGroup,
    authority: ProposalPersistenceAuthority,
    comparisons: tuple[ProposalComparisonInput, ProposalComparisonInput, ProposalComparisonInput],
) -> OptimizationProposalGroupRecord:
    """Stage group, its exact children, positions, and comparison rows atomically."""
    if tuple(item.variant for item in comparisons) != (
        "hold_current",
        "minimum_change",
        "strategy_target",
    ):
        raise ValueError("comparisons must contain exact ordered variants")
    if group.valuation_time != authority.valuation_time:
        raise ValueError("group valuation time must match its authority")
    if group.portfolio_scope != authority.portfolio_scope:
        raise ValueError("group portfolio scope must match its authority")
    record = OptimizationProposalGroupRecord(
        id=group.proposal_group_id,
        baseline_portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
        portfolio_scope=group.portfolio_scope,
        selection_authority_id=authority.selection_authority_id,
        status=group.status,
        version=group.version,
        policy_id=authority.optimizer_policy_id,
        diagnostic_report_id=authority.diagnostic_report_id,
        data_snapshot_id=authority.data_snapshot_id,
        source_policy_id=authority.source_policy_id,
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
    )
    session.add(record)
    for proposal, comparison in zip(group.proposals, comparisons, strict=True):
        child = OptimizationProposalRecord(
            id=proposal.proposal_id,
            proposal_group_id=record.id,
            variant=proposal.variant,
            baseline_portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
            analysis_only=proposal.analysis_only,
            strategy_evidence_complete=proposal.strategy_evidence_complete,
            portfolio_scope=group.portfolio_scope,
        )
        session.add(child)
        session.add_all(
            ProposalPositionRecord(
                proposal_id=child.id,
                instrument_id=position.instrument_id,
                current_weight=position.current_weight,
                target_weight=position.target_weight,
                action=position.action,
                reason_code=position.reason_code,
            )
            for position in proposal.positions
        )
        session.add(
            ProposalComparisonRecord(
                proposal_id=child.id,
                metrics=comparison.metrics_payload(),
                tax_impact="unknown",
                requires_fx=comparison.requires_fx,
                non_executable=comparison.non_executable,
                cost_complete=comparison.cost_complete,
            )
        )
    await session.flush()
    return record

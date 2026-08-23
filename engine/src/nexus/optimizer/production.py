"""Idempotent production proposal-group construction without a worker/lease."""

from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.diagnostics.repository import _read_report, load_frozen_snapshot
from nexus.optimizer.authority import load_admission_authority
from nexus.optimizer.minimum_change import ReductionCandidate
from nexus.optimizer.models import (
    OptimizationPolicy,
    OptimizationProposalGroup,
    build_proposal_group,
)
from nexus.optimizer.persistence import (
    ProposalComparisonInput,
    ProposalPersistenceAuthority,
    persist_proposal_group,
)
from nexus.optimizer.selection_authority import load_selection_authority
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
)
from nexus.providers.broker.base import PortfolioSnapshot
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.strategy.intents import ResearchIntent

_SCHEMA_VERSION = "optimization_group_v1"


@dataclass(frozen=True)
class PersistedProposalGroup:
    group_id: UUID
    replayed: bool


async def build_and_persist_proposal_group(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    admission_authority_id: UUID,
    research: tuple[ResearchIntent, ...],
) -> PersistedProposalGroup:
    """Load frozen inputs, compose real variants, then invoke the sole atomic writer."""
    admission = await session.get(
        OptimizationAdmissionAuthorityRecord, admission_authority_id
    )
    if admission is None:
        raise ValueError("optimization admission authority is missing")
    loaded = await load_admission_authority(
        session,
        portfolio_snapshot_id=admission.portfolio_snapshot_id,
        data_snapshot_id=admission.data_snapshot_id,
        cost_policy_id=admission.cost_policy_id,
    )
    if loaded is None or loaded.admission_authority_id != admission_authority_id:
        raise ValueError("optimization admission authority is invalid")
    snapshot = await load_frozen_snapshot(
        session,
        portfolio_id=loaded.portfolio_id,
        snapshot_id=admission.portfolio_snapshot_id,
    )
    report_record = await session.get(
        PortfolioDiagnosticReportRecord, loaded.persistence.diagnostic_report_id
    )
    if report_record is None:
        raise ValueError("optimization diagnostic authority is missing")
    report = await _read_report(session, report_record)
    selection = (
        None
        if loaded.persistence.selection_authority_id is None
        else await load_selection_authority(
            session,
            loaded.persistence.selection_authority_id,
            data_snapshot_id=loaded.persistence.data_snapshot_id,
            valuation_time=loaded.persistence.valuation_time,
        )
    )
    candidates, excess_weight = _minimum_change_inputs(snapshot, report)
    group = build_proposal_group(
        snapshot,
        report,
        research,
        OptimizationPolicy.v1(),
        selection_authority=selection,
        minimum_change_candidates=candidates,
        excess_weight=excess_weight,
        portfolio_scope=loaded.persistence.portfolio_scope,  # type: ignore[arg-type]
    ).model_copy(
        update={
            "diagnostic_input_hash": loaded.persistence.diagnostic_input_hash,
            "data_snapshot_id": loaded.persistence.data_snapshot_id,
            "source_policy_version": "source_policy_v1",
            "cost_model_version": "cost_policy_v1",
        }
    )
    comparisons = _comparison_inputs(group, report, excess_weight)
    return await create_proposal_group_idempotently(
        session,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        group=group,
        authority=loaded.persistence,
        comparisons=comparisons,
    )


def _minimum_change_inputs(
    snapshot: PortfolioSnapshot, report: PortfolioDiagnosticReport
) -> tuple[tuple[ReductionCandidate, ...], Decimal]:
    """Convert only typed reduce-only diagnostic excess into exact one-bp inputs."""
    violations = tuple(
        Decimal(item.metric_value) - Decimal(item.reference_limit)
        for item in report.items
        if item.actionability == "reduce_only"
        and item.metric_value is not None
        and item.reference_limit is not None
        and Decimal(item.metric_value) > Decimal(item.reference_limit)
    )
    if not violations:
        return (), Decimal("0")
    one_bp = Decimal("0.0001")
    excess = (max(violations) / one_bp).to_integral_value(rounding=ROUND_CEILING) * one_bp
    values = tuple(
        (position.instrument_id, position.quantity * (position.current_price or Decimal("0")))
        for position in snapshot.positions
    )
    total = sum((value for _, value in values), Decimal("0")) + sum(
        (cash.cash_balance for cash in snapshot.cash), Decimal("0")
    )
    if total <= 0:
        raise ValueError("minimum-change baseline value is unavailable")
    candidates = tuple(
        ReductionCandidate(
            instrument_id=instrument_id,
            current_weight=value / total,
            contribution=value / total,
            severity_rank=2,
            liquidity_score=Decimal("0"),
        )
        for instrument_id, value in values
    )
    return candidates, excess


def _comparison_inputs(
    group: OptimizationProposalGroup,
    report: PortfolioDiagnosticReport,
    excess_weight: Decimal,
) -> tuple[ProposalComparisonInput, ProposalComparisonInput, ProposalComparisonInput]:
    violations = sum(
        item.severity in {"warning", "critical"} for item in report.items
    )
    result = []
    for proposal in group.proposals:
        incomplete_strategy = (
            proposal.variant == "strategy_target"
            and not proposal.strategy_evidence_complete
        )
        result.append(
            ProposalComparisonInput(
                variant=proposal.variant,
                violation_count=max(
                    0,
                    violations
                    - int(
                        proposal.variant == "minimum_change" and excess_weight > 0
                    ),
                ),
                turnover=proposal.turnover,
                cost=Decimal("0") if not incomplete_strategy else None,
                unknown_metrics=("strategy_evidence",) if incomplete_strategy else (),
                requires_fx=False,
                source_conflict=False,
                partial=False,
                cost_complete=True,
                evidence_complete=not incomplete_strategy,
            )
        )
    return result[0], result[1], result[2]


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

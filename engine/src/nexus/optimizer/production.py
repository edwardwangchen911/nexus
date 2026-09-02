"""Idempotent production proposal-group construction without a worker/lease."""

from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.diagnostics.repository import _read_report, load_frozen_snapshot
from nexus.optimizer.authority import load_admission_authority
from nexus.optimizer.compare import evaluate_dimensions
from nexus.optimizer.cost_policy import load_cost_policy_v1
from nexus.optimizer.minimum_change import (
    MinimumChangeResult,
    ReductionCandidate,
    reduce_to_concentration_limits,
)
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


@dataclass(frozen=True)
class MinimumChangeInputs:
    candidates: tuple[ReductionCandidate, ...]
    excess_weight: Decimal
    evidence_complete: bool
    reduction: MinimumChangeResult | None = None


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
    cost_policy = await load_cost_policy_v1(session)
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
    minimum_inputs = _minimum_change_inputs(snapshot, report)
    group = build_proposal_group(
        snapshot,
        report,
        research,
        OptimizationPolicy.v1(),
        selection_authority=selection,
        minimum_change_candidates=minimum_inputs.candidates,
        excess_weight=minimum_inputs.excess_weight,
        minimum_change_result=minimum_inputs.reduction,
        portfolio_scope=loaded.persistence.portfolio_scope,  # type: ignore[arg-type]
    ).model_copy(
        update={
            "diagnostic_input_hash": loaded.persistence.diagnostic_input_hash,
            "data_snapshot_id": loaded.persistence.data_snapshot_id,
            "source_policy_version": "source_policy_v1",
            "cost_model_version": "cost_policy_v1",
        }
    )
    comparisons = _comparison_inputs(
        group, report, minimum_inputs, snapshot, cost_policy.parameters
    )
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
) -> MinimumChangeInputs:
    """Derive reductions only when Task 11 proves excess, contribution, and liquidity."""
    violations = tuple(item for item in report.items if item.actionability == "reduce_only")
    if not violations:
        return MinimumChangeInputs((), Decimal("0"), True)
    as_of = report.snapshot_as_of
    if as_of is None or any(
        item.metric_value is None
        or item.reference_limit is None
        or item.unit != "ratio"
        or Decimal(item.metric_value) <= Decimal(item.reference_limit)
        or not item.evidence_refs
        or any(ref.available_at > as_of for ref in item.evidence_refs)
        or item.diagnostic_code not in {"CONCENTRATION_TOP1", "CONCENTRATION_TOP3"}
        for item in violations
    ):
        return MinimumChangeInputs((), Decimal("0"), False)
    values = tuple(
        (position.instrument_id, position.quantity * (position.current_price or Decimal("0")))
        for position in snapshot.positions
    )
    total = sum((value for _, value in values), Decimal("0")) + sum(
        (cash.cash_balance for cash in snapshot.cash), Decimal("0")
    )
    if total <= 0:
        raise ValueError("minimum-change baseline value is unavailable")
    security_total = sum((value for _, value in values), Decimal("0"))
    if security_total <= 0:
        return MinimumChangeInputs((), Decimal("0"), False)
    currencies = {position.currency for position in snapshot.positions}
    if (
        len(currencies) != 1
        or any(cash.currency not in currencies and cash.cash_balance != 0 for cash in snapshot.cash)
        or any(item.policy_scope != f"currency:{next(iter(currencies))}" for item in violations)
    ):
        return MinimumChangeInputs((), Decimal("0"), False)
    ranked = tuple(sorted(values, key=lambda row: (-row[1], row[0])))
    contribution: dict[UUID, Decimal] = {instrument_id: Decimal("0") for instrument_id, _ in values}
    severity: dict[UUID, int] = {instrument_id: 0 for instrument_id, _ in values}
    ranks = {"info": 0, "warning": 1, "critical": 2}
    for violation in violations:
        contributor_count = 1 if violation.diagnostic_code == "CONCENTRATION_TOP1" else 3
        for instrument_id, value in ranked[:contributor_count]:
            contribution[instrument_id] = max(contribution[instrument_id], value / security_total)
            severity[instrument_id] = max(severity[instrument_id], ranks[violation.severity])
    liquidity: dict[UUID, Decimal] = {}
    for instrument_id, _ in values:
        matches = tuple(
            item for item in report.items
            if item.diagnostic_code == f"LIQUIDITY_DAYS_TO_EXIT:{instrument_id}"
        )
        if len(matches) != 1:
            return MinimumChangeInputs((), Decimal("0"), False)
        item = matches[0]
        if (
            item.metric_value is None
            or item.unknown_reason is not None
            or Decimal(item.metric_value) < 0
            or not item.evidence_refs
            or any(ref.available_at > as_of for ref in item.evidence_refs)
        ):
            return MinimumChangeInputs((), Decimal("0"), False)
        liquidity[instrument_id] = Decimal("1") / (Decimal("1") + Decimal(item.metric_value))
    candidates = tuple(
        ReductionCandidate(
            instrument_id=instrument_id,
            current_weight=value / total,
            contribution=contribution[instrument_id],
            severity_rank=severity[instrument_id],
            liquidity_score=liquidity[instrument_id],
        )
        for instrument_id, value in values
    )
    limits = tuple(
        (1 if item.diagnostic_code == "CONCENTRATION_TOP1" else 3, Decimal(item.reference_limit))
        for item in violations if item.reference_limit is not None
    )
    try:
        reduction = reduce_to_concentration_limits(candidates, limits)
    except ValueError:
        return MinimumChangeInputs((), Decimal("0"), False)
    return MinimumChangeInputs(candidates, reduction.cash_released, True, reduction)


def _comparison_inputs(
    group: OptimizationProposalGroup,
    report: PortfolioDiagnosticReport,
    minimum_inputs: MinimumChangeInputs,
    snapshot: PortfolioSnapshot,
    cost_parameters: dict[str, object],
) -> tuple[ProposalComparisonInput, ProposalComparisonInput, ProposalComparisonInput]:
    currencies = {position.instrument_id: position.currency for position in snapshot.positions}
    commission = Decimal(str(cost_parameters["commission_bps"]))
    slippage = Decimal(str(cost_parameters["slippage_bps"]))
    result = []
    for proposal in group.proposals:
        incomplete_strategy = (
            proposal.variant == "strategy_target"
            and not proposal.strategy_evidence_complete
        )
        incomplete_minimum = (
            proposal.variant == "minimum_change" and not minimum_inputs.evidence_complete
        )
        cost = proposal.turnover * (commission + slippage) / Decimal("10000")
        dimensions = evaluate_dimensions(
            proposal,
            report,
            cost=None if incomplete_strategy or incomplete_minimum else cost,
            instrument_currencies=currencies,
        )
        result.append(
            ProposalComparisonInput(
                variant=proposal.variant,
                violation_count=dimensions.hard_violations,
                turnover=proposal.turnover,
                cost=cost if not incomplete_strategy and not incomplete_minimum else None,
                unknown_metrics=dimensions.unknown_ranked_dimensions
                + (("strategy_evidence",) if incomplete_strategy else ())
                + (("minimum_change_evidence",) if incomplete_minimum else ()),
                requires_fx=False,
                source_conflict=False,
                partial=False,
                cost_complete=True,
                evidence_complete=not incomplete_strategy and not incomplete_minimum,
                dimensions=dimensions,
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

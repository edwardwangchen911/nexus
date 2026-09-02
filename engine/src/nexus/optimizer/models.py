# ruff: noqa: E501
"""Immutable three-way proposal models and pure construction entry point."""

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.providers.broker.base import PortfolioSnapshot

if TYPE_CHECKING:
    from nexus.optimizer.minimum_change import ReductionCandidate
    from nexus.optimizer.strategy_target import DeterministicSelectionAuthority
    from nexus.strategy.intents import ResearchIntent

ProposalVariant = Literal["hold_current", "minimum_change", "strategy_target"]
ProposalGroupStatus = Literal["draft", "compared", "shadowing", "validated", "rejected", "expired"]
ProposalAction = Literal["none", "reduce", "maintain", "increase"]
PortfolioScope = Literal["real_read_only", "internal_paper", "fosun_simulation"]
_ONE = Decimal("1")
_ZERO = Decimal("0")


class _StrictModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _reject_floats(cls, value: object) -> object:
        _reject_float(value)
        return value


def _reject_float(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("optimizer values must not be binary floats")
    if isinstance(value, Mapping):
        for nested in value.values():
            _reject_float(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _reject_float(nested)


class ProposalPosition(_StrictModel):
    instrument_id: UUID
    current_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    target_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    action: ProposalAction
    reason_code: str = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def _action_matches_weights(self) -> "ProposalPosition":
        expected = (
            "none"
            if self.current_weight == self.target_weight == _ZERO
            else "reduce"
            if self.target_weight < self.current_weight
            else "increase"
            if self.target_weight > self.current_weight
            else "maintain"
        )
        if self.action != expected:
            raise ValueError("proposal action must match the weight delta")
        return self


class OptimizationProposal(_StrictModel):
    proposal_id: UUID = Field(default_factory=new_uuid7)
    variant: ProposalVariant
    baseline_portfolio_snapshot_id: UUID
    positions: tuple[ProposalPosition, ...]
    cash_current_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    cash_target_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    analysis_only: bool
    strategy_evidence_complete: bool
    portfolio_scope: PortfolioScope = "real_read_only"

    @property
    def turnover(self) -> Decimal:
        return sum(
            (abs(position.target_weight - position.current_weight) for position in self.positions),
            _ZERO,
        ) / Decimal("2")

    @model_validator(mode="after")
    def _weights_are_balanced(self) -> "OptimizationProposal":
        if sum((item.target_weight for item in self.positions), self.cash_target_weight) != _ONE:
            raise ValueError("target weights must sum to one")
        if sum((item.current_weight for item in self.positions), self.cash_current_weight) != _ONE:
            raise ValueError("current weights must sum to one")
        if self.variant == "hold_current" and self.turnover != _ZERO:
            raise ValueError("hold_current must have zero turnover")
        return self


class OptimizationPolicy(_StrictModel):
    version: Literal["optimizer_v1"]
    minimum_step: Decimal = Decimal("0.0001")

    @classmethod
    def v1(cls) -> "OptimizationPolicy":
        return cls(version="optimizer_v1")


class ShadowAuthority(_StrictModel):
    """The complete frozen authority set required before a group can shadow."""

    baseline_ready: bool
    diagnostic_complete: bool
    source_conflict: bool
    partial: bool
    requires_fx: bool
    cost_complete: bool
    authority_stale: bool = False

    @property
    def executable(self) -> bool:
        return (
            self.baseline_ready
            and self.diagnostic_complete
            and not self.source_conflict
            and not self.partial
            and not self.requires_fx
            and self.cost_complete
            and not self.authority_stale
        )


class OptimizationProposalGroup(_StrictModel):
    proposal_group_id: UUID = Field(default_factory=new_uuid7)
    status: ProposalGroupStatus = "draft"
    version: int = Field(default=0, ge=0)
    baseline_portfolio_snapshot_id: UUID
    portfolio_scope: PortfolioScope = "real_read_only"
    proposals: tuple[OptimizationProposal, OptimizationProposal, OptimizationProposal]
    diagnostic_input_hash: str | None = None
    data_snapshot_id: UUID | None = None
    source_policy_version: str | None = None
    cost_model_version: str | None = None
    valuation_time: datetime | None = None
    shadow_authority: ShadowAuthority | None = None

    @field_validator("valuation_time")
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @model_validator(mode="after")
    def _three_variants_share_baseline(self) -> "OptimizationProposalGroup":
        variants = tuple(item.variant for item in self.proposals)
        if variants != ("hold_current", "minimum_change", "strategy_target"):
            raise ValueError("proposal group requires exactly ordered unique variants")
        if len({item.proposal_id for item in self.proposals}) != 3:
            raise ValueError("proposal group requires unique child identifiers")
        if any(
            item.baseline_portfolio_snapshot_id != self.baseline_portfolio_snapshot_id
            for item in self.proposals
        ):
            raise ValueError("proposal variants must share the group baseline")
        if any(item.portfolio_scope != self.portfolio_scope for item in self.proposals):
            raise ValueError("proposal variants must share the group portfolio scope")
        expected_analysis_only = self.portfolio_scope == "real_read_only"
        if any(item.analysis_only != expected_analysis_only for item in self.proposals):
            raise ValueError("proposal analysis_only must be derived from portfolio scope")
        return self


def build_proposal_group(
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    research: tuple["ResearchIntent", ...],
    policy: OptimizationPolicy,
    *,
    selection_authority: "DeterministicSelectionAuthority | None" = None,
    minimum_change_candidates: tuple["ReductionCandidate", ...] = (),
    excess_weight: Decimal = _ZERO,
    portfolio_scope: PortfolioScope = "real_read_only",
) -> OptimizationProposalGroup:
    """Compose three simultaneous deterministic variants without accepting Agent weights."""
    if report.optimization_need != "OPTIMIZATION_CANDIDATE":
        raise ValueError("diagnostic report is not an optimization candidate")
    return _compose_group(
        snapshot, report, research, policy,
        selection_authority=selection_authority,
        minimum_change_candidates=minimum_change_candidates,
        excess_weight=excess_weight, portfolio_scope=portfolio_scope,
    )


def build_initial_proposal_draft(
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    policy: OptimizationPolicy,
    *,
    minimum_change_candidates: tuple["ReductionCandidate", ...],
    excess_weight: Decimal,
) -> OptimizationProposalGroup:
    """Build a read-only scenario before formal admission, without rewriting diagnosis."""
    if report.optimization_need not in {"RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE"}:
        raise ValueError("diagnostic report does not support an initial draft")
    if not minimum_change_candidates or excess_weight <= _ZERO:
        raise ValueError("initial draft requires an evidenced reduction scenario")
    return _compose_group(
        snapshot, report, (), policy, selection_authority=None,
        minimum_change_candidates=minimum_change_candidates,
        excess_weight=excess_weight, portfolio_scope="real_read_only",
    )


def _compose_group(
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    research: tuple["ResearchIntent", ...],
    policy: OptimizationPolicy,
    *,
    selection_authority: "DeterministicSelectionAuthority | None",
    minimum_change_candidates: tuple["ReductionCandidate", ...],
    excess_weight: Decimal,
    portfolio_scope: PortfolioScope,
) -> OptimizationProposalGroup:
    if snapshot.status != "READY":
        raise ValueError("READY baseline is required")
    if report.portfolio_snapshot_id != snapshot.id:
        raise ValueError("diagnostic report must bind the baseline")
    if policy.version != "optimizer_v1":
        raise ValueError("unsupported optimization policy")
    current, cash_weight = _current_positions(snapshot)
    analysis_only = portfolio_scope == "real_read_only"
    hold = OptimizationProposal(
        variant="hold_current",
        baseline_portfolio_snapshot_id=snapshot.id,
        positions=current,
        cash_current_weight=cash_weight,
        cash_target_weight=cash_weight,
        analysis_only=analysis_only,
        strategy_evidence_complete=False,
        portfolio_scope=portfolio_scope,
    )
    minimum_positions = current
    minimum_cash = cash_weight
    if excess_weight > _ZERO:
        from nexus.optimizer.minimum_change import reduce_until_satisfied

        minimum_result = reduce_until_satisfied(
            minimum_change_candidates, excess_weight=excess_weight
        )
        if {item.instrument_id for item in minimum_result.positions} != {
            item.instrument_id for item in current
        }:
            raise ValueError("minimum-change candidates must exactly match baseline positions")
        minimum_positions = minimum_result.positions
        minimum_cash += minimum_result.cash_released
    minimum = OptimizationProposal(
        proposal_id=new_uuid7(),
        variant="minimum_change",
        baseline_portfolio_snapshot_id=snapshot.id,
        positions=minimum_positions,
        cash_current_weight=cash_weight,
        cash_target_weight=minimum_cash,
        analysis_only=analysis_only,
        strategy_evidence_complete=False,
        portfolio_scope=portfolio_scope,
    )
    current_weights = tuple((item.instrument_id, item.current_weight) for item in current)
    from nexus.optimizer.strategy_target import build_strategy_target

    strategy_result = build_strategy_target(current_weights, authority=selection_authority)
    effective_selection = selection_authority
    if research:
        if selection_authority is None:
            raise ValueError("research cannot create selection authority")
        if snapshot.as_of is None:
            raise ValueError("research requires a frozen valuation time")
        by_instrument = {intent.instrument_id: intent for intent in research}
        if len(by_instrument) != len(research):
            raise ValueError("research intents must be unique per instrument")
        if any(
            intent.data_snapshot_id != selection_authority.data_snapshot_id
            or intent.decision_time > snapshot.as_of
            or intent.valid_until < snapshot.as_of
            for intent in research
        ):
            raise ValueError("research intent does not bind the frozen authority")
        if any(
            intent.quality_status != "complete" or intent.evidence_audit != "complete"
            for intent in research
        ):
            effective_selection = None
            strategy_result = build_strategy_target(current_weights, authority=None)
        else:
            current_by_id = dict(current_weights)
            controlled_targets = tuple(
                (
                    instrument_id,
                    _ZERO
                    if (intent := by_instrument.get(instrument_id)) is not None
                    and intent.stance == "reduce_candidate"
                    else min(target, current_by_id.get(instrument_id, _ZERO))
                    if intent is not None
                    and (
                        intent.exposure_action != "eligible_for_increase"
                        or intent.stance == "watch"
                    )
                    else target,
                )
                for instrument_id, target in strategy_result.targets
            )
            strategy_result = strategy_result.__class__(
                targets=controlled_targets,
                cash_target_weight=_ONE
                - sum((target for _, target in controlled_targets), _ZERO),
                evidence_complete=True,
            )
    if effective_selection is None:
        strategy_positions = current
        strategy_cash = cash_weight
    else:
        strategy_positions = tuple(
            ProposalPosition(
                instrument_id=instrument_id,
                current_weight=dict(current_weights).get(instrument_id, _ZERO),
                target_weight=target_weight,
                action=(
                    "none"
                    if target_weight
                    == dict(current_weights).get(instrument_id, _ZERO)
                    == _ZERO
                    else "increase"
                    if target_weight > dict(current_weights).get(instrument_id, _ZERO)
                    else "reduce"
                    if target_weight < dict(current_weights).get(instrument_id, _ZERO)
                    else "maintain"
                ),
                reason_code="BASELINE_SELECTION_AUTHORITY",
            )
            for instrument_id, target_weight in strategy_result.targets
        )
        strategy_cash = strategy_result.cash_target_weight
    strategy = OptimizationProposal(
        proposal_id=new_uuid7(),
        variant="strategy_target",
        baseline_portfolio_snapshot_id=snapshot.id,
        positions=strategy_positions,
        cash_current_weight=cash_weight,
        cash_target_weight=strategy_cash,
        analysis_only=analysis_only,
        strategy_evidence_complete=strategy_result.evidence_complete,
        portfolio_scope=portfolio_scope,
    )
    return OptimizationProposalGroup(
        baseline_portfolio_snapshot_id=snapshot.id,
        portfolio_scope=portfolio_scope,
        proposals=(hold, minimum, strategy),
        valuation_time=snapshot.as_of,
    )


def _current_positions(snapshot: PortfolioSnapshot) -> tuple[tuple[ProposalPosition, ...], Decimal]:
    values = tuple(
        (position.instrument_id, position.quantity * (position.current_price or _ZERO))
        for position in snapshot.positions
    )
    cash = sum((item.cash_balance for item in snapshot.cash), _ZERO)
    total = cash + sum((value for _, value in values), _ZERO)
    if total <= _ZERO:
        raise ValueError("baseline requires positive valuated value")
    positions = tuple(
        ProposalPosition(
            instrument_id=instrument_id,
            current_weight=value / total,
            target_weight=value / total,
            action="maintain",
            reason_code="BASELINE_WEIGHT",
        )
        for instrument_id, value in sorted(values, key=lambda entry: entry[0])
    )
    return positions, cash / total

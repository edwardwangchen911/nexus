# ruff: noqa: E501
"""Immutable three-way proposal models and pure construction entry point."""

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.providers.broker.base import PortfolioSnapshot

ProposalVariant = Literal["hold_current", "minimum_change", "strategy_target"]
ProposalGroupStatus = Literal["draft", "compared", "shadowing", "validated", "rejected", "expired"]
ProposalAction = Literal["none", "reduce", "maintain", "increase"]
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


class OptimizationProposalGroup(_StrictModel):
    proposal_group_id: UUID = Field(default_factory=new_uuid7)
    status: ProposalGroupStatus = "draft"
    version: int = Field(default=0, ge=0)
    baseline_portfolio_snapshot_id: UUID
    proposals: tuple[OptimizationProposal, OptimizationProposal, OptimizationProposal]
    diagnostic_input_hash: str | None = None
    data_snapshot_id: UUID | None = None
    source_policy_version: str | None = None
    cost_model_version: str | None = None
    valuation_time: datetime | None = None

    @field_validator("valuation_time")
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @model_validator(mode="after")
    def _three_variants_share_baseline(self) -> "OptimizationProposalGroup":
        variants = tuple(item.variant for item in self.proposals)
        if variants != ("hold_current", "minimum_change", "strategy_target"):
            raise ValueError("proposal group requires exactly ordered unique variants")
        if any(
            item.baseline_portfolio_snapshot_id != self.baseline_portfolio_snapshot_id
            for item in self.proposals
        ):
            raise ValueError("proposal variants must share the group baseline")
        return self


def build_proposal_group(
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    research: tuple[object, ...],
    policy: OptimizationPolicy,
) -> OptimizationProposalGroup:
    """Build a fail-closed, analysis-only group without deriving targets from Agent input."""
    del research
    if snapshot.status != "READY":
        raise ValueError("READY baseline is required")
    if report.portfolio_snapshot_id != snapshot.id:
        raise ValueError("diagnostic report must bind the baseline")
    if report.optimization_need != "OPTIMIZATION_CANDIDATE":
        raise ValueError("diagnostic report is not an optimization candidate")
    if policy.version != "optimizer_v1":
        raise ValueError("unsupported optimization policy")
    current, cash_weight = _current_positions(snapshot)
    hold = OptimizationProposal(
        variant="hold_current",
        baseline_portfolio_snapshot_id=snapshot.id,
        positions=current,
        cash_current_weight=cash_weight,
        cash_target_weight=cash_weight,
        analysis_only=True,
        strategy_evidence_complete=False,
    )
    minimum = hold.model_copy(update={"proposal_id": new_uuid7(), "variant": "minimum_change"})
    strategy = hold.model_copy(update={"proposal_id": new_uuid7(), "variant": "strategy_target"})
    return OptimizationProposalGroup(
        baseline_portfolio_snapshot_id=snapshot.id,
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

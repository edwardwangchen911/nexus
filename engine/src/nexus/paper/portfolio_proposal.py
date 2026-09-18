"""Exact, bounded allocation from frozen choices and an authoritative baseline."""

from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
)
from nexus.audit.canonical import Hash64
from nexus.domain.time import require_utc
from nexus.paper.portfolio_policy import (
    PORTFOLIO_ALLOCATION_POLICY_HASH,
    PORTFOLIO_ALLOCATION_V1,
    PortfolioAllocationPolicyV1,
    canonical_portfolio_allocation_policy_hash,
)
from nexus.risk.models import PaperPortfolioProjection, exact_decimal


class PortfolioBaselineV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    weights: dict[UUID, Decimal]
    cash_weight: Decimal
    target_version_id: UUID | None = None
    initial_approval_id: UUID | None = None
    content_hash: Hash64
    roles: dict[UUID, Literal["core", "tactical", "none"]] = Field(default_factory=dict)

    @field_validator("weights", mode="before")
    @classmethod
    def _exact_weights(cls, value: dict[UUID, object]) -> dict[UUID, Decimal]:
        return {key: exact_decimal(item, field="weight") for key, item in value.items()}

    @field_validator("cash_weight", mode="before")
    @classmethod
    def _exact_cash(cls, value: object) -> Decimal:
        return exact_decimal(value, field="cash_weight")

    @model_validator(mode="after")
    def _valid(self) -> "PortfolioBaselineV1":
        if (
            (self.target_version_id is None) == (self.initial_approval_id is None)
            or any(weight < 0 or weight > 1 for weight in self.weights.values())
            or not 0 <= self.cash_weight <= 1
            or sum(self.weights.values(), self.cash_weight) != 1
            or not set(self.roles) <= set(self.weights)
        ):
            raise ValueError("PORTFOLIO_BASELINE_INVALID")
        return self


class PortfolioProposalV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["agent_portfolio_proposal_v1"] = "agent_portfolio_proposal_v1"
    baseline: PortfolioBaselineV1
    synthesis: PortfolioSynthesisIntentV1
    synthesis_output_id: UUID
    synthesis_output_hash: Hash64
    policy_hash: Hash64 = PORTFOLIO_ALLOCATION_POLICY_HASH
    weights: dict[UUID, Decimal]
    cash_weight: Decimal
    roles: dict[UUID, Literal["core", "tactical", "none"]]
    priorities: dict[UUID, int]
    reasons: dict[UUID, str]
    inherited_constraint_breaches: tuple[str, ...]
    expires_at: datetime
    decision_hash: Hash64


def canonical_portfolio_proposal_hash(proposal: PortfolioProposalV1) -> str:
    return canonical_portfolio_synthesis_hash(
        proposal.model_dump(mode="json", exclude={"decision_hash"})
    )


def canonical_portfolio_target_content_hash(proposal: PortfolioProposalV1) -> str:
    """Allocation identity excludes evidence, decisions and absent zero candidates."""
    return canonical_portfolio_synthesis_hash(
        {
            "schema_version": "portfolio_target_content_v1",
            "targets": [
                {
                    "instrument_id": str(key),
                    "target_weight": format(value, ".12f"),
                    "role": proposal.roles[key],
                }
                for key, value in sorted(proposal.weights.items(), key=lambda item: item[0].int)
                if value > 0
            ],
            "cash_weight": format(proposal.cash_weight, ".12f"),
            "policy_hash": proposal.policy_hash,
        }
    )


def initial_projection_baseline(
    projection: PaperPortfolioProjection,
    *,
    approval_id: UUID,
    approval_hash: str,
) -> PortfolioBaselineV1:
    quantum = Decimal("0.000000000001")
    with localcontext() as context:
        context.prec = 80
        weights = {
            position.instrument_id: (
                position.quantity * position.reference_close / projection.nav
            ).quantize(quantum, rounding=ROUND_HALF_EVEN)
            for position in projection.positions
        }
        cash = Decimal(1) - sum(weights.values(), Decimal(0))
    actual_cash = Fraction(projection.available_cash + projection.reserved_cash) / Fraction(
        projection.nav
    )
    if cash < 0 or abs(Fraction(cash) - actual_cash) > len(weights) * Fraction(quantum) / 2:
        raise ValueError("INITIAL_PROJECTION_WEIGHT_PRECISION_UNSUPPORTED")
    return PortfolioBaselineV1(
        weights=weights,
        cash_weight=cash,
        initial_approval_id=approval_id,
        content_hash=approval_hash,
    )


def build_portfolio_proposal(
    *,
    active_target: PortfolioBaselineV1,
    synthesis: PortfolioSynthesisIntentV1,
    synthesis_output_id: UUID,
    synthesis_output_hash: str,
    expires_at: datetime,
    policy: PortfolioAllocationPolicyV1 = PORTFOLIO_ALLOCATION_V1,
) -> PortfolioProposalV1:
    """Apply atomic actions; never infer authority from a prior unactivated proposal."""
    if canonical_portfolio_allocation_policy_hash(policy) != PORTFOLIO_ALLOCATION_POLICY_HASH:
        raise ValueError("PORTFOLIO_POLICY_INVALID")
    base = PortfolioBaselineV1.model_validate(active_target.model_dump())
    synthesis = PortfolioSynthesisIntentV1.model_validate(synthesis.model_dump())
    weights = dict(base.weights)
    roles = {key: base.roles.get(key, "none") for key in weights}
    reasons = {key: "INHERITED_TARGET" for key in weights}
    priorities = {key: 100 for key in weights}
    for choice in synthesis.choices:
        old = weights.setdefault(choice.instrument_id, Decimal(0))
        roles.setdefault(choice.instrument_id, "none")
        priorities[choice.instrument_id] = choice.rank or 100
        if (choice.action == "include" and old != 0) or (
            choice.action in {"increase", "reduce", "exit", "maintain"} and old == 0
        ):
            raise ValueError("PORTFOLIO_BASELINE_ACTION_CONFLICT")
        reasons[choice.instrument_id] = choice.action.upper()
        if choice.action == "exit":
            weights[choice.instrument_id] = Decimal(0)
        elif choice.action == "reduce":
            if old < policy.adjustment_step:
                reasons[choice.instrument_id] = "ATOMIC_STEP_REQUIRED"
            else:
                weights[choice.instrument_id] = old - policy.adjustment_step
    reductions = sum(
        (base.weights.get(key, Decimal(0)) - value for key, value in weights.items()), Decimal(0)
    )
    if reductions > policy.turnover_cap:
        raise ValueError(
            "EXIT_TURNOVER_CONFLICT"
            if any(c.action == "exit" for c in synthesis.choices)
            else "REDUCTION_TURNOVER_CONFLICT"
        )
    increases = 0
    added = Decimal(0)
    cash = base.cash_weight + reductions
    for choice in sorted(
        (c for c in synthesis.choices if c.rank is not None), key=lambda c: c.rank or 0
    ):
        key = choice.instrument_id
        old = base.weights.get(key, Decimal(0))
        desired = (
            (policy.core_target if choice.role == "core" else policy.tactical_target)
            if choice.action == "include"
            else old + policy.adjustment_step
        )
        delta = desired - old
        reason = None
        if desired > policy.single_name_target_cap:
            reason = "SINGLE_NAME_CAP"
        elif increases >= policy.max_increases:
            reason = "MAX_INCREASES"
        elif base.cash_weight < policy.target_cash_floor or cash - delta < policy.target_cash_floor:
            reason = "CASH_FLOOR"
        elif added + delta > policy.turnover_cap:
            reason = "TURNOVER_LIMIT"
        if reason:
            reasons[key] = reason
            continue
        weights[key] = desired
        roles[key] = choice.role
        cash -= delta
        added += delta
        increases += 1
    breaches = []
    if base.cash_weight < policy.target_cash_floor:
        breaches.append("INHERITED_CASH_BELOW_FLOOR")
    if any(value > policy.single_name_target_cap for value in base.weights.values()):
        breaches.append("INHERITED_NAME_ABOVE_CAP")
    ordered = sorted(weights, key=lambda key: key.int)
    proposal = PortfolioProposalV1(
        baseline=base,
        synthesis=synthesis,
        synthesis_output_id=synthesis_output_id,
        synthesis_output_hash=synthesis_output_hash,
        weights={key: weights[key] for key in ordered},
        cash_weight=cash,
        roles={key: roles[key] for key in ordered},
        priorities=priorities,
        reasons=reasons,
        inherited_constraint_breaches=tuple(breaches),
        expires_at=require_utc(expires_at),
        decision_hash="0" * 64,
    )
    return proposal.model_copy(
        update={"decision_hash": canonical_portfolio_proposal_hash(proposal)}
    )

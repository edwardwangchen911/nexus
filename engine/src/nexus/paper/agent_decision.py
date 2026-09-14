"""Deterministic direction-to-target sizing for internal Agent paper cycles."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator, model_validator

from nexus.agents.actionable_research import ActionableResearchDecision
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
)
from nexus.risk.models import PaperPortfolioProjection, RiskMarketSnapshot, exact_decimal
from nexus.strategy.forward import ForwardStrategyTarget

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_QUANTUM = Decimal("0.000000000001")
_ZERO = Decimal(0)


class AgentPaperDecisionError(ValueError):
    """Stable fail-closed sizing rejection."""


class AgentPaperSizingPolicyV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal["agent_paper_sizing_v1"] = "agent_paper_sizing_v1"
    target_step: Decimal = Decimal("0.02")
    cycle_turnover_limit: Decimal = Decimal("0.10")

    @field_validator("target_step", "cycle_turnover_limit", mode="before")
    @classmethod
    def _exact_policy_decimal(cls, value: object) -> Decimal:
        return exact_decimal(value, field="Agent paper sizing policy")


AGENT_PAPER_SIZING_V1 = AgentPaperSizingPolicyV1()


class AgentPaperDecisionBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    output_id: UUID
    instrument_id: UUID
    intent_hash: Hash64
    sizing_action: Literal["increase", "reduce", "maintain", "none"]
    binding_hash: Hash64


class AgentPaperDecision(BaseModel):
    """Immutable Agent-origin paper decision; it carries no registry authority."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    account_id: UUID
    portfolio_id: UUID
    decision_session: date
    decision_time: datetime
    execution_session: date
    source_policy_id: UUID
    execution_source_policy_id: UUID
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID | None
    portfolio_projection_hash: Hash64
    authority_hash: Hash64
    sizing_policy_hash: Hash64
    decision_hash: Hash64
    cash_weight: Decimal
    targets: tuple[ForwardStrategyTarget, ...]
    bindings: tuple[AgentPaperDecisionBinding, ...]

    @field_validator("cash_weight", mode="before")
    @classmethod
    def _exact_cash_weight(cls, value: object) -> Decimal:
        return exact_decimal(value, field="cash_weight")

    @model_validator(mode="after")
    def _valid_decision(self) -> AgentPaperDecision:
        if self.decision_time.tzinfo is None or self.decision_time.utcoffset() != UTC.utcoffset(
            self.decision_time
        ):
            raise ValueError("decision time must use UTC")
        legacy = (
            self.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
            and self.execution_source_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
            and self.execution_session > self.decision_session
        )
        schwab = (
            self.source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
            and self.execution_source_policy_id
            == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
            and self.execution_session == self.decision_session
        )
        if not (legacy or schwab):
            raise ValueError("Agent decision session and source policies are invalid")
        target_ids = [target.instrument_id for target in self.targets]
        binding_ids = [binding.instrument_id for binding in self.bindings]
        if len(target_ids) != len(set(target_ids)) or len(binding_ids) != len(set(binding_ids)):
            raise ValueError("Agent decision instruments must be unique")
        if (
            self.quality_report_id is not None
            or self.sizing_policy_hash != AGENT_PAPER_SIZING_POLICY_HASH
            or not set(binding_ids) <= set(target_ids)
        ):
            raise ValueError("Agent decision authority is invalid")
        if self.cash_weight + sum(
            (target.target_weight for target in self.targets), _ZERO
        ) != Decimal(1):
            raise ValueError("target and cash weights must equal one")
        return self


def _q(value: Decimal) -> Decimal:
    return value.quantize(_QUANTUM, rounding=ROUND_HALF_EVEN)


def _digest(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def canonical_agent_binding_hash(
    *, cycle_id: UUID, decision_hash: str, binding: AgentPaperDecisionBinding
) -> str:
    return _digest(
        {
            "schema": "agent_paper_decision_binding_sha256_v1",
            "cycle_id": str(cycle_id).lower(),
            "decision_hash": decision_hash,
            "instrument_id": str(binding.instrument_id).lower(),
            "output_id": str(binding.output_id).lower(),
            "intent_hash": binding.intent_hash,
            "sizing_action": binding.sizing_action,
        }
    )


def canonical_agent_paper_decision_hash(decision: AgentPaperDecision) -> str:
    targets = [
        {
            "exit": target.exit,
            "instrument_id": str(target.instrument_id).lower(),
            "reason": target.reason,
            "strategy_priority": target.strategy_priority,
            "target_weight": format(_q(target.target_weight), ".12f"),
        }
        for target in sorted(decision.targets, key=lambda item: item.instrument_id.int)
    ]
    return _digest(
        {
            "schema": "agent_paper_decision_sha256_v1",
            "cycle_id": str(decision.cycle_id).lower(),
            "intent_hashes": sorted(binding.intent_hash for binding in decision.bindings),
            "sizing_policy_hash": decision.sizing_policy_hash,
            "targets": targets,
            "cash_weight": format(_q(decision.cash_weight), ".12f"),
            "authority_hash": decision.authority_hash,
        }
    )


def _validate_inputs(
    cycle: AgentPaperCycleRecord,
    actions: tuple[ActionableResearchDecision, ...],
    projection: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
) -> None:
    from nexus.risk.canonical import canonical_risk_market_authority_hash

    if projection.open_orders:
        raise AgentPaperDecisionError("OPEN_ORDER_EXISTS")
    if not actions:
        raise AgentPaperDecisionError("NO_ELIGIBLE_CANDIDATE")
    legacy_policy = (
        cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
        and cycle.execution_price_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
        and cycle.execution_session > cycle.decision_session
    )
    schwab_policy = (
        cycle.decision_price_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
        and cycle.execution_price_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
        and cycle.execution_session == cycle.decision_session
    )
    if (
        cycle.account_id != projection.account_id
        or cycle.portfolio_id != projection.portfolio_id
        or not (legacy_policy or schwab_policy)
        or cycle.sizing_policy_version != AGENT_PAPER_SIZING_V1.version
        or cycle.sizing_policy_hash != AGENT_PAPER_SIZING_POLICY_HASH
        or not cycle.execution_locked
        or cycle.decision_session != market.decision_session
        or cycle.decision_time != market.decision_time
        or cycle.execution_session != market.execution_session
        or cycle.decision_price_policy_id != market.source_policy_id
        or cycle.universe_snapshot_id != market.universe_snapshot_id
        or cycle.quality_report_id != market.quality_report_id
        or projection.projection_hash == "0" * 64
    ):
        raise AgentPaperDecisionError("PAPER_PROJECTION_INVALID")
    action_ids = [action.instrument_id for action in actions]
    if len(action_ids) != len(set(action_ids)):
        raise AgentPaperDecisionError("DUPLICATE_INSTRUMENT")
    market_ids = set(market.required_instrument_ids)
    position_ids = {position.instrument_id for position in projection.positions}
    if not (set(action_ids) | position_ids) <= market_ids:
        raise AgentPaperDecisionError("RISK_AUTHORITY_INCOMPLETE")
    if canonical_risk_market_authority_hash(market) != market.authority_hash:
        raise AgentPaperDecisionError("RISK_AUTHORITY_INCOMPLETE")


def _sizing_policy_hash() -> str:
    return _digest(
        {
            "schema": "agent_paper_sizing_policy_sha256_v1",
            "version": AGENT_PAPER_SIZING_V1.version,
            "target_step": format(_q(AGENT_PAPER_SIZING_V1.target_step), ".12f"),
            "cycle_turnover_limit": format(_q(AGENT_PAPER_SIZING_V1.cycle_turnover_limit), ".12f"),
        }
    )


AGENT_PAPER_SIZING_POLICY_HASH = _sizing_policy_hash()


def build_agent_paper_decision(
    *,
    cycle: AgentPaperCycleRecord,
    actions: tuple[ActionableResearchDecision, ...],
    projection: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
) -> AgentPaperDecision:
    """Apply reductions before increases and bind every direction to frozen facts."""
    _validate_inputs(cycle, actions, projection, market)
    prices = {price.instrument_id: price.close for price in market.prices}
    current = {
        position.instrument_id: _q(
            position.quantity * prices[position.instrument_id] / projection.nav
        )
        for position in projection.positions
    }
    actions_by_id = {action.instrument_id: action for action in actions}
    target_weights = dict(current)
    turnover = _ZERO
    ordered_actions = sorted(
        actions,
        key=lambda item: (
            {"reduce": 0, "maintain": 1, "none": 1, "increase": 2}[item.sizing_action],
            item.instrument_id.int,
        ),
    )
    for action in ordered_actions:
        old = target_weights.get(action.instrument_id, _ZERO)
        if action.sizing_action in {"maintain", "none"}:
            target_weights.setdefault(action.instrument_id, old)
            continue
        if (
            turnover + AGENT_PAPER_SIZING_V1.target_step
            > AGENT_PAPER_SIZING_V1.cycle_turnover_limit
        ):
            target_weights.setdefault(action.instrument_id, old)
            continue
        if action.sizing_action == "reduce":
            new = (
                _ZERO
                if old <= AGENT_PAPER_SIZING_V1.target_step
                else _q(old - AGENT_PAPER_SIZING_V1.target_step)
            )
        else:
            new = _q(old + AGENT_PAPER_SIZING_V1.target_step)
        change = abs(new - old)
        target_weights[action.instrument_id] = new
        turnover = _q(turnover + change)

    total = sum(target_weights.values(), _ZERO)
    if total > Decimal(1):
        raise AgentPaperDecisionError("PAPER_PROJECTION_INVALID")
    targets: list[ForwardStrategyTarget] = []
    for instrument_id, weight in target_weights.items():
        target_action = actions_by_id.get(instrument_id)
        sizing_action = "maintain" if target_action is None else target_action.sizing_action
        exit_target = sizing_action == "reduce" and weight == _ZERO and instrument_id in current
        priority = 0 if exit_target else (2 if sizing_action == "increase" else 1)
        targets.append(
            ForwardStrategyTarget(
                instrument_id=instrument_id,
                target_weight=weight,
                exit=exit_target,
                strategy_priority=priority,
                reason=f"AGENT_RESEARCH_{sizing_action.upper()}",
            )
        )
    targets.sort(key=lambda item: (item.strategy_priority, item.instrument_id.int))
    empty_hash = "0" * 64
    bindings = tuple(
        AgentPaperDecisionBinding(
            output_id=action.output_id,
            instrument_id=action.instrument_id,
            intent_hash=action.intent_hash,
            sizing_action=action.sizing_action,
            binding_hash=empty_hash,
        )
        for action in sorted(actions, key=lambda item: item.instrument_id.int)
    )
    draft = AgentPaperDecision(
        cycle_id=cycle.id,
        account_id=cycle.account_id,
        portfolio_id=cycle.portfolio_id,
        decision_session=cycle.decision_session,
        decision_time=cycle.decision_time,
        execution_session=cycle.execution_session,
        source_policy_id=cycle.decision_price_policy_id,
        execution_source_policy_id=cycle.execution_price_policy_id,
        calendar_authority_id=market.calendar_authority_id,
        universe_snapshot_id=cycle.universe_snapshot_id,
        quality_report_id=cycle.quality_report_id,
        portfolio_projection_hash=projection.projection_hash,
        authority_hash=market.authority_hash,
        sizing_policy_hash=cycle.sizing_policy_hash,
        decision_hash=empty_hash,
        cash_weight=_q(Decimal(1) - total),
        targets=tuple(targets),
        bindings=bindings,
    )
    decision_hash = canonical_agent_paper_decision_hash(draft)
    return draft.model_copy(
        update={
            "decision_hash": decision_hash,
            "bindings": tuple(
                binding.model_copy(
                    update={
                        "binding_hash": canonical_agent_binding_hash(
                            cycle_id=draft.cycle_id,
                            decision_hash=decision_hash,
                            binding=binding,
                        )
                    }
                )
                for binding in bindings
            ),
        }
    )

"""Deterministic, evidence-bound internal-paper strategy handover preflight."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.backtest.canonical import canonical_decimal
from nexus.evolution.models import _reject_binary_float
from nexus.evolution.shadow.canonical import content_hash
from nexus.risk.policy import RiskPolicy

HandoverStatus = Literal["PASS", "FAIL", "UNKNOWN"]
_MINIMUM_ADV20_USD = Decimal("50000000")


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _no_binary_float(cls, value: object) -> object:
        _reject_binary_float(value)
        return value


class HandoverSectorExposureV1(_FrozenModel):
    sector: str
    weight: Decimal = Field(gt=0, le=1)

    @field_validator("weight", mode="after")
    @classmethod
    def _canonical_weight(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class HandoverInstrumentV1(_FrozenModel):
    instrument_id: UUID
    current_weight: Decimal = Field(ge=0, le=1)
    market: Literal["US"]
    currency: Literal["USD"]
    asset_type: Literal["ETF", "common_stock"]
    reference_close: Decimal = Field(gt=0)
    adv20_usd: Decimal = Field(gt=0)
    available_at: datetime
    blocked: bool
    sector_exposures: tuple[HandoverSectorExposureV1, ...]

    @field_validator("current_weight", "reference_close", "adv20_usd", mode="after")
    @classmethod
    def _canonical_weight(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @field_validator("available_at", mode="after")
    @classmethod
    def _utc_available_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("handover instrument authority time must use UTC")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _unique_sectors(self) -> HandoverInstrumentV1:
        sectors = [item.sector for item in self.sector_exposures]
        if len(sectors) != len(set(sectors)):
            raise ValueError("handover sector exposures must be unique")
        if sum((item.weight for item in self.sector_exposures), start=Decimal(0)) != 1:
            raise ValueError("handover sector exposures must sum to one")
        return self


class HandoverSnapshotV1(_FrozenModel):
    paper_snapshot_id: UUID
    nav: Decimal = Field(gt=0)
    current_cash_weight: Decimal = Field(ge=0, le=1)
    drawdown_state: Literal["unlocked", "locked", "recovery_eligible"]
    instruments: tuple[HandoverInstrumentV1, ...]
    shadow_excess_return: Decimal

    @field_validator("nav", "current_cash_weight", "shadow_excess_return", mode="after")
    @classmethod
    def _canonical_amount(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @model_validator(mode="after")
    def _valid_current_weights(self) -> HandoverSnapshotV1:
        ids = [item.instrument_id for item in self.instruments]
        if len(ids) != len(set(ids)):
            raise ValueError("handover instruments must be unique")
        total = self.current_cash_weight + sum(
            (item.current_weight for item in self.instruments), start=Decimal(0)
        )
        if total != 1:
            raise ValueError("handover current weights must sum to one")
        return self


class HandoverTargetV1(_FrozenModel):
    instrument_id: UUID
    weight: Decimal = Field(gt=0, le=1)

    @field_validator("weight", mode="after")
    @classmethod
    def _canonical_weight(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class HandoverChallengerV1(_FrozenModel):
    strategy_definition_id: UUID
    target_cash_weight: Decimal = Field(ge=0, le=1)
    targets: tuple[HandoverTargetV1, ...]

    @field_validator("target_cash_weight", mode="after")
    @classmethod
    def _canonical_weight(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @model_validator(mode="after")
    def _valid_targets(self) -> HandoverChallengerV1:
        ids = [item.instrument_id for item in self.targets]
        if len(ids) != len(set(ids)):
            raise ValueError("handover targets must be unique")
        if (
            self.target_cash_weight + sum((item.weight for item in self.targets), start=Decimal(0))
            != 1
        ):
            raise ValueError("handover target weights must sum to one")
        return self


class HandoverAuthorityV1(_FrozenModel):
    source_policy_id: UUID
    cost_policy_id: UUID
    risk_policy_id: UUID
    decision_time: datetime
    available_at: datetime
    estimated_cost_rate: Decimal = Field(ge=0)
    complete: bool
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("estimated_cost_rate", mode="after")
    @classmethod
    def _canonical_rate(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @field_validator("decision_time", "available_at", mode="after")
    @classmethod
    def _utc_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("handover times must use UTC")
        return value.astimezone(UTC)


class HandoverGateResultV1(_FrozenModel):
    gate: Literal["authority_complete", "economic_residual", "hard_risk_clear"]
    status: HandoverStatus
    observed: str | None
    threshold: str
    reason_code: str


class HandoverEvaluationV1(_FrozenModel):
    schema_version: Literal["strategy_handover_evaluation_v1"] = "strategy_handover_evaluation_v1"
    status: HandoverStatus
    snapshot: HandoverSnapshotV1
    challenger: HandoverChallengerV1
    authority: HandoverAuthorityV1
    risk_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    paper_snapshot_id: UUID
    strategy_definition_id: UUID
    turnover: Decimal | None
    estimated_handover_cost_ratio: Decimal | None
    economic_residual: Decimal | None
    risk_violations: tuple[str, ...]
    gates: tuple[HandoverGateResultV1, ...]
    evidence_ids: tuple[UUID, ...]
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evaluation_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason_code: str
    live_execution_enabled: Literal[False] = False

    @field_validator("turnover", "estimated_handover_cost_ratio", "economic_residual", mode="after")
    @classmethod
    def _canonical_metric(cls, value: Decimal | None) -> Decimal | None:
        return None if value is None else canonical_decimal(value)

    @model_validator(mode="after")
    def _fixed_gates(self) -> HandoverEvaluationV1:
        if self.paper_snapshot_id != self.snapshot.paper_snapshot_id:
            raise ValueError("handover snapshot identity must match")
        if self.strategy_definition_id != self.challenger.strategy_definition_id:
            raise ValueError("handover strategy identity must match")
        if tuple(item.gate for item in self.gates) != (
            "authority_complete",
            "economic_residual",
            "hard_risk_clear",
        ):
            raise ValueError("handover gates must use the fixed order")
        return self


def evaluate_handover(
    snapshot: HandoverSnapshotV1,
    challenger: HandoverChallengerV1,
    authority: HandoverAuthorityV1,
    risk_policy: RiskPolicy,
) -> HandoverEvaluationV1:
    """Evaluate transition economics and hard risk without creating any order."""
    instruments = {item.instrument_id: item for item in snapshot.instruments}
    targets = {item.instrument_id: item.weight for item in challenger.targets}
    authority_complete = (
        authority.complete
        and authority.available_at <= authority.decision_time
        and set(targets) <= set(instruments)
        and all(item.available_at <= authority.decision_time for item in snapshot.instruments)
    )
    if authority_complete:
        turnover = canonical_decimal(
            (
                abs(snapshot.current_cash_weight - challenger.target_cash_weight)
                + sum(
                    (
                        abs(item.current_weight - targets.get(item.instrument_id, Decimal(0)))
                        for item in snapshot.instruments
                    ),
                    start=Decimal(0),
                )
            )
            / Decimal(2)
        )
        estimated_cost = canonical_decimal(turnover * authority.estimated_cost_rate)
        economic_residual = canonical_decimal(snapshot.shadow_excess_return - estimated_cost)
        risk_violations = _risk_violations(
            snapshot=snapshot,
            challenger=challenger,
            instruments=instruments,
            policy=risk_policy,
        )
    else:
        turnover = None
        estimated_cost = None
        economic_residual = None
        risk_violations = ()
    gates = (
        HandoverGateResultV1(
            gate="authority_complete",
            status="PASS" if authority_complete else "UNKNOWN",
            observed="complete" if authority_complete else None,
            threshold="complete",
            reason_code=(
                "HANDOVER_AUTHORITY_COMPLETE"
                if authority_complete
                else "HANDOVER_AUTHORITY_INCOMPLETE"
            ),
        ),
        HandoverGateResultV1(
            gate="economic_residual",
            status=(
                "UNKNOWN"
                if economic_residual is None
                else "PASS"
                if economic_residual >= 0
                else "FAIL"
            ),
            observed=None if economic_residual is None else format(economic_residual, "f"),
            threshold="0",
            reason_code=(
                "HANDOVER_ECONOMICS_UNKNOWN"
                if economic_residual is None
                else "HANDOVER_ECONOMICS_PASSED"
                if economic_residual >= 0
                else "HANDOVER_COST_EXCEEDS_SHADOW_EXCESS"
            ),
        ),
        HandoverGateResultV1(
            gate="hard_risk_clear",
            status=(
                "UNKNOWN" if not authority_complete else "PASS" if not risk_violations else "FAIL"
            ),
            observed=None if not authority_complete else ",".join(risk_violations) or "clear",
            threshold="no_violations",
            reason_code=(
                "HANDOVER_RISK_UNKNOWN"
                if not authority_complete
                else "HANDOVER_HARD_RISK_CLEAR"
                if not risk_violations
                else "HANDOVER_HARD_RISK_VIOLATION"
            ),
        ),
    )
    statuses = {item.status for item in gates}
    status: HandoverStatus = (
        "UNKNOWN" if "UNKNOWN" in statuses else "FAIL" if "FAIL" in statuses else "PASS"
    )
    reason_code = {
        "PASS": "HANDOVER_GATES_PASSED",
        "FAIL": "HANDOVER_GATES_FAILED",
        "UNKNOWN": "PROMOTION_WAITING_HANDOVER",
    }[status]
    evidence_ids = (
        snapshot.paper_snapshot_id,
        challenger.strategy_definition_id,
        authority.source_policy_id,
        authority.cost_policy_id,
        authority.risk_policy_id,
    )
    authority_hash = content_hash(
        {
            "schema_version": "strategy_handover_authority_v1",
            "snapshot": snapshot.model_dump(mode="json"),
            "challenger": challenger.model_dump(mode="json"),
            "authority": authority.model_dump(mode="json"),
            "risk_policy_hash": risk_policy.content_hash,
        }
    )
    provisional = HandoverEvaluationV1(
        status=status,
        snapshot=snapshot,
        challenger=challenger,
        authority=authority,
        risk_policy_hash=risk_policy.content_hash,
        paper_snapshot_id=snapshot.paper_snapshot_id,
        strategy_definition_id=challenger.strategy_definition_id,
        turnover=turnover,
        estimated_handover_cost_ratio=estimated_cost,
        economic_residual=economic_residual,
        risk_violations=risk_violations,
        gates=gates,
        evidence_ids=evidence_ids,
        authority_hash=authority_hash,
        evaluation_hash="0" * 64,
        reason_code=reason_code,
    )
    return provisional.model_copy(
        update={
            "evaluation_hash": content_hash(
                provisional.model_dump(mode="json", exclude={"evaluation_hash"})
            )
        }
    )


def _risk_violations(
    *,
    snapshot: HandoverSnapshotV1,
    challenger: HandoverChallengerV1,
    instruments: dict[UUID, HandoverInstrumentV1],
    policy: RiskPolicy,
) -> tuple[str, ...]:
    violations: list[str] = []
    targets = {item.instrument_id: item.weight for item in challenger.targets}
    if any(
        item.market != policy.supported_market
        or item.currency != policy.supported_currency
        or item.asset_type not in policy.supported_asset_types
        for item in instruments.values()
    ):
        violations.append("UNSUPPORTED_SCOPE")
    if any(item.blocked for item in instruments.values()):
        violations.append("TRADING_BLOCKED")
    if any(item.adv20_usd < _MINIMUM_ADV20_USD for item in instruments.values()):
        violations.append("LIQUIDITY_LIMIT")
    increases = tuple(
        target - instruments[instrument_id].current_weight
        for instrument_id, target in targets.items()
        if target > instruments[instrument_id].current_weight
    )
    if snapshot.drawdown_state != "unlocked" and increases:
        violations.append("DRAWDOWN_LOCKED")
    if any(delta * snapshot.nav < policy.minimum_buy_notional for delta in increases):
        violations.append("BELOW_MINIMUM_ORDER")
    if challenger.target_cash_weight < policy.minimum_cash_ratio:
        violations.append("MINIMUM_CASH")
    if any(item.weight > policy.single_name_limit for item in challenger.targets):
        violations.append("SINGLE_NAME_LIMIT")
    sector_weights: dict[str, Decimal] = {}
    for target in challenger.targets:
        for exposure in instruments[target.instrument_id].sector_exposures:
            sector_weights[exposure.sector] = sector_weights.get(exposure.sector, Decimal(0)) + (
                target.weight * exposure.weight
            )
    if any(value > policy.sector_limit for value in sector_weights.values()):
        violations.append("SECTOR_LIMIT")
    return tuple(violations)

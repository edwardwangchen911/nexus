"""Immutable value objects at the deterministic hard-risk boundary."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
RiskOutcome = Literal["approved", "reduced", "rejected"]
OrderSide = Literal["buy", "sell"]
DrawdownState = Literal["unlocked", "locked", "recovery_eligible"]
AssetType = Literal["common_stock", "ETF"]
_QUANTUM = Decimal("0.000000000001")
_MAX_ABSOLUTE = Decimal("1e26")
_QUERY_KINDS = frozenset(("corporate_action", "suspension", "delisting"))


def exact_decimal(value: object, *, field: str) -> Decimal:
    """Accept only finite values exactly representable as NUMERIC(38,12)."""
    if isinstance(value, float):
        raise ValueError(f"{field} must not contain a binary float")
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"{field} must be a Decimal")
    result = Decimal(value)
    if not result.is_finite() or abs(result) >= _MAX_ABSOLUTE:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)")
    try:
        with localcontext() as context:
            context.prec = 50
            canonical = result.quantize(_QUANTUM)
    except InvalidOperation as error:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)") from error
    if canonical != result:
        raise ValueError(f"{field} must have no more than 12 decimal places")
    return result


def _aware(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


class FrozenRiskModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class RiskPrice(FrozenRiskModel):
    instrument_id: UUID
    authority_id: UUID
    market: Literal["US"]
    currency: Literal["USD"]
    asset_type: AssetType
    close: Decimal
    source: str = Field(min_length=1, max_length=100)
    delivery_mode: str = Field(min_length=1, max_length=50)
    available_at: datetime
    content_hash: Hash64

    @field_validator("close", mode="before")
    @classmethod
    def _close_is_exact(cls, value: object) -> Decimal:
        result = exact_decimal(value, field="close")
        if result <= 0:
            raise ValueError("close must be positive")
        return result

    @field_validator("available_at")
    @classmethod
    def _available_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="available_at")


class RiskSectorExposure(FrozenRiskModel):
    instrument_id: UUID
    authority_id: UUID
    sector: str = Field(min_length=1, max_length=100)
    weight: Decimal
    content_hash: Hash64

    @field_validator("weight", mode="before")
    @classmethod
    def _weight_is_exact(cls, value: object) -> Decimal:
        result = exact_decimal(value, field="sector weight")
        if not Decimal(0) < result <= Decimal(1):
            raise ValueError("sector weight must be positive and no greater than one")
        return result


class AuthorityQueryProof(FrozenRiskModel):
    kind: Literal["corporate_action", "suspension", "delisting"]
    authority_id: UUID
    complete: bool
    blocked: bool
    available_at: datetime
    content_hash: Hash64

    @field_validator("available_at")
    @classmethod
    def _available_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="available_at")


class RiskMarketSnapshot(FrozenRiskModel):
    decision_session: date
    execution_session: date
    decision_time: datetime
    source_policy_id: UUID
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID
    required_instrument_ids: tuple[UUID, ...]
    whitelisted_instrument_ids: tuple[UUID, ...]
    prices: tuple[RiskPrice, ...]
    sector_exposures: tuple[RiskSectorExposure, ...]
    query_proofs: tuple[AuthorityQueryProof, ...]
    authority_hash: Hash64

    @field_validator("decision_time")
    @classmethod
    def _decision_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="decision_time")

    @model_validator(mode="after")
    def _complete_authority(self) -> RiskMarketSnapshot:
        if self.execution_session <= self.decision_session:
            raise ValueError("execution session must follow decision session")
        required = set(self.required_instrument_ids)
        if len(required) != len(self.required_instrument_ids):
            raise ValueError("required instruments must be unique")
        if set(self.whitelisted_instrument_ids) != required:
            raise ValueError("whitelist must exactly cover required instruments")
        price_ids = [price.instrument_id for price in self.prices]
        if len(price_ids) != len(set(price_ids)) or set(price_ids) != required:
            raise ValueError("prices must exactly cover required instruments")
        sector_ids = {exposure.instrument_id for exposure in self.sector_exposures}
        if sector_ids != required:
            raise ValueError("sector exposures must exactly cover required instruments")
        for instrument_id in required:
            total = sum(
                (
                    exposure.weight
                    for exposure in self.sector_exposures
                    if exposure.instrument_id == instrument_id
                ),
                start=Decimal(0),
            )
            if total != Decimal(1):
                raise ValueError("sector exposures must sum to one")
        proof_kinds = [proof.kind for proof in self.query_proofs]
        if len(proof_kinds) != len(set(proof_kinds)) or set(proof_kinds) != _QUERY_KINDS:
            raise ValueError("query proofs must exactly cover required kinds")
        if any(not proof.complete for proof in self.query_proofs):
            raise ValueError("query proofs must be complete")
        late_price = any(price.available_at > self.decision_time for price in self.prices)
        late_proof = any(proof.available_at > self.decision_time for proof in self.query_proofs)
        if late_price or late_proof:
            raise ValueError("authority was available after decision time")
        return self


class PaperPositionBalance(FrozenRiskModel):
    instrument_id: UUID
    quantity: Decimal
    reserved_quantity: Decimal
    cost_basis: Decimal
    reference_close: Decimal

    @field_validator(
        "quantity",
        "reserved_quantity",
        "cost_basis",
        "reference_close",
        mode="before",
    )
    @classmethod
    def _amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @model_validator(mode="after")
    def _valid_position(self) -> PaperPositionBalance:
        if self.quantity < 0 or self.reserved_quantity < 0 or self.cost_basis < 0:
            raise ValueError("position amounts must be nonnegative")
        if self.reference_close <= 0:
            raise ValueError("reference close must be positive")
        if self.reserved_quantity > self.quantity:
            raise ValueError("reserved quantity cannot exceed quantity")
        if self.quantity != self.quantity.to_integral_value():
            raise ValueError("position quantity must be whole shares")
        if self.reserved_quantity != self.reserved_quantity.to_integral_value():
            raise ValueError("reserved quantity must be whole shares")
        return self


class PaperOpenOrder(FrozenRiskModel):
    order_id: UUID
    instrument_id: UUID
    side: OrderSide
    quantity: Decimal
    worst_price: Decimal
    reserved_cash: Decimal
    reserved_quantity: Decimal
    status: Literal[
        "risk_approved",
        "submitted",
        "partially_filled",
        "reconciling",
    ]

    @field_validator(
        "quantity",
        "worst_price",
        "reserved_cash",
        "reserved_quantity",
        mode="before",
    )
    @classmethod
    def _amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @model_validator(mode="after")
    def _valid_order(self) -> PaperOpenOrder:
        if self.quantity <= 0 or self.worst_price <= 0:
            raise ValueError("open order quantity and worst price must be positive")
        if self.quantity != self.quantity.to_integral_value():
            raise ValueError("open order quantity must be whole shares")
        if self.reserved_cash < 0 or self.reserved_quantity < 0:
            raise ValueError("open order reservations must be nonnegative")
        return self


class PaperPortfolioProjection(FrozenRiskModel):
    account_id: UUID
    portfolio_id: UUID
    as_of: datetime
    currency: Literal["USD"]
    account_model: Literal["cash"]
    position_model: Literal["long_only"]
    available_cash: Decimal
    reserved_cash: Decimal
    positions: tuple[PaperPositionBalance, ...]
    open_orders: tuple[PaperOpenOrder, ...]
    nav: Decimal
    peak_nav: Decimal
    drawdown_state: DrawdownState
    source_facts_hash: Hash64
    projection_hash: Hash64

    @field_validator("as_of")
    @classmethod
    def _as_of_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="as_of")

    @field_validator("available_cash", "reserved_cash", "nav", "peak_nav", mode="before")
    @classmethod
    def _amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @model_validator(mode="after")
    def _valid_projection(self) -> PaperPortfolioProjection:
        if self.available_cash < 0 or self.reserved_cash < 0:
            raise ValueError("cash balances must be nonnegative")
        if self.nav <= 0 or self.peak_nav < self.nav:
            raise ValueError("NAV must be positive and no greater than peak NAV")
        position_ids = [position.instrument_id for position in self.positions]
        if len(position_ids) != len(set(position_ids)):
            raise ValueError("positions must be unique")
        order_ids = [order.order_id for order in self.open_orders]
        if len(order_ids) != len(set(order_ids)):
            raise ValueError("open orders must be unique")
        return self


class OrderCandidate(FrozenRiskModel):
    instrument_id: UUID
    side: OrderSide
    desired_quantity: Decimal
    target_weight: Decimal
    strategy_priority: int = Field(ge=0, le=2)
    reference_close: Decimal
    asset_type: AssetType
    currency: Literal["USD"]
    sector_exposures: tuple[RiskSectorExposure, ...]

    @field_validator("desired_quantity", "target_weight", "reference_close", mode="before")
    @classmethod
    def _amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @model_validator(mode="after")
    def _valid_candidate(self) -> OrderCandidate:
        if self.desired_quantity <= 0:
            raise ValueError("desired quantity must be positive")
        if self.desired_quantity != self.desired_quantity.to_integral_value():
            raise ValueError("desired quantity must be whole shares")
        if not Decimal(0) <= self.target_weight <= Decimal(1):
            raise ValueError("target weight must be between zero and one")
        if self.reference_close <= 0:
            raise ValueError("reference close must be positive")
        sectors = [exposure.sector for exposure in self.sector_exposures]
        if len(sectors) != len(set(sectors)):
            raise ValueError("candidate sector exposures must be unique")
        return self


class RiskRuleEvidence(FrozenRiskModel):
    rule_code: str = Field(min_length=1, max_length=100)
    passed: bool
    limit: Decimal | None = None
    observed: Decimal | None = None
    remaining: Decimal | None = None
    authority_ids: tuple[UUID, ...] = ()

    @field_validator("limit", "observed", "remaining", mode="before")
    @classmethod
    def _optional_amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal | None:
        if value is None:
            return None
        return exact_decimal(value, field=info.field_name or "value")


class RiskEvaluation(FrozenRiskModel):
    instrument_id: UUID
    side: OrderSide
    outcome: RiskOutcome
    desired_quantity: Decimal
    approved_quantity: Decimal
    target_weight: Decimal
    strategy_priority: int = Field(ge=0, le=2)
    reference_close: Decimal
    worst_price: Decimal
    reserve_amount: Decimal
    projected_available_cash: Decimal
    projected_cash_ratio: Decimal
    projected_name_ratio: Decimal
    projected_sector_ratios: tuple[tuple[str, Decimal], ...]
    rules: tuple[RiskRuleEvidence, ...]
    result_hash: Hash64

    @field_validator(
        "desired_quantity",
        "approved_quantity",
        "target_weight",
        "reference_close",
        "worst_price",
        "reserve_amount",
        "projected_available_cash",
        "projected_cash_ratio",
        "projected_name_ratio",
        mode="before",
    )
    @classmethod
    def _amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @field_validator("projected_sector_ratios", mode="before")
    @classmethod
    def _sector_ratios_are_exact(cls, value: object) -> object:
        if not isinstance(value, tuple | list):
            return value
        return tuple(
            (sector, exact_decimal(ratio, field=f"sector ratio {sector}"))
            for sector, ratio in value
        )

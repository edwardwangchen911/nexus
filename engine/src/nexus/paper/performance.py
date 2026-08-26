"""Pure position folding and point-in-time paper performance projection."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from nexus.paper.models import Hash64, exact_decimal

_QUANTUM = Decimal("0.000000000001")


def _quantize(value: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        return value.quantize(_QUANTUM, rounding=ROUND_HALF_EVEN)


def _decimal(value: Decimal) -> str:
    return format(_quantize(value), ".12f")


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PositionAdjustment(_FrozenModel):
    event_id: UUID
    account_id: UUID
    instrument_id: UUID
    event_seq: int = Field(ge=1)
    event_type: Literal["fill", "split", "dividend", "delisting", "correction"]
    quantity_delta: Decimal
    cost_delta: Decimal
    occurred_at: datetime
    payload_hash: Hash64

    @field_validator("quantity_delta", "cost_delta", mode="before")
    @classmethod
    def _exact_amount(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @field_validator("occurred_at")
    @classmethod
    def _aware_time(cls, value: datetime) -> datetime:
        return _utc(value, field="occurred_at")

    @model_validator(mode="after")
    def _whole_quantity(self) -> PositionAdjustment:
        if self.quantity_delta != self.quantity_delta.to_integral_value():
            raise ValueError("quantity delta must be whole shares")
        return self


class PositionProjection(_FrozenModel):
    account_id: UUID
    instrument_id: UUID
    quantity: Decimal
    cost_basis: Decimal
    average_cost: Decimal
    source_fact_hash: Hash64

    @field_validator("quantity", "cost_basis", "average_cost", mode="before")
    @classmethod
    def _exact_amount(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")


class MarkedPosition(_FrozenModel):
    instrument_id: UUID
    quantity: Decimal
    cost_basis: Decimal
    reference_close: Decimal
    price_authority_id: UUID
    price_content_hash: Hash64

    @field_validator("quantity", "cost_basis", "reference_close", mode="before")
    @classmethod
    def _exact_amount(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @model_validator(mode="after")
    def _valid_values(self) -> MarkedPosition:
        if self.quantity < 0 or self.quantity != self.quantity.to_integral_value():
            raise ValueError("quantity must be nonnegative whole shares")
        if self.cost_basis < 0:
            raise ValueError("cost basis must be nonnegative")
        if self.quantity == 0 and self.cost_basis != 0:
            raise ValueError("zero quantity must have zero cost basis")
        if self.reference_close <= 0:
            raise ValueError("reference close must be positive")
        return self


class PaperPerformance(_FrozenModel):
    account_id: UUID
    as_of: datetime
    cash: Decimal
    market_value: Decimal
    nav: Decimal
    peak_nav: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    fees: Decimal
    drawdown_ratio: Decimal
    content_hash: Hash64

    @field_validator(
        "cash",
        "market_value",
        "nav",
        "peak_nav",
        "realized_pnl",
        "unrealized_pnl",
        "fees",
        "drawdown_ratio",
        mode="before",
    )
    @classmethod
    def _exact_amount(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @field_validator("as_of")
    @classmethod
    def _aware_time(cls, value: datetime) -> datetime:
        return _utc(value, field="as_of")


def fold_position_adjustments(
    adjustments: tuple[PositionAdjustment, ...],
) -> PositionProjection:
    if not adjustments:
        raise ValueError("position fold requires at least one adjustment")
    first = adjustments[0]
    quantity = Decimal(0)
    cost_basis = Decimal(0)
    fact_hashes: list[str] = []
    event_ids: set[UUID] = set()
    for expected_sequence, event in enumerate(adjustments, start=1):
        if event.account_id != first.account_id or event.instrument_id != first.instrument_id:
            raise ValueError("position adjustments must share account and instrument")
        if event.event_seq != expected_sequence:
            raise ValueError("position adjustment sequence must be continuous")
        if event.event_id in event_ids:
            raise ValueError("position adjustment ids must be unique")
        event_ids.add(event.event_id)
        quantity = _quantize(quantity + event.quantity_delta)
        cost_basis = _quantize(cost_basis + event.cost_delta)
        if quantity < 0:
            raise ValueError("position adjustment creates negative quantity")
        if cost_basis < 0:
            raise ValueError("position adjustment creates negative cost")
        if quantity == 0 and cost_basis != 0:
            raise ValueError("zero quantity must have zero cost basis")
        fact_hashes.append(event.payload_hash)
    average_cost = Decimal(0) if quantity == 0 else _quantize(cost_basis / quantity)
    source_hash = _hash(
        {
            "schema": "paper_position_projection_sha256_v1",
            "account_id": str(first.account_id),
            "instrument_id": str(first.instrument_id),
            "quantity": _decimal(quantity),
            "cost_basis": _decimal(cost_basis),
            "facts": fact_hashes,
        }
    )
    return PositionProjection(
        account_id=first.account_id,
        instrument_id=first.instrument_id,
        quantity=quantity,
        cost_basis=cost_basis,
        average_cost=average_cost,
        source_fact_hash=source_hash,
    )


def allocate_average_cost(position: PositionProjection, sell_quantity: Decimal) -> Decimal:
    quantity = exact_decimal(sell_quantity, field="sell quantity")
    if quantity <= 0 or quantity != quantity.to_integral_value():
        raise ValueError("sell quantity must be positive whole shares")
    if quantity > position.quantity:
        raise ValueError("sell quantity exceeds position")
    if quantity == position.quantity:
        return position.cost_basis
    return _quantize(position.cost_basis * quantity / position.quantity)


def project_paper_performance(
    *,
    account_id: UUID,
    as_of: datetime,
    cash: Decimal,
    realized_pnl: Decimal,
    fees: Decimal,
    previous_peak_nav: Decimal | None,
    positions: tuple[MarkedPosition, ...],
    source_fact_hashes: tuple[Hash64, ...],
    require_positions: bool = False,
) -> PaperPerformance:
    effective_at = _utc(as_of, field="as_of")
    checked_cash = exact_decimal(cash, field="cash")
    checked_realized = exact_decimal(realized_pnl, field="realized pnl")
    checked_fees = exact_decimal(fees, field="fees")
    if checked_cash < 0:
        raise ValueError("cash must be nonnegative")
    if checked_fees < 0:
        raise ValueError("fees must be nonnegative")
    if require_positions and not positions:
        raise ValueError("performance requires at least one position")
    if len({position.instrument_id for position in positions}) != len(positions):
        raise ValueError("marked positions must be unique by instrument")
    if not source_fact_hashes:
        raise ValueError("performance requires source fact hashes")
    if any(
        len(content_hash) != 64
        or any(character not in "0123456789abcdef" for character in content_hash)
        for content_hash in source_fact_hashes
    ):
        raise ValueError("performance source fact hashes must be canonical sha256")
    if len(set(source_fact_hashes)) != len(source_fact_hashes):
        raise ValueError("performance source fact hashes must be unique")

    ordered_positions = tuple(sorted(positions, key=lambda item: item.instrument_id.int))
    market_value = _quantize(
        sum(
            (position.quantity * position.reference_close for position in ordered_positions),
            start=Decimal(0),
        )
    )
    unrealized = _quantize(
        sum(
            (
                position.quantity * position.reference_close - position.cost_basis
                for position in ordered_positions
            ),
            start=Decimal(0),
        )
    )
    nav = _quantize(checked_cash + market_value)
    if nav <= 0:
        raise ValueError("nav must be positive")
    prior_peak = (
        None
        if previous_peak_nav is None
        else exact_decimal(previous_peak_nav, field="previous peak nav")
    )
    if prior_peak is not None and prior_peak <= 0:
        raise ValueError("previous peak nav must be positive")
    peak_nav = nav if prior_peak is None else max(nav, prior_peak)
    drawdown = _quantize((peak_nav - nav) / peak_nav)
    payload = {
        "schema": "paper_performance_sha256_v1",
        "account_id": str(account_id),
        "as_of": effective_at.isoformat().replace("+00:00", "Z"),
        "cash": _decimal(checked_cash),
        "market_value": _decimal(market_value),
        "nav": _decimal(nav),
        "peak_nav": _decimal(peak_nav),
        "realized_pnl": _decimal(checked_realized),
        "unrealized_pnl": _decimal(unrealized),
        "fees": _decimal(checked_fees),
        "drawdown_ratio": _decimal(drawdown),
        "positions": [
            {
                "instrument_id": str(position.instrument_id),
                "quantity": _decimal(position.quantity),
                "cost_basis": _decimal(position.cost_basis),
                "reference_close": _decimal(position.reference_close),
                "price_authority_id": str(position.price_authority_id),
                "price_content_hash": position.price_content_hash,
            }
            for position in ordered_positions
        ],
        "source_fact_hashes": sorted(source_fact_hashes),
    }
    return PaperPerformance(
        account_id=account_id,
        as_of=effective_at,
        cash=_quantize(checked_cash),
        market_value=market_value,
        nav=nav,
        peak_nav=_quantize(peak_nav),
        realized_pnl=_quantize(checked_realized),
        unrealized_pnl=unrealized,
        fees=_quantize(checked_fees),
        drawdown_ratio=drawdown,
        content_hash=_hash(payload),
    )

"""Deterministic hard-risk revalidation for external simulation intents."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from nexus.broker_simulation.domain import BrokerOrderDocument


def _decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, float):
        raise ValueError(f"{field} must not contain a binary float")
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"{field} must be a Decimal")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BrokerRiskPosition(_FrozenModel):
    instrument_id: UUID
    quantity: Decimal

    @field_validator("quantity", mode="before")
    @classmethod
    def _quantity_is_exact(cls, value: object) -> Decimal:
        result = _decimal(value, field="position quantity")
        if result < 0:
            raise ValueError("position quantity cannot be negative")
        return result


class BrokerRiskInstrument(_FrozenModel):
    instrument_id: UUID
    asset_type: Literal["common_stock", "ETF"]
    sector: str
    whitelisted: bool
    existing_notional: Decimal

    @field_validator("existing_notional", mode="before")
    @classmethod
    def _notional_is_exact(cls, value: object) -> Decimal:
        result = _decimal(value, field="existing notional")
        if result < 0:
            raise ValueError("existing notional cannot be negative")
        return result


class BrokerRiskContext(_FrozenModel):
    available_cash: Decimal
    nav: Decimal
    drawdown_locked: bool
    minimum_cash_ratio: Decimal
    single_name_limit: Decimal
    sector_limit: Decimal
    positions: tuple[BrokerRiskPosition, ...]
    instruments: tuple[BrokerRiskInstrument, ...]
    sector_notionals: dict[str, Decimal]

    @field_validator(
        "available_cash",
        "nav",
        "minimum_cash_ratio",
        "single_name_limit",
        "sector_limit",
        mode="before",
    )
    @classmethod
    def _amount_is_exact(cls, value: object, info: object) -> Decimal:
        field_name = getattr(info, "field_name", "risk value")
        return _decimal(value, field=field_name)

    @field_validator("sector_notionals", mode="before")
    @classmethod
    def _sector_values_are_exact(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise ValueError("sector notionals must be a mapping")
        return {key: _decimal(item, field="sector notional") for key, item in value.items()}

    @model_validator(mode="after")
    def _context_is_valid(self) -> BrokerRiskContext:
        if self.available_cash < 0 or self.nav <= 0:
            raise ValueError("risk cash and NAV are invalid")
        if not (
            Decimal(0) <= self.minimum_cash_ratio <= Decimal(1)
            and Decimal(0) < self.single_name_limit <= Decimal(1)
            and Decimal(0) < self.sector_limit <= Decimal(1)
            and self.single_name_limit <= self.sector_limit
        ):
            raise ValueError("risk ratios are invalid")
        position_ids = tuple(item.instrument_id for item in self.positions)
        instrument_ids = tuple(item.instrument_id for item in self.instruments)
        if len(position_ids) != len(set(position_ids)):
            raise ValueError("risk positions must be unique")
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("risk instruments must be unique")
        if any(not sector or notional < 0 for sector, notional in self.sector_notionals.items()):
            raise ValueError("sector notionals are invalid")
        return self


class BrokerRiskDecision(_FrozenModel):
    passed: bool
    reason_codes: tuple[str, ...]
    projected_cash: Decimal


def evaluate_broker_order_batch(
    orders: tuple[BrokerOrderDocument, ...],
    context: BrokerRiskContext,
) -> BrokerRiskDecision:
    """Evaluate a fixed whole-share DAY-limit batch without probabilistic inputs."""
    reasons: list[str] = []
    if context.drawdown_locked:
        reasons.append("DRAWDOWN_LOCKED")
    if not orders or len(orders) > 100:
        reasons.append("ORDER_COUNT_INVALID")
    positions = {item.instrument_id: item.quantity for item in context.positions}
    instruments = {item.instrument_id: item for item in context.instruments}
    projected_names = {
        item.instrument_id: item.existing_notional for item in context.instruments
    }
    projected_sectors = dict(context.sector_notionals)
    projected_cash = context.available_cash
    for order in orders:
        instrument = instruments.get(order.instrument_id)
        if instrument is None or not instrument.whitelisted:
            reasons.append("INSTRUMENT_NOT_WHITELISTED")
            continue
        notional = order.limit_price * order.quantity
        if order.side == "buy":
            projected_cash -= notional
            projected_names[order.instrument_id] = (
                projected_names.get(order.instrument_id, Decimal(0)) + notional
            )
            projected_sectors[instrument.sector] = (
                projected_sectors.get(instrument.sector, Decimal(0)) + notional
            )
        else:
            if Decimal(order.quantity) > positions.get(order.instrument_id, Decimal(0)):
                reasons.append("LONG_ONLY_POSITION_EXCEEDED")
            projected_names[order.instrument_id] = max(
                Decimal(0), projected_names.get(order.instrument_id, Decimal(0)) - notional
            )
            projected_sectors[instrument.sector] = max(
                Decimal(0), projected_sectors.get(instrument.sector, Decimal(0)) - notional
            )
    if projected_cash < 0:
        reasons.append("INSUFFICIENT_CASH")
    if projected_cash / context.nav < context.minimum_cash_ratio:
        reasons.append("MINIMUM_CASH_RATIO_BREACHED")
    if any(value / context.nav > context.single_name_limit for value in projected_names.values()):
        reasons.append("SINGLE_NAME_LIMIT_EXCEEDED")
    if any(value / context.nav > context.sector_limit for value in projected_sectors.values()):
        reasons.append("SECTOR_LIMIT_EXCEEDED")
    stable_reasons = tuple(dict.fromkeys(reasons))
    return BrokerRiskDecision(
        passed=not stable_reasons,
        reason_codes=stable_reasons,
        projected_cash=projected_cash,
    )

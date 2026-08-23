"""Canonical broker-fact hashing and two-read reconciliation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.providers.broker.base import (
    BrokerOrderFact,
    BrokerPosition,
    CashBalance,
    SourceObservation,
)

POSITION_HASH_VERSION = "broker_position_hash_v1"
ORDER_HASH_VERSION = "broker_order_hash_v1"
_FIVE_BASIS_POINTS = Decimal("0.0005")
_ONE_ORIGINAL_CURRENCY_UNIT = Decimal("1")


@dataclass(frozen=True)
class DerivedCurrencyNav:
    """Original-currency NAV derived from one reviewed Provider read set."""

    currency: str
    value: Decimal
    provider_as_of: datetime
    source_request_ids: tuple[str, ...]


@dataclass(frozen=True)
class BrokerReadSet:
    cash: tuple[CashBalance, ...]
    positions: tuple[BrokerPosition, ...]
    open_orders: tuple[BrokerOrderFact, ...]
    completed_at: datetime | None
    complete: bool = True
    source_request_ids: tuple[str, ...] = ()
    source_observations: tuple[SourceObservation, ...] = ()
    nav_by_currency: tuple[DerivedCurrencyNav, ...] = ()


@dataclass(frozen=True)
class ReconciliationResult:
    consistent: bool
    reasons: tuple[str, ...]


class DuplicateBrokerFact(ValueError):
    """A Provider returned more than one fact for one canonical identity."""


def canonical_position_hash(positions: tuple[BrokerPosition, ...]) -> str:
    """Hash versioned position facts in canonical total order."""
    identities: set[UUID] = set()
    rows: list[tuple[object, ...]] = []
    for item in positions:
        if item.instrument_id in identities:
            raise DuplicateBrokerFact("duplicate position identity")
        identities.add(item.instrument_id)
        rows.append(
            (
                str(item.instrument_id),
                item.currency,
                _decimal_string(item.quantity),
                _decimal_string(item.available_quantity),
                _decimal_string(item.frozen_quantity),
                _optional_decimal_string(item.avg_cost),
                _optional_decimal_string(item.diluted_cost),
            )
        )
    return _canonical_hash(POSITION_HASH_VERSION, sorted(rows))


def canonical_order_hash(orders: tuple[BrokerOrderFact, ...]) -> str:
    """Hash versioned open-order facts in canonical total order."""
    identities: set[str] = set()
    rows: list[tuple[object, ...]] = []
    for item in orders:
        if item.provider_order_id in identities:
            raise DuplicateBrokerFact("duplicate order identity")
        identities.add(item.provider_order_id)
        rows.append(
            (
                item.provider_order_id,
                str(item.instrument_id),
                item.side,
                _decimal_string(item.quantity),
                _decimal_string(item.filled_quantity),
                _optional_decimal_string(item.price),
                item.status,
                _optional_utc_string(item.updated_at),
            )
        )
    return _canonical_hash(ORDER_HASH_VERSION, sorted(rows))


def compare_read_sets(
    a: BrokerReadSet,
    b: BrokerReadSet,
) -> ReconciliationResult:
    """Compare both cash balances and canonical hashes, failing closed on NAV gaps."""
    reasons: list[str] = []
    cash_a = cash_by_currency(a.cash)
    cash_b = cash_by_currency(b.cash)
    currencies_a = set(cash_a)
    currencies_b = set(cash_b)
    if currencies_a != currencies_b:
        reasons.append("cash_currencies")
    currencies = currencies_a | currencies_b
    nav_a = _nav_by_currency(a.nav_by_currency)
    nav_b = _nav_by_currency(b.nav_by_currency)
    missing_nav = currencies - (nav_a.keys() & nav_b.keys())
    reasons.extend(f"nav:{currency}" for currency in sorted(missing_nav))
    if (set(nav_a) | set(nav_b)) - currencies or set(nav_a) != set(nav_b):
        reasons.append("nav_currencies")
    for currency in sorted(currencies_a & currencies_b):
        left_nav = nav_a.get(currency)
        right_nav = nav_b.get(currency)
        if left_nav is None or right_nav is None:
            if currency not in missing_nav:
                reasons.append(f"nav:{currency}")
            continue
        tolerance = max(
            _ONE_ORIGINAL_CURRENCY_UNIT,
            min(abs(left_nav.value), abs(right_nav.value)) * _FIVE_BASIS_POINTS,
        )
        left = cash_a[currency]
        right = cash_b[currency]
        if abs(left.cash_balance - right.cash_balance) > tolerance:
            reasons.append(f"cash:{currency}")
        if abs(left.available_cash - right.available_cash) > tolerance:
            reasons.append(f"available_cash:{currency}")
    if canonical_position_hash(a.positions) != canonical_position_hash(b.positions):
        reasons.append("positions")
    if canonical_order_hash(a.open_orders) != canonical_order_hash(b.open_orders):
        reasons.append("open_orders")
    return ReconciliationResult(consistent=not reasons, reasons=tuple(reasons))


def cash_by_currency(cash: tuple[CashBalance, ...]) -> dict[str, CashBalance]:
    result: dict[str, CashBalance] = {}
    for item in cash:
        if item.currency in result:
            raise DuplicateBrokerFact("duplicate cash currency")
        result[item.currency] = item
    return result


def _nav_by_currency(values: tuple[DerivedCurrencyNav, ...]) -> dict[str, DerivedCurrencyNav]:
    result: dict[str, DerivedCurrencyNav] = {}
    for value in values:
        if value.currency in result:
            raise DuplicateBrokerFact("duplicate derived NAV currency")
        if not value.value.is_finite() or not value.source_request_ids:
            continue
        result[value.currency] = value
    return result


def _decimal_string(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("broker decimals must be finite")
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _optional_decimal_string(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_string(value)


def _optional_utc_string(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("broker time must be timezone-aware")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _canonical_hash(version: str, rows: list[tuple[object, ...]]) -> str:
    payload = json.dumps(
        {"version": version, "facts": rows},
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

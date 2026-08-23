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

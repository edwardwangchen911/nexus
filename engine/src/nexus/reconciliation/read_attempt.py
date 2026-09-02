"""Bounded six-step broker read attempt."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from nexus.providers.broker.base import (
    BrokerOrderFact,
    BrokerPage,
    BrokerPosition,
    BrokerProvider,
    CashBalance,
    SourceObservation,
)
from nexus.reconciliation.facts import BrokerReadSet, DerivedCurrencyNav, cash_by_currency
from nexus.reconciliation.pagination import (
    PAGE_LIMIT,
    AtomicWindowInvalidated,
    collect_pages,
    require_before_deadline,
)


@dataclass
class AttemptProgress:
    attempt: int
    phase: str = "cash_a"
    current_fact_obtained: bool = False
    latest_provider_as_of: datetime | None = None
    source_request_ids: list[str] = field(default_factory=list)
    source_observations: list[dict[str, object]] = field(default_factory=list)
    raw_facts: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    derived_nav: dict[str, tuple[dict[str, object], ...]] = field(default_factory=dict)

    def observe(self, page: BrokerPage[Any], *, phase: str) -> None:
        self.phase = phase
        self.current_fact_obtained = self.current_fact_obtained or bool(page.items)
        fact_times = tuple(
            value
            for item in page.items
            for value in (
                getattr(item, "provider_as_of", None),
                getattr(item, "updated_at", None),
            )
            if isinstance(value, datetime)
        )
        if fact_times:
            latest = max(fact_times)
            self.latest_provider_as_of = (
                latest
                if self.latest_provider_as_of is None
                else max(self.latest_provider_as_of, latest)
            )
        self.source_request_ids.append(page.source_request_id)
        self.source_observations.append(
            {
                "phase": phase,
                "source_request_id": page.source_request_id,
                "response_as_of": page.as_of.isoformat(),
                "quality": page.quality,
                "time_basis": page.time_basis,
            }
        )
        phase_facts = self.raw_facts.setdefault(phase, [])
        for item in page.items:
            if not isinstance(item, BaseModel):
                raise AtomicWindowInvalidated("provider fact schema is invalid")
            phase_facts.append(item.model_dump(mode="json"))

    def observe_nav(self, label: str, values: tuple[DerivedCurrencyNav, ...]) -> None:
        self.derived_nav[label] = tuple(
            {
                "currency": value.currency,
                "value": str(value.value),
                "provider_as_of": (
                    value.provider_as_of.isoformat() if value.provider_as_of else None
                ),
                **({"observed_at": value.observed_at.isoformat()} if value.observed_at else {}),
                "source_request_ids": value.source_request_ids,
            }
            for value in values
        )

    def trace(self, *, completed: bool, reason: str) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "phase": self.phase,
            "completed": completed,
            "current_fact_obtained": self.current_fact_obtained,
            "reason": reason,
            "source_request_ids": tuple(self.source_request_ids),
            "source_observations": tuple(self.source_observations),
            "raw_facts": {
                **{phase: tuple(facts) for phase, facts in self.raw_facts.items()},
                "_derived_nav": dict(self.derived_nav),
            },
        }


async def read_atomic_window(
    account_id: UUID,
    provider: BrokerProvider,
    sync_run_id: UUID,
    progress: AttemptProgress,
    *,
    attempt_deadline: float,
    monotonic: Callable[[], float],
) -> tuple[BrokerReadSet, BrokerReadSet]:
    cash_a_page = await provider.read_cash(account_id, sync_run_id=sync_run_id)
    _validate_cash_page(cash_a_page)
    progress.observe(cash_a_page, phase="cash_a")
    require_before_deadline(attempt_deadline, monotonic)
    positions_a, positions_a_pages = await collect_pages(
        lambda cursor: provider.read_positions_page(
            account_id, sync_run_id=sync_run_id, cursor=cursor, limit=PAGE_LIMIT
        ),
        identity=lambda item: item.instrument_id,
        observe=lambda page: progress.observe(page, phase="positions_a"),
        attempt_deadline=attempt_deadline,
        monotonic=monotonic,
    )
    orders_a, orders_a_pages = await collect_pages(
        lambda cursor: provider.read_open_orders_page(
            account_id, sync_run_id=sync_run_id, cursor=cursor, limit=PAGE_LIMIT
        ),
        identity=lambda item: item.provider_order_id,
        observe=lambda page: progress.observe(page, phase="orders_a"),
        attempt_deadline=attempt_deadline,
        monotonic=monotonic,
    )
    positions_b, positions_b_pages = await collect_pages(
        lambda cursor: provider.read_positions_page(
            account_id, sync_run_id=sync_run_id, cursor=cursor, limit=PAGE_LIMIT
        ),
        identity=lambda item: item.instrument_id,
        observe=lambda page: progress.observe(page, phase="positions_b"),
        attempt_deadline=attempt_deadline,
        monotonic=monotonic,
    )
    orders_b, orders_b_pages = await collect_pages(
        lambda cursor: provider.read_open_orders_page(
            account_id, sync_run_id=sync_run_id, cursor=cursor, limit=PAGE_LIMIT
        ),
        identity=lambda item: item.provider_order_id,
        observe=lambda page: progress.observe(page, phase="orders_b"),
        attempt_deadline=attempt_deadline,
        monotonic=monotonic,
    )
    cash_b_page = await provider.read_cash(account_id, sync_run_id=sync_run_id)
    _validate_cash_page(cash_b_page)
    progress.observe(cash_b_page, phase="cash_b")
    require_before_deadline(attempt_deadline, monotonic)
    pages_a: tuple[BrokerPage[Any], ...] = (
        cash_a_page,
        *positions_a_pages,
        *orders_a_pages,
    )
    pages_b: tuple[BrokerPage[Any], ...] = (
        *positions_b_pages,
        *orders_b_pages,
        cash_b_page,
    )
    all_pages = (*pages_a, *pages_b)
    if len({page.time_basis for page in all_pages}) != 1:
        raise AtomicWindowInvalidated("broker snapshot time bases differ")
    if all_pages[0].time_basis == "gateway_observed":
        times = [page.as_of for page in all_pages]
        if times != sorted(times) or (times[-1] - times[0]).total_seconds() > 60:
            raise AtomicWindowInvalidated("broker observation window is invalid")
        if len({page.source_request_id for page in all_pages}) != len(all_pages):
            raise AtomicWindowInvalidated("broker observations reuse a request")
    read_a = BrokerReadSet(
        cash=cash_a_page.items,
        positions=positions_a,
        open_orders=orders_a,
        completed_at=_snapshot_as_of(pages_a, cash_a_page.items, positions_a, orders_a),
        complete=_pages_complete(pages_a, cash_a_page.items, positions_a, orders_a),
        source_request_ids=tuple(page.source_request_id for page in pages_a),
        source_observations=_source_observations(pages_a),
        nav_by_currency=_derive_nav(cash_a_page.items, positions_a, cash_a_page, positions_a_pages),
    )
    read_b = BrokerReadSet(
        cash=cash_b_page.items,
        positions=positions_b,
        open_orders=orders_b,
        completed_at=_snapshot_as_of(pages_b, cash_b_page.items, positions_b, orders_b),
        complete=_pages_complete(pages_b, cash_b_page.items, positions_b, orders_b),
        source_request_ids=tuple(page.source_request_id for page in pages_b),
        source_observations=_source_observations(pages_b),
        nav_by_currency=_derive_nav(cash_b_page.items, positions_b, cash_b_page, positions_b_pages),
    )
    progress.observe_nav("read_a", read_a.nav_by_currency)
    progress.observe_nav("read_b", read_b.nav_by_currency)
    return (
        read_a,
        read_b,
    )


def _derive_nav(
    cash: tuple[CashBalance, ...],
    positions: tuple[BrokerPosition, ...],
    cash_page: BrokerPage[CashBalance],
    position_pages: tuple[BrokerPage[BrokerPosition], ...],
) -> tuple[DerivedCurrencyNav, ...]:
    observed = cash_page.time_basis == "gateway_observed"
    nav_pages: tuple[BrokerPage[Any], ...] = (cash_page, *position_pages)
    observed_at = max(page.as_of for page in nav_pages) if observed else None
    cash_map = cash_by_currency(cash)
    positions_by_currency: dict[str, list[BrokerPosition]] = {}
    request_ids_by_currency = {currency: [cash_page.source_request_id] for currency in cash_map}
    for page in position_pages:
        for position in page.items:
            if position.currency not in cash_map:
                raise AtomicWindowInvalidated("position currency lacks reviewed cash balance")
            positions_by_currency.setdefault(position.currency, []).append(position)
            request_ids_by_currency[position.currency].append(page.source_request_id)
    values: list[DerivedCurrencyNav] = []
    for currency, cash_item in sorted(cash_map.items()):
        if cash_item.provider_as_of is None and not observed:
            raise AtomicWindowInvalidated("cash NAV time is unavailable")
        value = cash_item.cash_balance
        fact_times = [cash_item.provider_as_of] if cash_item.provider_as_of is not None else []
        for position in positions_by_currency.get(currency, []):
            if position.current_price is None or (position.provider_as_of is None and not observed):
                raise AtomicWindowInvalidated("position NAV input is unavailable")
            value += position.quantity * position.current_price
            if position.provider_as_of is not None:
                fact_times.append(position.provider_as_of)
        if not value.is_finite():
            raise AtomicWindowInvalidated("derived NAV is non-finite")
        values.append(
            DerivedCurrencyNav(
                currency=currency,
                value=value,
                provider_as_of=None if observed else max(fact_times),
                source_request_ids=tuple(dict.fromkeys(request_ids_by_currency[currency])),
                observed_at=observed_at,
            )
        )
    return tuple(values)


def _validate_cash_page(page: BrokerPage[CashBalance]) -> None:
    if page.has_more or page.next_cursor is not None:
        raise AtomicWindowInvalidated("cash summary must not paginate")
    cash_by_currency(page.items)


def _pages_complete(
    pages: tuple[BrokerPage[Any], ...],
    cash: tuple[CashBalance, ...],
    positions: tuple[BrokerPosition, ...],
    orders: tuple[BrokerOrderFact, ...],
) -> bool:
    observed = all(page.time_basis == "gateway_observed" for page in pages)
    return (
        all(page.quality == "PASS" for page in pages)
        and bool(cash)
        and (observed or all(item.provider_as_of is not None for item in cash))
        and all(
            position.current_price is not None and (observed or position.provider_as_of is not None)
            for position in positions
        )
        and (observed or all(order.updated_at is not None for order in orders))
    )


def _snapshot_as_of(
    pages: tuple[BrokerPage[Any], ...],
    cash: tuple[CashBalance, ...],
    positions: tuple[BrokerPosition, ...],
    orders: tuple[BrokerOrderFact, ...],
) -> datetime | None:
    if all(page.time_basis == "gateway_observed" for page in pages):
        return max(page.as_of for page in pages)
    values = (
        *(item.provider_as_of for item in cash if item.provider_as_of is not None),
        *(item.provider_as_of for item in positions if item.provider_as_of is not None),
        *(item.updated_at for item in orders if item.updated_at is not None),
    )
    return max(values) if values else None


def _source_observations(
    pages: tuple[BrokerPage[Any], ...],
) -> tuple[SourceObservation, ...]:
    return tuple(
        SourceObservation(
            source_request_id=page.source_request_id,
            response_as_of=page.as_of,
            quality=page.quality,
            time_basis=page.time_basis,
        )
        for page in pages
    )

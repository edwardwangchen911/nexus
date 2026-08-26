"""Deterministic paper-account projections folded from append-only facts."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.paper.ledger import balance_ledger
from nexus.paper.models import (
    InternalOrderStatus,
    Ledger,
    LedgerAccount,
    LedgerEntry,
    LedgerTransaction,
    OrderEventInput,
    PaperOrder,
    Reservation,
    ReservationEvent,
    ReservationEventType,
    exact_decimal,
)
from nexus.paper.orders import current_order_status, fold_reservation_events
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperDrawdownLockEventRecord,
    PaperFillRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionAdjustmentEventRecord,
    PaperPositionSnapshotRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
)
from nexus.risk.canonical import canonical_paper_projection_hash
from nexus.risk.models import (
    DrawdownState,
    PaperOpenOrder,
    PaperPortfolioProjection,
    PaperPositionBalance,
)


class DrawdownLockEvent(BaseModel):
    """One append-only drawdown lock transition used by the projection fold."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: UUID
    event_seq: int = Field(ge=1)
    from_state: DrawdownState | None
    to_state: DrawdownState
    drawdown_ratio: Decimal
    user_acknowledged: bool
    reason_code: str = Field(min_length=1, max_length=100)
    occurred_at: datetime
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("drawdown_ratio", mode="before")
    @classmethod
    def _ratio_is_exact(cls, value: object) -> Decimal:
        return exact_decimal(value, field="drawdown_ratio")

    @field_validator("occurred_at")
    @classmethod
    def _time_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("drawdown event time must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _ratio_is_bounded(self) -> DrawdownLockEvent:
        if not Decimal(0) <= self.drawdown_ratio <= Decimal(1):
            raise ValueError("drawdown ratio must be between zero and one")
        return self


def fold_drawdown_events(events: Sequence[DrawdownLockEvent]) -> DrawdownState:
    """Validate and fold the exact paper drawdown chain."""
    if not events:
        raise ValueError("drawdown event chain is required")
    state: DrawdownState | None = None
    event_ids: set[UUID] = set()
    previous_time: datetime | None = None
    for expected_seq, event in enumerate(events, start=1):
        if event.event_id in event_ids:
            raise ValueError("drawdown event ids must be unique")
        event_ids.add(event.event_id)
        if event.event_seq != expected_seq:
            raise ValueError("drawdown event sequence must be continuous")
        if previous_time is not None and event.occurred_at < previous_time:
            raise ValueError("drawdown event time must be monotonic")
        previous_time = event.occurred_at
        if expected_seq == 1:
            if (
                event.from_state is not None
                or event.to_state != "unlocked"
                or event.drawdown_ratio != 0
                or event.user_acknowledged
            ):
                raise ValueError("drawdown genesis event is invalid")
            state = "unlocked"
            continue
        if event.from_state != state:
            raise ValueError("drawdown event from_state must match prior state")
        allowed = (
            (
                state == "unlocked"
                and event.to_state == "locked"
                and event.drawdown_ratio >= Decimal("0.08")
                and not event.user_acknowledged
            )
            or (
                state == "locked"
                and event.to_state == "recovery_eligible"
                and event.drawdown_ratio < Decimal("0.06")
                and not event.user_acknowledged
            )
            or (
                state == "recovery_eligible"
                and event.to_state == "locked"
                and event.drawdown_ratio >= Decimal("0.06")
                and not event.user_acknowledged
            )
            or (
                state == "recovery_eligible"
                and event.to_state == "unlocked"
                and event.drawdown_ratio < Decimal("0.06")
                and event.user_acknowledged
            )
        )
        if not allowed:
            raise ValueError("drawdown transition is invalid")
        state = event.to_state
    if state is None:
        raise ValueError("drawdown event chain is empty")
    return state


def _source_decimal(value: Decimal) -> str:
    return format(exact_decimal(value, field="projection source fact"), ".12f")


def _source_time(value: datetime) -> str:
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("projection source fact time must use UTC")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def canonical_paper_source_facts_hash(
    *,
    ledger: Ledger,
    orders: Sequence[PaperOrder],
    reservations: Sequence[Reservation],
    drawdown_events: Sequence[DrawdownLockEvent],
    additional_source_facts: Sequence[tuple[str, str]] = (),
) -> str:
    """Hash every append-only fact used by a paper projection, excluding row IDs."""
    order_keys = {order.order_id: order.client_order_id for order in orders}
    if len(order_keys) != len(orders):
        raise ValueError("paper order ids must be unique")
    additional = []
    for kind, content_hash in additional_source_facts:
        if not kind or len(content_hash) != 64 or any(
            character not in "0123456789abcdef" for character in content_hash
        ):
            raise ValueError("additional projection source fact is invalid")
        additional.append({"kind": kind, "content_hash": content_hash})
    payload = {
        "schema": "paper_projection_source_facts_sha256_v1",
        "ledger_accounts": [
            {
                "code": account.code,
                "normal_side": account.normal_side,
                "currency": account.currency,
            }
            for account in sorted(ledger.accounts, key=lambda item: item.code)
        ],
        "ledger_transactions": sorted(
            transaction.transaction_hash for transaction in ledger.transactions
        ),
        "orders": [
            {
                "client_order_id": order.client_order_id,
                "instrument_id": str(order.instrument_id).lower(),
                "side": order.side,
                "quantity": _source_decimal(order.quantity),
                "worst_price": (
                    _source_decimal(order.worst_price)
                    if order.worst_price is not None
                    else None
                ),
                "events": [
                    {
                        "event_seq": event.event_seq,
                        "occurred_at": _source_time(event.occurred_at),
                        "from_status": event.from_status,
                        "to_status": event.to_status,
                        "actor_kind": event.actor_kind,
                        "reason_code": event.reason_code,
                        "payload_hash": event.payload_hash,
                    }
                    for event in sorted(order.events, key=lambda item: item.event_seq)
                ],
            }
            for order in sorted(orders, key=lambda item: item.client_order_id)
        ],
        "reservations": [
            {
                "client_order_id": order_keys.get(reservation.order_id),
                "reservation_type": reservation.reservation_type,
                "currency": reservation.currency,
                "instrument_id": (
                    str(reservation.instrument_id).lower()
                    if reservation.instrument_id is not None
                    else None
                ),
                "amount": _source_decimal(reservation.amount),
                "events": [
                    {
                        "event_seq": event.event_seq,
                        "event_type": event.event_type,
                        "amount": _source_decimal(event.amount),
                        "occurred_at": _source_time(event.occurred_at),
                        "payload_hash": event.payload_hash,
                    }
                    for event in sorted(reservation.events, key=lambda item: item.event_seq)
                ],
            }
            for reservation in sorted(
                reservations,
                key=lambda item: (order_keys.get(item.order_id, ""), item.reservation_type),
            )
        ],
        "drawdown_events": [
            {
                "event_seq": event.event_seq,
                "from_state": event.from_state,
                "to_state": event.to_state,
                "drawdown_ratio": _source_decimal(event.drawdown_ratio),
                "user_acknowledged": event.user_acknowledged,
                "reason_code": event.reason_code,
                "occurred_at": _source_time(event.occurred_at),
                "payload_hash": event.payload_hash,
            }
            for event in sorted(drawdown_events, key=lambda item: item.event_seq)
        ],
        "additional_source_facts": sorted(
            additional,
            key=lambda item: (item["kind"], item["content_hash"]),
        ),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _open_orders(
    *,
    account_id: UUID,
    orders: Sequence[PaperOrder],
    reservations: Sequence[Reservation],
) -> tuple[PaperOpenOrder, ...]:
    order_by_id: dict[UUID, PaperOrder] = {}
    status_by_id: dict[UUID, str | None] = {}
    for order in orders:
        if order.order_id in order_by_id:
            raise ValueError("paper order ids must be unique")
        if order.account_id != account_id:
            raise ValueError("paper order belongs to another account")
        order_by_id[order.order_id] = order
        status_by_id[order.order_id] = current_order_status(order)

    reservation_by_order: dict[UUID, tuple[Reservation, Decimal]] = {}
    for reservation in reservations:
        if reservation.order_id in reservation_by_order:
            raise ValueError("paper order has duplicate reservations")
        if reservation.account_id != account_id:
            raise ValueError("reservation belongs to another account")
        matching_order = order_by_id.get(reservation.order_id)
        status = status_by_id.get(reservation.order_id)
        if matching_order is None or status not in {
            "risk_approved",
            "submitted",
            "partially_filled",
            "reconciling",
        }:
            raise ValueError("reservation has no matching open order")
        outstanding = fold_reservation_events(reservation)
        if outstanding <= 0:
            raise ValueError("open order reservation must remain positive")
        if matching_order.side == "buy" and reservation.reservation_type != "cash":
            raise ValueError("buy order requires a cash reservation")
        if matching_order.side == "sell" and (
            reservation.reservation_type != "position"
            or reservation.instrument_id != matching_order.instrument_id
        ):
            raise ValueError("sell order requires a matching position reservation")
        reservation_by_order[reservation.order_id] = (reservation, outstanding)

    open_orders: list[PaperOpenOrder] = []
    for order_id, order in order_by_id.items():
        status = status_by_id[order_id]
        if status not in {
            "risk_approved",
            "submitted",
            "partially_filled",
            "reconciling",
        }:
            continue
        reservation_pair = reservation_by_order.get(order_id)
        if reservation_pair is None:
            raise ValueError("open order requires a reservation")
        if order.worst_price is None:
            raise ValueError("open order requires a worst price")
        reservation, outstanding = reservation_pair
        open_orders.append(
            PaperOpenOrder(
                order_id=order.order_id,
                instrument_id=order.instrument_id,
                side=order.side,
                quantity=order.quantity,
                worst_price=order.worst_price,
                reserved_cash=outstanding if reservation.reservation_type == "cash" else Decimal(0),
                reserved_quantity=(
                    outstanding if reservation.reservation_type == "position" else Decimal(0)
                ),
                status=cast(
                    Literal[
                        "risk_approved",
                        "submitted",
                        "partially_filled",
                        "reconciling",
                    ],
                    status,
                ),
            )
        )
    return tuple(sorted(open_orders, key=lambda order: order.order_id.int))


def build_paper_projection(
    *,
    account_id: UUID,
    portfolio_id: UUID,
    as_of: datetime,
    ledger: Ledger,
    orders: Sequence[PaperOrder],
    reservations: Sequence[Reservation],
    positions: Sequence[PaperPositionBalance],
    peak_nav: Decimal,
    drawdown_events: Sequence[DrawdownLockEvent],
    additional_source_facts: Sequence[tuple[str, str]] = (),
) -> PaperPortfolioProjection:
    """Fold one coherent transaction snapshot into the sole risk account view."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("projection time must be timezone-aware")
    if ledger.paper_account_id != account_id:
        raise ValueError("ledger belongs to another paper account")
    balances = balance_ledger(ledger)
    available_cash = balances["asset.cash.available.usd"]
    reserved_cash = balances["asset.cash.reserved.usd"]
    open_orders = _open_orders(
        account_id=account_id,
        orders=orders,
        reservations=reservations,
    )
    open_reserved_cash = sum((order.reserved_cash for order in open_orders), start=Decimal(0))
    if open_reserved_cash != reserved_cash:
        raise ValueError("reserved cash does not reconcile to reservation events")

    position_tuple = tuple(sorted(positions, key=lambda position: position.instrument_id.int))
    if len({position.instrument_id for position in position_tuple}) != len(position_tuple):
        raise ValueError("positions must be unique")
    reserved_by_instrument: dict[UUID, Decimal] = defaultdict(Decimal)
    for order in open_orders:
        if order.reserved_quantity > 0:
            reserved_by_instrument[order.instrument_id] += order.reserved_quantity
    if any(
        reserved_by_instrument.get(position.instrument_id, Decimal(0)) != position.reserved_quantity
        for position in position_tuple
    ) or set(reserved_by_instrument) - {position.instrument_id for position in position_tuple}:
        raise ValueError("position reservations do not reconcile")

    nav = (
        available_cash
        + reserved_cash
        + sum(
            (position.quantity * position.reference_close for position in position_tuple),
            start=Decimal(0),
        )
    )
    peak = exact_decimal(peak_nav, field="peak_nav")
    state = fold_drawdown_events(drawdown_events)
    source_facts_hash = canonical_paper_source_facts_hash(
        ledger=ledger,
        orders=orders,
        reservations=reservations,
        drawdown_events=drawdown_events,
        additional_source_facts=additional_source_facts,
    )
    draft = PaperPortfolioProjection(
        account_id=account_id,
        portfolio_id=portfolio_id,
        as_of=as_of,
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=available_cash,
        reserved_cash=reserved_cash,
        positions=position_tuple,
        open_orders=open_orders,
        nav=nav,
        peak_nav=peak,
        drawdown_state=state,
        source_facts_hash=source_facts_hash,
        projection_hash="0" * 64,
    )
    return draft.model_copy(update={"projection_hash": canonical_paper_projection_hash(draft)})


async def load_paper_projection(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
) -> PaperPortfolioProjection:
    """Load and fold one account from a caller-owned transaction snapshot."""
    account = await session.get(PaperAccountRecord, account_id)
    if account is None:
        raise ValueError("paper account is missing")
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("projection time must be timezone-aware")

    ledger_account_rows = tuple(
        (
            await session.scalars(
                select(PaperLedgerAccountRecord)
                .where(PaperLedgerAccountRecord.paper_account_id == account_id)
                .order_by(PaperLedgerAccountRecord.code)
            )
        ).all()
    )
    transaction_rows = tuple(
        (
            await session.scalars(
                select(PaperLedgerTransactionRecord)
                .where(
                    PaperLedgerTransactionRecord.account_id == account_id,
                    PaperLedgerTransactionRecord.occurred_at <= as_of,
                )
                .order_by(PaperLedgerTransactionRecord.occurred_at)
            )
        ).all()
    )
    transaction_ids = [row.id for row in transaction_rows]
    entry_rows = (
        tuple(
            (
                await session.scalars(
                    select(PaperLedgerEntryRecord)
                    .where(PaperLedgerEntryRecord.transaction_id.in_(transaction_ids))
                    .order_by(
                        PaperLedgerEntryRecord.transaction_id,
                        PaperLedgerEntryRecord.entry_seq,
                    )
                )
            ).all()
        )
        if transaction_ids
        else ()
    )
    entries_by_transaction: dict[UUID, list[PaperLedgerEntryRecord]] = defaultdict(list)
    for entry_row in entry_rows:
        entries_by_transaction[entry_row.transaction_id].append(entry_row)
    ledger = Ledger(
        paper_account_id=account_id,
        accounts=tuple(
            LedgerAccount(
                account_id=row.id,
                paper_account_id=row.paper_account_id,
                code=row.code,
                normal_side=cast(Literal["debit", "credit"], row.normal_side),
                currency="USD",
            )
            for row in ledger_account_rows
        ),
        transactions=tuple(
            LedgerTransaction(
                transaction_id=row.id,
                account_id=row.account_id,
                transaction_type=cast(
                    Literal[
                        "funding",
                        "cash_reservation",
                        "fill",
                        "release",
                        "adjustment",
                    ],
                    row.transaction_type,
                ),
                reference_type=cast(
                    Literal[
                        "initial_funding",
                        "cash_reservation",
                        "fill",
                        "release",
                        "adjustment",
                    ],
                    row.reference_type,
                ),
                reference_hash=row.reference_hash,
                occurred_at=row.occurred_at,
                entries=tuple(
                    LedgerEntry(
                        entry_id=entry.id,
                        entry_seq=entry.entry_seq,
                        account_code=entry.account_code,
                        currency="USD",
                        debit_amount=entry.debit_amount,
                        credit_amount=entry.credit_amount,
                    )
                    for entry in entries_by_transaction[row.id]
                ),
                transaction_hash=row.transaction_hash,
            )
            for row in transaction_rows
        ),
    )

    order_rows = tuple(
        (
            await session.scalars(
                select(PaperOrderRecord).where(
                    PaperOrderRecord.account_id == account_id,
                    exists(
                        select(PaperOrderEventRecord.id).where(
                            PaperOrderEventRecord.order_id == PaperOrderRecord.id,
                            PaperOrderEventRecord.event_seq == 1,
                            PaperOrderEventRecord.occurred_at <= as_of,
                        )
                    ),
                )
            )
        ).all()
    )
    order_ids = [row.id for row in order_rows]
    order_event_rows = (
        tuple(
            (
                await session.scalars(
                    select(PaperOrderEventRecord)
                    .where(
                        PaperOrderEventRecord.order_id.in_(order_ids),
                        PaperOrderEventRecord.occurred_at <= as_of,
                    )
                    .order_by(
                        PaperOrderEventRecord.order_id,
                        PaperOrderEventRecord.event_seq,
                    )
                )
            ).all()
        )
        if order_ids
        else ()
    )
    events_by_order: dict[UUID, list[PaperOrderEventRecord]] = defaultdict(list)
    for order_event_row in order_event_rows:
        events_by_order[order_event_row.order_id].append(order_event_row)
    orders = tuple(
        PaperOrder(
            order_id=row.id,
            account_id=row.account_id,
            strategy_decision_id=row.strategy_decision_id,
            client_order_id=row.client_order_id,
            instrument_id=row.instrument_id,
            side=cast(Literal["buy", "sell"], row.side),
            quantity=row.quantity,
            worst_price=row.worst_price,
            events=tuple(
                OrderEventInput(
                    event_id=event.id,
                    event_seq=event.event_seq,
                    occurred_at=event.occurred_at,
                    from_status=cast(InternalOrderStatus | None, event.from_status),
                    to_status=cast(InternalOrderStatus, event.to_status),
                    actor_kind=cast(
                        Literal["task18", "task19", "user", "reconciliation"],
                        event.actor_kind,
                    ),
                    reason_code=event.reason_code,
                    payload_hash=event.payload_hash,
                )
                for event in events_by_order[row.id]
            ),
        )
        for row in order_rows
    )

    reservation_rows = tuple(
        (
            await session.scalars(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.account_id == account_id,
                    exists(
                        select(PaperReservationEventRecord.id).where(
                            PaperReservationEventRecord.reservation_id
                            == PaperReservationRecord.id,
                            PaperReservationEventRecord.event_seq == 1,
                            PaperReservationEventRecord.occurred_at <= as_of,
                        )
                    ),
                )
            )
        ).all()
    )
    reservation_ids = [row.id for row in reservation_rows]
    reservation_event_rows = (
        tuple(
            (
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(
                        PaperReservationEventRecord.reservation_id.in_(reservation_ids),
                        PaperReservationEventRecord.occurred_at <= as_of,
                    )
                    .order_by(
                        PaperReservationEventRecord.reservation_id,
                        PaperReservationEventRecord.event_seq,
                    )
                )
            ).all()
        )
        if reservation_ids
        else ()
    )
    events_by_reservation: dict[UUID, list[PaperReservationEventRecord]] = defaultdict(list)
    for reservation_event_row in reservation_event_rows:
        events_by_reservation[reservation_event_row.reservation_id].append(
            reservation_event_row
        )
    reservations = tuple(
        Reservation(
            reservation_id=row.id,
            order_id=row.order_id,
            account_id=row.account_id,
            reservation_type=cast(Literal["cash", "position"], row.reservation_type),
            currency=cast(Literal["USD"] | None, row.currency),
            instrument_id=row.instrument_id,
            amount=row.amount,
            events=tuple(
                ReservationEvent(
                    event_id=event.id,
                    event_seq=event.event_seq,
                    event_type=cast(ReservationEventType, event.event_type),
                    amount=event.amount,
                    occurred_at=event.occurred_at,
                    payload_hash=event.payload_hash,
                )
                for event in events_by_reservation[row.id]
            ),
        )
        for row in reservation_rows
    )

    latest_position_time = await session.scalar(
        select(PaperPositionSnapshotRecord.as_of)
        .where(
            PaperPositionSnapshotRecord.account_id == account_id,
            PaperPositionSnapshotRecord.as_of <= as_of,
        )
        .order_by(PaperPositionSnapshotRecord.as_of.desc())
        .limit(1)
    )
    position_rows = (
        tuple(
            (
                await session.scalars(
                    select(PaperPositionSnapshotRecord).where(
                        PaperPositionSnapshotRecord.account_id == account_id,
                        PaperPositionSnapshotRecord.as_of == latest_position_time,
                    )
                )
            ).all()
        )
        if latest_position_time is not None
        else ()
    )
    positions = tuple(
        PaperPositionBalance(
            instrument_id=row.instrument_id,
            quantity=row.quantity,
            reserved_quantity=row.reserved_quantity,
            cost_basis=row.cost_basis,
            reference_close=row.reference_close,
        )
        for row in position_rows
    )
    performance = await session.scalar(
        select(PaperPerformanceSnapshotRecord)
        .where(
            PaperPerformanceSnapshotRecord.account_id == account_id,
            PaperPerformanceSnapshotRecord.as_of <= as_of,
        )
        .order_by(PaperPerformanceSnapshotRecord.as_of.desc())
        .limit(1)
    )
    drawdown_rows = tuple(
        (
            await session.scalars(
                select(PaperDrawdownLockEventRecord)
                .where(
                    PaperDrawdownLockEventRecord.account_id == account_id,
                    PaperDrawdownLockEventRecord.occurred_at <= as_of,
                )
                .order_by(PaperDrawdownLockEventRecord.event_seq)
            )
        ).all()
    )
    fill_hashes = tuple(
        (
            await session.scalars(
                select(PaperFillRecord.payload_hash).where(
                    PaperFillRecord.account_id == account_id,
                    PaperFillRecord.occurred_at <= as_of,
                )
            )
        ).all()
    )
    adjustment_hashes = tuple(
        (
            await session.scalars(
                select(PaperPositionAdjustmentEventRecord.payload_hash).where(
                    PaperPositionAdjustmentEventRecord.account_id == account_id,
                    PaperPositionAdjustmentEventRecord.occurred_at <= as_of,
                )
            )
        ).all()
    )
    projection = build_paper_projection(
        account_id=account_id,
        portfolio_id=account.portfolio_id,
        as_of=as_of,
        ledger=ledger,
        orders=orders,
        reservations=reservations,
        positions=positions,
        peak_nav=account.initial_capital if performance is None else performance.peak_nav,
        additional_source_facts=(
            *(("paper_fill", value) for value in fill_hashes),
            *(("position_adjustment", value) for value in adjustment_hashes),
            *(("position_snapshot", row.projection_hash) for row in position_rows),
            *(
                (("performance_snapshot", performance.content_hash),)
                if performance is not None
                else ()
            ),
        ),
        drawdown_events=tuple(
            DrawdownLockEvent(
                event_id=row.id,
                event_seq=row.event_seq,
                from_state=cast(DrawdownState | None, row.from_state),
                to_state=cast(DrawdownState, row.to_state),
                drawdown_ratio=row.drawdown_ratio,
                user_acknowledged=row.user_acknowledged,
                reason_code=row.reason_code,
                occurred_at=row.occurred_at,
                payload_hash=row.payload_hash,
            )
            for row in drawdown_rows
        ),
    )
    if performance is not None and (
        performance.nav != projection.nav
        or performance.cash != projection.available_cash + projection.reserved_cash
        or performance.peak_nav != projection.peak_nav
    ):
        raise ValueError("paper performance does not reconcile to projection")
    return projection

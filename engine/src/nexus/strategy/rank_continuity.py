"""Read-only holding episodes for rank checkpoints, from immutable paper facts."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.paper.execution_service import _decimal, _fact_hash, _time
from nexus.paper.ledger import balance_ledger
from nexus.paper.models import LedgerTransaction
from nexus.paper.performance import (
    PositionAdjustment,
    _load_ledger_as_of,
    fold_position_adjustments,
)
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderRecord,
    PaperPositionAdjustmentEventRecord,
)


@dataclass(frozen=True)
class PositionContinuity:
    instrument_id: UUID
    quantity_at_previous: Decimal | None
    quantity_at_cutoff: Decimal
    previous_event_seq: int | None
    last_event_seq: int
    previous_prefix_hash: str | None
    prefix_hash: str
    continuous_since_previous: bool
    opening_event_seq: int | None
    opening_fill_id: UUID | None
    kind: Literal["continuous", "opened", "reopened", "closed", "unavailable"]
    reason: Literal["NON_FILL_OPENING"] | None


@dataclass(frozen=True)
class HeldPositionContinuity:
    account_id: UUID
    previous_cutoff: datetime | None
    cutoff: datetime
    positions: tuple[PositionContinuity, ...]


@dataclass(frozen=True)
class _ContinuityEvent:
    adjustment: PositionAdjustment
    visible_at: datetime
    verified_fill_id: UUID | None
    source_hash: str


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _prefix_hash(account_id: UUID, instrument_id: UUID, events: list[_ContinuityEvent]) -> str:
    return _hash(
        {
            "schema": "rank_position_prefix_v1",
            "account_id": str(account_id),
            "instrument_id": str(instrument_id),
            "events": [
                {
                    "event_id": str(item.adjustment.event_id),
                    "event_seq": item.adjustment.event_seq,
                    "event_type": item.adjustment.event_type,
                    "quantity_delta": _decimal(item.adjustment.quantity_delta),
                    "cost_delta": _decimal(item.adjustment.cost_delta),
                    "occurred_at": _time(item.adjustment.occurred_at),
                    "payload_hash": item.adjustment.payload_hash,
                    "visible_at": _time(item.visible_at),
                    "source_hash": item.source_hash,
                    "verified_fill_id": (
                        str(item.verified_fill_id) if item.verified_fill_id else None
                    ),
                }
                for item in events
            ],
        }
    )


def _fold_continuity(
    *,
    account_id: UUID,
    events: tuple[_ContinuityEvent, ...],
    previous_cutoff: datetime | None,
    cutoff: datetime,
) -> HeldPositionContinuity:
    cutoff = require_utc(cutoff)
    if previous_cutoff is not None and require_utc(previous_cutoff) >= cutoff:
        raise ValueError("RANK_CONTINUITY_CUTOFF_INVALID")
    grouped: dict[UUID, list[_ContinuityEvent]] = defaultdict(list)
    for item in events:
        if item.adjustment.account_id != account_id:
            raise ValueError("RANK_CONTINUITY_ACCOUNT_MISMATCH")
        grouped[item.adjustment.instrument_id].append(item)
    positions = []
    for instrument_id, items in sorted(grouped.items(), key=lambda pair: pair[0].int):
        items.sort(key=lambda item: item.adjustment.event_seq)
        final = fold_position_adjustments(tuple(item.adjustment for item in items))
        prior_items = []
        prior_time = None
        quantity = Decimal(0)
        previous_quantity = Decimal(0)
        opening = None
        opening_count = 0
        zero_after_previous = False
        for item in items:
            event = item.adjustment
            visible_at = require_utc(item.visible_at)
            if event.occurred_at > cutoff or visible_at > cutoff:
                raise ValueError("RANK_CONTINUITY_FACT_NOT_VISIBLE")
            if prior_time is not None and event.occurred_at < prior_time:
                raise ValueError("RANK_CONTINUITY_TIME_REVERSED")
            prior_time = event.occurred_at
            at_previous = previous_cutoff is not None and event.occurred_at <= previous_cutoff
            if at_previous and previous_cutoff is not None:
                if visible_at > previous_cutoff:
                    raise ValueError("RANK_CONTINUITY_BACKDATED_FACT")
                prior_items.append(item)
            next_quantity = quantity + event.quantity_delta
            if quantity == 0 and next_quantity > 0:
                opening, opening_count = item, opening_count + 1
            if next_quantity == 0 and not at_previous:
                zero_after_previous = True
            quantity = next_quantity
            if at_previous:
                previous_quantity = quantity
        continuous = (
            previous_cutoff is not None
            and previous_quantity > 0
            and quantity > 0
            and not zero_after_previous
        )
        reason: Literal["NON_FILL_OPENING"] | None = None
        kind: Literal["continuous", "opened", "reopened", "closed", "unavailable"]
        if quantity == 0:
            kind = "closed"
        elif opening is None or opening.verified_fill_id is None:
            kind, reason = "unavailable", "NON_FILL_OPENING"
        else:
            kind = "continuous" if continuous else "reopened" if opening_count > 1 else "opened"
        positions.append(
            PositionContinuity(
                instrument_id,
                previous_quantity if previous_cutoff is not None else None,
                final.quantity,
                len(prior_items) if previous_cutoff is not None else None,
                items[-1].adjustment.event_seq,
                _prefix_hash(account_id, instrument_id, prior_items)
                if previous_cutoff is not None
                else None,
                _prefix_hash(account_id, instrument_id, items),
                continuous,
                opening.adjustment.event_seq if opening else None,
                opening.verified_fill_id if opening else None,
                kind,
                reason,
            )
        )
    return HeldPositionContinuity(account_id, previous_cutoff, cutoff, tuple(positions))


def _verify_fill(
    event: PaperPositionAdjustmentEventRecord,
    fill: PaperFillRecord,
    order: PaperOrderRecord,
    transaction: PaperLedgerTransactionRecord,
    window: PaperExecutionWindowAuthorityRecord,
    ledger: LedgerTransaction,
) -> str:
    if (
        event.source_fill_id != fill.id
        or fill.order_id != order.id
        or len(
            {
                event.account_id,
                fill.account_id,
                order.account_id,
                transaction.account_id,
                ledger.account_id,
                window.account_id,
            }
        )
        != 1
        or len({event.instrument_id, fill.instrument_id, order.instrument_id, window.instrument_id})
        != 1
        or transaction.id != fill.ledger_transaction_id
        or ledger.transaction_id != transaction.id
        or transaction.order_id != order.id
        or window.order_id != order.id
        or window.id != fill.execution_window_authority_id
        or len(
            {
                event.occurred_at,
                fill.occurred_at,
                fill.evaluated_at,
                transaction.occurred_at,
                window.evaluated_at,
            }
        )
        != 1
        or order.side not in ("buy", "sell")
        or event.quantity_delta != (fill.quantity if order.side == "buy" else -fill.quantity)
        or fill.notional != fill.quantity * fill.price
        or ledger.reference_hash != fill.payload_hash
        or ledger.reference_type != "fill"
        or ledger.transaction_type != "fill"
    ):
        raise ValueError("RANK_CONTINUITY_FILL_BINDING_INVALID")
    expected_event = _fact_hash(
        "paper_position_adjustment",
        {
            "account_id": str(event.account_id),
            "instrument_id": str(event.instrument_id),
            "event_seq": event.event_seq,
            "event_type": "fill",
            "quantity_delta": _decimal(event.quantity_delta),
            "cost_delta": _decimal(event.cost_delta),
            "source_fill_id": str(fill.id),
            "occurred_at": _time(event.occurred_at),
        },
    )
    expected_fill = _fact_hash(
        "paper_fill",
        {
            "order_id": str(order.id),
            "fill_seq": fill.fill_seq,
            "instrument_id": str(fill.instrument_id),
            "side": order.side,
            "quantity": _decimal(fill.quantity),
            "price": _decimal(fill.price),
            "notional": _decimal(fill.notional),
            "fee": _decimal(fill.fee),
            "execution_model_hash": fill.execution_model_hash,
            "window_content_hash": window.content_hash,
            "evaluated_at": _time(fill.evaluated_at),
        },
    )
    if event.payload_hash != expected_event or fill.payload_hash != expected_fill:
        raise ValueError("RANK_CONTINUITY_FILL_HASH_INVALID")
    if order.side == "buy":
        if event.cost_delta != fill.notional:
            raise ValueError("RANK_CONTINUITY_FILL_COST_INVALID")
        expected = [
            ("asset.position_cost.usd", fill.notional, Decimal(0)),
            ("asset.cash.reserved.usd", Decimal(0), fill.notional + fill.fee),
        ]
    else:
        if event.cost_delta >= 0:
            raise ValueError("RANK_CONTINUITY_FILL_COST_INVALID")
        realized = fill.notional + event.cost_delta
        expected = [
            ("asset.cash.available.usd", fill.notional - fill.fee, Decimal(0)),
            ("asset.position_cost.usd", Decimal(0), -event.cost_delta),
        ]
        if realized:
            expected.append(
                ("income.realized_pnl.usd", max(-realized, Decimal(0)), max(realized, Decimal(0)))
            )
    if fill.fee:
        expected.append(("expense.fees.usd", fill.fee, Decimal(0)))
    actual = [
        (entry.account_code, entry.debit_amount, entry.credit_amount) for entry in ledger.entries
    ]
    if sorted(actual) != sorted(expected):
        raise ValueError("RANK_CONTINUITY_LEDGER_BINDING_INVALID")
    return _hash(
        [event.payload_hash, fill.payload_hash, ledger.transaction_hash, window.content_hash]
    )


async def load_held_position_continuity(
    session: AsyncSession,
    *,
    account_id: UUID,
    previous_cutoff: datetime | None,
    cutoff: datetime,
) -> HeldPositionContinuity:
    """Read full event prefixes, including closed positions, in a caller-owned snapshot.

    No snapshot publication, locks, business writes, source-policy changes or recovery.
    The atomic checkpoint writer must compare both endpoints with its bound projections.
    Late-created dependencies reject historical replay instead of changing an old prefix.
    """
    with session.no_autoflush:
        cutoff = require_utc(cutoff)
        if previous_cutoff is not None and require_utc(previous_cutoff) >= cutoff:
            raise ValueError("RANK_CONTINUITY_CUTOFF_INVALID")
        account = await session.get(PaperAccountRecord, account_id)
        if (
            account is None
            or account.created_at > cutoff
            or (previous_cutoff is not None and account.created_at > previous_cutoff)
        ):
            raise ValueError("RANK_CONTINUITY_ACCOUNT_UNAVAILABLE")
        rows = (
            await session.execute(
                select(
                    PaperPositionAdjustmentEventRecord,
                    PaperFillRecord,
                    PaperOrderRecord,
                    PaperLedgerTransactionRecord,
                    PaperExecutionWindowAuthorityRecord,
                )
                .outerjoin(
                    PaperFillRecord,
                    PaperFillRecord.id == PaperPositionAdjustmentEventRecord.source_fill_id,
                )
                .outerjoin(PaperOrderRecord, PaperOrderRecord.id == PaperFillRecord.order_id)
                .outerjoin(
                    PaperLedgerTransactionRecord,
                    PaperLedgerTransactionRecord.id == PaperFillRecord.ledger_transaction_id,
                )
                .outerjoin(
                    PaperExecutionWindowAuthorityRecord,
                    PaperExecutionWindowAuthorityRecord.id
                    == PaperFillRecord.execution_window_authority_id,
                )
                .where(
                    PaperPositionAdjustmentEventRecord.account_id == account_id,
                    PaperPositionAdjustmentEventRecord.occurred_at <= cutoff,
                )
                .order_by(
                    PaperPositionAdjustmentEventRecord.instrument_id,
                    PaperPositionAdjustmentEventRecord.event_seq,
                )
            )
        ).all()
        fill_ids = set(
            await session.scalars(
                select(PaperFillRecord.id).where(
                    PaperFillRecord.account_id == account_id, PaperFillRecord.occurred_at <= cutoff
                )
            )
        )
        if fill_ids != {row.source_fill_id for row, *_ in rows if row.event_type == "fill"}:
            raise ValueError("RANK_CONTINUITY_FILL_PREFIX_INCOMPLETE")
        ledger = await _load_ledger_as_of(session, account_id=account_id, as_of=cutoff)
        balance_ledger(ledger)
        transactions = {item.transaction_id: item for item in ledger.transactions}
        transaction_ids = [
            transaction.id for _, _, _, transaction, _ in rows if transaction is not None
        ]
        entry_time_rows = (
            await session.execute(
                select(
                    PaperLedgerEntryRecord.transaction_id,
                    func.max(PaperLedgerEntryRecord.created_at),
                )
                .where(PaperLedgerEntryRecord.transaction_id.in_(transaction_ids))
                .group_by(PaperLedgerEntryRecord.transaction_id)
            )
        ).all()
        entry_times = {transaction_id: created_at for transaction_id, created_at in entry_time_rows}
        events = []
        for row, fill, order, transaction, window in rows:
            visible_at = max(account.created_at, row.created_at)
            verified_fill_id = None
            source_hash = row.payload_hash
            if row.event_type == "fill":
                if (
                    any(item is None for item in (fill, order, transaction, window))
                    or transaction.id not in transactions
                ):
                    raise ValueError("RANK_CONTINUITY_FILL_BINDING_INVALID")
                source_hash = _verify_fill(
                    row, fill, order, transaction, window, transactions[transaction.id]
                )
                visible_at = max(
                    visible_at,
                    fill.created_at,
                    order.created_at,
                    transaction.created_at,
                    window.created_at,
                    entry_times[transaction.id],
                )
                verified_fill_id = fill.id
            events.append(
                _ContinuityEvent(
                    PositionAdjustment(
                        event_id=row.id,
                        account_id=row.account_id,
                        instrument_id=row.instrument_id,
                        event_seq=row.event_seq,
                        event_type=cast(
                            Literal["fill", "split", "dividend", "delisting", "correction"],
                            row.event_type,
                        ),
                        quantity_delta=row.quantity_delta,
                        cost_delta=row.cost_delta,
                        occurred_at=row.occurred_at,
                        payload_hash=row.payload_hash,
                    ),
                    visible_at,
                    verified_fill_id,
                    source_hash,
                )
            )
        return _fold_continuity(
            account_id=account_id,
            events=tuple(events),
            previous_cutoff=previous_cutoff,
            cutoff=cutoff,
        )

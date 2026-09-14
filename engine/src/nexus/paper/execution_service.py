"""Atomic deterministic execution for internal paper orders only.

This module has no provider transport and no broker capability. It consumes only
immutable PostgreSQL authorities already available at the caller supplied time.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.market.price_bar_authority import (
    HASH_VERSION,
    POLICY_VERSION,
    SOURCE_POLICY_ID,
    SOURCE_POLICY_V2_ID,
)
from nexus.paper.errors import PaperExecutionError
from nexus.paper.fills import (
    EXECUTION_MODEL_V1,
    ExecutionBar,
    ExecutionCalendarSession,
    ExecutionModel,
    ExecutionQuote,
    QualifiedExecutionWindow,
    calculate_fill,
    execution_model_hash,
    qualify_execution_window,
)
from nexus.paper.ledger import (
    balance_ledger,
    make_buy_fill_transaction,
    make_cash_release_transaction,
    make_sell_fill_transaction,
    post_ledger_transaction,
)
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
)
from nexus.paper.orders import current_order_status, fold_reservation_events
from nexus.paper.performance import (
    PositionAdjustment,
    allocate_average_cost,
    fold_position_adjustments,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.data import (
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import Instrument, LatestQuote
from nexus.persistence.models.paper import (
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperMarketWindowRecord,
    PaperAccountRecord,
    PaperExecutionModelRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperExecutionWindowMemberRecord,
    PaperFillRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPositionAdjustmentEventRecord,
    PaperRequestIdempotencyRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import OutboxEvent
from nexus.quality.source_policy import paper_execution_price_authorized

_SOURCE_PRIORITY = {"fosun": 0, "alpaca": 1, "fixture": 2}


def _paper_execution_authority_available(source_policy_id: UUID) -> bool:
    return paper_execution_price_authorized(source_policy_id)


@dataclass(frozen=True)
class PaperExecutionHooks:
    """Optional deterministic phase observer used only for tracing and fault tests."""

    after_phase: Callable[[str], None]


class PaperAdvanceResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    order_id: UUID
    account_id: UUID
    action: Literal[
        "not_due",
        "no_data",
        "no_capacity",
        "partially_filled",
        "filled",
        "cancelled",
        "terminal",
    ]
    status: InternalOrderStatus
    filled_quantity: Decimal
    remaining_quantity: Decimal
    result_event_seq: int | None
    fill_id: UUID | None
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("filled_quantity", "remaining_quantity", mode="before")
    @classmethod
    def _exact_quantity(cls, value: object) -> Decimal:
        if isinstance(value, float) or not isinstance(value, Decimal | int):
            raise ValueError("paper execution quantities must be exact")
        result = Decimal(value)
        if result < 0 or result != result.to_integral_value():
            raise ValueError("paper execution quantities must be nonnegative whole shares")
        return result


class PaperCancelResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    order_id: UUID
    account_id: UUID
    status: Literal["cancelled"]
    filled_quantity: Decimal
    unfilled_quantity: Decimal
    result_event_seq: int = Field(ge=1)
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    replayed: bool = False

    @field_validator("filled_quantity", "unfilled_quantity", mode="before")
    @classmethod
    def _exact_quantity(cls, value: object) -> Decimal:
        if isinstance(value, float) or not isinstance(value, Decimal | int):
            raise ValueError("paper cancellation quantities must be exact")
        result = Decimal(value)
        if result < 0 or result != result.to_integral_value():
            raise ValueError("paper cancellation quantities must be nonnegative whole shares")
        return result


def _observe(hooks: PaperExecutionHooks | None, phase: str) -> None:
    if hooks is not None:
        hooks.after_phase(phase)


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    return value.astimezone(UTC)


def _time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _decimal(value: Decimal) -> str:
    return format(value, ".12f")


def _digest(schema: str, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            {"schema": schema, **payload},
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def _request_hash(*, order_id: UUID, evaluated_at: datetime) -> str:
    return _digest(
        "paper_advance_request_sha256_v1",
        {"order_id": str(order_id), "evaluated_at": _time(evaluated_at)},
    )


def _fact_hash(kind: str, payload: dict[str, object]) -> str:
    return _digest(
        "paper_fact_payload_sha256_v1",
        {"kind": kind, "payload": payload},
    )


def _result(
    *,
    order_id: UUID,
    account_id: UUID,
    action: Literal[
        "not_due",
        "no_data",
        "no_capacity",
        "partially_filled",
        "filled",
        "cancelled",
        "terminal",
    ],
    status: InternalOrderStatus,
    filled_quantity: Decimal,
    total_quantity: Decimal,
    result_event_seq: int | None,
    fill_id: UUID | None,
) -> PaperAdvanceResult:
    remaining = total_quantity - filled_quantity
    payload = {
        "order_id": str(order_id),
        "account_id": str(account_id),
        "action": action,
        "status": status,
        "filled_quantity": _decimal(filled_quantity),
        "remaining_quantity": _decimal(remaining),
        "result_event_seq": result_event_seq,
        "fill_id": str(fill_id) if fill_id is not None else None,
    }
    return PaperAdvanceResult(
        order_id=order_id,
        account_id=account_id,
        action=action,
        status=status,
        filled_quantity=filled_quantity,
        remaining_quantity=remaining,
        result_event_seq=result_event_seq,
        fill_id=fill_id,
        result_hash=_digest("paper_advance_result_sha256_v1", payload),
    )


def _cancel_result(
    *,
    order: PaperOrderRecord,
    filled_quantity: Decimal,
    result_event_seq: int,
    replayed: bool = False,
) -> PaperCancelResult:
    unfilled = order.quantity - filled_quantity
    payload = {
        "order_id": str(order.id),
        "account_id": str(order.account_id),
        "status": "cancelled",
        "filled_quantity": _decimal(filled_quantity),
        "unfilled_quantity": _decimal(unfilled),
        "result_event_seq": result_event_seq,
    }
    return PaperCancelResult(
        order_id=order.id,
        account_id=order.account_id,
        status="cancelled",
        filled_quantity=filled_quantity,
        unfilled_quantity=unfilled,
        result_event_seq=result_event_seq,
        result_hash=_digest("paper_cancel_result_sha256_v1", payload),
        replayed=replayed,
    )


def _order_domain(
    row: PaperOrderRecord,
    events: tuple[PaperOrderEventRecord, ...],
) -> PaperOrder:
    return PaperOrder(
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
            for event in events
        ),
    )


def _reservation_domain(
    row: PaperReservationRecord,
    events: tuple[PaperReservationEventRecord, ...],
) -> Reservation:
    return Reservation(
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
                event_type=cast(Literal["reserved", "consumed", "released"], event.event_type),
                amount=event.amount,
                occurred_at=event.occurred_at,
                payload_hash=event.payload_hash,
            )
            for event in events
        ),
    )


async def _ledger(session: AsyncSession, account_id: UUID) -> Ledger:
    account_rows = tuple(
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
                .where(PaperLedgerTransactionRecord.account_id == account_id)
                .order_by(
                    PaperLedgerTransactionRecord.occurred_at,
                    PaperLedgerTransactionRecord.id,
                )
            )
        ).all()
    )
    transaction_ids = tuple(row.id for row in transaction_rows)
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
    by_transaction: dict[UUID, list[PaperLedgerEntryRecord]] = defaultdict(list)
    for entry in entry_rows:
        by_transaction[entry.transaction_id].append(entry)
    return Ledger(
        paper_account_id=account_id,
        accounts=tuple(
            LedgerAccount(
                account_id=row.id,
                paper_account_id=row.paper_account_id,
                code=row.code,
                normal_side=cast(Literal["debit", "credit"], row.normal_side),
                currency="USD",
            )
            for row in account_rows
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
                    for entry in by_transaction[row.id]
                ),
                transaction_hash=row.transaction_hash,
            )
            for row in transaction_rows
        ),
    )


async def _persist_ledger_transaction(
    session: AsyncSession,
    *,
    order_id: UUID,
    transaction: LedgerTransaction,
    ledger_accounts: dict[str, PaperLedgerAccountRecord],
) -> None:
    session.add(
        PaperLedgerTransactionRecord(
            id=transaction.transaction_id,
            account_id=transaction.account_id,
            order_id=order_id,
            transaction_type=transaction.transaction_type,
            reference_type=transaction.reference_type,
            reference_hash=transaction.reference_hash,
            occurred_at=transaction.occurred_at,
            transaction_hash=transaction.transaction_hash,
        )
    )
    await session.flush()
    session.add_all(
        PaperLedgerEntryRecord(
            id=entry.entry_id,
            transaction_id=transaction.transaction_id,
            account_id=transaction.account_id,
            entry_seq=entry.entry_seq,
            ledger_account_id=ledger_accounts[entry.account_code].id,
            account_code=entry.account_code,
            currency=entry.currency,
            debit_amount=entry.debit_amount,
            credit_amount=entry.credit_amount,
        )
        for entry in transaction.entries
    )
    await session.flush()


async def _verify_account_projections(
    session: AsyncSession,
    *,
    order: PaperOrderRecord,
    expected_status: InternalOrderStatus,
) -> None:
    """Re-fold every mutable projection touched by an execution before commit."""
    try:
        order_events = tuple(
            (
                await session.scalars(
                    select(PaperOrderEventRecord)
                    .where(PaperOrderEventRecord.order_id == order.id)
                    .order_by(PaperOrderEventRecord.event_seq)
                )
            ).all()
        )
        if current_order_status(_order_domain(order, order_events)) != expected_status:
            raise ValueError("paper order projection does not match expected status")
        reservation = await session.scalar(
            select(PaperReservationRecord).where(PaperReservationRecord.order_id == order.id)
        )
        if reservation is None:
            raise ValueError("paper reservation projection is missing")
        reservation_events = tuple(
            (
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id == reservation.id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            ).all()
        )
        outstanding = fold_reservation_events(_reservation_domain(reservation, reservation_events))
        if expected_status in {"filled", "cancelled", "rejected"}:
            if outstanding != 0:
                raise ValueError("terminal paper reservation remains open")
        elif outstanding <= 0:
            raise ValueError("open paper order has no reservation")
        position_rows = tuple(
            (
                await session.scalars(
                    select(PaperPositionAdjustmentEventRecord)
                    .where(
                        PaperPositionAdjustmentEventRecord.account_id == order.account_id,
                        PaperPositionAdjustmentEventRecord.instrument_id == order.instrument_id,
                    )
                    .order_by(PaperPositionAdjustmentEventRecord.event_seq)
                )
            ).all()
        )
        if position_rows:
            fold_position_adjustments(
                tuple(
                    PositionAdjustment(
                        event_id=row.id,
                        account_id=row.account_id,
                        instrument_id=row.instrument_id,
                        event_seq=row.event_seq,
                        event_type=cast(
                            Literal[
                                "fill",
                                "split",
                                "dividend",
                                "delisting",
                                "correction",
                            ],
                            row.event_type,
                        ),
                        quantity_delta=row.quantity_delta,
                        cost_delta=row.cost_delta,
                        occurred_at=row.occurred_at,
                        payload_hash=row.payload_hash,
                    )
                    for row in position_rows
                )
            )
        fill_total = await session.scalar(
            select(func.coalesce(func.sum(PaperFillRecord.quantity), 0)).where(
                PaperFillRecord.order_id == order.id
            )
        ) or Decimal(0)
        if fill_total > order.quantity:
            raise ValueError("paper fills exceed order quantity")
        balance_ledger(await _ledger(session, order.account_id))
    except ValueError as error:
        raise PaperExecutionError("PAPER_EXECUTION_CONFLICT") from error


async def _qualified_window(
    session: AsyncSession,
    *,
    order: PaperOrderRecord,
    source_policy_id: UUID,
    calendar: ExecutionCalendarSession,
    model: ExecutionModel,
    window_seq: int,
    evaluated_at: datetime,
    prior_window: PaperExecutionWindowAuthorityRecord | None,
) -> tuple[QualifiedExecutionWindow, PriceBarSetAuthority] | None:
    agent_paper = source_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
    schwab_agent_paper = source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
    if source_policy_id == SOURCE_POLICY_V2_ID:
        # Fosun v2 currently proves daily bars only. The separate intraday
        # authority is display-only and cannot authorize simulated fills.
        return None
    if source_policy_id not in {
        SOURCE_POLICY_ID,
        PAPER_EXECUTION_SOURCE_POLICY_ID,
        PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    }:
        return None
    schwab_binding: AgentPaperMarketWindowRecord | None = None
    if schwab_agent_paper:
        if prior_window is not None or window_seq != 1:
            return None
        schwab_binding = await session.scalar(
            select(AgentPaperMarketWindowRecord).where(
                AgentPaperMarketWindowRecord.order_id == order.id,
                AgentPaperMarketWindowRecord.instrument_id == order.instrument_id,
                AgentPaperMarketWindowRecord.purpose == "paper_fill",
                AgentPaperMarketWindowRecord.evaluated_at <= evaluated_at,
            )
        )
        if schwab_binding is None:
            return None
        window_start = schwab_binding.window_start_at
        window_end = schwab_binding.window_end_at
    else:
        window_start = calendar.market_open_at + timedelta(
            minutes=(window_seq - 1) * model.window_minutes
        )
        window_end = window_start + timedelta(minutes=model.window_minutes)
    statement = (
        select(PriceBarSetAuthority, PriceSeriesManifest)
        .join(
            PriceSeriesManifest,
            PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
        )
        .where(
            PriceSeriesManifest.instrument == str(order.instrument_id),
            PriceSeriesManifest.interval == model.bar_interval,
            PriceSeriesManifest.start_at == window_start,
            PriceSeriesManifest.end_at == window_end - timedelta(minutes=1),
            PriceSeriesManifest.row_count == model.window_minutes,
            PriceBarSetAuthority.available_at <= evaluated_at,
            PriceBarSetAuthority.source_policy_id == source_policy_id,
            PriceBarSetAuthority.hash_version == HASH_VERSION,
            PriceBarSetAuthority.policy_version == POLICY_VERSION,
            PriceSeriesManifest.adjustment == "raw",
        )
    )
    if agent_paper or schwab_agent_paper:
        statement = statement.join(
            AgentPaperMarketWindowRecord,
            AgentPaperMarketWindowRecord.source_authority_id == PriceBarSetAuthority.id,
        ).where(
            AgentPaperMarketWindowRecord.order_id == order.id,
            AgentPaperMarketWindowRecord.instrument_id == order.instrument_id,
            AgentPaperMarketWindowRecord.purpose == "paper_fill",
            AgentPaperMarketWindowRecord.window_start_at == window_start,
            AgentPaperMarketWindowRecord.window_end_at == window_end,
            AgentPaperMarketWindowRecord.evaluated_at <= evaluated_at,
            AgentPaperMarketWindowRecord.price_series_manifest_id == PriceSeriesManifest.id,
            PriceSeriesManifest.source == (
                "schwab" if schwab_agent_paper else "alpaca"
            ),
        )
    else:
        statement = statement.where(PriceSeriesManifest.source.in_(("alpaca", "fixture")))
    if prior_window is not None:
        statement = statement.where(
            PriceSeriesManifest.source == prior_window.source,
            PriceSeriesManifest.feed == prior_window.feed,
            PriceSeriesManifest.delivery_mode == prior_window.delivery_mode,
        )
    statement = statement.order_by(
        case(_SOURCE_PRIORITY, value=PriceSeriesManifest.source, else_=99),
        PriceBarSetAuthority.available_at.desc(),
        PriceBarSetAuthority.content_hash,
    )
    candidate = (await session.execute(statement.limit(1))).one_or_none()
    if candidate is None:
        return None
    authority, manifest = candidate
    if not (
        (
            agent_paper
            and manifest.source == "alpaca"
            and manifest.feed == "iex"
            and manifest.delivery_mode == "real_time"
        )
        or (
            schwab_agent_paper
            and manifest.source == "schwab"
            and manifest.feed == "price_history"
            and manifest.delivery_mode == "unknown"
        )
        or (
            not agent_paper
            and manifest.source == "alpaca"
            and manifest.feed == "sip"
            and manifest.delivery_mode == "delayed"
        )
        or (
            not agent_paper and manifest.source == "fixture" and manifest.delivery_mode == "fixture"
        )
    ):
        return None
    bar_rows = tuple(
        (
            await session.scalars(
                select(PriceBarRecord)
                .where(
                    PriceBarRecord.price_series_manifest_id == manifest.id,
                    PriceBarRecord.instrument_id == order.instrument_id,
                )
                .order_by(PriceBarRecord.observed_at)
            )
        ).all()
    )
    bars = tuple(
        ExecutionBar(
            bar_id=row.id,
            authority_id=authority.id,
            instrument_id=row.instrument_id,
            source=manifest.source,
            feed=manifest.feed,
            delivery_mode=cast(
                Literal["real_time", "delayed", "fixture", "unknown"],
                manifest.delivery_mode,
            ),
            observed_at=row.observed_at,
            available_at=row.available_at,
            close=row.close,
            volume=row.volume,
        )
        for row in bar_rows
    )
    quote_row = await session.scalar(
        select(LatestQuote).where(
            LatestQuote.instrument_id == order.instrument_id,
            LatestQuote.source == manifest.source,
            LatestQuote.feed == manifest.feed,
            LatestQuote.observed_at >= window_start,
            LatestQuote.observed_at < window_end,
            LatestQuote.available_at <= evaluated_at,
        )
    )
    quote = (
        ExecutionQuote(
            quote_id=quote_row.id,
            instrument_id=quote_row.instrument_id,
            source=quote_row.source,
            feed=quote_row.feed,
            bid=quote_row.bid,
            ask=quote_row.ask,
            observed_at=quote_row.observed_at,
            available_at=quote_row.available_at,
            revision=quote_row.revision,
            quality_flags=tuple(quote_row.quality_flags),
        )
        if quote_row is not None and quote_row.bid is not None and quote_row.ask is not None
        else None
    )
    qualification_calendar = calendar
    qualification_model = model
    if schwab_binding is not None:
        # The persisted Agent window is the source-frozen execution boundary for
        # this order. Regular-session offsets belong to the legacy EOD policy.
        qualification_calendar = ExecutionCalendarSession(
            session=calendar.session,
            market_open_at=schwab_binding.window_start_at,
            market_close_at=calendar.market_close_at,
        )
        qualification_model = model.model_copy(
            update={"first_evaluation_offset_minutes": 0}
        )
    try:
        window = qualify_execution_window(
            model=qualification_model,
            calendar=qualification_calendar,
            bars=bars,
            quote=quote,
            evaluated_at=evaluated_at,
        )
    except ValueError:
        return None
    return window, authority


async def _insert_window(
    session: AsyncSession,
    *,
    order: PaperOrderRecord,
    window_seq: int,
    window: QualifiedExecutionWindow,
    authority: PriceBarSetAuthority,
) -> PaperExecutionWindowAuthorityRecord:
    root = PaperExecutionWindowAuthorityRecord(
        id=new_uuid7(),
        order_id=order.id,
        account_id=order.account_id,
        instrument_id=order.instrument_id,
        window_seq=window_seq,
        window_start_at=window.window_start_at,
        window_end_at=window.window_end_at,
        evaluated_at=window.evaluated_at,
        data_available_at=max(window.data_available_at, authority.available_at),
        source_authority_id=authority.id,
        source=window.source,
        source_content_hash=authority.content_hash,
        feed=window.feed,
        delivery_mode=window.delivery_mode,
        vwap=window.vwap,
        volume=window.volume,
        capacity=window.capacity,
        quote_id=window.quote_id,
        quote_revision=window.quote_revision,
        bid=window.bid,
        ask=window.ask,
        half_spread=window.half_spread,
        content_hash=window.content_hash,
    )
    session.add(root)
    session.add_all(
        PaperExecutionWindowMemberRecord(
            window_authority_id=root.id,
            bar_id=bar_id,
            member_seq=index,
        )
        for index, bar_id in enumerate(window.bar_ids, start=1)
    )
    await session.flush()
    return root


async def _append_order_event(
    session: AsyncSession,
    *,
    order: PaperOrderRecord,
    event_seq: int,
    source: InternalOrderStatus,
    target: InternalOrderStatus,
    reason_code: str,
    occurred_at: datetime,
    actor_kind: Literal["task19", "user"] = "task19",
    source_fill_id: UUID | None = None,
) -> PaperOrderEventRecord:
    payload = {
        "order_id": str(order.id),
        "event_seq": event_seq,
        "from_status": source,
        "to_status": target,
        "actor_kind": actor_kind,
        "reason_code": reason_code,
        "occurred_at": _time(occurred_at),
        "source_fill_id": str(source_fill_id) if source_fill_id is not None else None,
    }
    event = PaperOrderEventRecord(
        id=new_uuid7(),
        order_id=order.id,
        account_id=order.account_id,
        event_seq=event_seq,
        from_status=source,
        to_status=target,
        actor_kind=actor_kind,
        reason_code=reason_code,
        occurred_at=occurred_at,
        payload_hash=_fact_hash("paper_order_event", payload),
        source_fill_id=source_fill_id,
    )
    session.add(event)
    await session.flush()
    return event


async def _insert_idempotency(
    session: AsyncSession,
    *,
    account_id: UUID,
    order_id: UUID,
    idempotency_key: str,
    request_hash: str,
    result: PaperAdvanceResult | PaperCancelResult,
    occurred_at: datetime,
    operation: Literal["advance", "cancel"] = "advance",
) -> None:
    session.add(
        PaperRequestIdempotencyRecord(
            id=new_uuid7(),
            account_id=account_id,
            order_id=order_id,
            operation=operation,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            result_event_seq=result.result_event_seq,
            result_hash=result.result_hash,
        )
    )
    await session.flush()
    action = (
        f"paper.execution.advance.{result.action}"
        if isinstance(result, PaperAdvanceResult)
        else "paper.execution.cancelled"
    )
    payload = {
        **result.model_dump(mode="json"),
        "operation": operation,
        "request_hash": request_hash,
    }
    await append_audit_event(
        session,
        actor_id=("system:task19" if operation == "advance" else "user:internal-paper"),
        action=action,
        schema_version="paper_execution_v1",
        payload=payload,
        occurred_at=occurred_at,
    )
    if operation == "cancel" or (
        isinstance(result, PaperAdvanceResult)
        and result.action in {"partially_filled", "filled", "cancelled"}
    ):
        session.add(
            OutboxEvent(
                id=new_uuid7(),
                event_type=action,
                aggregate_id=order_id,
                schema_version="paper_execution_v1",
                payload=payload,
                published_at=None,
            )
        )


async def _existing_idempotency(
    session: AsyncSession,
    *,
    account_id: UUID,
    idempotency_key: str,
    request_hash: str,
    operation: Literal["advance", "cancel"] = "advance",
) -> PaperRequestIdempotencyRecord | None:
    record = await session.scalar(
        select(PaperRequestIdempotencyRecord)
        .where(
            PaperRequestIdempotencyRecord.account_id == account_id,
            PaperRequestIdempotencyRecord.operation == operation,
            PaperRequestIdempotencyRecord.idempotency_key == idempotency_key,
        )
        .with_for_update()
    )
    if record is not None and record.request_hash != request_hash:
        raise PaperExecutionError("PAPER_EXECUTION_IDEMPOTENCY_CONFLICT")
    return record


async def _replay_result(
    session: AsyncSession,
    *,
    order: PaperOrderRecord,
    evaluated_at: datetime,
    record: PaperRequestIdempotencyRecord,
    calendar: ExecutionCalendarSession,
    execution_source_policy_id: UUID,
) -> PaperAdvanceResult:
    fills = tuple(
        (
            await session.scalars(
                select(PaperFillRecord)
                .where(
                    PaperFillRecord.order_id == order.id,
                    PaperFillRecord.evaluated_at <= evaluated_at,
                )
                .order_by(PaperFillRecord.fill_seq)
            )
        ).all()
    )
    event = (
        await session.scalar(
            select(PaperOrderEventRecord).where(
                PaperOrderEventRecord.order_id == order.id,
                PaperOrderEventRecord.event_seq == record.result_event_seq,
            )
        )
        if record.result_event_seq is not None
        else None
    )
    historical_event = await session.scalar(
        select(PaperOrderEventRecord)
        .where(
            PaperOrderEventRecord.order_id == order.id,
            PaperOrderEventRecord.occurred_at <= evaluated_at,
        )
        .order_by(PaperOrderEventRecord.event_seq.desc())
        .limit(1)
    )
    if historical_event is None:
        raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")
    status = cast(
        InternalOrderStatus,
        event.to_status if event is not None else historical_event.to_status,
    )
    fill = next((item for item in reversed(fills) if item.evaluated_at == evaluated_at), None)
    if event is not None and event.to_status == "cancelled":
        action: Literal[
            "not_due",
            "no_data",
            "no_capacity",
            "partially_filled",
            "filled",
            "cancelled",
            "terminal",
        ] = "cancelled"
    elif (
        event is not None
        and event.to_status
        in {
            "risk_rejected",
            "filled",
            "rejected",
        }
        and fill is None
    ):
        action = "terminal"
    elif fill is not None:
        action = "filled" if status == "filled" else "partially_filled"
    elif (
        execution_source_policy_id != PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
        and evaluated_at
        < calendar.market_open_at
        + timedelta(minutes=EXECUTION_MODEL_V1.first_evaluation_offset_minutes)
    ):
        action = "not_due"
    else:
        window = await session.scalar(
            select(PaperExecutionWindowAuthorityRecord).where(
                PaperExecutionWindowAuthorityRecord.order_id == order.id,
                PaperExecutionWindowAuthorityRecord.evaluated_at == evaluated_at,
            )
        )
        action = "no_capacity" if window is not None else "no_data"
    replay = _result(
        order_id=order.id,
        account_id=order.account_id,
        action=action,
        status=status,
        filled_quantity=sum((row.quantity for row in fills), start=Decimal(0)),
        total_quantity=order.quantity,
        result_event_seq=record.result_event_seq,
        fill_id=fill.id if fill is not None else None,
    )
    if replay.result_hash != record.result_hash:
        raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")
    return replay


async def _cancel(
    session: AsyncSession,
    *,
    order: PaperOrderRecord,
    status: InternalOrderStatus,
    event_seq: int,
    reservation: PaperReservationRecord,
    reservation_events: tuple[PaperReservationEventRecord, ...],
    outstanding: Decimal,
    occurred_at: datetime,
    reason_code: str,
    ledger: Ledger,
    ledger_accounts: dict[str, PaperLedgerAccountRecord],
    actor_kind: Literal["task19", "user"] = "task19",
) -> PaperOrderEventRecord:
    if outstanding <= 0:
        raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID")
    release_payload = {
        "reservation_id": str(reservation.id),
        "event_seq": len(reservation_events) + 1,
        "event_type": "released",
        "amount": _decimal(outstanding),
        "occurred_at": _time(occurred_at),
        "reason_code": reason_code,
    }
    release_hash = _fact_hash("paper_reservation_event", release_payload)
    if reservation.reservation_type == "cash":
        transaction = make_cash_release_transaction(
            transaction_id=new_uuid7(),
            debit_entry_id=new_uuid7(),
            credit_entry_id=new_uuid7(),
            paper_account_id=order.account_id,
            reference_hash=release_hash,
            amount=outstanding,
            occurred_at=occurred_at,
        )
        post_ledger_transaction(ledger, transaction)
        await _persist_ledger_transaction(
            session,
            order_id=order.id,
            transaction=transaction,
            ledger_accounts=ledger_accounts,
        )
    session.add(
        PaperReservationEventRecord(
            id=new_uuid7(),
            reservation_id=reservation.id,
            account_id=order.account_id,
            event_seq=len(reservation_events) + 1,
            event_type="released",
            amount=outstanding,
            occurred_at=occurred_at,
            payload_hash=release_hash,
            source_fill_id=None,
        )
    )
    await session.flush()
    event = await _append_order_event(
        session,
        order=order,
        event_seq=event_seq,
        source=status,
        target="cancelled",
        reason_code=reason_code,
        occurred_at=occurred_at,
        actor_kind=actor_kind,
    )
    await _verify_account_projections(
        session,
        order=order,
        expected_status="cancelled",
    )
    return event


async def advance_internal_paper_order(
    session: AsyncSession,
    *,
    order_id: UUID,
    evaluated_at: datetime,
    idempotency_key: str,
    hooks: PaperExecutionHooks | None = None,
) -> PaperAdvanceResult:
    """Advance one submitted internal-paper order using frozen database facts."""
    evaluated_at = _utc(evaluated_at, field="evaluated_at")
    if not 1 <= len(idempotency_key) <= 255:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    unlocked_order = await session.get(PaperOrderRecord, order_id)
    if unlocked_order is None:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{unlocked_order.account_id}"},
    )
    order = await session.scalar(
        select(PaperOrderRecord).where(PaperOrderRecord.id == order_id).with_for_update()
    )
    account = await session.scalar(
        select(PaperAccountRecord)
        .where(PaperAccountRecord.id == unlocked_order.account_id)
        .with_for_update()
    )
    if (
        order is None
        or account is None
        or account.capability != "simulation_write_internal"
        or order.account_id != account.id
    ):
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    _observe(hooks, "order_locked")

    events = tuple(
        (
            await session.scalars(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == order.id)
                .order_by(PaperOrderEventRecord.event_seq)
                .with_for_update()
            )
        ).all()
    )
    order_domain = _order_domain(order, events)
    status = current_order_status(order_domain)
    if status is None:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
    if decision is None:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    calendar_row = await session.get(
        ExchangeCalendarSessionRecord,
        (decision.calendar_authority_id, decision.execution_session),
    )
    if calendar_row is None:
        raise PaperExecutionError("PAPER_EXECUTION_AUTHORITY_INCOMPLETE")
    calendar = ExecutionCalendarSession(
        session=calendar_row.session,
        market_open_at=calendar_row.market_open_at,
        market_close_at=calendar_row.market_close_at,
    )
    if evaluated_at.astimezone(ZoneInfo("America/New_York")).date() != calendar.session:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    model_row = await session.scalar(
        select(PaperExecutionModelRecord).where(
            PaperExecutionModelRecord.version == "execution_model_v1"
        )
    )
    if model_row is None:
        raise PaperExecutionError("PAPER_EXECUTION_AUTHORITY_INCOMPLETE")
    try:
        model_document = dict(model_row.document)
        model_document["volume_participation"] = Decimal(
            str(model_document.get("volume_participation"))
        )
        model = ExecutionModel.model_validate(model_document)
    except ValueError as error:
        raise PaperExecutionError("PAPER_EXECUTION_AUTHORITY_INCOMPLETE") from error
    if execution_model_hash(model) != model_row.content_hash:
        raise PaperExecutionError("PAPER_EXECUTION_AUTHORITY_INCOMPLETE")

    execution_source_policy_id = decision.execution_source_policy_id or decision.source_policy_id
    request_hash = _request_hash(order_id=order.id, evaluated_at=evaluated_at)
    existing_request = await _existing_idempotency(
        session,
        account_id=account.id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    if existing_request is not None:
        return await _replay_result(
            session,
            order=order,
            evaluated_at=evaluated_at,
            record=existing_request,
            calendar=calendar,
            execution_source_policy_id=execution_source_policy_id,
        )
    if events and evaluated_at < events[-1].occurred_at:
        raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")

    fill_rows = tuple(
        (
            await session.scalars(
                select(PaperFillRecord)
                .where(PaperFillRecord.order_id == order.id)
                .order_by(PaperFillRecord.fill_seq)
            )
        ).all()
    )
    filled_before = sum((row.quantity for row in fill_rows), start=Decimal(0))
    terminal = status in {"risk_rejected", "filled", "cancelled", "rejected"}
    if terminal:
        result = _result(
            order_id=order.id,
            account_id=order.account_id,
            action="terminal",
            status=status,
            filled_quantity=filled_before,
            total_quantity=order.quantity,
            result_event_seq=events[-1].event_seq,
            fill_id=None,
        )
        await _insert_idempotency(
            session,
            account_id=account.id,
            order_id=order.id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            result=result,
            occurred_at=evaluated_at,
        )
        return result
    if status not in {"submitted", "partially_filled", "reconciling"}:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    price_authority_available = _paper_execution_authority_available(execution_source_policy_id)

    reservation = await session.scalar(
        select(PaperReservationRecord)
        .where(PaperReservationRecord.order_id == order.id)
        .with_for_update()
    )
    if reservation is None:
        raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID")
    reservation_events = tuple(
        (
            await session.scalars(
                select(PaperReservationEventRecord)
                .where(PaperReservationEventRecord.reservation_id == reservation.id)
                .order_by(PaperReservationEventRecord.event_seq)
                .with_for_update()
            )
        ).all()
    )
    try:
        outstanding = fold_reservation_events(_reservation_domain(reservation, reservation_events))
    except ValueError as error:
        raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID") from error
    if outstanding <= 0:
        raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID")
    ledger = await _ledger(session, account.id)
    ledger_accounts = {
        row.code: row
        for row in (
            await session.scalars(
                select(PaperLedgerAccountRecord).where(
                    PaperLedgerAccountRecord.paper_account_id == account.id
                )
            )
        ).all()
    }

    first_evaluation_at = calendar.market_open_at + timedelta(
        minutes=model.first_evaluation_offset_minutes
    )
    missing_cancel_at = calendar.market_open_at + timedelta(
        minutes=model.missing_data_cancel_offset_minutes
    )
    prior_window = await session.scalar(
        select(PaperExecutionWindowAuthorityRecord)
        .where(PaperExecutionWindowAuthorityRecord.order_id == order.id)
        .order_by(PaperExecutionWindowAuthorityRecord.window_seq.desc())
        .limit(1)
    )
    if prior_window is not None and evaluated_at < prior_window.evaluated_at:
        raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")
    if (
        execution_source_policy_id != PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
        and evaluated_at < first_evaluation_at
    ):
        result = _result(
            order_id=order.id,
            account_id=order.account_id,
            action="not_due",
            status=status,
            filled_quantity=filled_before,
            total_quantity=order.quantity,
            result_event_seq=None,
            fill_id=None,
        )
        await _insert_idempotency(
            session,
            account_id=account.id,
            order_id=order.id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            result=result,
            occurred_at=evaluated_at,
        )
        return result

    if evaluated_at >= calendar.market_close_at:
        reason = (
            "paper_execution_price_authority_unavailable"
            if not price_authority_available
            else (
                "missing_fill_data"
                if filled_before == 0 and prior_window is None
                else "paper_eod_remainder"
            )
        )
        event = await _cancel(
            session,
            order=order,
            status=status,
            event_seq=len(events) + 1,
            reservation=reservation,
            reservation_events=reservation_events,
            outstanding=outstanding,
            occurred_at=evaluated_at,
            reason_code=reason,
            ledger=ledger,
            ledger_accounts=ledger_accounts,
        )
        result = _result(
            order_id=order.id,
            account_id=order.account_id,
            action="cancelled",
            status="cancelled",
            filled_quantity=filled_before,
            total_quantity=order.quantity,
            result_event_seq=event.event_seq,
            fill_id=None,
        )
        await _insert_idempotency(
            session,
            account_id=account.id,
            order_id=order.id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            result=result,
            occurred_at=evaluated_at,
        )
        return result

    window_seq = 1 if prior_window is None else prior_window.window_seq + 1
    qualified = (
        await _qualified_window(
            session,
            order=order,
            source_policy_id=execution_source_policy_id,
            calendar=calendar,
            model=model,
            window_seq=window_seq,
            evaluated_at=evaluated_at,
            prior_window=prior_window,
        )
        if price_authority_available
        else None
    )
    if qualified is None:
        if (
            execution_source_policy_id != PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
            and evaluated_at >= missing_cancel_at
            and filled_before == 0
            and prior_window is None
        ):
            event = await _cancel(
                session,
                order=order,
                status=status,
                event_seq=len(events) + 1,
                reservation=reservation,
                reservation_events=reservation_events,
                outstanding=outstanding,
                occurred_at=evaluated_at,
                reason_code=(
                    "paper_execution_price_authority_unavailable"
                    if not price_authority_available
                    else "missing_fill_data"
                ),
                ledger=ledger,
                ledger_accounts=ledger_accounts,
            )
            result = _result(
                order_id=order.id,
                account_id=order.account_id,
                action="cancelled",
                status="cancelled",
                filled_quantity=filled_before,
                total_quantity=order.quantity,
                result_event_seq=event.event_seq,
                fill_id=None,
            )
        else:
            result = _result(
                order_id=order.id,
                account_id=order.account_id,
                action="no_data",
                status=status,
                filled_quantity=filled_before,
                total_quantity=order.quantity,
                result_event_seq=None,
                fill_id=None,
            )
        await _insert_idempotency(
            session,
            account_id=account.id,
            order_id=order.id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            result=result,
            occurred_at=evaluated_at,
        )
        return result

    window, authority = qualified
    window_root = await _insert_window(
        session,
        order=order,
        window_seq=window_seq,
        window=window,
        authority=authority,
    )
    _observe(hooks, "window_persisted")
    instrument = await session.get(Instrument, order.instrument_id)
    if instrument is None:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    calculation = calculate_fill(
        model=model,
        order=order_domain,
        existing_filled_quantity=filled_before,
        reservation_outstanding=outstanding,
        window=window,
        tick_size=instrument.tick_size,
    )
    if calculation.quantity == 0:
        if window.capacity > 0 and outstanding < calculation.price:
            event = await _cancel(
                session,
                order=order,
                status=status,
                event_seq=len(events) + 1,
                reservation=reservation,
                reservation_events=reservation_events,
                outstanding=outstanding,
                occurred_at=evaluated_at,
                reason_code="paper_reservation_insufficient",
                ledger=ledger,
                ledger_accounts=ledger_accounts,
            )
            result = _result(
                order_id=order.id,
                account_id=order.account_id,
                action="cancelled",
                status="cancelled",
                filled_quantity=filled_before,
                total_quantity=order.quantity,
                result_event_seq=event.event_seq,
                fill_id=None,
            )
        else:
            result = _result(
                order_id=order.id,
                account_id=order.account_id,
                action="no_capacity",
                status=status,
                filled_quantity=filled_before,
                total_quantity=order.quantity,
                result_event_seq=None,
                fill_id=None,
            )
        await _insert_idempotency(
            session,
            account_id=account.id,
            order_id=order.id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            result=result,
            occurred_at=evaluated_at,
        )
        return result

    fill_id = new_uuid7()
    fill_seq = len(fill_rows) + 1
    fill_payload = {
        "order_id": str(order.id),
        "fill_seq": fill_seq,
        "instrument_id": str(order.instrument_id),
        "side": order.side,
        "quantity": _decimal(calculation.quantity),
        "price": _decimal(calculation.price),
        "notional": _decimal(calculation.notional),
        "fee": _decimal(calculation.fee),
        "execution_model_hash": calculation.execution_model_hash,
        "window_content_hash": calculation.window_content_hash,
        "evaluated_at": _time(evaluated_at),
    }
    fill_hash = _fact_hash("paper_fill", fill_payload)
    if order.side == "buy":
        transaction = make_buy_fill_transaction(
            transaction_id=new_uuid7(),
            entry_ids=(new_uuid7(), new_uuid7(), new_uuid7()),
            paper_account_id=order.account_id,
            reference_hash=fill_hash,
            principal=calculation.notional,
            fee=calculation.fee,
            occurred_at=evaluated_at,
        )
        cost_delta = calculation.notional
        quantity_delta = calculation.quantity
    else:
        position_rows = tuple(
            (
                await session.scalars(
                    select(PaperPositionAdjustmentEventRecord)
                    .where(
                        PaperPositionAdjustmentEventRecord.account_id == order.account_id,
                        PaperPositionAdjustmentEventRecord.instrument_id == order.instrument_id,
                    )
                    .order_by(PaperPositionAdjustmentEventRecord.event_seq)
                    .with_for_update()
                )
            ).all()
        )
        if not position_rows:
            raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID")
        projection = fold_position_adjustments(
            tuple(
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
                )
                for row in position_rows
            )
        )
        allocated_cost = allocate_average_cost(projection, calculation.quantity)
        transaction = make_sell_fill_transaction(
            transaction_id=new_uuid7(),
            entry_ids=(new_uuid7(), new_uuid7(), new_uuid7(), new_uuid7()),
            paper_account_id=order.account_id,
            reference_hash=fill_hash,
            proceeds=calculation.notional,
            allocated_cost=allocated_cost,
            fee=calculation.fee,
            occurred_at=evaluated_at,
        )
        cost_delta = -allocated_cost
        quantity_delta = -calculation.quantity
    post_ledger_transaction(ledger, transaction)
    await _persist_ledger_transaction(
        session,
        order_id=order.id,
        transaction=transaction,
        ledger_accounts=ledger_accounts,
    )
    _observe(hooks, "ledger_persisted")
    session.add(
        PaperFillRecord(
            id=fill_id,
            order_id=order.id,
            account_id=order.account_id,
            fill_seq=fill_seq,
            instrument_id=order.instrument_id,
            quantity=calculation.quantity,
            price=calculation.price,
            notional=calculation.notional,
            fee=calculation.fee,
            occurred_at=evaluated_at,
            source_authority_id=calculation.source_authority_id,
            execution_model_hash=calculation.execution_model_hash,
            payload_hash=fill_hash,
            execution_model_id=model_row.id,
            execution_window_authority_id=window_root.id,
            ledger_transaction_id=transaction.transaction_id,
            window_vwap=calculation.window_vwap,
            half_spread=calculation.half_spread,
            slippage_bps=calculation.slippage_bps,
            slippage_amount=calculation.slippage_amount,
            commission=calculation.commission,
            regulatory_fee=calculation.regulatory_fee,
            evaluated_at=evaluated_at,
        )
    )
    await session.flush()
    _observe(hooks, "fill_persisted")

    consumed_amount = (
        calculation.notional + calculation.fee
        if reservation.reservation_type == "cash"
        else calculation.quantity
    )
    if consumed_amount > outstanding:
        raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID")
    reservation_payload = {
        "reservation_id": str(reservation.id),
        "event_seq": len(reservation_events) + 1,
        "event_type": "consumed",
        "amount": _decimal(consumed_amount),
        "source_fill_id": str(fill_id),
        "occurred_at": _time(evaluated_at),
    }
    session.add(
        PaperReservationEventRecord(
            id=new_uuid7(),
            reservation_id=reservation.id,
            account_id=order.account_id,
            event_seq=len(reservation_events) + 1,
            event_type="consumed",
            amount=consumed_amount,
            occurred_at=evaluated_at,
            payload_hash=_fact_hash("paper_reservation_event", reservation_payload),
            source_fill_id=fill_id,
        )
    )
    await session.flush()
    _observe(hooks, "reservation_persisted")
    position_seq = (
        int(
            await session.scalar(
                select(func.count())
                .select_from(PaperPositionAdjustmentEventRecord)
                .where(
                    PaperPositionAdjustmentEventRecord.account_id == order.account_id,
                    PaperPositionAdjustmentEventRecord.instrument_id == order.instrument_id,
                )
            )
            or 0
        )
        + 1
    )
    position_payload = {
        "account_id": str(order.account_id),
        "instrument_id": str(order.instrument_id),
        "event_seq": position_seq,
        "event_type": "fill",
        "quantity_delta": _decimal(quantity_delta),
        "cost_delta": _decimal(cost_delta),
        "source_fill_id": str(fill_id),
        "occurred_at": _time(evaluated_at),
    }
    session.add(
        PaperPositionAdjustmentEventRecord(
            id=new_uuid7(),
            account_id=order.account_id,
            instrument_id=order.instrument_id,
            event_seq=position_seq,
            event_type="fill",
            quantity_delta=quantity_delta,
            cost_delta=cost_delta,
            source_fill_id=fill_id,
            occurred_at=evaluated_at,
            payload_hash=_fact_hash("paper_position_adjustment", position_payload),
        )
    )
    await session.flush()
    _observe(hooks, "position_persisted")

    filled_after = filled_before + calculation.quantity
    target_status: InternalOrderStatus = (
        "filled" if filled_after == order.quantity else "partially_filled"
    )
    event = await _append_order_event(
        session,
        order=order,
        event_seq=len(events) + 1,
        source=status,
        target=target_status,
        reason_code=(
            "PAPER_ORDER_FILLED" if target_status == "filled" else "PAPER_ORDER_PARTIALLY_FILLED"
        ),
        occurred_at=evaluated_at,
        source_fill_id=fill_id,
    )
    _observe(hooks, "order_event_persisted")

    outstanding_after = outstanding - consumed_amount
    if target_status == "filled" and outstanding_after > 0:
        release_payload = {
            "reservation_id": str(reservation.id),
            "event_seq": len(reservation_events) + 2,
            "event_type": "released",
            "amount": _decimal(outstanding_after),
            "occurred_at": _time(evaluated_at),
            "reason_code": "paper_order_filled_release",
        }
        release_hash = _fact_hash("paper_reservation_event", release_payload)
        if reservation.reservation_type == "cash":
            release_transaction = make_cash_release_transaction(
                transaction_id=new_uuid7(),
                debit_entry_id=new_uuid7(),
                credit_entry_id=new_uuid7(),
                paper_account_id=order.account_id,
                reference_hash=release_hash,
                amount=outstanding_after,
                occurred_at=evaluated_at,
            )
            post_ledger_transaction(
                post_ledger_transaction(ledger, transaction), release_transaction
            )
            await _persist_ledger_transaction(
                session,
                order_id=order.id,
                transaction=release_transaction,
                ledger_accounts=ledger_accounts,
            )
        session.add(
            PaperReservationEventRecord(
                id=new_uuid7(),
                reservation_id=reservation.id,
                account_id=order.account_id,
                event_seq=len(reservation_events) + 2,
                event_type="released",
                amount=outstanding_after,
                occurred_at=evaluated_at,
                payload_hash=release_hash,
                source_fill_id=None,
            )
        )
        await session.flush()
    await _verify_account_projections(
        session,
        order=order,
        expected_status=target_status,
    )
    result = _result(
        order_id=order.id,
        account_id=order.account_id,
        action=cast(
            Literal[
                "not_due",
                "no_data",
                "no_capacity",
                "partially_filled",
                "filled",
                "cancelled",
                "terminal",
            ],
            target_status,
        ),
        status=target_status,
        filled_quantity=filled_after,
        total_quantity=order.quantity,
        result_event_seq=event.event_seq,
        fill_id=fill_id,
    )
    await _insert_idempotency(
        session,
        account_id=account.id,
        order_id=order.id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        result=result,
        occurred_at=evaluated_at,
    )
    _observe(hooks, "idempotency_persisted")
    return result


async def cancel_internal_paper_order(
    session: AsyncSession,
    *,
    order_id: UUID,
    expected_event_seq: int,
    requested_at: datetime,
    idempotency_key: str,
) -> PaperCancelResult:
    """Cancel one open internal-paper order and release its reservation atomically."""
    requested_at = _utc(requested_at, field="requested_at")
    if expected_event_seq < 1 or not 1 <= len(idempotency_key) <= 255:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    unlocked_order = await session.get(PaperOrderRecord, order_id)
    if unlocked_order is None:
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{unlocked_order.account_id}"},
    )
    order = await session.scalar(
        select(PaperOrderRecord).where(PaperOrderRecord.id == order_id).with_for_update()
    )
    account = await session.scalar(
        select(PaperAccountRecord)
        .where(PaperAccountRecord.id == unlocked_order.account_id)
        .with_for_update()
    )
    if (
        order is None
        or account is None
        or account.capability != "simulation_write_internal"
        or order.account_id != account.id
    ):
        raise PaperExecutionError("PAPER_EXECUTION_INVALID")
    request_hash = _digest(
        "paper_cancel_request_sha256_v1",
        {"order_id": str(order.id), "expected_event_seq": expected_event_seq},
    )
    existing_request = await _existing_idempotency(
        session,
        account_id=account.id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        operation="cancel",
    )
    fill_rows = tuple(
        (
            await session.scalars(
                select(PaperFillRecord)
                .where(PaperFillRecord.order_id == order.id)
                .order_by(PaperFillRecord.fill_seq)
            )
        ).all()
    )
    filled_quantity = sum((row.quantity for row in fill_rows), start=Decimal(0))
    if existing_request is not None:
        if existing_request.result_event_seq is None:
            raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")
        event = await session.scalar(
            select(PaperOrderEventRecord).where(
                PaperOrderEventRecord.order_id == order.id,
                PaperOrderEventRecord.event_seq == existing_request.result_event_seq,
                PaperOrderEventRecord.to_status == "cancelled",
            )
        )
        if event is None:
            raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")
        replay = _cancel_result(
            order=order,
            filled_quantity=sum(
                (row.quantity for row in fill_rows if row.occurred_at <= event.occurred_at),
                start=Decimal(0),
            ),
            result_event_seq=event.event_seq,
            replayed=True,
        )
        if replay.result_hash != existing_request.result_hash:
            raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")
        return replay

    events = tuple(
        (
            await session.scalars(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == order.id)
                .order_by(PaperOrderEventRecord.event_seq)
                .with_for_update()
            )
        ).all()
    )
    status = current_order_status(_order_domain(order, events))
    if (
        status not in {"submitted", "partially_filled"}
        or not events
        or events[-1].event_seq != expected_event_seq
        or requested_at < events[-1].occurred_at
    ):
        raise PaperExecutionError("PAPER_EXECUTION_CONFLICT")
    reservation = await session.scalar(
        select(PaperReservationRecord)
        .where(PaperReservationRecord.order_id == order.id)
        .with_for_update()
    )
    if reservation is None:
        raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID")
    reservation_events = tuple(
        (
            await session.scalars(
                select(PaperReservationEventRecord)
                .where(PaperReservationEventRecord.reservation_id == reservation.id)
                .order_by(PaperReservationEventRecord.event_seq)
                .with_for_update()
            )
        ).all()
    )
    try:
        outstanding = fold_reservation_events(_reservation_domain(reservation, reservation_events))
    except ValueError as error:
        raise PaperExecutionError("PAPER_EXECUTION_RESERVATION_INVALID") from error
    ledger = await _ledger(session, account.id)
    ledger_accounts = {
        row.code: row
        for row in (
            await session.scalars(
                select(PaperLedgerAccountRecord).where(
                    PaperLedgerAccountRecord.paper_account_id == account.id
                )
            )
        ).all()
    }
    event = await _cancel(
        session,
        order=order,
        status=status,
        event_seq=len(events) + 1,
        reservation=reservation,
        reservation_events=reservation_events,
        outstanding=outstanding,
        occurred_at=requested_at,
        reason_code="paper_user_cancelled",
        ledger=ledger,
        ledger_accounts=ledger_accounts,
        actor_kind="user",
    )
    result = _cancel_result(
        order=order,
        filled_quantity=filled_quantity,
        result_event_seq=event.event_seq,
    )
    await _insert_idempotency(
        session,
        account_id=account.id,
        order_id=order.id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        result=result,
        occurred_at=requested_at,
        operation="cancel",
    )
    return result

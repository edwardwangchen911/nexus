"""Pure position folding and point-in-time paper performance projection."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator
from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.market.price_bar_authority import (
    HASH_VERSION,
    POLICY_VERSION,
    SOURCE_POLICY_ID,
    SOURCE_POLICY_V2_ID,
)
from nexus.paper.errors import PaperPerformanceError
from nexus.paper.ledger import balance_ledger
from nexus.paper.models import (
    Hash64,
    Ledger,
    LedgerAccount,
    LedgerEntry,
    LedgerTransaction,
    exact_decimal,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import SectorExposure
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperFillRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderRecord,
    PaperPerformanceAttributionRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionAdjustmentEventRecord,
    PaperPositionSnapshotRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
    StrategyDecisionRecord,
)
from nexus.quality.source_policy import current_paper_performance_price_authority_available

_QUANTUM = Decimal("0.000000000001")
_SOURCE_PRIORITY = {"fosun": 0, "alpaca": 1, "fixture": 2}


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


class PublishedPaperPosition(_FrozenModel):
    instrument_id: UUID
    quantity: Decimal
    reserved_quantity: Decimal
    cost_basis: Decimal
    average_cost: Decimal
    reference_close: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    price_authority_id: UUID
    price_content_hash: Hash64
    price_source: str
    price_feed: str
    price_available_at: datetime
    projection_hash: Hash64

    @field_validator(
        "quantity",
        "reserved_quantity",
        "cost_basis",
        "average_cost",
        "reference_close",
        "market_value",
        "unrealized_pnl",
        mode="before",
    )
    @classmethod
    def _exact_amount(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @field_validator("price_available_at")
    @classmethod
    def _aware_available_at(cls, value: datetime) -> datetime:
        return _utc(value, field="price_available_at")


class PaperPerformanceAttribution(_FrozenModel):
    dimension: Literal["cash", "instrument", "sector"]
    dimension_key: str = Field(min_length=1, max_length=100)
    market_value: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    fees: Decimal
    slippage: Decimal
    completeness: Literal["FULL", "PARTIAL"]
    content_hash: Hash64

    @field_validator(
        "market_value",
        "realized_pnl",
        "unrealized_pnl",
        "fees",
        "slippage",
        mode="before",
    )
    @classmethod
    def _exact_amount(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")


class PublishedPaperPerformance(_FrozenModel):
    snapshot_id: UUID
    replayed: bool
    performance: PaperPerformance
    positions: tuple[PublishedPaperPosition, ...]
    attributions: tuple[PaperPerformanceAttribution, ...]


@dataclass(frozen=True)
class PaperPerformanceHooks:
    """Optional phase observer used only for tracing and transaction fault tests."""

    after_phase: Callable[[str], None]


def _observe(hooks: PaperPerformanceHooks | None, phase: str) -> None:
    if hooks is not None:
        hooks.after_phase(phase)


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


async def _load_ledger_as_of(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
) -> Ledger:
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
                .where(
                    PaperLedgerTransactionRecord.account_id == account_id,
                    PaperLedgerTransactionRecord.occurred_at <= as_of,
                )
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
    entries_by_transaction: dict[UUID, list[PaperLedgerEntryRecord]] = defaultdict(list)
    for entry in entry_rows:
        entries_by_transaction[entry.transaction_id].append(entry)
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
                    for entry in entries_by_transaction[row.id]
                ),
                transaction_hash=row.transaction_hash,
            )
            for row in transaction_rows
        ),
    )


async def _position_projections_as_of(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
) -> dict[UUID, PositionProjection]:
    rows = tuple(
        (
            await session.scalars(
                select(PaperPositionAdjustmentEventRecord)
                .where(
                    PaperPositionAdjustmentEventRecord.account_id == account_id,
                    PaperPositionAdjustmentEventRecord.occurred_at <= as_of,
                )
                .order_by(
                    PaperPositionAdjustmentEventRecord.instrument_id,
                    PaperPositionAdjustmentEventRecord.event_seq,
                )
            )
        ).all()
    )
    grouped: dict[UUID, list[PaperPositionAdjustmentEventRecord]] = defaultdict(list)
    for row in rows:
        grouped[row.instrument_id].append(row)
    projections: dict[UUID, PositionProjection] = {}
    for instrument_id, events in grouped.items():
        projections[instrument_id] = fold_position_adjustments(
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
                for row in events
            )
        )
    return projections


async def _reserved_positions_as_of(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
) -> dict[UUID, Decimal]:
    reservations = tuple(
        (
            await session.scalars(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.account_id == account_id,
                    PaperReservationRecord.reservation_type == "position",
                )
            )
        ).all()
    )
    result: dict[UUID, Decimal] = defaultdict(Decimal)
    for reservation in reservations:
        if reservation.instrument_id is None:
            raise ValueError("position reservation is missing its instrument")
        events = tuple(
            (
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(
                        PaperReservationEventRecord.reservation_id == reservation.id,
                        PaperReservationEventRecord.occurred_at <= as_of,
                    )
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            ).all()
        )
        outstanding = Decimal(0)
        for expected_seq, event in enumerate(events, start=1):
            if event.event_seq != expected_seq:
                raise ValueError("position reservation sequence is not continuous")
            if event.event_type == "reserved":
                outstanding += event.amount
            elif event.event_type in {"consumed", "released"}:
                outstanding -= event.amount
            else:
                raise ValueError("position reservation event is unknown")
            if outstanding < 0:
                raise ValueError("position reservation is negative")
        result[reservation.instrument_id] += outstanding
    return dict(result)


async def _select_mark(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    session_date: object,
    as_of: datetime,
    source_policy_id: UUID,
) -> tuple[PriceBarRecord, PriceBarSetAuthority, PriceSeriesManifest] | None:
    if source_policy_id == SOURCE_POLICY_V2_ID:
        # Current v2 positions cannot be valued from legacy closes. Until the
        # paper flow has a complete corporate-action-safe v2 mark contract,
        # publication remains incomplete.
        return None
    if source_policy_id != SOURCE_POLICY_ID:
        return None
    statement = (
        select(PriceBarRecord, PriceBarSetAuthority, PriceSeriesManifest)
        .join(
            PriceSeriesManifest,
            PriceSeriesManifest.id == PriceBarRecord.price_series_manifest_id,
        )
        .join(
            PriceBarSetAuthority,
            PriceBarSetAuthority.price_series_manifest_id == PriceSeriesManifest.id,
        )
        .where(
            PriceBarRecord.instrument_id == instrument_id,
            PriceBarRecord.session == session_date,
            PriceBarRecord.available_at <= as_of,
            PriceBarRecord.received_at <= as_of,
            PriceBarSetAuthority.available_at <= as_of,
            PriceSeriesManifest.instrument == str(instrument_id),
            PriceSeriesManifest.interval == "1Day",
            PriceBarSetAuthority.source_policy_id == SOURCE_POLICY_ID,
            PriceBarSetAuthority.hash_version == HASH_VERSION,
            PriceBarSetAuthority.policy_version == POLICY_VERSION,
            PriceSeriesManifest.source.in_(("alpaca", "fixture")),
            PriceSeriesManifest.adjustment == "raw",
        )
        .order_by(
            case(_SOURCE_PRIORITY, value=PriceSeriesManifest.source, else_=99),
            PriceBarSetAuthority.available_at.desc(),
            PriceBarRecord.available_at.desc(),
            PriceBarSetAuthority.content_hash,
            PriceBarRecord.revision.desc(),
        )
        .limit(1)
    )
    candidate = (await session.execute(statement)).one_or_none()
    if candidate is None:
        return None
    bar, authority, manifest = candidate
    if not (
        (
            manifest.source == "alpaca"
            and manifest.feed == "sip"
            and manifest.delivery_mode == "delayed"
        )
        or (manifest.source == "fixture" and manifest.delivery_mode == "fixture")
    ):
        return None
    return bar, authority, manifest


def _attribution(
    *,
    dimension: Literal["cash", "instrument", "sector"],
    dimension_key: str,
    market_value: Decimal,
    realized_pnl: Decimal,
    unrealized_pnl: Decimal,
    fees: Decimal,
    slippage: Decimal,
    completeness: Literal["FULL", "PARTIAL"],
) -> PaperPerformanceAttribution:
    values = {
        "market_value": _quantize(market_value),
        "realized_pnl": _quantize(realized_pnl),
        "unrealized_pnl": _quantize(unrealized_pnl),
        "fees": _quantize(fees),
        "slippage": _quantize(slippage),
    }
    content_hash = _hash(
        {
            "schema": "paper_performance_attribution_sha256_v1",
            "dimension": dimension,
            "dimension_key": dimension_key,
            **{key: _decimal(value) for key, value in values.items()},
            "completeness": completeness,
        }
    )
    return PaperPerformanceAttribution(
        dimension=dimension,
        dimension_key=dimension_key,
        **values,
        completeness=completeness,
        content_hash=content_hash,
    )


async def _build_attributions(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
    session_date: object,
    cash: Decimal,
    positions: tuple[PublishedPaperPosition, ...],
    ledger_realized: Decimal,
    ledger_fees: Decimal,
) -> tuple[tuple[PaperPerformanceAttribution, ...], tuple[Hash64, ...]]:
    fill_rows = tuple(
        (
            await session.execute(
                select(PaperFillRecord, PaperOrderRecord, PaperPositionAdjustmentEventRecord)
                .join(PaperOrderRecord, PaperOrderRecord.id == PaperFillRecord.order_id)
                .join(
                    PaperPositionAdjustmentEventRecord,
                    PaperPositionAdjustmentEventRecord.source_fill_id == PaperFillRecord.id,
                )
                .where(
                    PaperFillRecord.account_id == account_id,
                    PaperFillRecord.occurred_at <= as_of,
                )
                .order_by(PaperFillRecord.occurred_at, PaperFillRecord.id)
            )
        ).all()
    )
    metrics: dict[UUID, dict[str, Decimal]] = defaultdict(
        lambda: {
            "market_value": Decimal(0),
            "realized_pnl": Decimal(0),
            "unrealized_pnl": Decimal(0),
            "fees": Decimal(0),
            "slippage": Decimal(0),
        }
    )
    for position in positions:
        metrics[position.instrument_id]["market_value"] = position.market_value
        metrics[position.instrument_id]["unrealized_pnl"] = position.unrealized_pnl
    for fill, order, position_event in fill_rows:
        item = metrics[fill.instrument_id]
        if order.side == "sell":
            item["realized_pnl"] += fill.notional + position_event.cost_delta
        item["fees"] += fill.fee
        item["slippage"] += fill.slippage_amount
    if _quantize(
        sum((item["realized_pnl"] for item in metrics.values()), start=Decimal(0))
    ) != _quantize(ledger_realized):
        raise ValueError("instrument realized pnl does not reconcile to the ledger")
    if _quantize(sum((item["fees"] for item in metrics.values()), start=Decimal(0))) != _quantize(
        ledger_fees
    ):
        raise ValueError("instrument fees do not reconcile to the ledger")

    cash_row = _attribution(
        dimension="cash",
        dimension_key="USD",
        market_value=cash,
        realized_pnl=Decimal(0),
        unrealized_pnl=Decimal(0),
        fees=Decimal(0),
        slippage=Decimal(0),
        completeness="FULL",
    )
    instrument_rows = tuple(
        _attribution(
            dimension="instrument",
            dimension_key=str(instrument_id),
            completeness="FULL",
            **item,
        )
        for instrument_id, item in sorted(metrics.items(), key=lambda pair: pair[0].int)
    )

    sector_metrics: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "market_value": Decimal(0),
            "realized_pnl": Decimal(0),
            "unrealized_pnl": Decimal(0),
            "fees": Decimal(0),
            "slippage": Decimal(0),
        }
    )
    sector_partial = False
    sector_source_hashes: set[Hash64] = set()
    for instrument_id, item in sorted(metrics.items(), key=lambda pair: pair[0].int):
        exposures = tuple(
            (
                await session.scalars(
                    select(SectorExposure)
                    .where(
                        SectorExposure.instrument_id == instrument_id,
                        SectorExposure.as_of == session_date,
                        SectorExposure.created_at <= as_of,
                        SectorExposure.reviewed.is_(True),
                        SectorExposure.synthetic.is_(False),
                    )
                    .order_by(SectorExposure.sector_code, SectorExposure.id)
                )
            ).all()
        )
        if not exposures or sum((row.weight for row in exposures), start=Decimal(0)) != 1:
            sector_partial = True
            exposures = ()
        if not exposures:
            for key, value in item.items():
                sector_metrics["unclassified"][key] += value
            continue
        for exposure in exposures:
            sector_source_hashes.add(exposure.content_hash)
            for key, value in item.items():
                sector_metrics[exposure.sector_code][key] += _quantize(value * exposure.weight)
    sector_completeness: Literal["FULL", "PARTIAL"] = "PARTIAL" if sector_partial else "FULL"
    sector_rows = tuple(
        _attribution(
            dimension="sector",
            dimension_key=sector,
            completeness=sector_completeness,
            **item,
        )
        for sector, item in sorted(sector_metrics.items())
    )
    return (
        (cash_row, *instrument_rows, *sector_rows),
        tuple(sorted(sector_source_hashes)),
    )


async def _load_publication(
    session: AsyncSession,
    *,
    snapshot: PaperPerformanceSnapshotRecord,
    replayed: bool,
) -> PublishedPaperPerformance:
    position_rows = tuple(
        (
            await session.execute(
                select(
                    PaperPositionSnapshotRecord,
                    PriceBarSetAuthority,
                    PriceSeriesManifest,
                )
                .join(
                    PriceBarSetAuthority,
                    PriceBarSetAuthority.id == PaperPositionSnapshotRecord.price_authority_id,
                )
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                )
                .where(
                    PaperPositionSnapshotRecord.account_id == snapshot.account_id,
                    PaperPositionSnapshotRecord.as_of == snapshot.as_of,
                )
                .order_by(PaperPositionSnapshotRecord.instrument_id)
            )
        ).all()
    )
    positions = tuple(
        PublishedPaperPosition(
            instrument_id=row.instrument_id,
            quantity=row.quantity,
            reserved_quantity=row.reserved_quantity,
            cost_basis=row.cost_basis,
            average_cost=(
                Decimal(0) if row.quantity == 0 else _quantize(row.cost_basis / row.quantity)
            ),
            reference_close=row.reference_close,
            market_value=_quantize(row.quantity * row.reference_close),
            unrealized_pnl=_quantize(row.quantity * row.reference_close - row.cost_basis),
            price_authority_id=authority.id,
            price_content_hash=authority.content_hash,
            price_source=manifest.source,
            price_feed=manifest.feed,
            price_available_at=authority.available_at,
            projection_hash=row.projection_hash,
        )
        for row, authority, manifest in position_rows
    )
    attribution_rows = tuple(
        (
            await session.scalars(
                select(PaperPerformanceAttributionRecord)
                .where(PaperPerformanceAttributionRecord.performance_snapshot_id == snapshot.id)
                .order_by(
                    PaperPerformanceAttributionRecord.dimension,
                    PaperPerformanceAttributionRecord.dimension_key,
                )
            )
        ).all()
    )
    attributions = tuple(
        PaperPerformanceAttribution(
            dimension=cast(Literal["cash", "instrument", "sector"], row.dimension),
            dimension_key=row.dimension_key,
            market_value=row.market_value,
            realized_pnl=row.realized_pnl,
            unrealized_pnl=row.unrealized_pnl,
            fees=row.fees,
            slippage=row.slippage,
            completeness=cast(Literal["FULL", "PARTIAL"], row.completeness),
            content_hash=row.content_hash,
        )
        for row in attribution_rows
    )
    market_value = _quantize(
        sum((position.market_value for position in positions), start=Decimal(0))
    )
    return PublishedPaperPerformance(
        snapshot_id=snapshot.id,
        replayed=replayed,
        performance=PaperPerformance(
            account_id=snapshot.account_id,
            as_of=snapshot.as_of,
            cash=snapshot.cash,
            market_value=market_value,
            nav=snapshot.nav,
            peak_nav=snapshot.peak_nav,
            realized_pnl=snapshot.realized_pnl,
            unrealized_pnl=snapshot.unrealized_pnl,
            fees=snapshot.fees,
            drawdown_ratio=snapshot.drawdown_ratio,
            content_hash=snapshot.content_hash,
        ),
        positions=positions,
        attributions=attributions,
    )


async def publish_paper_performance_snapshot(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
    hooks: PaperPerformanceHooks | None = None,
) -> PublishedPaperPerformance:
    """Atomically publish one internal-paper close from frozen PostgreSQL facts."""
    effective_at = _utc(as_of, field="as_of")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1919))"),
        {"scope": f"internal-paper-performance:{account_id}"},
    )
    account = await session.scalar(
        select(PaperAccountRecord).where(PaperAccountRecord.id == account_id).with_for_update()
    )
    if account is None or account.capability != "simulation_write_internal":
        raise PaperPerformanceError("PAPER_PERFORMANCE_INVALID")
    existing = await session.scalar(
        select(PaperPerformanceSnapshotRecord).where(
            PaperPerformanceSnapshotRecord.account_id == account_id,
            PaperPerformanceSnapshotRecord.as_of == effective_at,
        )
    )
    if existing is not None:
        return await _load_publication(session, snapshot=existing, replayed=True)
    latest_as_of = await session.scalar(
        select(func.max(PaperPerformanceSnapshotRecord.as_of)).where(
            PaperPerformanceSnapshotRecord.account_id == account_id
        )
    )
    if latest_as_of is not None and effective_at < latest_as_of:
        raise PaperPerformanceError("PAPER_PERFORMANCE_CONFLICT")

    calendar = await session.scalar(
        select(ExchangeCalendarSessionRecord)
        .join(
            ExchangeCalendarAuthorityRecord,
            ExchangeCalendarAuthorityRecord.id
            == ExchangeCalendarSessionRecord.calendar_authority_id,
        )
        .where(
            ExchangeCalendarSessionRecord.market_close_at == effective_at,
            ExchangeCalendarAuthorityRecord.completeness_status == "complete",
            ExchangeCalendarAuthorityRecord.published_at <= effective_at,
        )
        .order_by(ExchangeCalendarAuthorityRecord.published_at.desc())
        .limit(1)
    )
    if calendar is None:
        raise PaperPerformanceError("PAPER_PERFORMANCE_INVALID")

    try:
        ledger = await _load_ledger_as_of(
            session,
            account_id=account_id,
            as_of=effective_at,
        )
        balances = balance_ledger(ledger)
        cash = _quantize(balances["asset.cash.available.usd"] + balances["asset.cash.reserved.usd"])
        realized_pnl = _quantize(balances["income.realized_pnl.usd"])
        fees = _quantize(balances["expense.fees.usd"])
        projections = await _position_projections_as_of(
            session,
            account_id=account_id,
            as_of=effective_at,
        )
        reserved = await _reserved_positions_as_of(
            session,
            account_id=account_id,
            as_of=effective_at,
        )
    except (KeyError, ValueError) as error:
        raise PaperPerformanceError("PAPER_PERFORMANCE_PROJECTION_INVALID") from error

    has_open_positions = any(
        projection.quantity != 0 for projection in projections.values()
    )
    if has_open_positions and not current_paper_performance_price_authority_available():
        raise PaperPerformanceError("PAPER_PERFORMANCE_PRICE_INCOMPLETE")

    source_policy_id: UUID | None = None
    if has_open_positions:
        source_policy_id = await session.scalar(
            select(StrategyDecisionRecord.source_policy_id)
            .where(
                StrategyDecisionRecord.account_id == account_id,
                StrategyDecisionRecord.decision_time <= effective_at,
            )
            .order_by(
                StrategyDecisionRecord.decision_time.desc(),
                StrategyDecisionRecord.id.desc(),
            )
            .limit(1)
        )
        if source_policy_id is None:
            raise PaperPerformanceError("PAPER_PERFORMANCE_PRICE_INCOMPLETE")

    marked_positions: list[MarkedPosition] = []
    published_positions: list[PublishedPaperPosition] = []
    for instrument_id, projection in sorted(projections.items(), key=lambda pair: pair[0].int):
        if projection.quantity == 0:
            if reserved.get(instrument_id, Decimal(0)) != 0:
                raise PaperPerformanceError("PAPER_PERFORMANCE_PROJECTION_INVALID")
            continue
        if source_policy_id is None:
            raise PaperPerformanceError("PAPER_PERFORMANCE_PRICE_INCOMPLETE")
        mark = await _select_mark(
            session,
            instrument_id=instrument_id,
            session_date=calendar.session,
            as_of=effective_at,
            source_policy_id=source_policy_id,
        )
        if mark is None:
            raise PaperPerformanceError("PAPER_PERFORMANCE_PRICE_INCOMPLETE")
        bar, authority, manifest = mark
        reserved_quantity = _quantize(reserved.get(instrument_id, Decimal(0)))
        if reserved_quantity < 0 or reserved_quantity > projection.quantity:
            raise PaperPerformanceError("PAPER_PERFORMANCE_PROJECTION_INVALID")
        marked = MarkedPosition(
            instrument_id=instrument_id,
            quantity=projection.quantity,
            cost_basis=projection.cost_basis,
            reference_close=bar.close,
            price_authority_id=authority.id,
            price_content_hash=authority.content_hash,
        )
        marked_positions.append(marked)
        projection_hash = _hash(
            {
                "schema": "paper_position_snapshot_sha256_v1",
                "account_id": str(account_id),
                "instrument_id": str(instrument_id),
                "as_of": effective_at.isoformat().replace("+00:00", "Z"),
                "quantity": _decimal(projection.quantity),
                "reserved_quantity": _decimal(reserved_quantity),
                "cost_basis": _decimal(projection.cost_basis),
                "reference_close": _decimal(bar.close),
                "price_authority_id": str(authority.id),
                "price_content_hash": authority.content_hash,
                "position_source_hash": projection.source_fact_hash,
            }
        )
        published_positions.append(
            PublishedPaperPosition(
                instrument_id=instrument_id,
                quantity=projection.quantity,
                reserved_quantity=reserved_quantity,
                cost_basis=projection.cost_basis,
                average_cost=projection.average_cost,
                reference_close=bar.close,
                market_value=_quantize(projection.quantity * bar.close),
                unrealized_pnl=_quantize(projection.quantity * bar.close - projection.cost_basis),
                price_authority_id=authority.id,
                price_content_hash=authority.content_hash,
                price_source=manifest.source,
                price_feed=manifest.feed,
                price_available_at=authority.available_at,
                projection_hash=projection_hash,
            )
        )

    try:
        attributions, sector_source_hashes = await _build_attributions(
            session,
            account_id=account_id,
            as_of=effective_at,
            session_date=calendar.session,
            cash=cash,
            positions=tuple(published_positions),
            ledger_realized=realized_pnl,
            ledger_fees=fees,
        )
    except ValueError as error:
        raise PaperPerformanceError("PAPER_PERFORMANCE_PROJECTION_INVALID") from error
    previous_peak = await session.scalar(
        select(PaperPerformanceSnapshotRecord.peak_nav)
        .where(
            PaperPerformanceSnapshotRecord.account_id == account_id,
            PaperPerformanceSnapshotRecord.as_of < effective_at,
        )
        .order_by(PaperPerformanceSnapshotRecord.as_of.desc())
        .limit(1)
    )
    source_hashes = {
        *(transaction.transaction_hash for transaction in ledger.transactions),
        *(projection.source_fact_hash for projection in projections.values()),
        *(position.price_content_hash for position in published_positions),
        *(attribution.content_hash for attribution in attributions),
        *sector_source_hashes,
    }
    fill_model_hashes = tuple(
        (
            await session.scalars(
                select(PaperFillRecord.execution_model_hash)
                .where(
                    PaperFillRecord.account_id == account_id,
                    PaperFillRecord.occurred_at <= effective_at,
                )
                .distinct()
            )
        ).all()
    )
    source_hashes.update(fill_model_hashes)
    try:
        performance = project_paper_performance(
            account_id=account_id,
            as_of=effective_at,
            cash=cash,
            realized_pnl=realized_pnl,
            fees=fees,
            previous_peak_nav=previous_peak,
            positions=tuple(marked_positions),
            source_fact_hashes=tuple(sorted(source_hashes)),
        )
    except ValueError as error:
        raise PaperPerformanceError("PAPER_PERFORMANCE_PROJECTION_INVALID") from error

    snapshot = PaperPerformanceSnapshotRecord(
        id=new_uuid7(),
        account_id=account_id,
        as_of=effective_at,
        nav=performance.nav,
        peak_nav=performance.peak_nav,
        cash=performance.cash,
        realized_pnl=performance.realized_pnl,
        unrealized_pnl=performance.unrealized_pnl,
        fees=performance.fees,
        drawdown_ratio=performance.drawdown_ratio,
        content_hash=performance.content_hash,
    )
    session.add(snapshot)
    await session.flush()
    _observe(hooks, "snapshot_persisted")
    session.add_all(
        PaperPositionSnapshotRecord(
            id=new_uuid7(),
            account_id=account_id,
            instrument_id=position.instrument_id,
            as_of=effective_at,
            quantity=position.quantity,
            reserved_quantity=position.reserved_quantity,
            cost_basis=position.cost_basis,
            reference_close=position.reference_close,
            price_authority_id=position.price_authority_id,
            projection_hash=position.projection_hash,
        )
        for position in published_positions
    )
    await session.flush()
    _observe(hooks, "positions_persisted")
    session.add_all(
        PaperPerformanceAttributionRecord(
            id=new_uuid7(),
            performance_snapshot_id=snapshot.id,
            account_id=account_id,
            dimension=attribution.dimension,
            dimension_key=attribution.dimension_key,
            market_value=attribution.market_value,
            realized_pnl=attribution.realized_pnl,
            unrealized_pnl=attribution.unrealized_pnl,
            fees=attribution.fees,
            slippage=attribution.slippage,
            completeness=attribution.completeness,
            content_hash=attribution.content_hash,
        )
        for attribution in attributions
    )
    await session.flush()
    _observe(hooks, "attributions_persisted")
    persisted = await _load_publication(session, snapshot=snapshot, replayed=False)
    if (
        persisted.performance != performance
        or persisted.positions != tuple(published_positions)
        or persisted.attributions
        != tuple(
            sorted(
                attributions,
                key=lambda item: (item.dimension, item.dimension_key),
            )
        )
    ):
        raise PaperPerformanceError("PAPER_PERFORMANCE_CONFLICT")
    return persisted

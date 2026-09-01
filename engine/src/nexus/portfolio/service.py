"""PostgreSQL-authoritative redacted portfolio projections."""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import and_, desc, or_, select, true, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.portfolio import (
    CashBucketResponse,
    InternalPaperPortfolioListItemResponse,
    OpenOrderResponse,
    PortfolioKind,
    PortfolioListItemResponse,
    PortfolioPageResponse,
    PortfolioSnapshotResponse,
    PositionResponse,
    derive_snapshot_readiness,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.time import require_utc
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.paper import PaperAccountRecord
from nexus.persistence.models.portfolio import (
    Account,
    BrokerOrderSnapshot,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.portfolio.cursor import PortfolioCursorCodec, PortfolioCursorState

_MAX_CASH_BUCKETS = 32
_MAX_CURRENCIES = 32
_MAX_POSITIONS = 2000
_MAX_OPEN_ORDERS = 2000


class PortfolioService:
    def __init__(
        self,
        session: AsyncSession,
        request_time: datetime,
        *,
        cursor_signing_secrets: Sequence[str] = (),
    ) -> None:
        self._session = session
        self._request_time = require_utc(request_time)
        self._cursor_secrets = tuple(cursor_signing_secrets)

    async def list_page(
        self,
        *,
        kind: PortfolioKind | None,
        cursor: str | None,
        limit: int,
    ) -> tuple[PortfolioPageResponse, datetime]:
        codec = PortfolioCursorCodec(self._cursor_secrets, self._request_time)
        state = codec.decode(cursor, requested_kind=kind) if cursor else None
        decision_time = state.decision_time if state else self._request_time
        if kind == "shadow":
            return PortfolioPageResponse(items=(), next_cursor=None), decision_time

        latest_snapshot = (
            select(
                PortfolioSnapshotRecord.id.label("snapshot_id"),
                PortfolioSnapshotRecord.status.label("snapshot_status"),
                PortfolioSnapshotRecord.as_of.label("snapshot_as_of"),
            )
            .where(
                PortfolioSnapshotRecord.portfolio_id == Portfolio.id,
                PortfolioSnapshotRecord.created_at <= decision_time,
            )
            .order_by(
                desc(PortfolioSnapshotRecord.created_at),
                desc(PortfolioSnapshotRecord.id),
            )
            .limit(1)
            .lateral()
        )
        latest_run = (
            select(BrokerSyncRun.status.label("run_status"))
            .where(
                BrokerSyncRun.portfolio_id == Portfolio.id,
                BrokerSyncRun.created_at <= decision_time,
            )
            .order_by(desc(BrokerSyncRun.created_at), desc(BrokerSyncRun.id))
            .limit(1)
            .lateral()
        )
        statement = (
            select(
                Portfolio.id.label("portfolio_id"),
                Portfolio.name,
                Account.provider,
                PaperAccountRecord.id.label("paper_account_id"),
                PaperAccountRecord.currency.label("paper_currency"),
                PaperAccountRecord.account_model,
                PaperAccountRecord.position_model,
                latest_snapshot.c.snapshot_id,
                latest_snapshot.c.snapshot_status,
                latest_snapshot.c.snapshot_as_of,
                latest_run.c.run_status,
            )
            .join(Account, Account.id == Portfolio.account_id)
            .outerjoin(
                PaperAccountRecord,
                PaperAccountRecord.portfolio_id == Portfolio.id,
            )
            .outerjoin(latest_snapshot, true())
            .outerjoin(latest_run, true())
            .where(Portfolio.created_at <= decision_time)
            .order_by(Portfolio.name, Portfolio.id)
            .limit(limit + 1)
        )
        if kind == "real_read_only":
            statement = statement.where(PaperAccountRecord.id.is_(None))
        elif kind == "internal_paper":
            statement = statement.where(PaperAccountRecord.id.is_not(None))
        if state is not None:
            statement = statement.where(
                or_(
                    Portfolio.name > state.after_name,
                    and_(
                        Portfolio.name == state.after_name,
                        Portfolio.id > state.after_portfolio_id,
                    ),
                )
            )
        rows = (await self._session.execute(statement)).all()
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        currencies = await self._currencies(
            tuple(
                row.snapshot_id
                for row in page_rows
                if row.paper_account_id is None and row.snapshot_id is not None
            )
        )
        items: list[PortfolioListItemResponse | InternalPaperPortfolioListItemResponse] = []
        for row in page_rows:
            if row.paper_account_id is None:
                items.append(
                    PortfolioListItemResponse(
                        portfolio_id=row.portfolio_id,
                        name=row.name,
                        kind="real_read_only",
                        coverage_scope="broker_account",
                        provider=row.provider,
                        latest_snapshot_status=row.snapshot_status,
                        latest_snapshot_as_of=row.snapshot_as_of,
                        original_currencies=currencies.get(row.snapshot_id, ()),
                        last_sync_run_status=row.run_status,
                    )
                )
                continue
            try:
                projection = await load_paper_projection(
                    self._session,
                    account_id=row.paper_account_id,
                    as_of=decision_time,
                )
            except ValueError:
                items.append(
                    InternalPaperPortfolioListItemResponse(
                        portfolio_id=row.portfolio_id,
                        name=row.name,
                        kind="internal_paper",
                        coverage_scope="internal_paper_account",
                        source="internal_ledger",
                        currency="USD",
                        account_model="cash",
                        position_model="long_only",
                        data_state="ERROR",
                        projection_error_code="PAPER_PROJECTION_INVALID",
                        nav=None,
                        available_cash=None,
                        reserved_cash=None,
                        as_of=None,
                    )
                )
                continue
            items.append(
                InternalPaperPortfolioListItemResponse(
                    portfolio_id=row.portfolio_id,
                    name=row.name,
                    kind="internal_paper",
                    coverage_scope="internal_paper_account",
                    source="internal_ledger",
                    currency="USD",
                    account_model="cash",
                    position_model="long_only",
                    data_state="READY",
                    projection_error_code=None,
                    nav=decimal_to_string(projection.nav),
                    available_cash=decimal_to_string(projection.available_cash),
                    reserved_cash=decimal_to_string(projection.reserved_cash),
                    as_of=projection.as_of,
                )
            )
        next_cursor = None
        if has_more and page_rows:
            last = page_rows[-1]
            next_cursor = codec.encode(
                PortfolioCursorState(decision_time, last.name, last.portfolio_id, kind)
            )
        return PortfolioPageResponse(items=tuple(items), next_cursor=next_cursor), decision_time

    async def _currencies(self, snapshot_ids: tuple[UUID, ...]) -> dict[UUID, tuple[str, ...]]:
        if not snapshot_ids:
            return {}
        values: dict[UUID, set[str]] = {snapshot_id: set() for snapshot_id in snapshot_ids}
        currency_facts = union_all(
            select(
                CashSnapshot.portfolio_snapshot_id.label("snapshot_id"),
                CashSnapshot.currency.label("currency"),
            ).where(CashSnapshot.portfolio_snapshot_id.in_(snapshot_ids)),
            select(
                PositionSnapshot.portfolio_snapshot_id.label("snapshot_id"),
                PositionSnapshot.currency.label("currency"),
            ).where(PositionSnapshot.portfolio_snapshot_id.in_(snapshot_ids)),
            select(
                BrokerOrderSnapshot.portfolio_snapshot_id.label("snapshot_id"),
                Instrument.currency.label("currency"),
            )
            .join(Instrument, Instrument.id == BrokerOrderSnapshot.instrument_id)
            .where(BrokerOrderSnapshot.portfolio_snapshot_id.in_(snapshot_ids)),
        ).subquery()
        rows = (
            await self._session.execute(
                select(currency_facts.c.snapshot_id, currency_facts.c.currency)
                .distinct()
                .order_by(currency_facts.c.snapshot_id, currency_facts.c.currency)
                .limit(len(snapshot_ids) * _MAX_CURRENCIES + 1)
            )
        ).all()
        if len(rows) > len(snapshot_ids) * _MAX_CURRENCIES:
            _raise_collection_overflow("original_currencies", _MAX_CURRENCIES)
        for snapshot_id, currency in rows:
            values[snapshot_id].add(currency)
            if len(values[snapshot_id]) > _MAX_CURRENCIES:
                _raise_collection_overflow("original_currencies", _MAX_CURRENCIES)
        return {
            snapshot_id: tuple(sorted(currencies))
            for snapshot_id, currencies in values.items()
        }

    async def latest_snapshot(self, portfolio_id: UUID) -> PortfolioSnapshotResponse:
        portfolio_exists = await self._session.scalar(
            select(Portfolio.id).where(Portfolio.id == portfolio_id)
        )
        if portfolio_exists is None:
            raise _snapshot_not_found(portfolio_id)
        snapshot = (
            await self._session.execute(
                select(
                    PortfolioSnapshotRecord.id.label("snapshot_id"),
                    PortfolioSnapshotRecord.portfolio_id,
                    PortfolioSnapshotRecord.status,
                    PortfolioSnapshotRecord.as_of,
                )
                .where(PortfolioSnapshotRecord.portfolio_id == portfolio_id)
                .order_by(
                    desc(PortfolioSnapshotRecord.created_at),
                    desc(PortfolioSnapshotRecord.id),
                )
                .limit(1)
            )
        ).first()
        if snapshot is None:
            raise _snapshot_not_found(portfolio_id)
        cash_rows = (
            await self._session.execute(
                select(
                    CashSnapshot.currency,
                    CashSnapshot.cash_balance,
                    CashSnapshot.available_cash,
                    CashSnapshot.provider_as_of,
                )
                .where(CashSnapshot.portfolio_snapshot_id == snapshot.snapshot_id)
                .order_by(CashSnapshot.currency, CashSnapshot.id)
                .limit(_MAX_CASH_BUCKETS + 1)
            )
        ).all()
        _require_bounded_collection("cash", cash_rows, _MAX_CASH_BUCKETS)
        position_rows = (
            await self._session.execute(
                select(
                    PositionSnapshot.instrument_id,
                    Instrument.exchange,
                    PositionSnapshot.currency,
                    PositionSnapshot.quantity,
                    PositionSnapshot.available_quantity,
                    PositionSnapshot.frozen_quantity,
                    PositionSnapshot.avg_cost,
                    PositionSnapshot.diluted_cost,
                    PositionSnapshot.current_price,
                    PositionSnapshot.provider_as_of,
                )
                .join(Instrument, Instrument.id == PositionSnapshot.instrument_id)
                .where(PositionSnapshot.portfolio_snapshot_id == snapshot.snapshot_id)
                .order_by(PositionSnapshot.instrument_id)
                .limit(_MAX_POSITIONS + 1)
            )
        ).all()
        _require_bounded_collection("positions", position_rows, _MAX_POSITIONS)
        order_rows = (
            await self._session.execute(
                select(
                    BrokerOrderSnapshot.id.label("snapshot_order_id"),
                    BrokerOrderSnapshot.instrument_id,
                    Instrument.currency,
                    BrokerOrderSnapshot.side,
                    BrokerOrderSnapshot.quantity,
                    BrokerOrderSnapshot.filled_quantity,
                    BrokerOrderSnapshot.price,
                    BrokerOrderSnapshot.status,
                    BrokerOrderSnapshot.updated_at,
                )
                .join(Instrument, Instrument.id == BrokerOrderSnapshot.instrument_id)
                .where(BrokerOrderSnapshot.portfolio_snapshot_id == snapshot.snapshot_id)
                .order_by(BrokerOrderSnapshot.id)
                .limit(_MAX_OPEN_ORDERS + 1)
            )
        ).all()
        _require_bounded_collection("open_orders", order_rows, _MAX_OPEN_ORDERS)
        instrument_ids = {row.instrument_id for row in position_rows}
        instrument_ids.update(row.instrument_id for row in order_rows)
        symbols = await self._symbols(tuple(instrument_ids), snapshot.as_of)
        display_readiness, decision_readiness = derive_snapshot_readiness(
            snapshot.status,
            has_display_facts=bool(cash_rows or position_rows),
        )
        return PortfolioSnapshotResponse(
            portfolio_snapshot_id=snapshot.snapshot_id,
            portfolio_id=snapshot.portfolio_id,
            coverage_scope="broker_account",
            status=snapshot.status,
            display_readiness=display_readiness,
            decision_readiness=decision_readiness,
            cash=tuple(
                CashBucketResponse(
                    currency=row.currency,
                    cash_balance=decimal_to_string(row.cash_balance),
                    available_cash=decimal_to_string(row.available_cash),
                    provider_as_of=row.provider_as_of,
                )
                for row in cash_rows
            ),
            positions=tuple(
                PositionResponse(
                    instrument_id=row.instrument_id,
                    symbol=symbols.get(row.instrument_id),
                    exchange=row.exchange,
                    currency=row.currency,
                    quantity=decimal_to_string(row.quantity),
                    available_quantity=decimal_to_string(row.available_quantity),
                    frozen_quantity=decimal_to_string(row.frozen_quantity),
                    avg_cost=_optional_decimal(row.avg_cost),
                    diluted_cost=_optional_decimal(row.diluted_cost),
                    current_price=_optional_decimal(row.current_price),
                    provider_as_of=row.provider_as_of,
                    read_only=True,
                )
                for row in position_rows
            ),
            open_orders=tuple(
                OpenOrderResponse(
                    broker_order_snapshot_id=row.snapshot_order_id,
                    instrument_id=row.instrument_id,
                    symbol=symbols.get(row.instrument_id),
                    currency=row.currency,
                    side=row.side,
                    quantity=decimal_to_string(row.quantity),
                    filled_quantity=decimal_to_string(row.filled_quantity),
                    price=_optional_decimal(row.price),
                    status=row.status,
                    updated_at=row.updated_at,
                    read_only=True,
                )
                for row in order_rows
            ),
            converted_view=None,
            as_of=snapshot.as_of,
        )

    async def _symbols(
        self,
        instrument_ids: tuple[UUID, ...],
        as_of: datetime | None,
    ) -> dict[UUID, str]:
        if not instrument_ids or as_of is None:
            return {}
        rows = await self._session.execute(
            select(InstrumentAlias.instrument_id, InstrumentAlias.symbol)
            .where(
                InstrumentAlias.instrument_id.in_(instrument_ids),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= as_of.date(),
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= as_of.date(),
                ),
            )
            .order_by(
                InstrumentAlias.instrument_id,
                desc(InstrumentAlias.valid_from),
                desc(InstrumentAlias.id),
            )
        )
        symbols: dict[UUID, str] = {}
        for instrument_id, symbol in rows:
            symbols.setdefault(instrument_id, symbol)
        return symbols


def _optional_decimal(value: Decimal | None) -> str | None:
    return decimal_to_string(value) if value is not None else None


def _snapshot_not_found(portfolio_id: UUID) -> NexusError:
    return NexusError(
        ErrorCode.NOT_FOUND,
        "The portfolio snapshot was not found.",
        status_code=404,
        details={"portfolio_id": str(portfolio_id)},
    )


def _require_bounded_collection(name: str, values: Sequence[object], maximum: int) -> None:
    if len(values) <= maximum:
        return
    _raise_collection_overflow(name, maximum)


def _raise_collection_overflow(name: str, maximum: int) -> None:
    raise NexusError(
        ErrorCode.DATA_PARTIAL,
        "The portfolio snapshot exceeds a safe response bound.",
        status_code=409,
        details={"collection": name, "maximum": maximum},
    )

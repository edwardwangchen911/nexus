"""PostgreSQL-authoritative redacted portfolio projections."""

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, desc, or_, select, true
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.portfolio import (
    PortfolioKind,
    PortfolioListItemResponse,
    PortfolioPageResponse,
)
from nexus.domain.time import require_utc
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.portfolio.cursor import PortfolioCursorCodec, PortfolioCursorState


class PortfolioService:
    def __init__(
        self,
        session: AsyncSession,
        request_time: datetime,
        *,
        cursor_signing_secrets: Sequence[str],
    ) -> None:
        self._session = session
        self._request_time = require_utc(request_time)
        self._cursor = PortfolioCursorCodec(cursor_signing_secrets, self._request_time)

    async def list_page(
        self,
        *,
        kind: PortfolioKind | None,
        cursor: str | None,
        limit: int,
    ) -> tuple[PortfolioPageResponse, datetime]:
        state = self._cursor.decode(cursor, requested_kind=kind) if cursor else None
        decision_time = state.decision_time if state else self._request_time
        if kind in ("internal_paper", "shadow"):
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
                latest_snapshot.c.snapshot_id,
                latest_snapshot.c.snapshot_status,
                latest_snapshot.c.snapshot_as_of,
                latest_run.c.run_status,
            )
            .join(Account, Account.id == Portfolio.account_id)
            .outerjoin(latest_snapshot, true())
            .outerjoin(latest_run, true())
            .where(Portfolio.created_at <= decision_time)
            .order_by(Portfolio.name, Portfolio.id)
            .limit(limit + 1)
        )
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
            tuple(row.snapshot_id for row in page_rows if row.snapshot_id is not None)
        )
        items = tuple(
            PortfolioListItemResponse(
                portfolio_id=row.portfolio_id,
                name=row.name,
                provider=row.provider,
                latest_snapshot_status=row.snapshot_status,
                latest_snapshot_as_of=row.snapshot_as_of,
                original_currencies=currencies.get(row.snapshot_id, ()),
                last_sync_run_status=row.run_status,
            )
            for row in page_rows
        )
        next_cursor = None
        if has_more and page_rows:
            last = page_rows[-1]
            next_cursor = self._cursor.encode(
                PortfolioCursorState(decision_time, last.name, last.portfolio_id, kind)
            )
        return PortfolioPageResponse(items=items, next_cursor=next_cursor), decision_time

    async def _currencies(self, snapshot_ids: tuple[UUID, ...]) -> dict[UUID, tuple[str, ...]]:
        if not snapshot_ids:
            return {}
        values: dict[UUID, set[str]] = {snapshot_id: set() for snapshot_id in snapshot_ids}
        for model in (CashSnapshot, PositionSnapshot):
            rows = await self._session.execute(
                select(model.portfolio_snapshot_id, model.currency).where(
                    model.portfolio_snapshot_id.in_(snapshot_ids)
                )
            )
            for snapshot_id, currency in rows:
                values[snapshot_id].add(currency)
        return {
            snapshot_id: tuple(sorted(currencies))
            for snapshot_id, currencies in values.items()
        }

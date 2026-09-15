"""Bounded persisted projection for the internal-paper workbench."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.paper import (
    AgentPaperCycleResponse,
    AgentPaperSessionSummaryResponse,
    PaperOrderResponse,
    PaperPortfolioResponse,
)
from nexus.api.schemas.workbench import (
    WorkbenchAccountSummaryResponse,
    WorkbenchAttentionResponse,
    WorkbenchLoopResponse,
    WorkbenchResponse,
)
from nexus.paper.read_service import PaperReadService
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.runtime import WorkerHeartbeat

_LOOP_STALE_AFTER = timedelta(seconds=30)


class WorkbenchReadService:
    def __init__(self, session: AsyncSession, request_time: datetime) -> None:
        if request_time.tzinfo is None or request_time.utcoffset() is None:
            raise ValueError("request_time must be timezone-aware")
        self._session = session
        self._request_time = request_time.astimezone(UTC)

    async def read(self) -> WorkbenchResponse:
        paper = PaperReadService(self._session, self._request_time)
        cycles = await paper.agent_cycles(decision_session=None, limit=20)
        portfolio = await paper.portfolio()
        active_orders = await paper.active_orders(limit=20)
        latest = cycles.items[0] if cycles.items else None
        loop = await self._loop_status()
        return WorkbenchResponse(
            mode="internal_paper",
            execution_locked=True,
            loop=loop,
            session=cycles.summary,
            latest_cycle=latest,
            active_orders=active_orders,
            account=_account_summary(portfolio),
            attention=_attention_items(
                latest=latest,
                orders=active_orders,
                loop=loop,
                session=cycles.summary,
            ),
        )

    async def _loop_status(self) -> WorkbenchLoopResponse:
        heartbeat = await self._session.scalar(
            select(WorkerHeartbeat)
            .where(
                WorkerHeartbeat.queue == "agent_paper",
                WorkerHeartbeat.migration_head == CURRENT_MIGRATION_HEAD,
                WorkerHeartbeat.seen_at <= self._request_time,
            )
            .order_by(WorkerHeartbeat.seen_at.desc(), WorkerHeartbeat.id.desc())
            .limit(1)
        )
        if heartbeat is None:
            return WorkbenchLoopResponse(
                status="unknown",
                started_at=None,
                last_seen_at=None,
                stale_after_seconds=30,
            )
        status: Literal["online", "offline"] = (
            "online"
            if self._request_time - heartbeat.seen_at <= _LOOP_STALE_AFTER
            else "offline"
        )
        return WorkbenchLoopResponse(
            status=status,
            started_at=heartbeat.started_at,
            last_seen_at=heartbeat.seen_at,
            stale_after_seconds=30,
        )


def _account_summary(portfolio: PaperPortfolioResponse) -> WorkbenchAccountSummaryResponse:
    performance = portfolio.performance
    return WorkbenchAccountSummaryResponse(
        portfolio_id=portfolio.portfolio_id,
        currency="USD",
        initial_capital=portfolio.initial_capital,
        data_state=portfolio.data_state,
        as_of=None if performance is None else performance.as_of,
        nav=None if performance is None else performance.nav,
        available_cash=None if performance is None else performance.available_cash,
        reserved_cash=None if performance is None else performance.reserved_cash,
        market_value=None if performance is None else performance.market_value,
        realized_pnl=None if performance is None else performance.realized_pnl,
        unrealized_pnl=None if performance is None else performance.unrealized_pnl,
    )


def _attention_items(
    *,
    latest: AgentPaperCycleResponse | None,
    orders: tuple[PaperOrderResponse, ...],
    loop: WorkbenchLoopResponse,
    session: AgentPaperSessionSummaryResponse,
) -> tuple[WorkbenchAttentionResponse, ...]:
    items: list[WorkbenchAttentionResponse] = []
    if latest is not None and latest.status == "unavailable":
        items.append(
            WorkbenchAttentionResponse(
                kind="data",
                severity="warning",
                title_code="AGENT_PAPER_DATA_UNAVAILABLE",
                reason_code=latest.result_code,
                safe_action="view_evidence",
                href=f"/decisions/{latest.cycle_id}",
                cycle_id=latest.cycle_id,
            )
        )
    if latest is not None and latest.status == "failed":
        items.append(
            WorkbenchAttentionResponse(
                kind="model",
                severity="critical",
                title_code="AGENT_PAPER_MODEL_FAILED",
                reason_code=latest.result_code,
                safe_action="view_decision",
                href=f"/decisions/{latest.cycle_id}",
                cycle_id=latest.cycle_id,
            )
        )
    if (
        latest is not None
        and latest.decision is not None
        and "rejected" in latest.decision.risk_outcomes
    ):
        items.append(
            WorkbenchAttentionResponse(
                kind="risk",
                severity="warning",
                title_code="AGENT_PAPER_RISK_REJECTED",
                reason_code="RISK_REJECTED",
                safe_action="view_decision",
                href=f"/decisions/{latest.cycle_id}",
                cycle_id=latest.cycle_id,
            )
        )
    for order in orders:
        if order.status == "reconciling":
            items.append(
                WorkbenchAttentionResponse(
                    kind="execution",
                    severity="critical",
                    title_code="PAPER_ORDER_RECONCILING",
                    reason_code=order.events[-1].reason_code,
                    safe_action="verify_original_result",
                    href="/accounts",
                    order_id=order.order_id,
                )
            )
    if loop.status == "offline" and session.status in {"ready", "in_progress", "completed"}:
        items.append(
            WorkbenchAttentionResponse(
                kind="service",
                severity="warning",
                title_code="AGENT_PAPER_LOOP_OFFLINE",
                reason_code="HEARTBEAT_STALE",
                safe_action="wait",
                href="/settings",
            )
        )
    return tuple(items[:10])

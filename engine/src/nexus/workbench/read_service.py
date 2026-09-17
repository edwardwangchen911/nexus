"""Bounded persisted projection for the internal-paper workbench."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select, text
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
    WorkbenchCompatibilityResponse,
    WorkbenchLoopResponse,
    WorkbenchResponse,
)
from nexus.ops.agent_paper_loop import _classify_agent_paper_service_heartbeat
from nexus.paper.read_service import PaperReadService
from nexus.persistence.migrations import (
    AGENT_PAPER_LOOP_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.workbench.readiness import cycle_blocker, runtime_readiness


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
        loop, compatibility = await self._loop_status()
        return WorkbenchResponse(
            mode="internal_paper",
            execution_locked=True,
            loop=loop,
            compatibility=compatibility,
            readiness=runtime_readiness(cycles.items, cycles.summary, active_orders),
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

    async def _loop_status(
        self,
    ) -> tuple[WorkbenchLoopResponse, WorkbenchCompatibilityResponse]:
        heads = tuple(
            await self._session.scalars(
                text("SELECT version_num FROM alembic_version ORDER BY version_num LIMIT 11")
            )
        )
        row = (
            await self._session.execute(
                select(func.clock_timestamp(), WorkerHeartbeat)
                .where(WorkerHeartbeat.queue == "agent_paper")
                .order_by(WorkerHeartbeat.seen_at.desc(), WorkerHeartbeat.id.desc())
                .limit(1)
            )
        ).one_or_none()
        heartbeat = row[1] if row else None
        classification = (
            "missing"
            if row is None
            else _classify_agent_paper_service_heartbeat(row[0], heartbeat)["status"]
        )
        compatibility = WorkbenchCompatibilityResponse(
            status="unknown"
            if classification in {"missing", "unavailable"}
            else (
                "incompatible"
                if classification == "incompatible" or heads != (CURRENT_MIGRATION_HEAD,)
                else "compatible"
            ),
            expected_process_version=AGENT_PAPER_LOOP_PROCESS_VERSION,
            expected_migration_head=CURRENT_MIGRATION_HEAD,
            reported_process_version=heartbeat.process_version if heartbeat else None,
            reported_migration_head=heartbeat.migration_head if heartbeat else None,
            database_migration_heads=heads,
        )
        if heads != (CURRENT_MIGRATION_HEAD,):
            compatibility.status = "incompatible"
            classification = "incompatible"
        reasons = {
            "missing": "HEARTBEAT_MISSING",
            "unavailable": "HEARTBEAT_INVALID",
            "stale": "HEARTBEAT_STALE",
            "incompatible": "HEARTBEAT_INCOMPATIBLE",
        }
        loop = WorkbenchLoopResponse.model_validate(
            {
                "status": "online"
                if classification == "fresh"
                else ("offline" if classification in {"stale", "incompatible"} else "unknown"),
                "reason_code": reasons.get(str(classification)),
                "started_at": heartbeat.started_at if heartbeat else None,
                "last_seen_at": heartbeat.seen_at if heartbeat else None,
                "stale_after_seconds": 30,
            }
        )
        return loop, compatibility


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
                kind=(
                    "data"
                    if cycle_blocker(latest) in {"data_missing", "stale"}
                    else (
                        "model" if cycle_blocker(latest) == "research_not_actionable" else "service"
                    )
                ),
                severity="warning",
                title_code="AGENT_PAPER_BLOCKED",
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
                reason_code=loop.reason_code or "HEARTBEAT_STALE",
                safe_action="wait",
                href="/settings",
            )
        )
    return tuple(items[:10])

"""Freeze one notification in the existing performance publication transaction."""

from datetime import UTC, datetime, time
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.paper.read_service import PaperReadService
from nexus.paper.session_summary import PaperSessionSummarySnapshot
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperFillRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import OutboxEvent


async def enqueue_paper_session_summary(
    session: AsyncSession,
    *,
    account: PaperAccountRecord,
    snapshot: PaperPerformanceSnapshotRecord,
    calendar: ExchangeCalendarSessionRecord,
) -> None:
    """Called once under the performance account lock; no transport or external reads."""
    exchange = await session.scalar(
        select(ExchangeCalendarAuthorityRecord.exchange).where(
            ExchangeCalendarAuthorityRecord.id == calendar.calendar_authority_id
        )
    )
    now = await session.scalar(select(func.clock_timestamp()))
    if exchange != "XNYS" or now is None or now < snapshot.as_of:
        return
    cycles = tuple(
        await session.scalars(
            select(AgentPaperCycleRecord.id).where(
                AgentPaperCycleRecord.account_id == account.id,
                AgentPaperCycleRecord.decision_session == calendar.session,
                AgentPaperCycleRecord.decision_time <= snapshot.as_of,
                AgentPaperCycleRecord.created_at <= now,
            )
        )
    )
    if not cycles:
        return
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(
                AgentPaperCycleEventRecord.cycle_id.in_(cycles),
                AgentPaperCycleEventRecord.occurred_at <= now,
                AgentPaperCycleEventRecord.created_at <= now,
            )
            .distinct(AgentPaperCycleEventRecord.cycle_id)
            .order_by(
                AgentPaperCycleEventRecord.cycle_id, AgentPaperCycleEventRecord.sequence.desc()
            )
        )
    )
    latest_orders = (
        select(PaperOrderEventRecord.order_id, PaperOrderEventRecord.to_status)
        .where(
            PaperOrderEventRecord.occurred_at <= now,
            PaperOrderEventRecord.created_at <= now,
        )
        .distinct(PaperOrderEventRecord.order_id)
        .order_by(PaperOrderEventRecord.order_id, PaperOrderEventRecord.event_seq.desc())
        .subquery()
    )
    cycle_orders: dict[UUID, list[str | None]] = {}
    for cycle_id, status in (
        await session.execute(
            select(StrategyDecisionRecord.agent_paper_cycle_id, latest_orders.c.to_status)
            .join(
                PaperOrderRecord, PaperOrderRecord.strategy_decision_id == StrategyDecisionRecord.id
            )
            .outerjoin(latest_orders, latest_orders.c.order_id == PaperOrderRecord.id)
            .where(
                StrategyDecisionRecord.agent_paper_cycle_id.in_(cycles),
                StrategyDecisionRecord.decision_origin == "agent_research",
                PaperOrderRecord.created_at <= now,
                StrategyDecisionRecord.created_at <= now,
            )
        )
    ).all():
        cycle_orders.setdefault(cycle_id, []).append(status)
    terminal = {"filled", "cancelled", "rejected", "risk_rejected"}
    completed = sum(
        event.to_status == "completed"
        or (
            event.to_status == "awaiting_market"
            and bool(cycle_orders.get(event.cycle_id))
            and all(status in terminal for status in cycle_orders[event.cycle_id])
        )
        for event in events
    )
    unavailable = sum(event.to_status in {"unavailable", "failed"} for event in events)
    no_order = sum(
        event.to_status == "completed" and event.result_code == "NO_ORDER_REQUIRED"
        for event in events
    )
    start = datetime.combine(calendar.session, time.min, ZoneInfo("America/New_York"))
    orders = tuple(
        await session.scalars(
            select(PaperOrderRecord)
            .join(PaperOrderBatchRecord, PaperOrderBatchRecord.id == PaperOrderRecord.batch_id)
            .where(
                PaperOrderRecord.account_id == account.id,
                PaperOrderBatchRecord.evaluated_at >= start,
                PaperOrderBatchRecord.evaluated_at <= snapshot.as_of,
            )
        )
    )
    fills = tuple(
        await session.scalars(
            select(PaperFillRecord).where(
                PaperFillRecord.account_id == account.id,
                PaperFillRecord.occurred_at >= start,
                PaperFillRecord.occurred_at <= snapshot.as_of,
            )
        )
    )
    previous_close = await session.scalar(
        select(func.max(ExchangeCalendarSessionRecord.market_close_at)).where(
            ExchangeCalendarSessionRecord.calendar_authority_id == calendar.calendar_authority_id,
            ExchangeCalendarSessionRecord.session < calendar.session,
        )
    )
    previous = (
        None
        if previous_close is None
        else await session.scalar(
            select(PaperPerformanceSnapshotRecord).where(
                PaperPerformanceSnapshotRecord.account_id == account.id,
                PaperPerformanceSnapshotRecord.as_of == previous_close,
            )
        )
    )
    pnl = None if previous is None else snapshot.nav - previous.nav
    daily_return = (
        None
        if previous is None
        else (snapshot.nav / previous.nav - Decimal(1)).quantize(Decimal("0.000000000001"))
    )
    comparison = await PaperReadService(session, now)._paper_comparison(
        account=account, snapshot=snapshot, data_state="READY"
    )
    rows = {row.key: row for row in comparison.rows}

    def comparable(key: Literal["hold_current", "market_benchmark"], field: str) -> Decimal | None:
        row = rows[key]
        value = getattr(row, field)
        if row.data_state != "READY" or value is None:
            return None
        return Decimal(value).quantize(Decimal("0.000000000001"))

    frozen = PaperSessionSummarySnapshot(
        schema_version="paper_session_summary_v1",
        mode="internal_paper",
        account_id=account.id,
        market="XNYS",
        session_date=calendar.session,
        as_of=snapshot.as_of.astimezone(UTC),
        frozen_at=now,
        currency="USD",
        cycle_count=len(cycles),
        completed_count=completed,
        unavailable_count=unavailable,
        pending_count=len(cycles) - completed - unavailable,
        no_order_count=no_order,
        order_count=len(orders),
        fill_count=len(fills),
        risk_rejected_count=sum(order.risk_outcome == "rejected" for order in orders),
        nav=snapshot.nav,
        daily_pnl=pnl,
        daily_return_ratio=daily_return,
        daily_fees=sum((fill.fee for fill in fills), Decimal(0)),
        drawdown_ratio=snapshot.drawdown_ratio,
        comparison_start=comparison.window_start.astimezone(ZoneInfo("America/New_York")).date(),
        hold_current_nav=comparable("hold_current", "nav"),
        hold_current_return_ratio=comparable("hold_current", "total_return_ratio"),
        spy_nav=comparable("market_benchmark", "nav"),
        spy_return_ratio=comparable("market_benchmark", "total_return_ratio"),
    )
    session.add(
        OutboxEvent(
            id=new_uuid7(),
            event_type="paper.session.summary.ready",
            aggregate_id=account.id,
            schema_version="paper_session_summary_v1",
            payload={"notification_snapshot": frozen.model_dump(mode="json")},
            published_at=None,
        )
    )
    await session.flush()

"""Bounded whole-stock pending work and immutable same-condition lineage."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.monitoring.contracts import PriceReviewCondition
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger

MAX_PENDING = 32


async def pending_stock(
    session: AsyncSession,
    *,
    account_id: UUID,
    instrument_id: UUID,
    now: datetime,
    allow_empty: bool = False,
    freeze_visible_only: bool = False,
) -> tuple[tuple[MonitorTrigger, MonitorCheck], ...]:
    from nexus.monitoring.daily_consumption import not_daily_covered

    visibility = (
        (
            MonitorTrigger.created_at <= now,
            MonitorCheck.created_at <= now,
            MonitorCheck.checked_at <= now,
        )
        if freeze_visible_only
        else ()
    )
    rows = (
        await session.execute(
            select(MonitorTrigger, MonitorCheck, func.count().over())
            .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
            .where(
                MonitorTrigger.account_id == account_id,
                MonitorTrigger.instrument_id == instrument_id,
                MonitorTrigger.status.in_(("pending", "blocked")),
                MonitorTrigger.research_run_id.is_(None),
                not_daily_covered(),
                *visibility,
            )
            .order_by(MonitorCheck.checked_at, MonitorTrigger.id)
            .limit(MAX_PENDING + 1)
        )
    ).all()
    if rows and rows[0][2] > MAX_PENDING:
        raise ValueError("MONITOR_PENDING_OVERFLOW")
    if (not rows and not allow_empty) or any(
        check.checked_at > now or trigger.created_at > now for trigger, check, _ in rows
    ):
        raise ValueError("MONITOR_PENDING_CONDITION_UNRESOLVED")
    return tuple((trigger, check) for trigger, check, _ in rows)


async def same_condition_lineage(
    session: AsyncSession, plan: MonitorPlan
) -> dict[UUID, MonitorPlan]:
    lineage = (
        select(MonitorPlan.id, MonitorPlan.predecessor_id, MonitorPlan.generation)
        .where(MonitorPlan.id == plan.id)
        .cte("monitor_lineage", recursive=True)
    )
    lineage = lineage.union_all(
        select(MonitorPlan.id, MonitorPlan.predecessor_id, MonitorPlan.generation)
        .join(lineage, MonitorPlan.id == lineage.c.predecessor_id)
        .where(
            MonitorPlan.account_id == plan.account_id,
            MonitorPlan.instrument_id == plan.instrument_id,
            MonitorPlan.generation == lineage.c.generation - 1,
            MonitorPlan.condition_hash == plan.condition_hash,
        )
    )
    return {
        row.id: row
        for row in await session.scalars(
            select(MonitorPlan).join(lineage, MonitorPlan.id == lineage.c.id)
        )
    }


def compatible_check(
    trigger: MonitorTrigger,
    check: MonitorCheck,
    plan: MonitorPlan,
    lineage: dict[UUID, MonitorPlan],
) -> bool:
    origin = lineage.get(check.plan_id)
    return (
        origin is not None
        and trigger.account_id == check.account_id == plan.account_id
        and trigger.instrument_id == check.instrument_id == plan.instrument_id
        and trigger.check_id == check.id
        and trigger.condition_hash == origin.condition_hash == plan.condition_hash
        and PriceReviewCondition.model_validate(origin.condition).semantic_hash
        == PriceReviewCondition.model_validate(plan.condition).semantic_hash
        == plan.condition_hash
        and origin.effective_at <= check.checked_at < origin.valid_until
        and check.result == "true"
        and check.observation is not None
    )

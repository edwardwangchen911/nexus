"""Read consumption receipts independently from terminal job and discovery status."""

from typing import TypedDict
from uuid import UUID

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from nexus.persistence.models.monitoring import MonitorTrigger


def not_mixed_consumed() -> ColumnElement[bool]:
    from nexus.monitoring.daily_v2_consumption import not_daily_event_consumed
    from nexus.persistence.models.monitoring import MonitorMixedEventReceipt

    return not_daily_event_consumed() & ~exists(
        select(MonitorMixedEventReceipt.id).where(
            MonitorMixedEventReceipt.trigger_id == MonitorTrigger.id
        )
    )


class MixedBatchReadback(TypedDict):
    impact_review: str
    coverage: str
    unresolved_event_count: int


async def mixed_batch_readback(session: AsyncSession, batch_id: UUID) -> MixedBatchReadback:
    from nexus.monitoring.daily_v2_consumption import validate_daily_event_receipt
    from nexus.persistence.models.monitoring import (
        MonitorCoverageReceipt,
        MonitorDailyEventReceipt,
        MonitorIncrement,
    )

    total = (
        await session.scalar(
            select(func.count())
            .select_from(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch_id)
        )
        or 0
    )
    unresolved = (
        await session.scalar(
            select(func.count())
            .select_from(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch_id, not_mixed_consumed())
        )
        or 0
    )
    rows = tuple(
        await session.scalars(
            select(MonitorCoverageReceipt)
            .join(MonitorIncrement, MonitorIncrement.run_id == MonitorCoverageReceipt.run_id)
            .where(MonitorIncrement.batch_id == batch_id)
        )
    )
    kinds = {row.kind for row in rows}
    invalid = 0
    for proof in await session.scalars(
        select(MonitorDailyEventReceipt)
        .join(MonitorTrigger, MonitorTrigger.id == MonitorDailyEventReceipt.trigger_id)
        .where(MonitorTrigger.batch_id == batch_id)
    ):
        if not await validate_daily_event_receipt(session, proof.id):
            invalid += 1
    unresolved += invalid
    return {
        "impact_review": "unverified"
        if invalid
        else "verified"
        if total > 0 and unresolved == 0
        else "not_verified",
        "coverage": (
            "unresolved"
            if kinds != {"price", "disclosure"}
            else "gap"
            if any(row.status == "gap" for row in rows)
            else "superseded"
            if any(row.status == "superseded" for row in rows)
            else "covered"
            if all(
                row.status in {"installed", "retained"} and row.active_plan_id is not None
                for row in rows
            )
            else "unresolved"
        ),
        "unresolved_event_count": unresolved,
    }


async def unadmitted_stock_count(
    session: AsyncSession,
    *,
    account_id: UUID,
    instrument_id: UUID,
) -> int:
    """The automatic capacity boundary covers the whole unresolved admission pool."""
    from nexus.monitoring.daily_consumption import not_daily_covered

    count = await session.scalar(
        select(func.count())
        .select_from(MonitorTrigger)
        .where(
            MonitorTrigger.account_id == account_id,
            MonitorTrigger.instrument_id == instrument_id,
            MonitorTrigger.status.in_(("pending", "blocked")),
            MonitorTrigger.research_run_id.is_(None),
            not_daily_covered(),
            not_mixed_consumed(),
        )
    )
    return int(count or 0)

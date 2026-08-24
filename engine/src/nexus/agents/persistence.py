"""PostgreSQL-authoritative UTC daily budget reservations for queued research runs."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord


async def reserve_daily_budget(
    session: AsyncSession,
    *,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    worst_case_usd: Decimal,
    pricing_version: str,
    pricing_fresh: bool,
) -> AgentDailyBudgetReservationRecord:
    """Serialize one UTC business day so concurrent sessions cannot oversubscribe it."""
    if now.tzinfo != UTC or not pricing_fresh:
        raise BudgetExhausted("stale pricing cannot reserve research budget")
    business_date = now.date()
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": str(business_date)}
    )
    used = await session.scalar(
        select(func.coalesce(func.sum(AgentDailyBudgetReservationRecord.reserved_usd), 0)).where(
            AgentDailyBudgetReservationRecord.business_date == business_date,
            AgentDailyBudgetReservationRecord.state == "reserved",
        )
    )
    if Decimal(str(used)) + worst_case_usd > daily_limit_usd:
        raise BudgetExhausted("daily research budget exhausted")
    reservation = AgentDailyBudgetReservationRecord(
        business_date=business_date,
        run_id=run_id,
        pricing_version=pricing_version,
        reserved_usd=worst_case_usd,
        settled_usd=None,
        state="reserved",
    )
    session.add(reservation)
    await session.flush()
    return reservation

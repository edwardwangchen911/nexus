"""Verified locked reads for Strategy Shadow."""

from __future__ import annotations

from datetime import date
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.engine import exchange_calendar_content_hash
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)


async def load_exact_calendar_sessions(
    session: AsyncSession,
    *,
    calendar_authority_id: UUID,
    after: date,
) -> tuple[date, ...]:
    calendar = await session.get(ExchangeCalendarAuthorityRecord, calendar_authority_id)
    rows = tuple(
        await session.scalars(
            select(ExchangeCalendarSessionRecord)
            .where(ExchangeCalendarSessionRecord.calendar_authority_id == calendar_authority_id)
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    if calendar is None or len(rows) < 68:
        raise ValueError("STRATEGY_SHADOW_CALENDAR_INCOMPLETE")
    all_sessions = tuple(item.session for item in rows)
    decision_times = {item.session: item.decision_at for item in rows}
    if (
        calendar.completeness_status != "complete"
        or all_sessions[0] != calendar.range_start
        or all_sessions[-1] != calendar.range_end
        or exchange_calendar_content_hash(all_sessions, decision_times) != calendar.content_hash
    ):
        raise ValueError("STRATEGY_SHADOW_CALENDAR_INVALID")
    window = tuple(item for item in all_sessions if item > after)[:68]
    if len(window) != 68:
        raise ValueError("STRATEGY_SHADOW_CALENDAR_INCOMPLETE")
    return window

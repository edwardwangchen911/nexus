"""Materialize the installed XNYS calendar, without inventing historical source evidence."""

from datetime import date, datetime, timedelta
from importlib.metadata import version

import exchange_calendars  # type: ignore[import-untyped]
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.authority_loader import _load_calendar
from nexus.backtest.engine import ExchangeCalendarAuthority, exchange_calendar_content_hash
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)


class CalendarPreparationError(ValueError):
    """Fixed, non-sensitive calendar failure codes."""


def calendar_definition(start: date, end: date) -> ExchangeCalendarAuthority:
    """Generate exact sessions; do not normalize unsupported or non-session endpoints."""
    official = exchange_calendars.get_calendar("XNYS")
    if (
        start > end
        or start < official.first_session.date()
        or end > official.last_session.date()
        or not official.is_session(start)
        or not official.is_session(end)
    ):
        raise CalendarPreparationError("BACKTEST_CALENDAR_RANGE_INVALID")
    sessions = tuple(value.date() for value in official.sessions_in_range(start, end))
    clocks = {
        value: official.session_close(value).to_pydatetime() + timedelta(minutes=30)
        for value in sessions
    }
    package_version = version("exchange-calendars")
    return ExchangeCalendarAuthority(
        authority_id=new_uuid7(),
        version=f"exchange_calendars_{package_version}_XNYS",
        source="exchange_calendars",
        range_start=start,
        range_end=end,
        query_parameters={"exchange": "XNYS", "package_version": package_version},
        content_hash=exchange_calendar_content_hash(sessions, clocks),
        sessions=sessions,
        decision_times=clocks,
    )


async def prepare_calendar(
    session: AsyncSession, start: date, end: date
) -> ExchangeCalendarAuthority:
    """Create root and all sessions atomically, or precisely replay an intact natural key."""
    expected = calendar_definition(start, end)
    official = exchange_calendars.get_calendar("XNYS")
    async with session.begin_nested():
        await session.execute(
            text(
                "SELECT pg_advisory_xact_lock(hashtextextended('backtest-authority-global', 1602))"
            )
        )
        root = await session.scalar(
            select(ExchangeCalendarAuthorityRecord)
            .where(
                ExchangeCalendarAuthorityRecord.exchange == expected.exchange,
                ExchangeCalendarAuthorityRecord.version == expected.version,
                ExchangeCalendarAuthorityRecord.range_start == start,
                ExchangeCalendarAuthorityRecord.range_end == end,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if root is None:
            materialized_at = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(materialized_at, datetime)
            root = ExchangeCalendarAuthorityRecord(
                id=expected.authority_id,
                exchange=expected.exchange,
                version=expected.version,
                source=expected.source,
                range_start=start,
                range_end=end,
                query_parameters=dict(expected.query_parameters),
                completeness_status=expected.completeness_status,
                content_hash=expected.content_hash,
                published_at=materialized_at,
                created_at=materialized_at,
            )
            session.add(root)
            await session.flush()
            session.add_all(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=root.id,
                    session=value,
                    market_open_at=official.session_open(value).to_pydatetime(),
                    market_close_at=official.session_close(value).to_pydatetime(),
                    decision_at=expected.decision_times[value],
                    created_at=materialized_at,
                )
                for value in expected.sessions
            )
            await session.flush()
        try:
            cutoff = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(cutoff, datetime)
            loaded = await _load_calendar(session, root.id, cutoff)
            if loaded != expected.model_copy(update={"authority_id": root.id}):
                raise ValueError("calendar definition differs")
            rows = tuple(
                await session.scalars(
                    select(ExchangeCalendarSessionRecord)
                    .where(ExchangeCalendarSessionRecord.calendar_authority_id == root.id)
                    .execution_options(populate_existing=True)
                )
            )
            if any(
                row.market_open_at != official.session_open(row.session).to_pydatetime()
                or row.market_close_at != official.session_close(row.session).to_pydatetime()
                for row in rows
            ):
                raise ValueError("calendar market bounds differ")
        except ValueError as error:
            raise CalendarPreparationError("BACKTEST_CALENDAR_AUTHORITY_INVALID") from error
    return loaded

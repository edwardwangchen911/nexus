"""Pure exchange-session schedule derivation for daily automation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

_NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class DailySessionSchedule:
    """All session-relative UTC times needed by the daily pipeline."""

    session: date
    planned_at: datetime
    quality_cutoff_at: datetime
    next_paper_execution_at: datetime | None
    last_session_of_week: bool


def build_daily_session_schedule(
    *,
    session: date | None,
    market_close_at: datetime | None,
    next_session: date | None,
) -> DailySessionSchedule | None:
    """Build one schedule from frozen XNYS sessions, or no-op off-session."""
    if session is None:
        if market_close_at is not None:
            raise ValueError("market_close_at requires a session")
        return None
    if market_close_at is None:
        raise ValueError("market_close_at is required for a session")
    if market_close_at.tzinfo is None or market_close_at.utcoffset() != timedelta(0):
        raise ValueError("market_close_at must use UTC")
    market_close_at = market_close_at.astimezone(UTC)
    if market_close_at.astimezone(_NEW_YORK).date() != session:
        raise ValueError("market_close_at must belong to the New York session")
    if next_session is not None and next_session <= session:
        raise ValueError("next_session must follow session")

    quality_cutoff_at = datetime.combine(
        session,
        time(18, 0),
        tzinfo=_NEW_YORK,
    ).astimezone(UTC)
    next_paper_execution_at = (
        None
        if next_session is None
        else datetime.combine(
            next_session,
            time(9, 55),
            tzinfo=_NEW_YORK,
        ).astimezone(UTC)
    )
    last_session_of_week = (
        next_session is not None
        and session.isocalendar()[:2] != next_session.isocalendar()[:2]
    )
    return DailySessionSchedule(
        session=session,
        planned_at=market_close_at + timedelta(minutes=30),
        quality_cutoff_at=quality_cutoff_at,
        next_paper_execution_at=next_paper_execution_at,
        last_session_of_week=last_session_of_week,
    )

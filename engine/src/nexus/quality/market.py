"""Pinned XNYS calendar semantics for market decisions."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import exchange_calendars as xcals  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict

NEW_YORK = ZoneInfo("America/New_York")


class ExchangeSession(BaseModel):
    """One XNYS session represented by aware UTC instants."""

    model_config = ConfigDict(frozen=True)

    session: date
    open_at: datetime
    close_at: datetime
    is_half_day: bool


def xnys_session_window(session_date: date) -> ExchangeSession | None:
    """Return the regular or observed half-day XNYS window for a fixed date."""
    calendar = xcals.get_calendar("XNYS")
    label = session_date.isoformat()
    if not calendar.is_session(label):
        return None
    open_at = calendar.session_open(label).to_pydatetime()
    close_at = calendar.session_close(label).to_pydatetime()
    close_local = close_at.astimezone(NEW_YORK)
    return ExchangeSession(
        session=session_date,
        open_at=open_at,
        close_at=close_at,
        is_half_day=close_local.hour < 16,
    )

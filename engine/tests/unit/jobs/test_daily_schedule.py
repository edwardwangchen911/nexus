"""Pure exchange-session scheduling contracts for the daily pipeline."""

from datetime import UTC, date, datetime

import pytest
from nexus.jobs.daily_schedule import build_daily_session_schedule


def test_normal_session_uses_close_plus_thirty_and_new_york_cutoff() -> None:
    schedule = build_daily_session_schedule(
        session=date(2026, 8, 27),
        market_close_at=datetime(2026, 8, 27, 20, 0, tzinfo=UTC),
        next_session=date(2026, 8, 28),
    )
    assert schedule is not None
    assert schedule.planned_at == datetime(2026, 8, 27, 20, 30, tzinfo=UTC)
    assert schedule.quality_cutoff_at == datetime(2026, 8, 27, 22, 0, tzinfo=UTC)
    assert schedule.next_paper_execution_at == datetime(
        2026, 8, 28, 13, 55, tzinfo=UTC
    )
    assert schedule.last_session_of_week is False


def test_half_day_and_dst_transition_use_authoritative_sessions() -> None:
    half_day = build_daily_session_schedule(
        session=date(2026, 11, 27),
        market_close_at=datetime(2026, 11, 27, 18, 0, tzinfo=UTC),
        next_session=date(2026, 11, 30),
    )
    assert half_day is not None
    assert half_day.planned_at == datetime(2026, 11, 27, 18, 30, tzinfo=UTC)
    assert half_day.quality_cutoff_at == datetime(2026, 11, 27, 23, 0, tzinfo=UTC)
    assert half_day.next_paper_execution_at == datetime(
        2026, 11, 30, 14, 55, tzinfo=UTC
    )
    assert half_day.last_session_of_week is True

    spring_transition = build_daily_session_schedule(
        session=date(2026, 3, 6),
        market_close_at=datetime(2026, 3, 6, 21, 0, tzinfo=UTC),
        next_session=date(2026, 3, 9),
    )
    assert spring_transition is not None
    assert spring_transition.quality_cutoff_at == datetime(
        2026, 3, 6, 23, 0, tzinfo=UTC
    )
    assert spring_transition.next_paper_execution_at == datetime(
        2026, 3, 9, 13, 55, tzinfo=UTC
    )


def test_non_session_and_incomplete_future_calendar_fail_closed() -> None:
    assert (
        build_daily_session_schedule(
            session=None,
            market_close_at=None,
            next_session=date(2026, 8, 28),
        )
        is None
    )
    schedule = build_daily_session_schedule(
        session=date(2026, 8, 28),
        market_close_at=datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        next_session=None,
    )
    assert schedule is not None
    assert schedule.next_paper_execution_at is None
    assert schedule.last_session_of_week is False


@pytest.mark.parametrize(
    "market_close_at",
    [
        datetime(2026, 8, 27, 20, 0),
        datetime(2026, 8, 28, 5, 0, tzinfo=UTC),
    ],
)
def test_market_close_must_be_utc_and_belong_to_session(
    market_close_at: datetime,
) -> None:
    with pytest.raises(ValueError, match="market_close_at"):
        build_daily_session_schedule(
            session=date(2026, 8, 27),
            market_close_at=market_close_at,
            next_session=date(2026, 8, 28),
        )

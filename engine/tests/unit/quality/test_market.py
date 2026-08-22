from datetime import UTC, date, datetime

from nexus.quality.market import xnys_session_window


def test_xnys_regular_session_uses_aware_utc_instants() -> None:
    session = xnys_session_window(date(2024, 7, 5))

    assert session is not None
    assert session.open_at == datetime(2024, 7, 5, 13, 30, tzinfo=UTC)
    assert session.close_at == datetime(2024, 7, 5, 20, 0, tzinfo=UTC)
    assert session.is_half_day is False


def test_xnys_holiday_has_no_session() -> None:
    assert xnys_session_window(date(2024, 7, 4)) is None


def test_xnys_observed_half_day_has_explicit_early_close() -> None:
    session = xnys_session_window(date(2024, 7, 3))

    assert session is not None
    assert session.close_at == datetime(2024, 7, 3, 17, 0, tzinfo=UTC)
    assert session.is_half_day is True

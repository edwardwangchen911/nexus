from datetime import UTC, date, datetime

import pytest
from nexus.ops.fosun_local_refresh import (
    _daily_window,
    _safe_failure_code,
    _symbol_and_exchange,
)


def test_daily_window_is_bounded_to_completed_xnys_sessions() -> None:
    start, end, rows = _daily_window(datetime(2026, 9, 1, 4, 0, tzinfo=UTC))

    assert end.date() == date(2026, 8, 31)
    assert start.date() == date(2025, 7, 27)
    assert 1 <= len(tuple(row for row in rows if row[0] <= end.date())) <= 500
    assert all(market_open < market_close for _, market_open, market_close in rows)


@pytest.mark.parametrize(
    ("provider_code", "symbol", "exchange"),
    (("usAAPL", "AAPL", "XNYS"), ("hk00656", "00656.HK", "XHKG")),
)
def test_provider_instrument_codes_are_mapped_without_guessing(
    provider_code: str,
    symbol: str,
    exchange: str,
) -> None:
    assert _symbol_and_exchange(provider_code) == (symbol, exchange)


def test_unsupported_provider_instrument_code_fails_closed() -> None:
    with pytest.raises(ValueError, match="LOCAL_INSTRUMENT_CODE_UNSUPPORTED"):
        _symbol_and_exchange("AAPL")


def test_failure_output_only_preserves_fixed_local_error_codes() -> None:
    assert _safe_failure_code(ValueError("LOCAL_KLINE_WINDOW_INCOMPLETE")) == (
        "LOCAL_KLINE_WINDOW_INCOMPLETE"
    )
    assert _safe_failure_code(ValueError("secret value")) == "LOCAL_READ_ONLY_REFRESH_FAILED"
    assert _safe_failure_code(RuntimeError("LOCAL_BAD-CHAR")) == (
        "LOCAL_READ_ONLY_REFRESH_FAILED"
    )

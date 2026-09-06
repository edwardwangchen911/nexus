"""A fixed 20-session public amount proxy, never an execution liquidity authority."""

from decimal import Decimal

import pytest
from unit.research.test_current_price_trends import source_bars


def calculate(bars, sessions):
    from nexus.research.liquidity_observation import calculate_liquidity_observation

    return calculate_liquidity_observation(bars, sessions)


def test_exact_twenty_session_amount_mean_keeps_decimal_precision():
    bars, sessions = source_bars(25)
    bars = tuple(b.model_copy(update={"volume": Decimal("1.234567890123")}) for b in bars)
    result = calculate(bars, sessions)
    assert result.status == "available"
    assert Decimal(result.value) == sum((b.close * b.volume for b in bars[-20:]), Decimal(0)) / 20
    assert result.window_start == sessions[-20] and result.window_end == sessions[-1]
    assert result.observed_sessions == 20 and result.missing_sessions == ()


@pytest.mark.parametrize(
    "count,missing,status", [(19, False, "insufficient_sessions"), (25, True, "missing_sessions")]
)
def test_no_shortened_denominator_or_reaching_back_over_missing_session(count, missing, status):
    bars, sessions = source_bars(count)
    if missing:
        bars = bars[:-1]
    result = calculate(bars, sessions)
    assert result.value is None and result.status == status and result.observed_sessions == 19
    assert result.missing_sessions == ((sessions[-1],) if missing else ())


def test_twenty_zero_volume_sessions_are_known_zero():
    bars, sessions = source_bars(20)
    result = calculate(bars, sessions)
    assert result.status == "available" and Decimal(result.value) == 0

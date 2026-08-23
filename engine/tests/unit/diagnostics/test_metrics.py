from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.metrics import (
    annualized_volatility,
    beta_and_correlation,
    beta_and_correlation_by_session,
    correlation_components,
    days_to_exit,
    maximum_drawdown,
    security_hhi,
    top_weights,
)


def test_top_weights_and_hhi_exclude_cash() -> None:
    """Adding cash to security normalization must fail this concentration example."""
    weights = top_weights((Decimal("60"), Decimal("30"), Decimal("10")))

    assert weights == (Decimal("0.6"), Decimal("0.3"), Decimal("0.1"))
    assert security_hhi((Decimal("60"), Decimal("30"), Decimal("10"))) == Decimal("0.46")


def test_top_weights_and_hhi_are_unknown_without_securities() -> None:
    assert top_weights(()) is None
    assert security_hhi(()) is None


@pytest.mark.parametrize("window", (20, 63))
def test_annualized_volatility_is_zero_for_complete_flat_history(window: int) -> None:
    """Replacing exact returns with price levels must fail this zero-volatility case."""
    closes = (Decimal("100"),) * (window + 1)

    assert annualized_volatility(closes, window) == Decimal("0")


def test_annualized_volatility_is_unknown_below_eighty_percent_coverage() -> None:
    assert annualized_volatility((Decimal("100"),) * 16, 20) is None


def test_maximum_drawdown_uses_the_worst_peak_to_trough_decline() -> None:
    """Using closing loss instead of running peak loss must fail this drawdown example."""
    closes = (Decimal("100"), Decimal("120"), Decimal("90")) + (Decimal("90"),) * 199

    assert maximum_drawdown(closes) == Decimal("0.25")


def test_maximum_drawdown_is_unknown_below_eighty_percent_coverage() -> None:
    assert maximum_drawdown((Decimal("100"),) * 201) is None


def test_beta_and_correlation_use_matched_returns() -> None:
    """Mismatching benchmark returns must fail this identical-series relationship."""
    result = beta_and_correlation(
        (Decimal("100"), Decimal("110"), Decimal("99")),
        (Decimal("200"), Decimal("220"), Decimal("198")),
        2,
        minimum_returns=2,
    )

    assert result == (Decimal("1"), Decimal("1"))


def test_beta_and_correlation_is_unknown_when_coverage_is_insufficient() -> None:
    assert beta_and_correlation((Decimal("100"),) * 100, (Decimal("100"),) * 100, 126) is None


def test_beta_by_session_fails_closed_when_shifted_sessions_lack_eighty_percent_overlap() -> None:
    start = date(2026, 1, 1)
    asset = {start + timedelta(days=index): Decimal(100 + index) for index in range(102)}
    benchmark = {
        start + timedelta(days=index + 1): Decimal(200 + index * 2) for index in range(102)
    }

    assert beta_and_correlation_by_session(asset, benchmark) is None


def test_correlation_components_connect_at_the_eighty_percent_boundary() -> None:
    """Changing >= 0.80 to > 0.80 must fail this exact-threshold graph edge."""
    first = UUID("0198d500-1000-7000-8000-000000000021")
    second = UUID("0198d500-1000-7000-8000-000000000022")
    returns = tuple(Decimal(value) for value in range(60))

    components = correlation_components({first: returns, second: returns})

    assert components == ((first, second),)


def test_correlation_components_require_sixty_overlapping_returns() -> None:
    first = UUID("0198d500-1000-7000-8000-000000000023")
    second = UUID("0198d500-1000-7000-8000-000000000024")
    returns = tuple(Decimal(value) for value in range(59))

    assert correlation_components({first: returns, second: returns}) == ()


def test_days_to_exit_uses_ten_percent_of_adv() -> None:
    """Dropping the ten-percent participation cap must fail this liquidity calculation."""
    assert days_to_exit(Decimal("100"), Decimal("200")) == Decimal("5")


def test_days_to_exit_is_unknown_for_missing_or_zero_adv() -> None:
    assert days_to_exit(Decimal("100"), None) is None
    assert days_to_exit(Decimal("100"), Decimal("0")) is None

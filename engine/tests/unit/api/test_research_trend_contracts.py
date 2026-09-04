"""Current split-price inputs never become PIT, total-return or trading authority."""

import pytest
from nexus.api.schemas import agents
from pydantic import ValidationError


def trend():
    return dict(
        evidence_ref_id="00000000-0000-0000-0000-000000000001",
        source="alpaca_current_trends",
        revision="a" * 64,
        content_hash="b" * 64,
        feed="sip",
        adjustment="split",
        currency="USD",
        timeframe="1Day",
        identity_asof="2026-08-28",
        query_start="2026-01-02",
        query_end="2026-08-28",
        first_observed_on="2026-01-02",
        last_observed_on="2026-08-28",
        observed_sessions=160,
        expected_sessions=161,
        contiguous_tail_sessions=21,
        missing_sessions=["2026-07-01"],
        missing_sessions_total=1,
        missing_sessions_truncated=False,
        received_at="2026-08-29T00:00:00Z",
        available_at="2026-08-29T00:01:00Z",
        features=dict(
            close="123.456789",
            sma_20="120.000000000001",
            sma_200=None,
            rsi_14="0",
            volatility_20="0.000000000001",
        ),
        historical_backtest_eligible=False,
        market_price_eligible=False,
        total_return_eligible=False,
        execution_locked=True,
    )


def test_old_inputs_default_trends_empty_and_values_preserve_precision():
    old = agents.ResearchInputsSummary(
        display_symbol=None, filings=(), facts=(), total_facts=0, truncated=False
    )
    assert old.current_price_trends == ()
    assert agents.ResearchInputPriceTrend.model_validate(trend()).model_dump(mode="json") == trend()


@pytest.mark.parametrize(
    "change",
    [
        {"source": "alpaca"},
        {"currency": None},
        {"feed": "iex"},
        {"adjustment": "raw"},
        {"historical_backtest_eligible": True},
        {"market_price_eligible": True},
        {"total_return_eligible": True},
        {"execution_locked": False},
        {"query_end": "2025-01-01"},
        {"observed_sessions": 162},
        {"contiguous_tail_sessions": 161},
        {"missing_sessions_total": 2},
        {"missing_sessions": ["2025-01-01"]},
        {"missing_sessions_truncated": True},
        {"available_at": "2026-08-28T00:00:00Z"},
    ],
)
def test_current_trend_rejects_wrong_scope_times_counts_and_qualification(change):
    with pytest.raises(ValidationError):
        agents.ResearchInputPriceTrend.model_validate({**trend(), **change})


@pytest.mark.parametrize(
    "change",
    [
        {"made_up_feature": "1"},
        {"close": "0"},
        {"sma_20": "-1"},
        {"rsi_14": "100.000000000001"},
        {"rsi_14": "-0.000000000001"},
        {"volatility_20": "-0.1"},
        {"close": 1},
        {"close": "NaN"},
        {"sma_200": "2"},
    ],
)
def test_fixed_feature_keys_values_and_warmup_are_closed(change):
    value = trend()
    value["features"].update(change)
    with pytest.raises(ValidationError):
        agents.ResearchInputPriceTrend.model_validate(value)

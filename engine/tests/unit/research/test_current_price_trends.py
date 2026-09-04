"""Current split-price features use real calendar sessions and synthetic source bars."""

import asyncio
import importlib
import json
from datetime import UTC, datetime, time
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars
import httpx
import pytest
from nexus.providers.base import PriceBar
from unit.ops.test_alpaca_prices_local import invoke as _invoke

invoke = _invoke


@pytest.mark.parametrize("bad_close", [False, True])
def test_normal_archive_builds_non_authority_trends_document(invoke, bad_close):
    from nexus.diagnostics.calendar_policy import (
        XNYS_CALENDAR_ADOPTION_ID,
        XNYS_CALENDAR_ADOPTION_KEY,
        XNYS_CALENDAR_ADOPTION_PAYLOAD,
        XNYS_CALENDAR_ADOPTION_VERSION,
        validate_xnys_calendar_adoption,
    )
    from nexus.persistence.models.diagnostics import DiagnosticPolicy
    from nexus.research import current_price_trends as trends
    from nexus.research.alpaca_prices_archive import load_price_collection

    build = getattr(trends, "build_current_price_trend", None)
    assert build is not None, "normal archive has no current trend input producer"
    bars, sessions = source_bars()
    raw = json.dumps(
        {
            "symbol": "AAPL",
            "bars": [
                {
                    "t": bar.observed_at.isoformat(),
                    "o": str(bar.open),
                    "h": str(bar.high),
                    "l": str(bar.low),
                    "c": None if bad_close else str(bar.close),
                    "v": str(bar.volume),
                }
                for bar in bars
            ],
            "next_page_token": None,
        }
    ).encode()
    code, directory, _ = invoke(
        [httpx.Response(200, content=raw)],
        extra=("--start", sessions[0].isoformat(), "--end", sessions[-1].isoformat()),
    )
    assert code == 0
    archive = load_price_collection(directory, instrument_id=bars[0].instrument_id)
    now = datetime.now(UTC)
    calendar = validate_xnys_calendar_adoption(
        DiagnosticPolicy(
            id=XNYS_CALENDAR_ADOPTION_ID,
            key=XNYS_CALENDAR_ADOPTION_KEY,
            version=XNYS_CALENDAR_ADOPTION_VERSION,
            schema_version="1",
            payload=XNYS_CALENDAR_ADOPTION_PAYLOAD,
            created_at=datetime(2026, 1, 2, tzinfo=UTC),
        ),
        now,
    )
    purpose = trends.purpose_policy_input(
        DiagnosticPolicy(
            id=trends.PURPOSE_POLICY_ID,
            key=trends.PURPOSE_POLICY_KEY,
            version=trends.PURPOSE_POLICY_VERSION,
            schema_version="1",
            payload=trends.PURPOSE_POLICY_PAYLOAD,
            created_at=datetime(2026, 1, 2, tzinfo=UTC),
        ),
        now,
    )
    if bad_close:
        with pytest.raises(ValueError, match="CURRENT_TRENDS_BARS_INVALID"):
            asyncio.run(
                build(archive, calendar=calendar, purpose_policy=purpose, materialized_at=now)
            )
        return
    document = asyncio.run(
        build(archive, calendar=calendar, purpose_policy=purpose, materialized_at=now)
    )
    assert document.features["sma_200"] == "100.500000000000"
    assert document.observed_sessions == document.contiguous_tail_sessions == 200
    assert document.received_at == archive.received_at <= document.available_at
    assert document.available_at == now
    assert document.source == "alpaca_current_trends"
    assert not document.historical_backtest_eligible
    assert not document.market_price_eligible
    assert not document.total_return_eligible
    assert document.execution_locked
    assert document.originals[0].raw_text.encode() == raw


def metrics(bars, sessions):
    name = "nexus.research.current_price_trends"
    assert importlib.util.find_spec(name) is not None, "current trend coverage consumer is missing"
    return importlib.import_module(name).trend_metrics(bars, sessions)


def source_bars(count=200):
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = tuple(
        x.date() for x in calendar.sessions_in_range("2025-01-02", "2025-12-31")[:count]
    )
    received = datetime(2026, 8, 1, tzinfo=UTC)
    bars = tuple(
        PriceBar(
            instrument_id=UUID("00000000-0000-4000-8000-000000000001"),
            session=day,
            open=Decimal(index),
            high=Decimal(index),
            low=Decimal(index),
            close=Decimal(index),
            volume=Decimal(0),
            observed_at=datetime.combine(day, time(), ZoneInfo("America/New_York")).astimezone(UTC),
            available_at=received,
            received_at=received,
        )
        for index, day in enumerate(sessions, 1)
    )
    return bars, sessions


def test_two_hundred_sessions_compute_actual_features():
    bars, sessions = source_bars()
    result = metrics(bars, sessions)
    assert result["observed_sessions"] == result["expected_sessions"] == 200
    assert result["contiguous_tail_sessions"] == 200
    assert result["missing_sessions"] == ()
    assert result["features"] == {
        "close": "200.000000000000",
        "sma_20": "190.500000000000",
        "sma_200": "100.500000000000",
        "rsi_14": "100.000000000000",
        "volatility_20": "0.002554228858",
    }


def test_missing_session_breaks_returns_and_warmup_not_observed_history():
    bars, sessions = source_bars()
    result = metrics(bars[:180] + bars[181:], sessions)
    assert result["observed_sessions"] == 199
    assert result["contiguous_tail_sessions"] == 19
    assert result["missing_sessions"] == (sessions[180],)
    assert result["first_observed_on"] == sessions[0]
    assert result["last_observed_on"] == sessions[-1]
    assert result["features"] == {
        "close": "200.000000000000",
        "sma_20": None,
        "sma_200": None,
        "rsi_14": "100.000000000000",
        "volatility_20": None,
    }


def test_missing_query_tail_retains_actual_observation_day_not_requested_end():
    bars, sessions = source_bars()
    result = metrics(bars[:-1], sessions)
    assert result["last_observed_on"] == sessions[-2]
    assert result["missing_sessions"] == (sessions[-1],)
    assert result["contiguous_tail_sessions"] == 199
    assert result["features"]["close"] == "199.000000000000"
    assert result["features"]["sma_200"] is None


@pytest.mark.parametrize(
    "count,known",
    [
        (0, set()),
        (1, {"close"}),
        (14, {"close"}),
        (15, {"close", "rsi_14"}),
        (20, {"close", "rsi_14", "sma_20"}),
        (21, {"close", "rsi_14", "sma_20", "volatility_20"}),
    ],
)
def test_literal_warmup_and_empty_window_remain_unknown(count, known):
    bars, sessions = source_bars()
    result = metrics(bars[:count], sessions)
    assert {key for key, value in result["features"].items() if value is not None} == known
    assert result["observed_sessions"] == count
    assert len(result["missing_sessions"]) == 200 - count


@pytest.mark.parametrize("kind", ["duplicate", "outside", "negative", "zero", "nan", "flagged"])
def test_invalid_source_bars_cannot_become_a_candidate(kind):
    bars, sessions = source_bars()
    if kind == "duplicate":
        bars = bars + (bars[0],)
    elif kind == "outside":
        sessions = sessions[1:]
    else:
        updates = {
            "close": {"negative": Decimal(-1), "zero": Decimal(0), "nan": Decimal("NaN")}.get(
                kind, Decimal(1)
            )
        }
        if kind == "flagged":
            updates = {"quality_flags": ("unknown",)}
        bars = (bars[0].model_copy(update=updates), *bars[1:])
    with pytest.raises(ValueError, match="CURRENT_TRENDS_BARS_INVALID"):
        metrics(bars, sessions)

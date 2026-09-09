"""Revalue a frozen-weight historical scenario; never copy baseline risk unchanged."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.diagnostics.assembler import DiagnosticAssembly
from nexus.diagnostics.models import MarketEvidence
from nexus.domain.evidence import EvidenceRef
from nexus.optimizer.scenario_risk import ScenarioEvidence, load_scenario_evidence, scenario_metrics
from nexus.providers.broker.base import BrokerPosition, PortfolioSnapshot


def test_cash_reduction_changes_drawdown_and_sector_exposure() -> None:
    instrument, baseline = UUID(int=1), UUID(int=2)
    sessions = tuple(date(2025, 1, 1) + timedelta(days=i) for i in range(253))
    closes = {day: Decimal("1") if i % 2 == 0 else Decimal("0.5") for i, day in enumerate(sessions)}
    evidence = ScenarioEvidence(
        baseline_id=baseline,
        valuation_time=datetime(2026, 1, 1, tzinfo=UTC),
        sessions=sessions,
        closes={instrument: closes},
        benchmark=closes,
        sectors={instrument: {"technology": Decimal("1")}},
        sector_required_ids=frozenset({instrument}),
        refs=(),
    )
    hold = scenario_metrics({instrument: Decimal("1")}, evidence)
    reduced = scenario_metrics({instrument: Decimal("0.5")}, evidence)
    assert hold["drawdown"].value == Decimal("0.5")
    # The 252-close horizon begins at price 0.5: half cash + one fixed share,
    # NAV peak 1.5 then trough 1, so drawdown is 1 - 1/1.5.
    assert reduced["drawdown"].value == Decimal(1) - Decimal(1) / Decimal("1.5")
    assert hold["sector_exposure"].value == Decimal("1")
    assert reduced["sector_exposure"].value == Decimal("0.5")
    assert reduced["volatility"].value < hold["volatility"].value
    assert hold["correlation"].value == Decimal("1")


def test_missing_history_or_sector_never_becomes_zero_risk() -> None:
    evidence = ScenarioEvidence(
        baseline_id=UUID(int=2),
        valuation_time=datetime(2026, 1, 1, tzinfo=UTC),
        sessions=(),
        closes={},
        benchmark={},
        sectors={},
        sector_required_ids=frozenset({UUID(int=1)}),
        refs=(),
    )
    result = scenario_metrics({UUID(int=1): Decimal("0.5")}, evidence)
    assert all(metric.status == "unknown" and metric.value is None for metric in result.values())


def _evidence(count=253):
    sessions = tuple(date(2025, 1, 1) + timedelta(days=i) for i in range(count))
    prices = {day: Decimal(100 + i % 3) for i, day in enumerate(sessions)}
    return ScenarioEvidence(
        UUID(int=2),
        datetime(2026, 1, 1, tzinfo=UTC),
        sessions,
        {UUID(int=1): prices},
        prices,
        {},
        frozenset({UUID(int=1)}),
        (),
    )


@pytest.mark.parametrize(
    "count,known",
    [
        (51, set()),
        (52, {"volatility"}),
        (64, {"volatility"}),
        (101, {"volatility"}),
        (102, {"volatility", "correlation"}),
        (127, {"volatility", "correlation"}),
        (201, {"volatility", "correlation"}),
        (202, {"volatility", "correlation", "drawdown"}),
    ],
)
def test_independent_windows_apply_their_own_eighty_percent_coverage(count, known):
    metrics = scenario_metrics({UUID(int=1): Decimal(1)}, _evidence(count))
    assert {name for name, value in metrics.items() if value.status == "known"} == known


def _volatility(nav, sessions):
    # Independent Decimal expectation: no return may bridge a missing session.
    returns = [nav[b] / nav[a] - 1 for a, b in zip(sessions, sessions[1:]) if a in nav and b in nav]
    mean = sum(returns) / len(returns)
    return (sum((value - mean) ** 2 for value in returns) / len(returns)).sqrt() * Decimal(
        252
    ).sqrt()


def _correlation(nav, benchmark, sessions):
    returns = [nav[b] / nav[a] - 1 for a, b in zip(sessions, sessions[1:])]
    market = [benchmark[b] / benchmark[a] - 1 for a, b in zip(sessions, sessions[1:])]
    centered = [value - sum(returns) / len(returns) for value in returns]
    market_centered = [value - sum(market) / len(market) for value in market]
    covariance = sum(a * b for a, b in zip(centered, market_centered))
    return (
        covariance
        / (sum(value**2 for value in centered) * sum(value**2 for value in market_centered)).sqrt()
    )


def _drawdown(nav, sessions):
    peak, worst = nav[sessions[0]], Decimal(0)
    for day in sessions:
        peak = max(peak, nav[day])
        worst = max(worst, (peak - nav[day]) / peak)
    return worst


def test_short_window_does_not_depend_on_the_old_253_day_anchor():
    evidence = _evidence()
    prices = {
        day: value
        for day, value in evidence.closes[UUID(int=1)].items()
        if day in evidence.sessions[-64:]
    }
    result = scenario_metrics(
        {UUID(int=1): Decimal(1)}, replace(evidence, closes={UUID(int=1): prices})
    )
    assert result["volatility"].value == _volatility(prices, evidence.sessions[-64:])
    assert result["correlation"].status == result["drawdown"].status == "unknown"


def test_missing_middle_price_does_not_form_a_cross_gap_return():
    evidence = _evidence(64)
    prices = dict(evidence.closes[UUID(int=1)])
    del prices[evidence.sessions[31]]
    result = scenario_metrics(
        {UUID(int=1): Decimal(1)}, replace(evidence, closes={UUID(int=1): prices})
    )
    assert result["volatility"].value == _volatility(prices, evidence.sessions)
    assert result["volatility"].value != _volatility(prices, tuple(prices))


def test_complete_history_rebases_each_window_without_daily_rebalancing():
    evidence = _evidence()
    first, second = UUID(int=1), UUID(int=3)
    prices_a = {
        day: Decimal(50 if i < 126 else 80 if i < 189 else 100 + 10 * (i % 3))
        for i, day in enumerate(evidence.sessions)
    }
    prices_b = {
        day: Decimal(200 if i < 126 else 160 if i < 189 else 100 - 20 * (i % 2))
        for i, day in enumerate(evidence.sessions)
    }
    evidence = replace(evidence, closes={first: prices_a, second: prices_b})
    for a_weight, b_weight in (
        (Decimal("0.5"), Decimal("0.25")),
        (Decimal("0.25"), Decimal("0.125")),
    ):
        result = scenario_metrics({first: a_weight, second: b_weight}, evidence)
        window = evidence.sessions[-64:]
        anchor = window[0]
        # Buy fixed shares at the common 63-return window anchor; cash is constant.
        a_shares, b_shares = a_weight / prices_a[anchor], b_weight / prices_b[anchor]
        cash = 1 - a_weight - b_weight
        expected_nav = {
            day: cash + a_shares * prices_a[day] + b_shares * prices_b[day] for day in window
        }
        expected = _volatility(expected_nav, window)
        legacy_nav = {
            day: cash
            + (a_weight / prices_a[evidence.sessions[0]]) * prices_a[day]
            + (b_weight / prices_b[evidence.sessions[0]]) * prices_b[day]
            for day in window
        }
        assert result["volatility"].value == expected
        assert expected != _volatility(legacy_nav, window)
        # Independently buy fixed shares at each other metric's own anchor.
        # This must not use the old 253-day anchor or daily target-weight returns.
        for name, count in (("correlation", 127), ("drawdown", 252)):
            metric_window = evidence.sessions[-count:]
            a_shares = a_weight / prices_a[metric_window[0]]
            b_shares = b_weight / prices_b[metric_window[0]]
            nav = {
                day: cash + a_shares * prices_a[day] + b_shares * prices_b[day]
                for day in metric_window
            }
            expected_metric = (
                _correlation(nav, evidence.benchmark, metric_window)
                if name == "correlation"
                else _drawdown(nav, metric_window)
            )
            # Algebraically equivalent Decimal operations can differ in the last digit.
            assert abs(result[name].value - expected_metric) < Decimal("1e-24")


@pytest.mark.parametrize(
    "count,metric", [(64, "volatility"), (127, "correlation"), (252, "drawdown")]
)
def test_missing_window_anchor_is_not_silently_shifted_for_a_candidate(count, metric):
    evidence = _evidence(count)
    prices = dict(evidence.closes[UUID(int=1)])
    del prices[evidence.sessions[0]]
    for weight in (Decimal(1), Decimal("0.5")):
        result = scenario_metrics(
            {UUID(int=1): weight}, replace(evidence, closes={UUID(int=1): prices})
        )
        assert result[metric].status == "unknown"


def test_future_reference_cannot_be_made_usable_by_a_shorter_window():
    evidence = _evidence()
    future_ref = EvidenceRef(
        source="fixture",
        object_id="future",
        location="fixture://future",
        available_at=evidence.valuation_time + timedelta(seconds=1),
        content_hash="a" * 64,
    )
    result = scenario_metrics({UUID(int=1): Decimal(1)}, replace(evidence, refs=(future_ref,)))
    assert all(metric.status == "unknown" for metric in result.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("quality,conflict", [("PARTIAL", False), ("PASS", True)])
async def test_loader_keeps_partial_or_conflicting_market_evidence_unusable(
    monkeypatch,
    quality,
    conflict,
):
    evidence = _evidence(64)
    snapshot = PortfolioSnapshot(
        id=evidence.baseline_id,
        portfolio_id=UUID(int=4),
        status="READY",
        cash=(),
        positions=(
            BrokerPosition(
                instrument_id=UUID(int=1),
                currency="USD",
                quantity="1",
                available_quantity="1",
                frozen_quantity="0",
                avg_cost=None,
                diluted_cost=None,
            ),
        ),
        open_orders=(),
        as_of=evidence.valuation_time,
    )
    # Only DB/assembly I/O is replaced. The loader's qualification branch is real:
    # even its published calendar must not be promoted to usable scenario inputs.
    market = MarketEvidence(
        quality=quality, source_conflict=conflict, trading_sessions=evidence.sessions
    )
    assembly = AsyncMock(return_value=DiagnosticAssembly(market, {}))
    monkeypatch.setattr(
        "nexus.optimizer.scenario_risk.load_instrument_identities", AsyncMock(return_value={})
    )
    monkeypatch.setattr("nexus.optimizer.scenario_risk.assemble_diagnostic_evidence", assembly)
    session = AsyncMock()
    session.scalars.return_value = []
    loaded = await load_scenario_evidence(session, snapshot, artifact_reader=AsyncMock())
    assembly.assert_awaited_once()
    assert loaded.sessions == ()
    assert all(
        value.status == "unknown"
        for value in scenario_metrics({UUID(int=1): Decimal(1)}, loaded).values()
    )

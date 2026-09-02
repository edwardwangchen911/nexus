"""Revalue a frozen-weight historical scenario; never copy baseline risk unchanged."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.optimizer.scenario_risk import ScenarioEvidence, scenario_metrics


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
        refs=(),
    )
    hold = scenario_metrics({instrument: Decimal("1")}, evidence)
    reduced = scenario_metrics({instrument: Decimal("0.5")}, evidence)
    assert hold["drawdown"].value == Decimal("0.5")
    assert reduced["drawdown"].value == Decimal("0.25")
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
        refs=(),
    )
    result = scenario_metrics({UUID(int=1): Decimal("0.5")}, evidence)
    assert all(metric.status == "unknown" and metric.value is None for metric in result.values())

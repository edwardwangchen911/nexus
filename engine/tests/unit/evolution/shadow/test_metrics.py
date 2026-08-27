"""Exact checkpoint metrics and final Strategy Shadow gates."""

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.evolution.shadow.metrics import (
    calculate_strategy_shadow_metrics,
    evaluate_strategy_shadow_final_gate,
)
from nexus.evolution.shadow.models import StrategyShadowSessionPointV1

EVIDENCE_ID = UUID("019d0000-0000-7000-8000-000000006201")


def _points(
    *,
    count: int = 63,
    final_nav: str = "106",
    fills_per_session: int = 1,
    risk_ordinal: int | None = None,
) -> tuple[StrategyShadowSessionPointV1, ...]:
    start = date(2026, 9, 1)
    final = Decimal(final_nav)
    return tuple(
        StrategyShadowSessionPointV1(
            session=start + timedelta(days=index),
            ordinal=index + 1,
            nav=(Decimal("100") + (final - Decimal("100")) * Decimal(index + 1) / Decimal(count)),
            turnover=Decimal("0.01"),
            fees=Decimal("0.001"),
            filled_order_count=fills_per_session,
            hard_risk_violation=(risk_ordinal == index + 1),
            authority_hash=f"{index + 1:064x}",
        )
        for index in range(count)
    )


def test_metrics_cover_cost_return_drawdown_volatility_worst_sessions_and_fills() -> None:
    metrics = calculate_strategy_shadow_metrics(_points(), checkpoint_ordinal=63)

    assert metrics.session_count == 63
    assert metrics.net_return == Decimal("0.060000000000")
    assert metrics.max_drawdown == Decimal("0E-12")
    assert len(metrics.worst_sessions) == 5
    assert metrics.cumulative_turnover == Decimal("0.630000000000")
    assert metrics.cumulative_fees == Decimal("0.063000000000")
    assert metrics.full_fill_count == 63
    assert metrics.hard_risk_violation_count == 0
    assert len(metrics.metrics_hash) == 64


def test_final_gate_passes_only_complete_nonstale_evidence() -> None:
    champion = calculate_strategy_shadow_metrics(_points(final_nav="105"), checkpoint_ordinal=63)
    challenger = calculate_strategy_shadow_metrics(_points(final_nav="106"), checkpoint_ordinal=63)

    gate = evaluate_strategy_shadow_final_gate(
        champion=champion,
        challenger=challenger,
        champion_generation=1,
        active_generation=1,
        evidence_ids=(EVIDENCE_ID,),
        evidence_hashes=("a" * 64,),
    )

    assert gate.status == "PASS"
    assert [item.gate for item in gate.gates] == [
        "complete_sessions",
        "minimum_full_fills",
        "hard_risk_clear",
        "nonnegative_excess_return",
        "maximum_drawdown_degradation",
    ]


def test_one_risk_violation_or_one_missing_session_fails_closed() -> None:
    champion = calculate_strategy_shadow_metrics(_points(final_nav="105"), checkpoint_ordinal=63)
    risk = calculate_strategy_shadow_metrics(
        _points(final_nav="106", risk_ordinal=21), checkpoint_ordinal=63
    )
    incomplete = calculate_strategy_shadow_metrics(
        _points(count=62, final_nav="106"), checkpoint_ordinal=63
    )

    risk_gate = evaluate_strategy_shadow_final_gate(
        champion=champion,
        challenger=risk,
        champion_generation=1,
        active_generation=1,
        evidence_ids=(EVIDENCE_ID,),
        evidence_hashes=("a" * 64,),
    )
    incomplete_gate = evaluate_strategy_shadow_final_gate(
        champion=champion,
        challenger=incomplete,
        champion_generation=1,
        active_generation=1,
        evidence_ids=(EVIDENCE_ID,),
        evidence_hashes=("a" * 64,),
    )

    assert risk_gate.status == "FAIL"
    assert risk_gate.gate("hard_risk_clear").status == "FAIL"
    assert incomplete_gate.status == "UNKNOWN"
    assert incomplete_gate.gate("complete_sessions").status == "UNKNOWN"


def test_generation_change_is_stale_not_an_economic_failure() -> None:
    metrics = calculate_strategy_shadow_metrics(_points(), checkpoint_ordinal=63)

    gate = evaluate_strategy_shadow_final_gate(
        champion=metrics,
        challenger=metrics,
        champion_generation=1,
        active_generation=2,
        evidence_ids=(EVIDENCE_ID,),
        evidence_hashes=("a" * 64,),
    )

    assert gate.status == "STALE"
    assert gate.reason_code == "STALE_CHAMPION_BASELINE"

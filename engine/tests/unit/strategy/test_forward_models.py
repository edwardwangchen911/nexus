from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.strategy.forward import ForwardStrategyDecision, ForwardStrategyTarget
from pydantic import ValidationError


def _target(
    instrument_id: UUID,
    *,
    target_weight: Decimal = Decimal("0.125"),
    exit: bool = False,
    priority: int = 2,
) -> ForwardStrategyTarget:
    return ForwardStrategyTarget(
        instrument_id=instrument_id,
        target_weight=target_weight,
        exit=exit,
        strategy_priority=priority,
        reason="BASELINE_SELECTED" if not exit else "BASELINE_EXIT",
    )


def _decision(**overrides: object) -> ForwardStrategyDecision:
    values: dict[str, object] = {
        "account_id": UUID(int=1),
        "portfolio_id": UUID(int=2),
        "strategy_id": "baseline",
        "strategy_version": "baseline_v1",
        "decision_session": date(2026, 8, 21),
        "decision_time": datetime(2026, 8, 21, 20, tzinfo=UTC),
        "execution_session": date(2026, 8, 24),
        "source_policy_id": UUID(int=3),
        "calendar_authority_id": UUID(int=4),
        "universe_snapshot_id": UUID(int=5),
        "quality_report_id": UUID(int=6),
        "portfolio_projection_hash": "a" * 64,
        "authority_hash": "b" * 64,
        "strategy_parameters_hash": "c" * 64,
        "decision_hash": "d" * 64,
        "cash_weight": Decimal("0.75"),
        "reason": "WEEKLY_REBALANCE",
        "targets": (
            _target(UUID(int=11)),
            _target(UUID(int=12)),
        ),
    }
    values.update(overrides)
    return ForwardStrategyDecision(**values)


def test_forward_decision_is_frozen_and_forbids_extra_fields() -> None:
    decision = _decision()

    with pytest.raises(ValidationError, match="frozen"):
        decision.cash_weight = Decimal("1")  # type: ignore[misc]
    with pytest.raises(ValidationError, match="Extra inputs"):
        _decision(caller_target_weight=Decimal("1"))


def test_forward_decision_requires_next_execution_session() -> None:
    with pytest.raises(ValidationError, match="execution session must follow"):
        _decision(execution_session=date(2026, 8, 21))


def test_forward_decision_rejects_duplicate_instruments() -> None:
    target = _target(UUID(int=11))
    with pytest.raises(ValidationError, match="targets must be unique"):
        _decision(targets=(target, target), cash_weight=Decimal("0.75"))


def test_exit_target_is_zero_weight_priority_zero() -> None:
    with pytest.raises(ValidationError, match="exit targets require zero weight and priority zero"):
        _target(UUID(int=11), target_weight=Decimal("0.125"), exit=True, priority=0)
    with pytest.raises(ValidationError, match="exit targets require zero weight and priority zero"):
        _target(UUID(int=11), target_weight=Decimal("0"), exit=True, priority=1)

    exit_target = _target(
        UUID(int=11),
        target_weight=Decimal("0"),
        exit=True,
        priority=0,
    )
    assert exit_target.target_weight == Decimal("0")


def test_forward_decision_requires_weights_and_cash_to_equal_one() -> None:
    with pytest.raises(ValidationError, match="target and cash weights must equal one"):
        _decision(cash_weight=Decimal("0.5"))


def test_forward_decision_rejects_binary_float() -> None:
    with pytest.raises(ValidationError, match="binary float"):
        _decision(cash_weight=0.75)


def test_consumable_forward_decision_is_weekly_baseline_only() -> None:
    with pytest.raises(ValidationError, match="WEEKLY_REBALANCE"):
        _decision(reason="NOT_WEEK_END", targets=(), cash_weight=Decimal("1"))
    with pytest.raises(ValidationError, match="baseline_v1"):
        _decision(strategy_version="agent_v1")

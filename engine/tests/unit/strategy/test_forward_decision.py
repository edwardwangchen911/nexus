from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.risk.canonical import canonical_forward_strategy_decision_hash
from nexus.strategy.baseline import StrategyDecision, StrategyTarget
from nexus.strategy.forward import build_forward_strategy_decision

ACCOUNT_ID = UUID(int=1)
PORTFOLIO_ID = UUID(int=2)
CURRENT_ID = UUID(int=10)
NEW_ID = UUID(int=11)
EXIT_ID = UUID(int=12)
DECISION_TIME = datetime(2026, 8, 21, 20, tzinfo=UTC)


def _baseline(
    *,
    targets: tuple[StrategyTarget, ...] | None = None,
    exits: tuple[UUID, ...] = (EXIT_ID,),
    cash_weight: Decimal | None = Decimal("0.75"),
    reason: str = "WEEKLY_REBALANCE",
) -> StrategyDecision:
    return StrategyDecision(
        decision_session=date(2026, 8, 21),
        targets=(
            targets
            if targets is not None
            else (
                StrategyTarget(
                    instrument_id=CURRENT_ID,
                    weight=Decimal("0.125"),
                    rank=1,
                    momentum_126_skip5=Decimal("0.20"),
                ),
                StrategyTarget(
                    instrument_id=NEW_ID,
                    weight=Decimal("0.125"),
                    rank=2,
                    momentum_126_skip5=Decimal("0.10"),
                ),
            )
        ),
        exits=exits,
        cash_weight=cash_weight,
        reason=reason,
    )


def _build(
    decision: StrategyDecision | None = None,
    **overrides: object,
):
    values: dict[str, object] = {
        "decision": decision or _baseline(),
        "account_id": ACCOUNT_ID,
        "portfolio_id": PORTFOLIO_ID,
        "current_positions": (CURRENT_ID, EXIT_ID),
        "whitelisted_instrument_ids": (CURRENT_ID, NEW_ID, EXIT_ID),
        "decision_time": DECISION_TIME,
        "execution_session": date(2026, 8, 24),
        "source_policy_id": UUID(int=3),
        "calendar_authority_id": UUID(int=4),
        "universe_snapshot_id": UUID(int=5),
        "quality_report_id": UUID(int=6),
        "portfolio_projection_hash": "a" * 64,
        "authority_hash": "b" * 64,
        "strategy_parameters_hash": "c" * 64,
        "strategy_definition_id": UUID(int=7),
        "registry_event_id": UUID(int=8),
        "champion_generation": 1,
    }
    values.update(overrides)
    return build_forward_strategy_decision(**values)


def test_build_forward_decision_maps_targets_exits_and_priorities() -> None:
    result = _build()

    assert result is not None
    assert [
        (target.instrument_id, target.target_weight, target.exit, target.strategy_priority)
        for target in result.targets
    ] == [
        (CURRENT_ID, Decimal("0.125"), False, 1),
        (NEW_ID, Decimal("0.125"), False, 2),
        (EXIT_ID, Decimal("0"), True, 0),
    ]
    assert result.decision_hash == canonical_forward_strategy_decision_hash(result)


def test_forward_decision_hash_ignores_input_order() -> None:
    baseline = _baseline()
    reversed_result = _build(
        baseline.model_copy(
            update={
                "targets": tuple(reversed(baseline.targets)),
                "exits": tuple(reversed(baseline.exits)),
            }
        ),
        current_positions=(EXIT_ID, CURRENT_ID),
        whitelisted_instrument_ids=(EXIT_ID, NEW_ID, CURRENT_ID),
    )

    assert reversed_result is not None
    assert reversed_result.decision_hash == _build().decision_hash  # type: ignore[union-attr]


def test_not_week_end_does_not_create_a_consumable_decision() -> None:
    assert (
        _build(
            _baseline(
                targets=(),
                exits=(),
                cash_weight=None,
                reason="NOT_WEEK_END",
            )
        )
        is None
    )


@pytest.mark.parametrize(
    ("decision", "overrides", "message"),
    (
        (
            _baseline(exits=(EXIT_ID, EXIT_ID)),
            {},
            "exit instruments must be unique",
        ),
        (
            _baseline(exits=(CURRENT_ID,)),
            {},
            "target and exit instruments must not overlap",
        ),
        (
            _baseline(),
            {"current_positions": (CURRENT_ID, CURRENT_ID)},
            "current positions must be unique",
        ),
        (
            _baseline(),
            {"whitelisted_instrument_ids": (CURRENT_ID, NEW_ID)},
            "forward instruments must be whitelisted",
        ),
        (
            _baseline(),
            {"execution_session": None},
            "execution session is required",
        ),
    ),
)
def test_invalid_forward_inputs_fail_closed(
    decision: StrategyDecision,
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _build(decision, **overrides)


def test_caller_cannot_supply_target_weights() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        _build(caller_targets={NEW_ID: Decimal("1")})

from decimal import Decimal

import pytest
from nexus.risk.policy import drawdown_allows_candidate, transition_drawdown


@pytest.mark.parametrize(
    ("drawdown", "expected"),
    (
        (Decimal("0"), "unlocked"),
        (Decimal("0.079999999999"), "unlocked"),
        (Decimal("0.08"), "locked"),
        (Decimal("0.10"), "locked"),
    ),
)
def test_unlocked_drawdown_locks_at_eight_percent(drawdown: Decimal, expected: str) -> None:
    assert transition_drawdown("unlocked", drawdown, user_acknowledged=False) == expected


@pytest.mark.parametrize(
    ("drawdown", "expected"),
    (
        (Decimal("0.06"), "locked"),
        (Decimal("0.059999999999"), "recovery_eligible"),
    ),
)
def test_locked_drawdown_requires_strictly_below_six_percent(
    drawdown: Decimal, expected: str
) -> None:
    assert transition_drawdown("locked", drawdown, user_acknowledged=False) == expected


def test_recovery_eligible_relocks_on_rebound() -> None:
    assert (
        transition_drawdown(
            "recovery_eligible",
            Decimal("0.06"),
            user_acknowledged=False,
        )
        == "locked"
    )


def test_recovery_eligible_requires_acknowledgement_and_server_recheck() -> None:
    assert (
        transition_drawdown(
            "recovery_eligible",
            Decimal("0.05"),
            user_acknowledged=False,
        )
        == "recovery_eligible"
    )
    assert (
        transition_drawdown(
            "recovery_eligible",
            Decimal("0.05"),
            user_acknowledged=True,
        )
        == "unlocked"
    )
    assert (
        transition_drawdown(
            "recovery_eligible",
            Decimal("0.06"),
            user_acknowledged=True,
        )
        == "locked"
    )


def test_locked_state_blocks_buys_but_allows_reduce_only_sell() -> None:
    assert drawdown_allows_candidate("locked", side="buy", is_reduction=False) is False
    assert drawdown_allows_candidate("locked", side="sell", is_reduction=True) is True
    assert drawdown_allows_candidate("recovery_eligible", side="buy", is_reduction=False) is False
    assert drawdown_allows_candidate("unlocked", side="buy", is_reduction=False) is True


@pytest.mark.parametrize(
    "drawdown",
    (Decimal("-0.01"), Decimal("1.01"), Decimal("NaN")),
)
def test_drawdown_rejects_invalid_ratios(drawdown: Decimal) -> None:
    with pytest.raises(ValueError, match="drawdown"):
        transition_drawdown("unlocked", drawdown, user_acknowledged=False)


def test_drawdown_rejects_binary_float() -> None:
    with pytest.raises(ValueError, match="binary float"):
        transition_drawdown("unlocked", 0.08, user_acknowledged=False)

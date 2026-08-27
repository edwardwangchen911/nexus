from __future__ import annotations

import pytest
from nexus.evolution.repository import aggregate_family_state, require_candidate_transition


def test_family_is_admitted_only_before_any_candidate_advances() -> None:
    assert aggregate_family_state(("ADMITTED", "ADMITTED", "ADMITTED")) == "ADMITTED"


def test_family_remains_active_while_one_of_three_candidates_finishes() -> None:
    assert aggregate_family_state(
        ("VALIDATION_FAILED", "ADMITTED", "ADMITTED")
    ) == "BACKTEST_RUNNING"


def test_one_promoted_candidate_dominates_family_terminal_state() -> None:
    assert aggregate_family_state(
        ("PROMOTED", "SHADOW_FAILED", "OOS_NOT_SELECTED")
    ) == "PROMOTED"


@pytest.mark.parametrize(
    ("states", "expected"),
    (
        (("PROMOTION_ELIGIBLE", "BACKTEST_FAILED"), "PROMOTION_ELIGIBLE"),
        (("SHADOW_RUNNING", "OOS_NOT_SELECTED"), "SHADOW_RUNNING"),
        (("BACKTEST_PASSED", "VALIDATION_FAILED"), "BACKTEST_PASSED"),
        (("OOS_RESERVED", "OOS_NOT_SELECTED"), "BACKTEST_RUNNING"),
        (("STALE_CHAMPION_BASELINE", "BACKTEST_FAILED"), "STALE_CHAMPION_BASELINE"),
        (("SHADOW_FAILED", "BACKTEST_FAILED"), "SHADOW_FAILED"),
        (("EXPIRED", "BACKTEST_FAILED"), "EXPIRED"),
        (("VETOED", "BACKTEST_FAILED"), "VETOED"),
        (("VALIDATION_FAILED", "OOS_NOT_SELECTED"), "BACKTEST_FAILED"),
    ),
)
def test_family_aggregate_uses_deterministic_priority(
    states: tuple[str, ...], expected: str
) -> None:
    assert aggregate_family_state(states) == expected


@pytest.mark.parametrize(
    ("before", "after"),
    (
        ("ADMITTED", "VALIDATION_PASSED"),
        ("ADMITTED", "VALIDATION_FAILED"),
        ("VALIDATION_PASSED", "OOS_SELECTED"),
        ("VALIDATION_PASSED", "OOS_NOT_SELECTED"),
        ("OOS_SELECTED", "OOS_RESERVED"),
        ("OOS_RESERVED", "BACKTEST_PASSED"),
        ("OOS_RESERVED", "BACKTEST_FAILED"),
        ("BACKTEST_PASSED", "SHADOW_RUNNING"),
        ("SHADOW_RUNNING", "PROMOTION_ELIGIBLE"),
        ("SHADOW_RUNNING", "SHADOW_FAILED"),
        ("SHADOW_RUNNING", "EXPIRED"),
        ("PROMOTION_ELIGIBLE", "PROMOTED"),
    ),
)
def test_candidate_state_accepts_legal_transition(before: str, after: str) -> None:
    require_candidate_transition(before, after)


@pytest.mark.parametrize(
    ("before", "after"),
    (
        ("ADMITTED", "OOS_RESERVED"),
        ("OOS_NOT_SELECTED", "OOS_SELECTED"),
        ("VALIDATION_FAILED", "ADMITTED"),
        ("PROMOTED", "VETOED"),
        ("SHADOW_FAILED", "SHADOW_RUNNING"),
        ("OOS_SELECTED", "BACKTEST_PASSED"),
    ),
)
def test_candidate_state_rejects_skipped_or_terminal_transitions(
    before: str, after: str
) -> None:
    with pytest.raises(ValueError, match="CANDIDATE_STATE_TRANSITION_INVALID"):
        require_candidate_transition(before, after)


def test_candidate_state_rejects_unknown_state_and_empty_family() -> None:
    with pytest.raises(ValueError, match="CANDIDATE_STATE_UNKNOWN"):
        require_candidate_transition("ADMITTED", "UNKNOWN")
    with pytest.raises(ValueError, match="CANDIDATE_STATE_UNKNOWN"):
        aggregate_family_state(())

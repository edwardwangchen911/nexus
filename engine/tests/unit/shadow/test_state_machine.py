"""Pure Proposal Shadow checkpoint and terminal lifecycle decisions."""

import pytest
from nexus.shadow.models import ShadowFinalGate
from nexus.shadow.state_machine import checkpoint_kind, decide_terminal_action


def _gate(**overrides: object) -> ShadowFinalGate:
    values: dict[str, object] = {
        "completed_sessions": 63,
        "complete_checkpoint_ordinals": (5, 21, 63),
        "checkpoint_metric_rows": 9,
        "initial_execution_complete": True,
        "authority_binding_valid": True,
        "has_data_gap": False,
        "grace_exhausted": False,
    }
    values.update(overrides)
    return ShadowFinalGate(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("ordinal", [5, 21])
def test_early_checkpoints_are_observations(ordinal: int) -> None:
    assert checkpoint_kind(ordinal) == "observation"


def test_sixty_third_checkpoint_is_validation_complete() -> None:
    assert checkpoint_kind(63) == "validation_complete"


def test_complete_sixty_three_session_gate_validates() -> None:
    assert decide_terminal_action(_gate()) == "validate"


def test_initial_execution_blocker_expires_at_complete_window() -> None:
    assert (
        decide_terminal_action(_gate(initial_execution_complete=False))
        == "expire_initial_execution"
    )


def test_data_gap_waits_during_grace_then_expires() -> None:
    assert decide_terminal_action(_gate(completed_sessions=62, has_data_gap=True)) == "continue"
    assert (
        decide_terminal_action(
            _gate(completed_sessions=62, has_data_gap=True, grace_exhausted=True)
        )
        == "expire_data"
    )


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"completed_sessions": 62}, "continue"),
        ({"complete_checkpoint_ordinals": (5, 21)}, "continue"),
        ({"checkpoint_metric_rows": 8}, "continue"),
        ({"authority_binding_valid": False}, "continue"),
    ],
)
def test_validation_requires_every_complete_gate(
    overrides: dict[str, object], expected: str
) -> None:
    assert decide_terminal_action(_gate(**overrides)) == expected

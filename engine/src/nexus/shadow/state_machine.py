"""Pure lifecycle gates for static Proposal Shadow."""

from typing import Literal

from nexus.shadow.models import ShadowFinalGate


def checkpoint_kind(ordinal: int) -> Literal["observation", "validation_complete"]:
    if ordinal in (5, 21):
        return "observation"
    if ordinal == 63:
        return "validation_complete"
    raise ValueError("shadow checkpoint ordinal must be 5, 21, or 63")


def decide_terminal_action(
    gate: ShadowFinalGate,
) -> Literal["continue", "validate", "expire_initial_execution", "expire_data"]:
    if gate.completed_sessions == 63 and not gate.initial_execution_complete:
        return "expire_initial_execution"
    if (
        gate.completed_sessions == 63
        and gate.complete_checkpoint_ordinals == (5, 21, 63)
        and gate.checkpoint_metric_rows == 9
        and gate.authority_binding_valid
        and not gate.has_data_gap
    ):
        return "validate"
    if gate.grace_exhausted and gate.has_data_gap:
        return "expire_data"
    return "continue"

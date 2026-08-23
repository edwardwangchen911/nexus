# ruff: noqa: E501
"""Deterministic strategy-target construction from explicit baseline authority only."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

_ONE = Decimal("1")
_ZERO = Decimal("0")


@dataclass(frozen=True)
class DeterministicSelectionAuthority:
    """Published baseline selection output, not an Agent recommendation."""

    policy_version: str
    selected_weights: Mapping[UUID, Decimal]
    evidence_hash: str

    def __post_init__(self) -> None:
        if self.policy_version != "baseline_v1":
            raise ValueError("strategy target requires baseline_v1 authority")
        if len(self.evidence_hash) != 64 or any(
            char not in "0123456789abcdef" for char in self.evidence_hash
        ):
            raise ValueError("selection authority requires an immutable SHA-256 evidence hash")
        if not self.selected_weights or any(
            weight < _ZERO for weight in self.selected_weights.values()
        ):
            raise ValueError("selection weights must be nonnegative and nonempty")
        if sum(self.selected_weights.values(), _ZERO) != _ONE:
            raise ValueError("selection weights must sum to one")


@dataclass(frozen=True)
class StrategyTargetResult:
    targets: dict[UUID, Decimal]
    evidence_complete: bool


def build_strategy_target(
    current_weights: Mapping[UUID, Decimal], *, authority: DeterministicSelectionAuthority | None
) -> StrategyTargetResult:
    """Return the baseline unchanged when the required selection authority is absent."""
    if authority is None:
        return StrategyTargetResult(targets=dict(current_weights), evidence_complete=False)
    targets = {instrument_id: _ZERO for instrument_id in current_weights}
    for instrument_id, target_weight in authority.selected_weights.items():
        targets[instrument_id] = target_weight
    return StrategyTargetResult(targets=targets, evidence_complete=True)

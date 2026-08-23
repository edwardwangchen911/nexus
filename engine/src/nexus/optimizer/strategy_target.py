# ruff: noqa: E501
"""Frozen baseline_v1 selection authority; research text never supplies weights."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from nexus.domain.time import require_utc

_TARGET_WEIGHT = Decimal("0.1250")
_MIN_CASH = Decimal("0.25")
_ONE = Decimal("1")
_ZERO = Decimal("0")


@dataclass(frozen=True)
class SelectionCandidate:
    """A deterministic candidate with immutable provenance, not an Agent suggestion."""

    instrument_id: UUID
    provenance_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.provenance_refs or any(not ref for ref in self.provenance_refs):
            raise ValueError("selection candidates require immutable provenance")


@dataclass(frozen=True)
class DeterministicSelectionAuthority:
    """Published, point-in-time baseline_v1 selection with fixed equal targets."""

    policy_version: str
    decision_at: datetime
    data_snapshot_id: UUID
    available_at: datetime
    evidence_refs: tuple[str, ...]
    candidates: tuple[SelectionCandidate, ...]
    selected_instrument_ids: tuple[UUID, ...]
    evidence_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision_at", require_utc(self.decision_at))
        object.__setattr__(self, "available_at", require_utc(self.available_at))
        if self.policy_version != "baseline_v1":
            raise ValueError("strategy target requires baseline_v1 authority")
        if self.available_at > self.decision_at:
            raise ValueError("selection authority cannot be available after its decision")
        if not self.evidence_refs or any(not ref for ref in self.evidence_refs):
            raise ValueError("selection authority requires immutable evidence")
        if len(self.evidence_hash) != 64 or any(
            char not in "0123456789abcdef" for char in self.evidence_hash
        ):
            raise ValueError("selection authority requires an immutable SHA-256 evidence hash")
        candidate_ids = tuple(item.instrument_id for item in self.candidates)
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("selection candidates must be unique")
        if not self.selected_instrument_ids or len(self.selected_instrument_ids) > 6:
            raise ValueError("selection must contain between one and six instruments")
        if len(self.selected_instrument_ids) != len(set(self.selected_instrument_ids)):
            raise ValueError("selected instruments must be unique")
        if not set(self.selected_instrument_ids).issubset(candidate_ids):
            raise ValueError("selected instruments must be deterministic candidates")
        if _ONE - _TARGET_WEIGHT * len(self.selected_instrument_ids) < _MIN_CASH:
            raise ValueError("baseline selection must retain at least 25 percent cash")

    @property
    def selected_targets(self) -> tuple[tuple[UUID, Decimal], ...]:
        return tuple((instrument_id, _TARGET_WEIGHT) for instrument_id in self.selected_instrument_ids)


@dataclass(frozen=True)
class StrategyTargetResult:
    targets: tuple[tuple[UUID, Decimal], ...]
    cash_target_weight: Decimal
    evidence_complete: bool


def build_strategy_target(
    current_weights: tuple[tuple[UUID, Decimal], ...], *, authority: DeterministicSelectionAuthority | None
) -> StrategyTargetResult:
    """Return the baseline unchanged when the required selection authority is absent."""
    if authority is None:
        return StrategyTargetResult(
            targets=current_weights, cash_target_weight=_ZERO, evidence_complete=False
        )
    current_ids = {instrument_id for instrument_id, _ in current_weights}
    selected = dict(authority.selected_targets)
    targets = tuple(
        (instrument_id, selected.get(instrument_id, _ZERO))
        for instrument_id, _ in current_weights
    )
    additions = tuple(
        (instrument_id, weight)
        for instrument_id, weight in authority.selected_targets
        if instrument_id not in current_ids
    )
    return StrategyTargetResult(
        targets=tuple(sorted((*targets, *additions), key=lambda item: item[0])),
        cash_target_weight=_ONE - _TARGET_WEIGHT * len(authority.selected_instrument_ids),
        evidence_complete=True,
    )

# ruff: noqa: E501
"""Reduce-only one-basis-point scenario builder; it is deliberately not an optimizer."""

from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from nexus.optimizer.models import ProposalPosition

_ONE_BP = Decimal("0.0001")
_ZERO = Decimal("0")


@dataclass(frozen=True)
class ReductionCandidate:
    """A deterministic constraint-contribution input, never an Agent target."""

    instrument_id: UUID
    current_weight: Decimal
    contribution: Decimal
    severity_rank: int
    liquidity_score: Decimal


@dataclass(frozen=True)
class MinimumChangeResult:
    positions: tuple[ProposalPosition, ...]
    cash_released: Decimal


def reduce_to_concentration_limits(
    candidates: tuple[ReductionCandidate, ...],
    limits: tuple[tuple[int, Decimal], ...],
) -> MinimumChangeResult:
    """Recheck security-only Top1/Top3 after each NAV-bp sale; never sell to a false zero risk."""
    if not limits or any(count not in {1, 3} or not _ZERO < limit <= 1 for count, limit in limits):
        raise ValueError("unsupported concentration limits")
    targets = {candidate.instrument_id: candidate.current_weight for candidate in candidates}
    if (len(targets) != len(candidates) or any(weight < 0 for weight in targets.values())
            or sum(targets.values(), _ZERO) > 1):
        raise ValueError("invalid concentration candidates")
    active_count = sum(weight > 0 for weight in targets.values())
    if not active_count or any(Decimal(min(count, active_count)) > limit * active_count for count, limit in limits):
        raise ValueError("reduce-only concentration constraints are infeasible")
    released = _ZERO
    while True:
        total = sum(targets.values(), _ZERO)
        ranked = sorted(candidates, key=lambda c: (-targets[c.instrument_id], c.instrument_id))
        violations = [
            (sum((targets[c.instrument_id] for c in ranked[:count]), _ZERO) - limit * total, count)
            for count, limit in limits
        ]
        excess, count = max(violations, key=lambda row: (row[0], -row[1]))
        if total > 0 and excess <= 0:
            break
        eligible = [c for c in ranked[:count] if targets[c.instrument_id] >= _ONE_BP]
        if total <= _ONE_BP or not eligible:
            raise ValueError("concentration constraints cannot be met at one-bp precision")
        chosen = min(eligible, key=lambda c: (
            -targets[c.instrument_id], -c.severity_rank, -c.liquidity_score, c.instrument_id,
        ))
        targets[chosen.instrument_id] -= _ONE_BP
        released += _ONE_BP
    return MinimumChangeResult(
        positions=tuple(ProposalPosition(
            instrument_id=c.instrument_id, current_weight=c.current_weight,
            target_weight=targets[c.instrument_id],
            action="reduce" if targets[c.instrument_id] < c.current_weight else "maintain" if c.current_weight else "none",
            reason_code="MINIMUM_CHANGE_HEURISTIC" if targets[c.instrument_id] < c.current_weight else "BASELINE_WEIGHT",
        ) for c in sorted(candidates, key=lambda c: c.instrument_id)),
        cash_released=released,
    )


def reduce_until_satisfied(
    candidates: tuple[ReductionCandidate, ...], *, excess_weight: Decimal
) -> MinimumChangeResult:
    """Release exactly one bp at a time by contribution, severity, liquidity, then UUID."""
    if excess_weight < _ZERO or excess_weight % _ONE_BP != _ZERO:
        raise ValueError("excess_weight must be a nonnegative multiple of one basis point")
    if any(
        candidate.current_weight < _ZERO or candidate.contribution < _ZERO
        for candidate in candidates
    ):
        raise ValueError("minimum-change weights and contributions must be nonnegative")
    targets = {candidate.instrument_id: candidate.current_weight for candidate in candidates}
    remaining = excess_weight
    while remaining > _ZERO:
        eligible = [
            candidate for candidate in candidates if targets[candidate.instrument_id] >= _ONE_BP
        ]
        if not eligible:
            raise ValueError("reduce-only candidates cannot satisfy the constraint")
        chosen = sorted(
            eligible,
            key=lambda candidate: (
                -candidate.contribution,
                -candidate.severity_rank,
                -candidate.liquidity_score,
                candidate.instrument_id,
            ),
        )[0]
        targets[chosen.instrument_id] -= _ONE_BP
        remaining -= _ONE_BP
    positions = tuple(
        ProposalPosition(
            instrument_id=candidate.instrument_id,
            current_weight=candidate.current_weight,
            target_weight=targets[candidate.instrument_id],
            action="reduce"
            if targets[candidate.instrument_id] < candidate.current_weight
            else "maintain",
            reason_code="MINIMUM_CHANGE_HEURISTIC"
            if targets[candidate.instrument_id] < candidate.current_weight
            else "BASELINE_WEIGHT",
        )
        for candidate in sorted(candidates, key=lambda candidate: candidate.instrument_id)
    )
    return MinimumChangeResult(positions=positions, cash_released=excess_weight)

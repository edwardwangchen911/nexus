"""Deterministic comparison that never turns unknown costs into zero."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from nexus.optimizer.models import ProposalVariant


@dataclass(frozen=True)
class ComparableProposal:
    variant: ProposalVariant
    violations: int
    turnover: Decimal
    cost: Decimal | None
    requires_fx: bool


@dataclass(frozen=True)
class ProposalComparison:
    recommendation: ProposalVariant
    ranked_variants: tuple[ProposalVariant, ...]
    tax_impact: Literal["unknown"] = "unknown"


def compare_proposals(proposals: tuple[ComparableProposal, ...]) -> ProposalComparison:
    """Rank only fully known executable scenarios; preserve hold when no proof beats it."""
    if tuple(item.variant for item in proposals) != (
        "hold_current",
        "minimum_change",
        "strategy_target",
    ):
        raise ValueError("comparison requires exactly the ordered three variants")
    ranked = tuple(
        item.variant
        for item in sorted(
            (item for item in proposals if item.cost is not None and not item.requires_fx),
            key=lambda item: (item.violations, item.cost, item.turnover, item.variant),
        )
    )
    hold = proposals[0]
    if not ranked or ranked[0] == "hold_current":
        return ProposalComparison(recommendation="hold_current", ranked_variants=ranked)
    winner = next(item for item in proposals if item.variant == ranked[0])
    if winner.violations >= hold.violations:
        return ProposalComparison(recommendation="hold_current", ranked_variants=ranked)
    return ProposalComparison(recommendation=winner.variant, ranked_variants=ranked)

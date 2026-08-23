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
    constraint_complete: bool = True
    eligibility_complete: bool = True
    evidence_complete: bool = True
    stale: bool = False
    partial: bool = False
    source_conflict: bool = False
    cost_complete: bool = True
    unknown_metrics: tuple[str, ...] = ()

    @property
    def ranking_eligible(self) -> bool:
        return (
            self.cost is not None
            and self.cost_complete
            and self.constraint_complete
            and self.eligibility_complete
            and self.evidence_complete
            and not self.requires_fx
            and not self.stale
            and not self.partial
            and not self.source_conflict
            and not self.unknown_metrics
        )


@dataclass(frozen=True)
class ProposalComparison:
    recommendation: ProposalVariant
    ranked_variants: tuple[ProposalVariant, ...]
    tax_impact: Literal["unknown"] = "unknown"
    excluded_variants: tuple[ProposalVariant, ...] = ()


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
            (item for item in proposals if item.ranking_eligible),
            key=lambda item: (item.violations, item.cost, item.turnover, item.variant),
        )
    )
    hold = proposals[0]
    if not ranked or ranked[0] == "hold_current":
        return ProposalComparison(
            recommendation="hold_current",
            ranked_variants=ranked,
            excluded_variants=tuple(
                item.variant for item in proposals if not item.ranking_eligible
            ),
        )
    winner = next(item for item in proposals if item.variant == ranked[0])
    if winner.violations >= hold.violations:
        return ProposalComparison(
            recommendation="hold_current",
            ranked_variants=ranked,
            excluded_variants=tuple(
                item.variant for item in proposals if not item.ranking_eligible
            ),
        )
    return ProposalComparison(
        recommendation=winner.variant,
        ranked_variants=ranked,
        excluded_variants=tuple(item.variant for item in proposals if not item.ranking_eligible),
    )

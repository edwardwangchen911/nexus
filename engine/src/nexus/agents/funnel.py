"""Pure, deterministic bounded pre-model research selection."""

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class Candidate:
    instrument_id: UUID
    real_read_only: bool = False
    hard_risk: bool = False
    data_exception: bool = False
    paper_holding: bool = False
    reduce_risk: bool = False


@dataclass(frozen=True)
class ResearchFunnel:
    light_research: tuple[Candidate, ...]
    full_debate: tuple[Candidate, ...]
    light_limit_watch: tuple[Candidate, ...]
    budget_exhausted_watch: tuple[Candidate, ...]


def select_research_funnel(
    candidates: tuple[Candidate, ...], *, debate_capacity: int | None = None
) -> ResearchFunnel:
    """Select deterministic 20-light/5-debate bounds before a model can be invoked."""
    if debate_capacity is not None and debate_capacity < 0:
        raise ValueError("debate_capacity must be non-negative")
    unique = {candidate.instrument_id: candidate for candidate in candidates}
    ranked = tuple(sorted(unique.values(), key=_priority))
    light = ranked[:20]
    normal = tuple(candidate for candidate in light if not candidate.reduce_risk)[:5]
    reduce = tuple(candidate for candidate in ranked if candidate.reduce_risk)
    planned = tuple(dict.fromkeys((*reduce, *normal)))
    debate = planned if debate_capacity is None else planned[:debate_capacity]
    return ResearchFunnel(
        light_research=light,
        full_debate=debate,
        light_limit_watch=ranked[20:],
        budget_exhausted_watch=() if debate_capacity is None else planned[debate_capacity:],
    )


def _priority(candidate: Candidate) -> tuple[int, int, int, int, str]:
    return (
        0 if candidate.real_read_only else 1,
        0 if candidate.hard_risk else 1,
        0 if candidate.data_exception else 1,
        0 if candidate.paper_holding else 1,
        str(candidate.instrument_id),
    )

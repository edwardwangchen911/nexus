"""Deterministic pre-model priority funnel tests."""

from uuid import UUID

from nexus.agents.funnel import Candidate, select_research_funnel


def test_funnel_prioritizes_real_hard_risk_then_bounds_light_and_debate_sets() -> None:
    """Dropping stable priority would let a model choose its own universe or exceed hard bounds."""
    candidates = tuple(
        Candidate(
            instrument_id=UUID(f"0198d500-1000-7000-8000-{index:012d}"),
            real_read_only=index == 1,
            hard_risk=index == 1,
            data_exception=index == 2,
            paper_holding=index == 3,
            reduce_risk=index == 1,
        )
        for index in range(1, 25)
    )
    result = select_research_funnel(candidates)
    assert len(result.light_research) == 20
    assert result.light_research[0].instrument_id == candidates[0].instrument_id
    assert len(result.full_debate) <= 6
    assert result.full_debate[0].instrument_id == candidates[0].instrument_id

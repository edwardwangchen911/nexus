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
    assert len(tuple(item for item in result.full_debate if not item.reduce_risk)) <= 5
    assert result.full_debate[0].instrument_id == candidates[0].instrument_id


def test_funnel_keeps_every_reduce_risk_holding_after_five_ordinary_opportunities() -> None:
    """An arbitrary total cap must not discard explicitly admitted reduce-risk holdings."""
    candidates = tuple(
        Candidate(
            instrument_id=UUID(f"0198d500-1000-7000-8000-{index:012d}"),
            real_read_only=index <= 8,
            hard_risk=index <= 8,
            reduce_risk=index <= 8,
        )
        for index in range(1, 18)
    )
    result = select_research_funnel(candidates)
    assert len(tuple(item for item in result.full_debate if item.reduce_risk)) == 8
    assert len(tuple(item for item in result.full_debate if not item.reduce_risk)) == 5


def test_funnel_labels_selected_but_unfunded_work_as_budget_exhausted_watch() -> None:
    """Budget denial must be explicit even for candidates inside the light-research bound."""
    candidates = tuple(
        Candidate(instrument_id=UUID(f"0198d500-1000-7000-8000-{index:012d}"))
        for index in range(1, 10)
    )
    result = select_research_funnel(candidates, debate_capacity=2)
    assert result.full_debate == candidates[:2]
    assert result.budget_exhausted_watch == candidates[2:5]


def test_funnel_conservatively_merges_duplicate_authority_flags() -> None:
    """Last-write-wins dedupe must not erase a real holding, risk, anomaly, or reduction."""
    instrument_id = UUID("0198d500-1000-7000-8000-000000000321")
    result = select_research_funnel(
        (
            Candidate(instrument_id=instrument_id, real_read_only=True, hard_risk=True),
            Candidate(instrument_id=instrument_id, data_exception=True, paper_holding=True),
            Candidate(instrument_id=instrument_id, reduce_risk=True),
        )
    )
    assert result.light_research == (
        Candidate(
            instrument_id=instrument_id,
            real_read_only=True,
            hard_risk=True,
            data_exception=True,
            paper_holding=True,
            reduce_risk=True,
        ),
    )
    assert result.full_debate == result.light_research

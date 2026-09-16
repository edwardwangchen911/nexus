from types import SimpleNamespace

import pytest
from nexus.paper.agent_research_universe import (
    AGENT_PAPER_RESEARCH_UNIVERSE_V1,
    ResearchCycleOutcome,
    ResearchTarget,
    choose_research_target,
    research_cycle_outcome_from_events,
    research_failure_result_code,
    research_target_facts,
    research_target_from_events,
)


def _outcome(
    *,
    ordinal: int,
    symbol: str,
    attempt: int,
    status: str,
    result_code: str,
    version: str = "agent_paper_research_v1",
) -> ResearchCycleOutcome:
    return ResearchCycleOutcome(
        version=version,
        ordinal=ordinal,
        symbol=symbol,
        attempt=attempt,
        status=status,
        result_code=result_code,
    )


def test_research_universe_v1_is_fixed_and_starts_with_aapl() -> None:
    assert AGENT_PAPER_RESEARCH_UNIVERSE_V1.symbols == ("AAPL", "MSFT", "JPM")
    assert choose_research_target(()) == ResearchTarget(
        version="agent_paper_research_v1",
        ordinal=0,
        symbol="AAPL",
        attempt=1,
    )


@pytest.mark.parametrize(
    ("history", "expected"),
    [
        (
            (
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=1,
                    status="completed",
                    result_code="NO_ORDER_REQUIRED",
                ),
            ),
            ResearchTarget(version="agent_paper_research_v1", ordinal=1, symbol="MSFT", attempt=1),
        ),
        (
            (
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=1,
                    status="unavailable",
                    result_code="CANDIDATE_EVIDENCE_UNAVAILABLE",
                ),
            ),
            ResearchTarget(version="agent_paper_research_v1", ordinal=0, symbol="AAPL", attempt=2),
        ),
        (
            (
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=1,
                    status="unavailable",
                    result_code="CANDIDATE_EVIDENCE_UNAVAILABLE",
                ),
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=2,
                    status="unavailable",
                    result_code="RESEARCH_OUTPUT_NOT_ACTIONABLE",
                ),
            ),
            ResearchTarget(version="agent_paper_research_v1", ordinal=0, symbol="AAPL", attempt=3),
        ),
        (
            (
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=1,
                    status="unavailable",
                    result_code="CANDIDATE_EVIDENCE_UNAVAILABLE",
                ),
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=2,
                    status="unavailable",
                    result_code="RESEARCH_OUTPUT_NOT_ACTIONABLE",
                ),
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=3,
                    status="unavailable",
                    result_code="RESEARCH_BLOCKED",
                ),
            ),
            ResearchTarget(version="agent_paper_research_v1", ordinal=1, symbol="MSFT", attempt=1),
        ),
        (
            (
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=1,
                    status="completed",
                    result_code="NO_ORDER_REQUIRED",
                ),
                _outcome(
                    ordinal=1,
                    symbol="MSFT",
                    attempt=1,
                    status="completed",
                    result_code="NO_ORDER_REQUIRED",
                ),
                _outcome(
                    ordinal=2,
                    symbol="JPM",
                    attempt=1,
                    status="completed",
                    result_code="NO_ORDER_REQUIRED",
                ),
            ),
            ResearchTarget(version="agent_paper_research_v1", ordinal=0, symbol="AAPL", attempt=1),
        ),
        (
            (
                _outcome(
                    ordinal=0,
                    symbol="AAPL",
                    attempt=1,
                    status="failed",
                    result_code="PAPER_BATCH_CONFLICT",
                ),
            ),
            ResearchTarget(version="agent_paper_research_v1", ordinal=0, symbol="AAPL", attempt=1),
        ),
    ],
)
def test_research_target_reduces_terminal_history(
    history: tuple[ResearchCycleOutcome, ...], expected: ResearchTarget
) -> None:
    assert choose_research_target(history) == expected


def test_research_target_ignores_other_versions() -> None:
    old = _outcome(
        ordinal=0,
        symbol="AAPL",
        attempt=1,
        status="completed",
        result_code="NO_ORDER_REQUIRED",
        version="agent_paper_research_v0",
    )

    assert choose_research_target((old,)) == ResearchTarget(
        version="agent_paper_research_v1", ordinal=0, symbol="AAPL", attempt=1
    )


def test_research_target_round_trips_through_prepared_and_terminal_events() -> None:
    target = ResearchTarget(version="agent_paper_research_v1", ordinal=1, symbol="MSFT", attempt=2)
    events = (
        SimpleNamespace(
            sequence=1,
            to_status="prepared",
            result_code="CYCLE_PREPARED",
            fact_ids=research_target_facts(target),
        ),
        SimpleNamespace(
            sequence=2,
            to_status="unavailable",
            result_code="RESEARCH_OUTPUT_NOT_ACTIONABLE",
            fact_ids={},
        ),
    )

    assert research_target_from_events(events) == target
    assert research_cycle_outcome_from_events(events) == ResearchCycleOutcome(
        version=target.version,
        ordinal=target.ordinal,
        symbol=target.symbol,
        attempt=target.attempt,
        status="unavailable",
        result_code="RESEARCH_OUTPUT_NOT_ACTIONABLE",
    )


def test_legacy_events_do_not_invent_a_rotation_target() -> None:
    events = (
        SimpleNamespace(
            sequence=1,
            to_status="prepared",
            result_code="CYCLE_PREPARED",
            fact_ids={},
        ),
        SimpleNamespace(
            sequence=2,
            to_status="completed",
            result_code="NO_ORDER_REQUIRED",
            fact_ids={},
        ),
    )

    assert research_target_from_events(events) is None
    assert research_cycle_outcome_from_events(events) is None


def test_third_candidate_failure_is_persisted_as_research_blocked() -> None:
    third = ResearchTarget(version="agent_paper_research_v1", ordinal=2, symbol="JPM", attempt=3)
    second = ResearchTarget(version="agent_paper_research_v1", ordinal=2, symbol="JPM", attempt=2)

    assert (
        research_failure_result_code(third, "RESEARCH_OUTPUT_NOT_ACTIONABLE") == "RESEARCH_BLOCKED"
    )
    assert (
        research_failure_result_code(second, "RESEARCH_OUTPUT_NOT_ACTIONABLE")
        == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
    )
    assert research_failure_result_code(third, "PAPER_BATCH_CONFLICT") == "PAPER_BATCH_CONFLICT"


@pytest.mark.parametrize(
    "outcome",
    [
        _outcome(
            ordinal=1, symbol="MSFT", attempt=1, status="completed", result_code="NO_ORDER_REQUIRED"
        ),
        _outcome(
            ordinal=0, symbol="MSFT", attempt=1, status="completed", result_code="NO_ORDER_REQUIRED"
        ),
        _outcome(
            ordinal=0, symbol="AAPL", attempt=2, status="completed", result_code="NO_ORDER_REQUIRED"
        ),
        _outcome(
            ordinal=0,
            symbol="AAPL",
            attempt=1,
            status="researching",
            result_code="RESEARCH_JOBS_PREPARED",
        ),
        _outcome(
            ordinal=0,
            symbol="AAPL",
            attempt=3,
            status="unavailable",
            result_code="RESEARCH_OUTPUT_NOT_ACTIONABLE",
        ),
    ],
)
def test_research_target_rejects_inconsistent_current_version_history(
    outcome: ResearchCycleOutcome,
) -> None:
    with pytest.raises(ValueError, match="AGENT_PAPER_ROTATION_INVALID"):
        choose_research_target((outcome,))

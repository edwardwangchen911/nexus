from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from nexus.agents import portfolio_synthesis_persistence
from nexus.paper import agent_cycle


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,error,expected_code",
    [
        ("research_admission", "RESEARCH_OUTPUT_NOT_ACTIONABLE", "RESEARCH_OUTPUT_NOT_ACTIONABLE"),
        (
            "synthesis_admission",
            "PORTFOLIO_SYNTHESIS_PROMPT_INVALID",
            "PORTFOLIO_SYNTHESIS_PROMPT_INVALID",
        ),
        (
            "synthesis_admission",
            "PORTFOLIO_SYNTHESIS_MODEL_INVALID",
            "PORTFOLIO_SYNTHESIS_MODEL_INVALID",
        ),
        ("synthesis_admission", "private provider response secret=example", "UNKNOWN"),
    ],
)
async def test_synthesis_rejection_records_safe_stage_and_code_without_retry(
    monkeypatch, phase, error, expected_code
):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[None, None]))
    cycle = SimpleNamespace(
        id=UUID(int=1), account_id=UUID(int=2), decision_time=datetime(2026, 10, 12, 14, tzinfo=UTC)
    )
    events = (SimpleNamespace(to_status="researching"),)
    monkeypatch.setattr(agent_cycle, "_require_cycle_candidate_profile", AsyncMock())
    research = AsyncMock(return_value=SimpleNamespace(output_id=UUID(int=3)))
    admission = AsyncMock(side_effect=ValueError(error))
    if phase == "research_admission":
        research.side_effect = ValueError(error)
    monkeypatch.setattr(agent_cycle, "load_actionable_research_output", research)
    monkeypatch.setattr(
        agent_cycle,
        "load_paper_projection",
        AsyncMock(return_value=SimpleNamespace(projection_hash="a" * 64)),
    )
    monkeypatch.setattr(portfolio_synthesis_persistence, "admit_portfolio_synthesis", admission)
    append = AsyncMock()
    monkeypatch.setattr(agent_cycle, "_append_event", append)
    monkeypatch.setattr(agent_cycle, "_events", AsyncMock(return_value=events))
    result = object()
    monkeypatch.setattr(agent_cycle, "_result", Mock(return_value=result))

    assert (
        await agent_cycle._resume_portfolio_synthesis(
            session,
            cycle=cycle,
            events=events,
            run_ids=(UUID(int=4),),
            now=cycle.decision_time,
            daily_limit_usd=Decimal("10"),
        )
        is result
    )

    append.assert_awaited_once()
    assert append.call_args.args[2:5] == (
        "researching",
        "unavailable",
        "PORTFOLIO_SYNTHESIS_UNAVAILABLE",
    )
    assert append.call_args.args[5] == {"failure_phase": phase, "failure_code": expected_code}
    assert "secret" not in str(append.call_args)
    assert research.await_count == 1
    assert admission.await_count == (0 if phase == "research_admission" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["job_failed", "invalid_output"])
async def test_synthesis_readback_and_output_validation_keep_safe_failure_phase(
    monkeypatch, failure
):
    from nexus.agents import portfolio_synthesis_worker

    run = SimpleNamespace(id=UUID(int=10), status="completed")
    output = SimpleNamespace(status="completed", intent={"private": "not-an-intent"})
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[run, output]))
    cycle = SimpleNamespace(id=UUID(int=1))
    events = (SimpleNamespace(to_status="researching"),)
    monkeypatch.setattr(agent_cycle, "_require_cycle_candidate_profile", AsyncMock())
    monkeypatch.setattr(
        portfolio_synthesis_persistence,
        "load_frozen_synthesis_authority",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        portfolio_synthesis_worker,
        "read_synthesis_job",
        AsyncMock(
            return_value=SimpleNamespace(
                status="failed" if failure == "job_failed" else "succeeded"
            )
        ),
    )
    append = AsyncMock()
    monkeypatch.setattr(agent_cycle, "_append_event", append)
    monkeypatch.setattr(agent_cycle, "_events", AsyncMock(return_value=events))
    monkeypatch.setattr(agent_cycle, "_result", Mock(return_value=object()))
    await agent_cycle._resume_portfolio_synthesis(
        session,
        cycle=cycle,
        events=events,
        run_ids=(),
        now=None,
        daily_limit_usd=None,
    )
    assert append.call_args.args[5] == {
        "failure_phase": "synthesis_readback" if failure == "job_failed" else "output_validation",
        "failure_code": "PORTFOLIO_SYNTHESIS_UNAVAILABLE" if failure == "job_failed" else "UNKNOWN",
    }
    assert "private" not in str(append.call_args)


@pytest.mark.asyncio
@pytest.mark.parametrize("clock_case", ["valid", "missing", "naive", "before_entry"])
async def test_formal_admission_uses_observation_clock_and_retains_safe_failure_context(
    monkeypatch, clock_case
):
    entered_at = datetime(2026, 10, 15, 14, tzinfo=UTC)
    observed_at = entered_at + timedelta(seconds=2)
    clock = {
        "valid": observed_at,
        "missing": None,
        "naive": observed_at.replace(tzinfo=None),
        "before_entry": entered_at - timedelta(seconds=1),
    }[clock_case]
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[None, clock]))
    cycle = SimpleNamespace(id=UUID(int=1))
    events = (SimpleNamespace(to_status="researching"),)
    monkeypatch.setattr(agent_cycle, "_require_cycle_candidate_profile", AsyncMock())
    research = AsyncMock(side_effect=ValueError("RESEARCH_OUTPUT_NOT_ACTIONABLE"))
    monkeypatch.setattr(agent_cycle, "load_actionable_research_output", research)
    append = AsyncMock()
    monkeypatch.setattr(agent_cycle, "_append_event", append)
    monkeypatch.setattr(agent_cycle, "_events", AsyncMock(return_value=events))
    monkeypatch.setattr(agent_cycle, "_result", Mock(return_value=object()))

    await agent_cycle._resume_portfolio_synthesis(
        session,
        cycle=cycle,
        events=events,
        run_ids=(UUID(int=4),),
        now=entered_at,
        daily_limit_usd=Decimal("10"),
        formal_stage_capacity=True,
    )

    facts = append.call_args.args[5]
    assert facts["resume_started_at"] == entered_at.isoformat()
    if clock_case == "valid":
        assert research.call_args.kwargs["decision_time"] == observed_at
        assert facts["research_admission_at"] == observed_at.isoformat()
        assert facts["failure_code"] == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
        research.assert_awaited_once()
    else:
        research.assert_not_awaited()
        assert facts["failure_code"] == "PORTFOLIO_SYNTHESIS_ADMISSION_CLOCK_INVALID"
        assert "research_admission_at" not in facts

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioDecisionInputV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_execution import (
    PortfolioDecisionExecutionError,
    mark_first_requested,
    mark_second_requested,
    record_tool_result,
)
from nexus.agents.responses_budget import UsageSnapshot

RUN_ID = UUID("018f0000-0000-7000-8000-000000000001")
NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)


@pytest.mark.asyncio
async def test_portfolio_decision_phases_advance_without_skips() -> None:
    run = _run()
    session = _Session(run)

    await mark_first_requested(session, run_id=RUN_ID)
    assert (run.status, run.provider_phase) == ("running", "first_requested")

    await record_tool_result(
        session,
        run_id=RUN_ID,
        projection=_projection(),
        usage=UsageSnapshot(input_tokens=100, output_tokens=50, tool_calls=1, provider_calls=1),
    )
    assert (run.provider_phase, run.tool_result_hash, run.input_tokens, run.output_tokens) == (
        "tool_recorded",
        "b" * 64,
        100,
        50,
    )
    with pytest.raises(PortfolioDecisionExecutionError, match="PHASE_INVALID"):
        await record_tool_result(
            session,
            run_id=RUN_ID,
            projection=_projection(),
            usage=UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=1,
                provider_calls=1,
            ),
        )

    await mark_second_requested(session, run_id=RUN_ID)
    assert run.provider_phase == "second_requested"
    with pytest.raises(PortfolioDecisionExecutionError, match="PHASE_INVALID"):
        await mark_second_requested(session, run_id=RUN_ID)
    assert session.flush.await_count == 3
    assert all(call.kwargs == {"with_for_update": True} for call in session.get.await_args_list)


@pytest.mark.asyncio
async def test_portfolio_decision_phase_rejects_repeat_or_skip() -> None:
    run = _run()
    session = _Session(run)

    with pytest.raises(PortfolioDecisionExecutionError, match="PHASE_INVALID"):
        await mark_second_requested(session, run_id=RUN_ID)
    await mark_first_requested(session, run_id=RUN_ID)
    with pytest.raises(PortfolioDecisionExecutionError, match="PHASE_INVALID"):
        await mark_first_requested(session, run_id=RUN_ID)


@pytest.mark.asyncio
async def test_tool_checkpoint_binds_frozen_hash_and_exact_first_turn_usage() -> None:
    run = _run()
    run.status = "running"
    run.provider_phase = "first_requested"
    session = _Session(run)

    changed = _projection().model_copy(update={"result_hash": "f" * 64})
    with pytest.raises(PortfolioDecisionExecutionError, match="TOOL_RESULT_MISMATCH"):
        await record_tool_result(
            session,
            run_id=RUN_ID,
            projection=changed,
            usage=UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=1,
                provider_calls=1,
            ),
        )
    run.input_hash = "f" * 64
    with pytest.raises(PortfolioDecisionExecutionError, match="TOOL_RESULT_MISMATCH"):
        await record_tool_result(
            session,
            run_id=RUN_ID,
            projection=_projection(),
            usage=UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=1,
                provider_calls=1,
            ),
        )
    run.input_hash = canonical_portfolio_decision_hash(_frozen())
    changed_objective = _projection().model_copy(update={"objective": "unsupported"})
    with pytest.raises(PortfolioDecisionExecutionError, match="TOOL_RESULT_MISMATCH"):
        await record_tool_result(
            session,
            run_id=RUN_ID,
            projection=changed_objective,
            usage=UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=1,
                provider_calls=1,
            ),
        )
    with pytest.raises(PortfolioDecisionExecutionError, match="USAGE_INVALID"):
        await record_tool_result(
            session,
            run_id=RUN_ID,
            projection=_projection(),
            usage=UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=1,
                provider_calls=2,
            ),
        )
    with pytest.raises(PortfolioDecisionExecutionError, match="USAGE_INVALID"):
        await record_tool_result(
            session,
            run_id=RUN_ID,
            projection=_projection(),
            usage=UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=0,
                provider_calls=1,
            ),
        )
    assert run.tool_projection is None
    session.flush.assert_not_awaited()


class _Session:
    def __init__(self, run: SimpleNamespace) -> None:
        self.get = AsyncMock(return_value=run)
        self.flush = AsyncMock()


def _run() -> SimpleNamespace:
    frozen = _frozen()
    return SimpleNamespace(
        id=RUN_ID,
        status="prepared",
        retry_count=0,
        provider_phase="none",
        frozen_input=frozen.model_dump(mode="json"),
        input_hash=canonical_portfolio_decision_hash(frozen),
        tool_result_hash=None,
        tool_projection=None,
        input_tokens=None,
        output_tokens=None,
    )


def _frozen() -> PortfolioDecisionInputV1:
    return PortfolioDecisionInputV1.model_validate(
        {
            "schema_version": "portfolio_decision_input_v1",
            "portfolio_id": "018f0000-0000-7000-8000-000000000002",
            "portfolio_snapshot_id": "018f0000-0000-7000-8000-000000000003",
            "diagnostic_report_id": "018f0000-0000-7000-8000-000000000004",
            "decision_time": NOW.isoformat(),
            "objective": "reduce_security_concentration",
            "max_turnover": "0.05",
            "minimum_cash": "0.1",
            "universe_policy": "current_holdings_only",
            "allow_new_positions": False,
            "scenario_request_hash": "c" * 64,
            "expected_scenario_result_hash": "b" * 64,
            "diagnostic_input_hash": "d" * 64,
            "policy_version": "real_advisory_policy_v1",
            "evidence_hash": "e" * 64,
        }
    )


def _projection() -> AgentScenarioProjectionV1:
    known = {"status": "known", "value": "0.4", "reason": None}
    unknown = {
        "status": "unknown",
        "value": None,
        "reason": "COST_EVIDENCE_INCOMPLETE",
    }
    metrics = {
        "security_top1": known,
        "security_top3": known,
        "security_hhi": known,
        "cash_weight": known,
        "one_way_turnover": known,
        "estimated_cost": unknown,
    }
    return AgentScenarioProjectionV1.model_validate(
        {
            "schema_version": "agent_scenario_projection_v1",
            "objective": "reduce_security_concentration",
            "objective_status": "improved_not_met",
            "analysis_status": "partial",
            "baseline": metrics,
            "candidate": metrics,
            "unknown_metrics": ["estimated_cost"],
            "hard_constraint_reasons": [],
            "changed_position_count": 1,
            "result_hash": "b" * 64,
            "citation_ids": ["a" * 64],
            "execution_locked": True,
        }
    )

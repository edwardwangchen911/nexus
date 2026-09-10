"""Persist fail-closed phase transitions for one portfolio decision run."""

from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioDecisionInputV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.persistence.models.agents import PortfolioDecisionRunRecord


class PortfolioDecisionExecutionError(ValueError):
    """Reject an execution checkpoint that is not bound to frozen authority."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


async def mark_first_requested(
    session: AsyncSession, *, run_id: UUID
) -> PortfolioDecisionRunRecord:
    """Record that the single allowed first provider request has started."""
    run = await _locked_run(session, run_id)
    if (
        run.status != "prepared"
        or run.retry_count != 0
        or run.provider_phase != "none"
        or run.tool_result_hash is not None
        or run.tool_projection is not None
        or run.input_tokens is not None
        or run.output_tokens is not None
    ):
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_PHASE_INVALID")
    run.status = "running"
    run.provider_phase = "first_requested"
    await session.flush()
    return run


async def record_tool_result(
    session: AsyncSession,
    *,
    run_id: UUID,
    projection: AgentScenarioProjectionV1,
    usage: UsageSnapshot,
) -> PortfolioDecisionRunRecord:
    """Bind the deterministic tool projection and exact first-turn usage to the run."""
    run = await _locked_run(session, run_id)
    if (
        run.status != "running"
        or run.retry_count != 0
        or run.provider_phase != "first_requested"
        or run.tool_result_hash is not None
        or run.tool_projection is not None
        or run.input_tokens is not None
        or run.output_tokens is not None
    ):
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_PHASE_INVALID")

    try:
        frozen = PortfolioDecisionInputV1.model_validate(run.frozen_input)
    except (TypeError, ValidationError) as error:
        raise PortfolioDecisionExecutionError(
            "PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH"
        ) from error
    if (
        canonical_portfolio_decision_hash(frozen) != run.input_hash
        or projection.objective != frozen.objective
        or projection.result_hash != frozen.expected_scenario_result_hash
    ):
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH")
    if not _valid_first_turn_usage(usage):
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_USAGE_INVALID")

    run.tool_result_hash = projection.result_hash
    run.tool_projection = projection.model_dump(mode="json")
    run.input_tokens = usage.input_tokens
    run.output_tokens = usage.output_tokens
    run.provider_phase = "tool_recorded"
    await session.flush()
    return run


async def mark_second_requested(
    session: AsyncSession, *, run_id: UUID
) -> PortfolioDecisionRunRecord:
    """Record that the final provider request may start after one bound tool call."""
    run = await _locked_run(session, run_id)
    if (
        run.status != "running"
        or run.retry_count != 0
        or run.provider_phase != "tool_recorded"
        or run.tool_result_hash is None
        or run.tool_projection is None
        or run.input_tokens is None
        or run.output_tokens is None
    ):
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_PHASE_INVALID")
    try:
        projection = AgentScenarioProjectionV1.model_validate(run.tool_projection)
    except (TypeError, ValidationError) as error:
        raise PortfolioDecisionExecutionError(
            "PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH"
        ) from error
    if projection.result_hash != run.tool_result_hash:
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH")

    run.provider_phase = "second_requested"
    await session.flush()
    return run


async def _locked_run(
    session: AsyncSession, run_id: UUID
) -> PortfolioDecisionRunRecord:
    run = await session.get(PortfolioDecisionRunRecord, run_id, with_for_update=True)
    if run is None:
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_RUN_MISSING")
    return run


def _valid_first_turn_usage(usage: UsageSnapshot) -> bool:
    values = (usage.input_tokens, usage.output_tokens, usage.tool_calls, usage.provider_calls)
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values):
        return False
    return usage.tool_calls == 1 and usage.provider_calls == 1

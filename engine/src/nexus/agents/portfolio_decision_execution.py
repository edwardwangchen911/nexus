"""Load frozen authority and persist fail-closed portfolio decision phases."""

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioDecisionInputV1,
    PortfolioDecisionModelContextV1,
    PortfolioDiagnosticSummaryV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_persistence import canonical_portfolio_intent_schema_hash
from nexus.agents.portfolio_decision_tools import (
    PortfolioCitationAuthority,
    PortfolioDecisionToolRegistry,
    build_portfolio_decision_tools,
)
from nexus.agents.responses import ResponsesModelConfig
from nexus.agents.responses_budget import UsageSnapshot
from nexus.diagnostics.models import DiagnosticItem
from nexus.diagnostics.repository import read_report
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import AdjustmentScenarioConstraintsV1
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord

_PROMPT_ROLE = "portfolio_decision"
_PROMPT_VERSION = "portfolio-decision-v1"
_MODEL_TIMEOUT_SECONDS = Decimal("45")
_DIAGNOSTIC_CODES = (
    "CONCENTRATION_TOP1",
    "CONCENTRATION_TOP3",
    "CONCENTRATION_HHI",
)


class PortfolioDecisionExecutionError(ValueError):
    """Reject an execution checkpoint that is not bound to frozen authority."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class PortfolioDecisionExecutionAuthority:
    """Validated inputs retained while provider calls run outside transactions."""

    frozen: PortfolioDecisionInputV1
    context: PortfolioDecisionModelContextV1
    prompt: str
    model: AgentModelConfigRecord
    model_config: ResponsesModelConfig
    tools: PortfolioDecisionToolRegistry


async def load_portfolio_decision_execution_authority(
    session: AsyncSession,
    *,
    run: PortfolioDecisionRunRecord,
) -> PortfolioDecisionExecutionAuthority:
    """Rebuild the aggregate model context from one immutable admitted run."""
    try:
        frozen = PortfolioDecisionInputV1.model_validate(run.frozen_input)
    except (TypeError, ValidationError) as error:
        raise _authority_error() from error
    if (
        run.status != "prepared"
        or run.retry_count != 0
        or run.provider_phase != "none"
        or canonical_portfolio_decision_hash(frozen) != run.input_hash
        or frozen.portfolio_id != run.portfolio_id
        or frozen.portfolio_snapshot_id != run.portfolio_snapshot_id
        or frozen.diagnostic_report_id != run.diagnostic_report_id
        or frozen.decision_time != run.decision_time
        or frozen.evidence_hash != run.evidence_hash
        or run.schema_hash != canonical_portfolio_intent_schema_hash()
    ):
        raise _authority_error()

    prompt = await session.get(AgentPromptVersionRecord, run.prompt_version_id)
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    report_record = await session.get(
        PortfolioDiagnosticReportRecord, run.diagnostic_report_id
    )
    if prompt is None or model is None or report_record is None:
        raise _authority_error()
    if (
        prompt.role != _PROMPT_ROLE
        or prompt.version != _PROMPT_VERSION
        or prompt.content_hash != run.prompt_hash
        or hashlib.sha256(prompt.content.encode()).hexdigest() != prompt.content_hash
        or prompt.created_at > run.decision_time
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
        or model.max_tool_calls < 1
        or model.created_at > run.decision_time
        or model.pricing_available_at > run.decision_time
        or model.pricing_expires_at <= run.decision_time
        or model.worst_case_cost_usd != run.reserved_cost_usd
        or report_record.portfolio_id != run.portfolio_id
        or report_record.portfolio_snapshot_id != run.portfolio_snapshot_id
        or report_record.created_at > run.decision_time
        or report_record.policy_version != frozen.policy_version
        or report_record.diagnostic_input_hash != frozen.diagnostic_input_hash
    ):
        raise _authority_error()

    try:
        citations = PortfolioCitationAuthority.from_manifest(
            run_id=run.id,
            manifest=run.evidence_manifest,
            evidence_hash=run.evidence_hash,
            decision_time=run.decision_time,
        )
        report = await read_report(session, report_record)
    except (TypeError, ValueError, ValidationError) as error:
        raise _authority_error() from error
    report_hash = _canonical_model_hash(report)
    report_citations = tuple(
        citation_id
        for citation_id in citations.citation_ids
        if _is_report_evidence(
            citations,
            citation_id=citation_id,
            report=report_record,
            content_hash=report_hash,
        )
    )
    if (
        len(report_citations) != 1
        or report.portfolio_snapshot_id != run.portfolio_snapshot_id
        or report.snapshot_as_of != report_record.snapshot_as_of
        or report.policy_version != frozen.policy_version
    ):
        raise _authority_error()

    items: dict[str, DiagnosticItem] = {}
    for code in _DIAGNOSTIC_CODES:
        matches = tuple(
            item
            for item in report.items
            if item.diagnostic_code == code and item.policy_scope == "currency:USD"
        )
        if len(matches) != 1:
            raise _authority_error()
        items[code] = matches[0]
    try:
        diagnostics = tuple(
            PortfolioDiagnosticSummaryV1.model_validate(
                {
                    "code": code,
                    "severity": items[code].severity,
                    "metric": TypedMetric.known(_known_usd_ratio(items[code])),
                    "unit": "ratio",
                    "citation_ids": report_citations,
                }
            )
            for code in _DIAGNOSTIC_CODES
        )
        constraints = AdjustmentScenarioConstraintsV1(
            schema_version="adjustment_scenario_constraints_v1",
            objective=frozen.objective,
            max_turnover=Decimal(frozen.max_turnover),
            minimum_cash=Decimal(frozen.minimum_cash),
            universe_policy=frozen.universe_policy,
            allow_new_positions=frozen.allow_new_positions,
        )
        context = PortfolioDecisionModelContextV1(
            schema_version="portfolio_decision_model_context_v1",
            run_id=run.id,
            portfolio_id=run.portfolio_id,
            portfolio_snapshot_id=run.portfolio_snapshot_id,
            diagnostic_report_id=run.diagnostic_report_id,
            decision_time=run.decision_time,
            objective=frozen.objective,
            constraints=constraints,
            current_diagnostics=diagnostics,
            citation_ids=citations.citation_ids,
            execution_locked=True,
        )
        model_config = ResponsesModelConfig(
            model_id=model.model_id,
            pricing_version=model.version,
            max_input_tokens=model.max_input_tokens,
            max_output_tokens=model.max_output_tokens,
            max_tool_calls=1,
            absolute_timeout_seconds=_MODEL_TIMEOUT_SECONDS,
            input_usd_per_million=model.input_usd_per_million,
            output_usd_per_million=model.output_usd_per_million,
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise _authority_error() from error
    try:
        tools = await build_portfolio_decision_tools(session, run)
    except (TypeError, ValueError, ValidationError) as error:
        raise _authority_error() from error
    return PortfolioDecisionExecutionAuthority(
        frozen=frozen,
        context=context,
        prompt=prompt.content,
        model=model,
        model_config=model_config,
        tools=tools,
    )


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


def _known_usd_ratio(item: DiagnosticItem) -> str:
    if (
        item.metric_value is None
        or item.unknown_reason is not None
        or item.unit != "ratio"
        or item.policy_scope != "currency:USD"
    ):
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_AUTHORITY_INVALID")
    return item.metric_value


def _canonical_model_hash(value: object) -> str:
    model_dump = getattr(value, "model_dump", None)
    if not callable(model_dump):
        raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_AUTHORITY_INVALID")
    payload = model_dump(mode="json")
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _is_report_evidence(
    authority: PortfolioCitationAuthority,
    *,
    citation_id: str,
    report: PortfolioDiagnosticReportRecord,
    content_hash: str,
) -> bool:
    evidence = authority.resolve(citation_id)
    return (
        evidence.source == "nexus.portfolio_diagnostic"
        and evidence.object_id == str(report.id)
        and evidence.available_at == report.created_at
        and evidence.location == f"postgresql:portfolio_diagnostic_reports/{report.id}"
        and evidence.content_hash == content_hash
    )


def _authority_error() -> PortfolioDecisionExecutionError:
    return PortfolioDecisionExecutionError("PORTFOLIO_DECISION_AUTHORITY_INVALID")

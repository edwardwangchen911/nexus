import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
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
    PortfolioDecisionExecutionAuthority,
    PortfolioDecisionExecutionError,
    load_portfolio_decision_execution_authority,
    mark_first_requested,
    mark_second_requested,
    record_tool_result,
)
from nexus.agents.portfolio_decision_persistence import canonical_portfolio_intent_schema_hash
from nexus.agents.portfolio_decision_tools import canonical_portfolio_evidence_manifest_hash
from nexus.agents.responses_budget import UsageSnapshot
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord

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


@pytest.mark.asyncio
async def test_execution_authority_builds_only_the_frozen_aggregate_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, prompt, model, report_record, report = _authority_fixture()
    tools = object()
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.read_report",
        AsyncMock(return_value=report),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.build_portfolio_decision_tools",
        AsyncMock(return_value=tools),
    )

    authority = await load_portfolio_decision_execution_authority(
        _AuthoritySession(prompt, model, report_record),
        run=run,
    )

    assert isinstance(authority, PortfolioDecisionExecutionAuthority)
    assert authority.prompt == "bounded portfolio decision prompt"
    assert authority.model_config.max_tool_calls == 1
    assert authority.model_config.absolute_timeout_seconds == Decimal("45")
    assert authority.tools is tools
    assert [item.code for item in authority.context.current_diagnostics] == [
        "CONCENTRATION_TOP1",
        "CONCENTRATION_TOP3",
        "CONCENTRATION_HHI",
    ]
    wire = authority.context.model_dump(mode="json")
    for forbidden in ("current_weight", "target_weight", "quantity", "order"):
        assert forbidden not in str(wire)
    assert authority.context.execution_locked is True


@pytest.mark.asyncio
async def test_execution_authority_rejects_drift_before_building_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, prompt, model, report_record, report = _authority_fixture()
    build_tools = AsyncMock()
    read_frozen_report = AsyncMock(return_value=report)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.read_report",
        read_frozen_report,
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.build_portfolio_decision_tools",
        build_tools,
    )
    prompt.content = "changed after admission"

    with pytest.raises(PortfolioDecisionExecutionError, match="AUTHORITY_INVALID"):
        await load_portfolio_decision_execution_authority(
            _AuthoritySession(prompt, model, report_record),
            run=run,
        )
    build_tools.assert_not_awaited()

    prompt.content = "bounded portfolio decision prompt"
    read_frozen_report.return_value = report.model_copy(update={"items": report.items[:2]})
    with pytest.raises(PortfolioDecisionExecutionError, match="AUTHORITY_INVALID"):
        await load_portfolio_decision_execution_authority(
            _AuthoritySession(prompt, model, report_record),
            run=run,
        )
    build_tools.assert_not_awaited()


class _Session:
    def __init__(self, run: SimpleNamespace) -> None:
        self.get = AsyncMock(return_value=run)
        self.flush = AsyncMock()


class _AuthoritySession:
    def __init__(
        self,
        prompt: SimpleNamespace,
        model: SimpleNamespace,
        report: SimpleNamespace,
    ) -> None:
        records = {
            (AgentPromptVersionRecord, prompt.id): prompt,
            (AgentModelConfigRecord, model.id): model,
            (PortfolioDiagnosticReportRecord, report.id): report,
        }

        async def get(record_type: object, record_id: UUID) -> object | None:
            return records.get((record_type, record_id))

        self.get = AsyncMock(side_effect=get)


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


def _authority_fixture() -> tuple[
    SimpleNamespace,
    SimpleNamespace,
    SimpleNamespace,
    SimpleNamespace,
    PortfolioDiagnosticReport,
]:
    prompt_id = UUID("018f0000-0000-7000-8000-000000000006")
    model_id = UUID("018f0000-0000-7000-8000-000000000007")
    portfolio_id = UUID("018f0000-0000-7000-8000-000000000002")
    snapshot_id = UUID("018f0000-0000-7000-8000-000000000003")
    report_id = UUID("018f0000-0000-7000-8000-000000000004")
    prompt_text = "bounded portfolio decision prompt"
    report_evidence = EvidenceRef(
        source="nexus.portfolio_diagnostic",
        object_id=str(report_id),
        available_at=NOW,
        location=f"postgresql:portfolio_diagnostic_reports/{report_id}",
        content_hash="9" * 64,
    )
    manifest = [report_evidence.model_dump(mode="json")]
    evidence_hash = canonical_portfolio_evidence_manifest_hash(manifest)
    frozen = _frozen().model_copy(
        update={
            "portfolio_id": portfolio_id,
            "portfolio_snapshot_id": snapshot_id,
            "diagnostic_report_id": report_id,
            "evidence_hash": evidence_hash,
        }
    )
    run = _run()
    run.portfolio_id = portfolio_id
    run.portfolio_snapshot_id = snapshot_id
    run.diagnostic_report_id = report_id
    run.prompt_version_id = prompt_id
    run.model_config_id = model_id
    run.decision_time = NOW
    run.frozen_input = frozen.model_dump(mode="json")
    run.input_hash = canonical_portfolio_decision_hash(frozen)
    run.schema_hash = canonical_portfolio_intent_schema_hash()
    run.prompt_hash = hashlib.sha256(prompt_text.encode()).hexdigest()
    run.model_hash = "8" * 64
    run.evidence_hash = evidence_hash
    run.evidence_manifest = manifest
    run.pricing_version = "fixture-portfolio-v1"
    run.reserved_cost_usd = Decimal("0.00015")
    prompt = SimpleNamespace(
        id=prompt_id,
        role="portfolio_decision",
        version="portfolio-decision-v1",
        content_hash=run.prompt_hash,
        content=prompt_text,
        created_at=NOW,
    )
    model = SimpleNamespace(
        id=model_id,
        model_id="fixture-model",
        version=run.pricing_version,
        config_hash=run.model_hash,
        max_input_tokens=100,
        max_output_tokens=100,
        max_tool_calls=4,
        input_usd_per_million=Decimal("0.5"),
        output_usd_per_million=Decimal("1"),
        pricing_available_at=NOW,
        pricing_expires_at=datetime(2026, 9, 11, 8, tzinfo=UTC),
        created_at=NOW,
        worst_case_cost_usd=Decimal("0.00015"),
    )
    report_record = SimpleNamespace(
        id=report_id,
        portfolio_id=portfolio_id,
        portfolio_snapshot_id=snapshot_id,
        snapshot_as_of=datetime(2026, 9, 10, 7, 59, tzinfo=UTC),
        policy_version="real_advisory_policy_v1",
        diagnostic_input_hash="d" * 64,
        created_at=NOW,
    )
    report = PortfolioDiagnosticReport(
        portfolio_snapshot_id=snapshot_id,
        snapshot_as_of=report_record.snapshot_as_of,
        trading_day=NOW.date(),
        policy_version="real_advisory_policy_v1",
        optimization_need="RESEARCH_REQUIRED",
        items=tuple(
            DiagnosticItem(
                diagnostic_code=code,
                metric_value=value,
                unit="ratio",
                reference_limit="0.3",
                policy_scope=scope,
                severity="warning",
                actionability="research",
                unknown_reason=None,
                evidence_refs=(report_evidence,),
            )
            for scope, values in (
                (
                    "currency:USD",
                    (
                        ("CONCENTRATION_TOP1", "0.4"),
                        ("CONCENTRATION_TOP3", "0.8"),
                        ("CONCENTRATION_HHI", "0.22"),
                    ),
                ),
                (
                    "currency:HKD",
                    (
                        ("CONCENTRATION_TOP1", "1"),
                        ("CONCENTRATION_TOP3", "1"),
                        ("CONCENTRATION_HHI", "1"),
                    ),
                ),
            )
            for code, value in values
        ),
    )
    report_hash = hashlib.sha256(
        json.dumps(
            report.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    report_evidence = report_evidence.model_copy(update={"content_hash": report_hash})
    manifest = [report_evidence.model_dump(mode="json")]
    evidence_hash = canonical_portfolio_evidence_manifest_hash(manifest)
    frozen = frozen.model_copy(update={"evidence_hash": evidence_hash})
    run.frozen_input = frozen.model_dump(mode="json")
    run.input_hash = canonical_portfolio_decision_hash(frozen)
    run.evidence_hash = evidence_hash
    run.evidence_manifest = manifest
    return run, prompt, model, report_record, report

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioDecisionInputV1,
    PortfolioResearchIntentV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_execution import (
    PortfolioDecisionExecutionError,
    persist_portfolio_decision_terminal,
)
from nexus.agents.portfolio_decision_tools import (
    PortfolioCitationAuthority,
    canonical_portfolio_evidence_manifest_hash,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    PortfolioDecisionOutputRecord,
    PortfolioDecisionRunRecord,
)

RUN_ID = UUID("018f0000-0000-7000-8000-000000000001")
PORTFOLIO_ID = UUID("018f0000-0000-7000-8000-000000000002")
SNAPSHOT_ID = UUID("018f0000-0000-7000-8000-000000000003")
REPORT_ID = UUID("018f0000-0000-7000-8000-000000000004")
MODEL_ID = UUID("018f0000-0000-7000-8000-000000000005")
NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)


@pytest.mark.asyncio
async def test_completed_terminal_persists_one_bound_intent_and_exact_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, model, citation_id = _terminal_fixture(phase="second_requested")
    session = _Session(run, model)
    settle = AsyncMock()
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.settle_portfolio_decision_budget",
        settle,
    )

    output = await persist_portfolio_decision_terminal(
        session,
        run_id=RUN_ID,
        status="completed",
        error_code=None,
        intent=_intent(citation_id),
        usage=UsageSnapshot(
            input_tokens=180,
            output_tokens=80,
            tool_calls=1,
            provider_calls=2,
        ),
        usage_known=True,
    )

    assert output.status == "completed"
    assert output.scenario_result_hash == "b" * 64
    assert output.intent == _intent(citation_id).model_dump(mode="json")
    assert output.details == {"execution_locked": True}
    assert run.status == "completed"
    assert (run.input_tokens, run.output_tokens) == (180, 80)
    assert session.added == [output]
    settle.assert_awaited_once_with(
        session,
        run_id=RUN_ID,
        actual_usd=Decimal("0.00017"),
        input_tokens=180,
        output_tokens=80,
    )


@pytest.mark.asyncio
async def test_terminal_rejects_unbound_intent_before_budget_or_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, model, citation_id = _terminal_fixture(phase="second_requested")
    session = _Session(run, model)
    settle = AsyncMock()
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.settle_portfolio_decision_budget",
        settle,
    )
    changed = _intent(citation_id).model_copy(update={"scenario_result_hash": "f" * 64})

    with pytest.raises(PortfolioDecisionExecutionError, match="TERMINAL_INVALID"):
        await persist_portfolio_decision_terminal(
            session,
            run_id=RUN_ID,
            status="completed",
            error_code=None,
            intent=changed,
            usage=UsageSnapshot(
                input_tokens=180,
                output_tokens=80,
                tool_calls=1,
                provider_calls=2,
            ),
            usage_known=True,
        )

    assert session.added == []
    settle.assert_not_awaited()
    assert run.status == "running"


@pytest.mark.asyncio
async def test_terminal_rejects_frozen_authority_detached_from_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, model, citation_id = _terminal_fixture(phase="second_requested")
    run.portfolio_id = UUID("018f0000-0000-7000-8000-000000000099")
    session = _Session(run, model)
    settle = AsyncMock()
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.settle_portfolio_decision_budget",
        settle,
    )

    with pytest.raises(PortfolioDecisionExecutionError, match="TERMINAL_INVALID"):
        await persist_portfolio_decision_terminal(
            session,
            run_id=RUN_ID,
            status="completed",
            error_code=None,
            intent=_intent(citation_id),
            usage=UsageSnapshot(
                input_tokens=180,
                output_tokens=80,
                tool_calls=1,
                provider_calls=2,
            ),
            usage_known=True,
        )

    assert session.added == []
    settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_usage_after_request_settles_full_without_raw_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, model, _ = _terminal_fixture(phase="second_requested")
    session = _Session(run, model)
    settle_full = AsyncMock()
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.settle_full_portfolio_decision_budget",
        settle_full,
    )

    output = await persist_portfolio_decision_terminal(
        session,
        run_id=RUN_ID,
        status="failed",
        error_code="PORTFOLIO_DECISION_MODEL_FAILED",
        intent=None,
        usage=None,
        usage_known=False,
    )

    assert output.intent is None
    assert output.details == {
        "error_code": "PORTFOLIO_DECISION_MODEL_FAILED",
        "execution_locked": True,
    }
    assert "provider" not in str(output.details).lower()
    settle_full.assert_awaited_once_with(session, run_id=RUN_ID)
    assert run.status == "failed"


@pytest.mark.asyncio
async def test_tool_recorded_terminal_settles_persisted_first_turn_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, model, _ = _terminal_fixture(phase="tool_recorded")
    session = _Session(run, model)
    settle = AsyncMock()
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.settle_portfolio_decision_budget",
        settle,
    )

    await persist_portfolio_decision_terminal(
        session,
        run_id=RUN_ID,
        status="abstained",
        error_code="PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH",
        intent=None,
        usage=None,
        usage_known=True,
    )

    settle.assert_awaited_once_with(
        session,
        run_id=RUN_ID,
        actual_usd=Decimal("0.0001"),
        input_tokens=100,
        output_tokens=50,
    )
    assert run.status == "abstained"


@pytest.mark.asyncio
async def test_terminal_rejects_projection_that_was_not_checkpointed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, model, _ = _terminal_fixture(phase="first_requested")
    session = _Session(run, model)
    settle = AsyncMock()
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_execution.settle_portfolio_decision_budget",
        settle,
    )

    with pytest.raises(PortfolioDecisionExecutionError, match="TERMINAL_INVALID"):
        await persist_portfolio_decision_terminal(
            session,
            run_id=RUN_ID,
            status="failed",
            error_code="PORTFOLIO_DECISION_MODEL_FAILED",
            intent=None,
            usage=UsageSnapshot(
                input_tokens=100,
                output_tokens=50,
                tool_calls=1,
                provider_calls=1,
            ),
            usage_known=True,
        )

    assert session.added == []
    settle.assert_not_awaited()


class _Session:
    def __init__(self, run: SimpleNamespace, model: SimpleNamespace) -> None:
        async def get(record_type: object, record_id: UUID, **_: object) -> object | None:
            if (record_type, record_id) == (PortfolioDecisionRunRecord, run.id):
                return run
            if (record_type, record_id) == (AgentModelConfigRecord, model.id):
                return model
            return None

        self.get = AsyncMock(side_effect=get)
        self.scalar = AsyncMock(return_value=None)
        self.flush = AsyncMock()
        self.added: list[PortfolioDecisionOutputRecord] = []

    def add(self, value: PortfolioDecisionOutputRecord) -> None:
        self.added.append(value)


def _terminal_fixture(phase: str) -> tuple[SimpleNamespace, SimpleNamespace, str]:
    evidence = EvidenceRef(
        source="nexus.portfolio_diagnostic",
        object_id=str(REPORT_ID),
        available_at=NOW,
        location=f"postgresql:portfolio_diagnostic_reports/{REPORT_ID}",
        content_hash="9" * 64,
    )
    manifest = [evidence.model_dump(mode="json")]
    evidence_hash = canonical_portfolio_evidence_manifest_hash(manifest)
    frozen = PortfolioDecisionInputV1.model_validate(
        {
            "schema_version": "portfolio_decision_input_v1",
            "portfolio_id": str(PORTFOLIO_ID),
            "portfolio_snapshot_id": str(SNAPSHOT_ID),
            "diagnostic_report_id": str(REPORT_ID),
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
            "evidence_hash": evidence_hash,
        }
    )
    citation_id = PortfolioCitationAuthority.from_manifest(
        run_id=RUN_ID,
        manifest=manifest,
        evidence_hash=evidence_hash,
        decision_time=NOW,
    ).citation_ids[0]
    projection = _projection(citation_id)
    run = SimpleNamespace(
        id=RUN_ID,
        status="running",
        retry_count=0,
        provider_phase=phase,
        portfolio_id=PORTFOLIO_ID,
        portfolio_snapshot_id=SNAPSHOT_ID,
        diagnostic_report_id=REPORT_ID,
        model_config_id=MODEL_ID,
        decision_time=NOW,
        frozen_input=frozen.model_dump(mode="json"),
        input_hash=canonical_portfolio_decision_hash(frozen),
        model_hash="8" * 64,
        evidence_hash=evidence_hash,
        evidence_manifest=manifest,
        tool_result_hash=projection.result_hash,
        tool_projection=projection.model_dump(mode="json"),
        reserved_cost_usd=Decimal("0.0003"),
        settled_cost_usd=None,
        pricing_version="fixture-portfolio-v1",
        input_tokens=100,
        output_tokens=50,
    )
    model = SimpleNamespace(
        id=MODEL_ID,
        version=run.pricing_version,
        config_hash=run.model_hash,
        max_input_tokens=1000,
        max_output_tokens=1000,
        input_usd_per_million=Decimal("0.5"),
        output_usd_per_million=Decimal("1"),
    )
    return run, model, citation_id


def _projection(citation_id: str) -> AgentScenarioProjectionV1:
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
            "citation_ids": [citation_id],
            "execution_locked": True,
        }
    )


def _intent(citation_id: str) -> PortfolioResearchIntentV1:
    return PortfolioResearchIntentV1.model_validate(
        {
            "schema_version": "portfolio_research_intent_v1",
            "run_id": str(RUN_ID),
            "portfolio_id": str(PORTFOLIO_ID),
            "portfolio_snapshot_id": str(SNAPSHOT_ID),
            "diagnostic_report_id": str(REPORT_ID),
            "decision_time": NOW.isoformat(),
            "assessment": "explore_adjustment",
            "objective": "reduce_security_concentration",
            "horizon": "medium_term",
            "confidence": "medium",
            "rationale": [{"text": "Concentration improves.", "citation_ids": [citation_id]}],
            "invalidation_conditions": [
                {
                    "signal": "portfolio.scenario_objective_status",
                    "operator": "eq",
                    "threshold": "unchanged",
                    "unit": "state",
                    "citation_ids": [citation_id],
                }
            ],
            "limitations": [
                {"code": "estimated_cost", "text": "Estimated cost remains unknown."}
            ],
            "scenario_result_hash": "b" * 64,
            "citation_ids": [citation_id],
            "execution_locked": True,
        }
    )

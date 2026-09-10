"""Bounded Agent access to the deterministic portfolio scenario."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from nexus.agents.portfolio_decision_contracts import (
    PortfolioDecisionInputV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_tools import (
    PortfolioCitationAuthority,
    PortfolioDecisionTools,
    build_portfolio_decision_tools,
    canonical_portfolio_evidence_manifest_hash,
    canonical_portfolio_tool_hash,
    project_scenario_for_agent,
)
from nexus.agents.tools import ToolInputError
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.domain.evidence import EvidenceRef
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import (
    PortfolioScenarioEvaluationV1,
    ScenarioMetricSetV1,
    ScenarioPositionV1,
    canonical_scenario_hash,
)
from nexus.persistence.models.agents import PortfolioDecisionRunRecord
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord

_RUN_ID = UUID("018f0000-0000-7000-8000-000000000001")
_SNAPSHOT_ID = UUID("018f0000-0000-7000-8000-000000000002")
_REPORT_ID = UUID("018f0000-0000-7000-8000-000000000003")
_INSTRUMENT_A = UUID("018f0000-0000-7000-8000-000000000004")
_INSTRUMENT_B = UUID("018f0000-0000-7000-8000-000000000005")
_NOW = datetime(2026, 9, 10, 8, 0, tzinfo=UTC)


def _metrics(*, top1: str, cash: str, turnover: str) -> ScenarioMetricSetV1:
    return ScenarioMetricSetV1(
        security_top1=TypedMetric.known(top1),
        security_top3=TypedMetric.known("0.9"),
        security_hhi=TypedMetric.known("0.41"),
        cash_weight=TypedMetric.known(cash),
        one_way_turnover=TypedMetric.known(turnover),
        estimated_cost=TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
    )


def _evaluation() -> PortfolioScenarioEvaluationV1:
    draft = PortfolioScenarioEvaluationV1(
        schema_version="portfolio_scenario_evaluation_v1",
        request_hash="a" * 64,
        result_hash="0" * 64,
        portfolio_snapshot_id=_SNAPSHOT_ID,
        diagnostic_report_id=_REPORT_ID,
        diagnostic_input_hash="b" * 64,
        valuation_time=_NOW,
        valuation_scope="currency:USD",
        objective="reduce_security_concentration",
        analysis_status="partial",
        objective_status="improved_not_met",
        baseline=_metrics(top1="0.6", cash="0.1", turnover="0"),
        candidate=_metrics(top1="0.55", cash="0.15", turnover="0.05"),
        positions=(
            ScenarioPositionV1(
                instrument_id=_INSTRUMENT_A,
                current_weight=Decimal("0.6"),
                target_weight=Decimal("0.55"),
                action="reduce",
                reason_code="CONCENTRATION_REDUCTION",
            ),
            ScenarioPositionV1(
                instrument_id=_INSTRUMENT_B,
                current_weight=Decimal("0.3"),
                target_weight=Decimal("0.3"),
                action="maintain",
                reason_code="BASELINE_WEIGHT",
            ),
        ),
        cash_current_weight=Decimal("0.1"),
        cash_target_weight=Decimal("0.15"),
        hard_constraint_reasons=("TURNOVER_BUDGET_EXHAUSTED",),
        unknown_metrics=("estimated_cost",),
        execution_locked=True,
    )
    return draft.model_copy(update={"result_hash": canonical_scenario_hash(draft)})


@pytest.mark.asyncio
async def test_registry_rejects_caller_authority_before_scenario_execution() -> None:
    calls = 0
    evaluation = _evaluation()

    def execute_scenario() -> PortfolioScenarioEvaluationV1:
        nonlocal calls
        calls += 1
        return evaluation

    tools = PortfolioDecisionTools(
        objective="reduce_security_concentration",
        expected_result_hash=evaluation.result_hash,
        citation_ids=("c" * 64,),
        executor=execute_scenario,
    )

    assert tools.names == ("evaluate_adjustment_scenario",)
    with pytest.raises(ToolInputError):
        await tools.execute(
            "evaluate_adjustment_scenario",
            {
                "objective": "reduce_security_concentration",
                "portfolio_snapshot_id": str(_SNAPSHOT_ID),
            },
        )
    with pytest.raises(ToolInputError):
        await tools.execute("evaluate_adjustment_scenario", {"objective": "increase_return"})
    with pytest.raises(ToolInputError):
        await tools.execute("other_tool", {"objective": "reduce_security_concentration"})
    assert calls == 0

    result = await tools.execute(
        "evaluate_adjustment_scenario", {"objective": "reduce_security_concentration"}
    )
    assert result["result_hash"] == evaluation.result_hash
    assert calls == 1


@pytest.mark.asyncio
async def test_projection_never_exposes_position_identity_or_execution_fields() -> None:
    evaluation = _evaluation()
    tools = PortfolioDecisionTools(
        objective="reduce_security_concentration",
        expected_result_hash=evaluation.result_hash,
        citation_ids=("c" * 64,),
        executor=lambda: evaluation,
    )

    result = await tools.execute(
        "evaluate_adjustment_scenario", {"objective": "reduce_security_concentration"}
    )
    wire = json.dumps(result, sort_keys=True)
    assert result["changed_position_count"] == 1
    for forbidden in (
        "positions",
        "instrument_id",
        "current_weight",
        "target_weight",
        "quantity",
        "amount",
        "order",
        str(_INSTRUMENT_A),
        "AAPL",
    ):
        assert forbidden not in wire


@pytest.mark.asyncio
async def test_registry_fails_closed_when_recomputed_result_hash_changes() -> None:
    evaluation = _evaluation()
    tools = PortfolioDecisionTools(
        objective="reduce_security_concentration",
        expected_result_hash="d" * 64,
        citation_ids=("c" * 64,),
        executor=lambda: evaluation,
    )

    with pytest.raises(ToolInputError, match="result authority changed"):
        await tools.execute(
            "evaluate_adjustment_scenario", {"objective": "reduce_security_concentration"}
        )


def test_projection_rejects_a_noncanonical_evaluation_hash() -> None:
    evaluation = _evaluation().model_copy(update={"result_hash": "e" * 64})

    with pytest.raises(ToolInputError, match="result hash is invalid"):
        project_scenario_for_agent(evaluation, citation_ids=("c" * 64,))


def test_citation_ids_bind_run_index_and_full_evidence_identity() -> None:
    refs = (
        EvidenceRef(
            source="diagnostic_b",
            object_id="same-content-b",
            available_at=_NOW,
            location="db://portfolio-diagnostic-reports/report-b",
            content_hash="f" * 64,
        ),
        EvidenceRef(
            source="diagnostic_a",
            object_id="same-content-a",
            available_at=_NOW,
            location="memory://sha256/" + "f" * 64,
            content_hash="f" * 64,
        ),
    )
    manifest = [ref.model_dump(mode="json") for ref in refs]
    authority = PortfolioCitationAuthority.from_manifest(
        run_id=_RUN_ID,
        manifest=manifest,
        evidence_hash=canonical_portfolio_evidence_manifest_hash(manifest),
        decision_time=_NOW,
    )

    assert len(set(authority.citation_ids)) == 2
    assert tuple(authority.resolve(citation_id) for citation_id in authority.citation_ids) == refs

    with pytest.raises(ToolInputError, match="future evidence"):
        PortfolioCitationAuthority.from_manifest(
            run_id=_RUN_ID,
            manifest=[
                refs[0]
                .model_copy(update={"available_at": _NOW + timedelta(seconds=1)})
                .model_dump(mode="json")
            ],
            evidence_hash=canonical_portfolio_evidence_manifest_hash(
                [
                    refs[0]
                    .model_copy(update={"available_at": _NOW + timedelta(seconds=1)})
                    .model_dump(mode="json")
                ]
            ),
            decision_time=_NOW,
        )


@pytest.mark.asyncio
async def test_builder_validates_tool_hash_and_accepts_frozen_valuation_before_decision_time(
    monkeypatch,
) -> None:
    valuation_time = _NOW - timedelta(hours=1)
    evidence = EvidenceRef(
        source="diagnostic",
        object_id="snapshot-and-report",
        available_at=valuation_time,
        location="memory://sha256/" + "e" * 64,
        content_hash="e" * 64,
    )
    manifest = [evidence.model_dump(mode="json")]
    evidence_hash = canonical_portfolio_evidence_manifest_hash(manifest)
    frozen = PortfolioDecisionInputV1(
        schema_version="portfolio_decision_input_v1",
        portfolio_id=UUID("018f0000-0000-7000-8000-000000000010"),
        portfolio_snapshot_id=_SNAPSHOT_ID,
        diagnostic_report_id=_REPORT_ID,
        decision_time=_NOW,
        objective="reduce_security_concentration",
        max_turnover="0.05",
        minimum_cash="0.1",
        universe_policy="current_holdings_only",
        allow_new_positions=False,
        scenario_request_hash="a" * 64,
        expected_scenario_result_hash="b" * 64,
        diagnostic_input_hash="c" * 64,
        policy_version="real_advisory_policy_v1",
        evidence_hash=evidence_hash,
    )
    snapshot_record = PortfolioSnapshotRecord(
        id=_SNAPSHOT_ID,
        portfolio_id=frozen.portfolio_id,
        as_of=valuation_time,
        created_at=valuation_time,
    )
    diagnostic_record = PortfolioDiagnosticReportRecord(
        id=_REPORT_ID,
        portfolio_id=frozen.portfolio_id,
        portfolio_snapshot_id=_SNAPSHOT_ID,
        snapshot_as_of=valuation_time,
        created_at=valuation_time,
        diagnostic_input_hash=frozen.diagnostic_input_hash,
        policy_version=frozen.policy_version,
    )
    run = PortfolioDecisionRunRecord(
        id=_RUN_ID,
        portfolio_id=frozen.portfolio_id,
        portfolio_snapshot_id=_SNAPSHOT_ID,
        diagnostic_report_id=_REPORT_ID,
        decision_time=_NOW,
        frozen_input=frozen.model_dump(mode="json"),
        input_hash=canonical_portfolio_decision_hash(frozen),
        evidence_hash=evidence_hash,
        evidence_manifest=manifest,
        tool_hash="f" * 64,
    )

    class ReadOnlySession:
        async def get(self, model, identity):
            return {
                (PortfolioSnapshotRecord, _SNAPSHOT_ID): snapshot_record,
                (PortfolioDiagnosticReportRecord, _REPORT_ID): diagnostic_record,
            }.get((model, identity))

    async def frozen_snapshot(*args, **kwargs):
        return object()

    async def report(*args, **kwargs):
        return object()

    async def identities(*args, **kwargs):
        return {}

    async def policy(*args, **kwargs):
        return RealAdvisoryPolicy.v1()

    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_tools.load_frozen_snapshot", frozen_snapshot
    )
    monkeypatch.setattr("nexus.agents.portfolio_decision_tools.read_report", report)
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_tools.load_instrument_identities", identities
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_tools.load_real_advisory_policy_v1", policy
    )

    with pytest.raises(ToolInputError, match="tool authority does not match"):
        await build_portfolio_decision_tools(ReadOnlySession(), run)  # type: ignore[arg-type]

    run.tool_hash = canonical_portfolio_tool_hash()
    original_read_bytes = Path.read_bytes

    def drift_projection_implementation(path: Path) -> bytes:
        content = original_read_bytes(path)
        if path.name == "portfolio_decision_tools.py":
            return content + b"\n# changed projection semantics"
        return content

    with monkeypatch.context() as implementation_drift:
        implementation_drift.setattr(Path, "read_bytes", drift_projection_implementation)
        with pytest.raises(ToolInputError, match="tool authority does not match"):
            await build_portfolio_decision_tools(ReadOnlySession(), run)  # type: ignore[arg-type]

    tools = await build_portfolio_decision_tools(ReadOnlySession(), run)  # type: ignore[arg-type]
    assert tools.names == ("evaluate_adjustment_scenario",)

"""Cross-layer proof for one admitted, executed, and readable portfolio decision."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers, settings
from nexus.agents.portfolio_decision_contracts import (
    PortfolioDecisionInputV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_execution import load_portfolio_decision_execution_authority
from nexus.agents.portfolio_decision_tools import (
    PortfolioDecisionTools,
    canonical_portfolio_evidence_manifest_hash,
)
from nexus.agents.portfolio_decision_worker import run_portfolio_decision_worker_once
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    PortfolioDecisionBudgetReservationRecord,
    PortfolioDecisionOutputRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
)
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.paper import PaperFillRecord, PaperOrderRecord
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.persistence.session import session_factory
from sqlalchemy import func, select

from tests.integration.test_portfolio_decision_worker_pg import _Client, _intent
from tests.integration.test_portfolio_scenario_api import (
    AS_OF,
    SNAPSHOT_ID,
    US_INSTRUMENT_ID,
    _seed_scenario_authority,
)
from tests.integration.test_portfolio_scenario_api import (
    _payload as scenario_payload,
)

PROMPT_ID = UUID("0198d500-1000-7000-8000-000000002104")
MODEL_ID = UUID("0198d500-1000-7000-8000-000000002105")
TOP3_ITEM_ID = UUID("0198d500-1000-7000-8000-000000002106")
HHI_ITEM_ID = UUID("0198d500-1000-7000-8000-000000002107")
NOW = AS_OF + timedelta(minutes=1)


@pytest.mark.asyncio
async def test_explicit_admission_reaches_locked_readable_decision_without_trade_side_effects(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_id = await _seed_production_authority(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    scenario_path = "/internal/v1/portfolio-scenarios/evaluate"
    scenario_body = scenario_payload(report_id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=NOW)),
        base_url="http://testserver",
    ) as client:
        scenario_response = await client.post(
            scenario_path,
            content=scenario_body,
            headers={
                **headers("POST", scenario_path, scenario_body),
                "Idempotency-Key": "decision-e2e-scenario",
            },
        )
    assert scenario_response.status_code == 200, scenario_response.text
    scenario = scenario_response.json()["data"]
    result_hash = scenario["result_hash"]
    assert len(scenario["positions"]) == 1
    private_position = scenario["positions"][0]

    create_path = "/internal/v1/portfolio-decision-runs"
    body = json.dumps(
        {
            "schema_version": "portfolio_decision_create_v1",
            "portfolio_snapshot_id": str(SNAPSHOT_ID),
            "diagnostic_report_id": str(report_id),
            "expected_scenario_result_hash": result_hash,
            "scenario_request": json.loads(scenario_body),
        },
        separators=(",", ":"),
    ).encode()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=NOW)),
        base_url="http://testserver",
    ) as client:
        created = await client.post(
            create_path,
            content=body,
            headers={**headers("POST", create_path, body), "Idempotency-Key": "decision-e2e"},
        )
    assert created.status_code == 202, created.text
    run_id = UUID(created.json()["data"]["run_id"])
    async with factory() as session:
        run = await session.get(PortfolioDecisionRunRecord, run_id)
        assert run is not None
        authority = await load_portfolio_decision_execution_authority(session, run=run)
    intent = _intent(authority)
    intent["assessment"] = "observe"
    intent["rationale"] = [
        {
            "text": "Concentration remains unresolved.",
            "citation_ids": [authority.context.citation_ids[0]],
        }
    ]
    intent["invalidation_conditions"] = [
        {
            "signal": "portfolio.scenario_objective_status",
            "operator": "eq",
            "threshold": "infeasible",
            "unit": "state",
            "citation_ids": [authority.context.citation_ids[0]],
        }
    ]
    intent["scenario_result_hash"] = result_hash
    responses = _Client(intent, factory)
    tool_calls: list[tuple[str, dict[str, object]]] = []
    production_execute = PortfolioDecisionTools.execute

    async def count_production_tool(
        tool: PortfolioDecisionTools, name: str, payload: dict[str, object]
    ) -> dict[str, object]:
        tool_calls.append((name, payload))
        return dict(await production_execute(tool, name, payload))

    monkeypatch.setattr(PortfolioDecisionTools, "execute", count_production_tool)
    worked = await run_portfolio_decision_worker_once(
        factory,
        responses_client=responses,
        worker_id="portfolio-decision-e2e",
        worker_started_at=NOW,
        process_version="test-worker-v1",
        migration_head="0049_pd_terminal_phases",
        clock=lambda: NOW,
    )

    read_path = f"/internal/v1/portfolio-decision-runs/{run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=NOW)),
        base_url="http://testserver",
    ) as client:
        loaded = await client.get(read_path, headers=headers("GET", read_path))

    assert worked is True
    assert len(responses.responses.calls) == 2
    assert tool_calls == [
        ("evaluate_adjustment_scenario", {"objective": "reduce_security_concentration"})
    ]
    model_keys = _wire_keys(responses.responses.calls)
    assert model_keys.isdisjoint(
        {"positions", "instrument_id", "symbol", "current_weight", "target_weight"}
    )
    model_values = _wire_values(responses.responses.calls)
    for private_value in (
        str(US_INSTRUMENT_ID),
        "SCENARIO",
        private_position["current_weight"],
        private_position["target_weight"],
    ):
        assert private_value not in model_values
    assert all(call["store"] is False for call in responses.responses.calls)

    assert loaded.status_code == 200
    data = loaded.json()["data"]
    assert data["status"] == "completed"
    assert data["output"]["intent"]["assessment"] == "observe"
    assert data["output"]["intent"]["limitations"] == [
        {"code": "estimated_cost", "text": "Estimated cost remains unknown."}
    ]
    assert data["expected_scenario_result_hash"] == result_hash
    assert data["output"]["intent"]["scenario_result_hash"] == result_hash
    assert len(data["citations"]) == 1
    assert data["execution_locked"] is True

    async with factory() as session:
        stored_run = await session.get(PortfolioDecisionRunRecord, run_id)
        output = await session.scalar(
            select(PortfolioDecisionOutputRecord).where(
                PortfolioDecisionOutputRecord.run_id == run_id
            )
        )
        reservation = await session.scalar(
            select(PortfolioDecisionBudgetReservationRecord).where(
                PortfolioDecisionBudgetReservationRecord.run_id == run_id
            )
        )
        side_effects = {
            model.__tablename__: await session.scalar(select(func.count()).select_from(model))
            for model in (
                OptimizationProposalGroupRecord,
                ShadowGroupRecord,
                PaperOrderRecord,
                PaperFillRecord,
            )
        }
    assert stored_run is not None and output is not None and reservation is not None
    assert stored_run.request_hash == hashlib.sha256(body).hexdigest()
    assert stored_run.input_hash == canonical_portfolio_decision_hash(
        PortfolioDecisionInputV1.model_validate(stored_run.frozen_input)
    )
    assert stored_run.evidence_hash == canonical_portfolio_evidence_manifest_hash(
        stored_run.evidence_manifest
    )
    assert stored_run.tool_result_hash == result_hash
    assert output.content_hash == canonical_portfolio_decision_hash(
        {
            "status": output.status,
            "scenario_result_hash": output.scenario_result_hash,
            "intent": output.intent,
            "details": output.details,
        }
    )
    assert reservation.state == "settled"
    assert reservation.settled_usd == Decimal("0.000250000000")
    assert reservation.pricing_version == stored_run.pricing_version
    assert side_effects == {
        "optimization_proposal_groups": 0,
        "shadow_groups": 0,
        "paper_orders": 0,
        "paper_fills": 0,
    }


async def _seed_production_authority(database_url: str) -> UUID:
    report_id = await _seed_scenario_authority(database_url)
    factory = session_factory(settings(database_url))
    prompt = "bounded portfolio decision prompt"
    async with factory() as session, session.begin():
        session.add_all(
            (
                AgentPromptVersionRecord(
                    id=PROMPT_ID,
                    role="portfolio_decision",
                    version="portfolio-decision-v1",
                    content_hash=hashlib.sha256(prompt.encode()).hexdigest(),
                    content=prompt,
                    created_at=AS_OF - timedelta(seconds=30),
                ),
                AgentModelConfigRecord(
                    id=MODEL_ID,
                    model_id="fixture-model-large-context",
                    version="fixture-portfolio-e2e-v1",
                    config_hash="9" * 64,
                    max_input_tokens=100_000,
                    max_output_tokens=1_000,
                    max_tool_calls=1,
                    input_usd_per_million=Decimal("1"),
                    output_usd_per_million=Decimal("1"),
                    pricing_available_at=NOW - timedelta(days=1),
                    pricing_expires_at=NOW + timedelta(days=1),
                    created_at=AS_OF - timedelta(seconds=30),
                ),
                PortfolioDiagnosticItemRecord(
                    id=TOP3_ITEM_ID,
                    report_id=report_id,
                    diagnostic_code="CONCENTRATION_TOP3",
                    metric_value=Decimal("1"),
                    unit="ratio",
                    reference_limit=Decimal("0.70"),
                    policy_scope="currency:USD",
                    severity="critical",
                    actionability="reduce_only",
                    unknown_reason=None,
                    created_at=AS_OF,
                ),
                PortfolioDiagnosticItemRecord(
                    id=HHI_ITEM_ID,
                    report_id=report_id,
                    diagnostic_code="CONCENTRATION_HHI",
                    metric_value=Decimal("0.68"),
                    unit="ratio",
                    reference_limit=Decimal("0.25"),
                    policy_scope="currency:USD",
                    severity="critical",
                    actionability="reduce_only",
                    unknown_reason=None,
                    created_at=AS_OF,
                ),
            )
        )
        await session.flush()
        session.add_all(
            (
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=TOP3_ITEM_ID,
                    source="fixture",
                    object_id="scenario-top3",
                    available_at=AS_OF,
                    location="fixture://scenario-top3",
                    content_hash="3" * 64,
                    created_at=AS_OF,
                ),
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=HHI_ITEM_ID,
                    source="fixture",
                    object_id="scenario-hhi",
                    available_at=AS_OF,
                    location="fixture://scenario-hhi",
                    content_hash="4" * 64,
                    created_at=AS_OF,
                ),
            )
        )
    return report_id


def _wire_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value).union(*(_wire_keys(item) for item in value.values()))
    if isinstance(value, list | tuple):
        return set().union(*(_wire_keys(item) for item in value))
    if isinstance(value, str) and value[:1] in {"{", "["}:
        try:
            return _wire_keys(json.loads(value))
        except json.JSONDecodeError:
            return set()
    return set()


def _wire_values(value: object) -> set[str]:
    if isinstance(value, dict):
        return set().union(*(_wire_values(item) for item in value.values()))
    if isinstance(value, list | tuple):
        return set().union(*(_wire_values(item) for item in value))
    if isinstance(value, str) and value[:1] in {"{", "["}:
        try:
            return _wire_values(json.loads(value))
        except json.JSONDecodeError:
            pass
    if value is None:
        return set()
    return {str(value)}

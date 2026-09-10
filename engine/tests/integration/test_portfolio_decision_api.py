"""Authenticated API contract for bounded portfolio decision runs."""

import json
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import headers, settings
from nexus.agents.budget import BudgetExhausted
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioResearchIntentV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_execution import (
    mark_first_requested,
    mark_second_requested,
    persist_portfolio_decision_terminal,
    record_tool_result,
)
from nexus.agents.portfolio_decision_tools import PortfolioCitationAuthority
from nexus.agents.responses_budget import UsageSnapshot
from nexus.api.routes.portfolio_decisions import _response
from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.main import create_engine_app
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    PortfolioDecisionOutputRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.paper import PaperFillRecord, PaperOrderRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.persistence.session import session_factory
from nexus.providers.broker.base import PortfolioSnapshot
from nexus.settings import AgentRuntimeMode
from pydantic import SecretStr
from sqlalchemy import func, select

from tests.integration.test_portfolio_decision_admission_pg import (
    NOW,
    PORTFOLIO_ID,
    REPORT_ID,
    SNAPSHOT_ID,
    _seed_authority,
)

RESULT_HASH = "e" * 64
RESPONSES_MODEL_CONFIG_ID = UUID("0198d500-1000-7000-8000-000000002091")
CODEX_MODEL_CONFIG_ID = UUID("0198d500-1000-7000-8000-000000002092")


def _app(database_url: str):
    engine_app = create_engine_app(settings(database_url))
    engine_app.state.utc_clock = lambda: NOW
    return engine_app


def _runtime_app(database_url: str, runtime_mode: AgentRuntimeMode):
    configured = settings(database_url).model_copy(
        update={
            "agent_runtime_mode": runtime_mode,
            "openai_api_key": SecretStr("fixture-key"),
            "openai_extraction_model": "gpt-5.4-mini-2026-03-17",
            "openai_reasoning_model": "gpt-5.4-mini-2026-03-17",
        }
    )
    engine_app = create_engine_app(configured)
    engine_app.state.utc_clock = lambda: NOW
    return engine_app


def _payload(*, expected_hash: str = RESULT_HASH) -> bytes:
    return json.dumps(
        {
            "schema_version": "portfolio_decision_create_v1",
            "portfolio_snapshot_id": str(SNAPSHOT_ID),
            "diagnostic_report_id": str(REPORT_ID),
            "expected_scenario_result_hash": expected_hash,
            "scenario_request": {
                "schema_version": "portfolio_scenario_request_v1",
                "portfolio_snapshot_id": str(SNAPSHOT_ID),
                "diagnostic_report_id": str(REPORT_ID),
                "as_of": "2026-09-10T07:59:00Z",
                "constraints": {
                    "schema_version": "adjustment_scenario_constraints_v1",
                    "objective": "reduce_security_concentration",
                    "max_turnover": "0.10",
                    "minimum_cash": "0.10",
                    "universe_policy": "current_holdings_only",
                    "allow_new_positions": False,
                },
            },
        },
        separators=(",", ":"),
    ).encode()


def _install_admission_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_frozen_snapshot",
        AsyncMock(
            return_value=PortfolioSnapshot(
                id=SNAPSHOT_ID,
                portfolio_id=PORTFOLIO_ID,
                status="READY",
                cash=(),
                positions=(),
                open_orders=(),
                as_of=NOW - timedelta(minutes=1),
                stale_from_snapshot_id=None,
                source_request_ids=(),
            )
        ),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.read_report",
        AsyncMock(
            return_value=PortfolioDiagnosticReport(
                portfolio_snapshot_id=SNAPSHOT_ID,
                snapshot_as_of=NOW - timedelta(minutes=1),
                trading_day=NOW.date(),
                policy_version="real_advisory_policy_v1",
                optimization_need="NO_ACTION",
                items=(),
            )
        ),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_instrument_identities",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.load_real_advisory_policy_v1",
        AsyncMock(return_value=RealAdvisoryPolicy.v1()),
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.evaluate_adjustment_scenario",
        lambda **_: type("Evaluation", (), {"result_hash": RESULT_HASH})(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runtime_mode", "expected_model_config_id"),
    (
        (AgentRuntimeMode.RESPONSES, RESPONSES_MODEL_CONFIG_ID),
        (AgentRuntimeMode.CODEX_SDK, CODEX_MODEL_CONFIG_ID),
    ),
)
async def test_post_selects_model_authority_for_the_active_runtime_before_admission(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    runtime_mode: AgentRuntimeMode,
    expected_model_config_id: UUID,
) -> None:
    from nexus.agents.codex_sdk import MODEL_ID as CODEX_MODEL_ID
    from nexus.agents.codex_versions import PORTFOLIO_DECISION_CONFIG_VERSION
    from nexus.ops.openai_local import MODEL_ID as RESPONSES_MODEL_ID

    factory = session_factory(settings(migrated_database_url))
    await _seed_authority(factory)
    async with factory() as session, session.begin():
        session.add_all(
            (
                AgentModelConfigRecord(
                    id=RESPONSES_MODEL_CONFIG_ID,
                    model_id=RESPONSES_MODEL_ID,
                    version="openai-standard-2026-09-02",
                    config_hash="1" * 64,
                    max_input_tokens=64_000,
                    max_output_tokens=8_192,
                    max_tool_calls=4,
                    input_usd_per_million="0.75",
                    output_usd_per_million="4.50",
                    pricing_available_at=NOW - timedelta(days=1),
                    pricing_expires_at=NOW + timedelta(days=1),
                    created_at=NOW - timedelta(seconds=2),
                ),
                AgentModelConfigRecord(
                    id=CODEX_MODEL_CONFIG_ID,
                    model_id=CODEX_MODEL_ID,
                    version=PORTFOLIO_DECISION_CONFIG_VERSION,
                    config_hash="2" * 64,
                    max_input_tokens=128_000,
                    max_output_tokens=16_384,
                    max_tool_calls=1,
                    input_usd_per_million="0",
                    output_usd_per_million="0",
                    pricing_available_at=NOW - timedelta(days=1),
                    pricing_expires_at=NOW + timedelta(days=1),
                    created_at=NOW - timedelta(seconds=1),
                ),
            )
        )
    _install_admission_authority(monkeypatch)
    path = "/internal/v1/portfolio-decision-runs"
    body = _payload()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_runtime_app(migrated_database_url, runtime_mode)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers={
                **headers("POST", path, body),
                "Idempotency-Key": f"runtime-model-{runtime_mode.value}",
            },
        )

    assert response.status_code == 202, response.text
    async with factory() as session:
        run = await session.scalar(select(PortfolioDecisionRunRecord))
    assert run is not None
    assert run.model_config_id == expected_model_config_id


def test_openapi_exposes_only_create_and_owned_read_routes(
    migrated_database_url: str,
) -> None:
    document = _app(migrated_database_url).openapi()

    create = document["paths"]["/internal/v1/portfolio-decision-runs"]["post"]
    read = document["paths"]["/internal/v1/portfolio-decision-runs/{run_id}"]["get"]

    assert create["operationId"] == "createPortfolioDecisionRun"
    assert create["responses"]["202"]["headers"]["Location"]["schema"] == {"type": "string"}
    assert read["operationId"] == "getPortfolioDecisionRun"
    assert "post" not in document["paths"]["/internal/v1/portfolio-decision-runs/{run_id}"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        (
            ValueError("frozen authority does not match"),
            "PORTFOLIO_DECISION_AUTHORITY_MISMATCH",
        ),
        (
            BudgetExhausted("daily budget exhausted"),
            "PORTFOLIO_DECISION_BUDGET_EXHAUSTED",
        ),
    ],
)
async def test_post_maps_authority_and_budget_failures_to_stable_conflicts(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected_code: str,
) -> None:
    path = "/internal/v1/portfolio-decision-runs"
    body = _payload()
    monkeypatch.setattr(
        "nexus.api.routes.portfolio_decisions.admit_portfolio_decision_run",
        AsyncMock(side_effect=error),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "rejected-run"},
        )

    assert response.status_code == 409
    assert response.json()["error"] == {
        "code": expected_code,
        "message": (
            "Portfolio decision budget is exhausted."
            if expected_code == "PORTFOLIO_DECISION_BUDGET_EXHAUSTED"
            else "Portfolio decision authority does not match the request."
        ),
        "request_id": response.json()["error"]["request_id"],
        "details": {},
    }


@pytest.mark.asyncio
async def test_post_replays_exact_raw_body_and_rejects_unsafe_fields(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(settings(migrated_database_url))
    await _seed_authority(factory)
    _install_admission_authority(monkeypatch)
    path = "/internal/v1/portfolio-decision-runs"
    body = _payload()
    changed_bytes = body + b" "
    unsafe = json.loads(body)
    unsafe["positions"] = [{"instrument_id": str(UUID(int=1)), "weight": "1"}]
    unsafe_body = json.dumps(unsafe, separators=(",", ":")).encode()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        first = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "decision-once"},
        )
        replay = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "decision-once"},
        )
        conflict = await client.post(
            path,
            content=changed_bytes,
            headers={
                **headers("POST", path, changed_bytes),
                "Idempotency-Key": "decision-once",
            },
        )
        rejected = await client.post(
            path,
            content=unsafe_body,
            headers={
                **headers("POST", path, unsafe_body),
                "Idempotency-Key": "unsafe-decision",
            },
        )

    assert first.status_code == replay.status_code == 202
    assert first.json()["data"]["run_id"] == replay.json()["data"]["run_id"]
    assert first.headers["Location"] == replay.headers["Location"]
    assert first.headers["Location"].startswith("/api/v1/portfolio-decision-runs/")
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert rejected.status_code == 422
    assert rejected.json()["error"]["code"] == "PORTFOLIO_DECISION_INPUT_UNSAFE"
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(PortfolioDecisionRunRecord)) == 1
        )
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
        assert {
            model.__tablename__: await session.scalar(select(func.count()).select_from(model))
            for model in (
                OptimizationProposalGroupRecord,
                ShadowGroupRecord,
                PaperOrderRecord,
                PaperFillRecord,
            )
        } == {
            "optimization_proposal_groups": 0,
            "shadow_groups": 0,
            "paper_orders": 0,
            "paper_fills": 0,
        }


@pytest.mark.asyncio
async def test_get_is_actor_scoped_and_resolves_only_intent_citations(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(settings(migrated_database_url))
    await _seed_authority(factory)
    _install_admission_authority(monkeypatch)
    create_path = "/internal/v1/portfolio-decision-runs"
    body = _payload()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        created = await client.post(
            create_path,
            content=body,
            headers={**headers("POST", create_path, body), "Idempotency-Key": "owned-run"},
        )
    assert created.status_code == 202
    run_id = UUID(created.json()["data"]["run_id"])

    async with factory() as session, session.begin():
        run = await session.get(PortfolioDecisionRunRecord, run_id)
        assert run is not None
        citation_authority = PortfolioCitationAuthority.from_manifest(
            run_id=run.id,
            manifest=run.evidence_manifest,
            evidence_hash=run.evidence_hash,
            decision_time=run.decision_time,
        )
        citation_id = citation_authority.citation_ids[0]
        def known(value: str) -> dict[str, object]:
            return {"status": "known", "value": value, "reason": None}

        metrics = {
            "security_top1": known("0.4"),
            "security_top3": known("0.8"),
            "security_hhi": known("0.22"),
            "cash_weight": known("0.1"),
            "one_way_turnover": known("0.05"),
            "estimated_cost": {
                "status": "unknown",
                "value": None,
                "reason": "COST_EVIDENCE_INCOMPLETE",
            },
        }
        projection = AgentScenarioProjectionV1.model_validate(
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
                "result_hash": RESULT_HASH,
                "citation_ids": [citation_id],
                "execution_locked": True,
            }
        )
        intent = PortfolioResearchIntentV1.model_validate(
            {
                "schema_version": "portfolio_research_intent_v1",
                "run_id": str(run.id),
                "portfolio_id": str(run.portfolio_id),
                "portfolio_snapshot_id": str(run.portfolio_snapshot_id),
                "diagnostic_report_id": str(run.diagnostic_report_id),
                "decision_time": run.decision_time.isoformat(),
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
                "scenario_result_hash": RESULT_HASH,
                "citation_ids": [citation_id],
                "execution_locked": True,
            }
        )
        await mark_first_requested(session, run_id=run.id)
        await record_tool_result(
            session,
            run_id=run.id,
            projection=projection,
            usage=UsageSnapshot(
                input_tokens=10,
                output_tokens=10,
                tool_calls=1,
                provider_calls=1,
            ),
        )
        await mark_second_requested(session, run_id=run.id)
        await persist_portfolio_decision_terminal(
            session,
            run_id=run.id,
            status="completed",
            error_code=None,
            intent=intent,
            usage=UsageSnapshot(
                input_tokens=20,
                output_tokens=20,
                tool_calls=1,
                provider_calls=2,
            ),
            usage_known=True,
        )

    read_path = f"/internal/v1/portfolio-decision-runs/{run_id}"
    owner_headers = headers("GET", read_path)
    other_headers = headers("GET", read_path, actor_id="different-local-user")
    tampered_headers = headers("GET", read_path)
    tampered_headers["X-Nexus-Actor-Id"] = "different-local-user"
    request_tampered_headers = headers("GET", read_path)
    request_tampered_headers["X-Request-Id"] = "0198f7a0-1111-7777-8111-111111111119"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        replay = await client.post(
            create_path,
            content=body,
            headers={**headers("POST", create_path, body), "Idempotency-Key": "owned-run"},
        )
        owned = await client.get(read_path, headers=owner_headers)
        hidden = await client.get(read_path, headers=other_headers)
        unauthenticated = await client.get(read_path, headers=tampered_headers)
        request_unauthenticated = await client.get(
            read_path, headers=request_tampered_headers
        )

    assert replay.status_code == 202
    assert replay.json()["data"]["status"] == "completed"
    assert replay.json()["data"]["output"]["intent"]["scenario_result_hash"] == RESULT_HASH
    assert replay.headers["Location"] == f"/api/v1/portfolio-decision-runs/{run_id}"
    assert owned.status_code == 200
    data = owned.json()["data"]
    assert data["run_id"] == str(run_id)
    assert data["expected_scenario_result_hash"] == RESULT_HASH
    assert data["output"]["intent"]["scenario_result_hash"] == RESULT_HASH
    assert data["citations"] == [
        {
            "citation_id": citation_id,
            "evidence": citation_authority.resolve(citation_id).model_dump(mode="json"),
        }
    ]
    assert data["execution_locked"] is True
    assert hidden.status_code == 404
    assert hidden.json()["error"]["code"] == "NOT_FOUND"
    assert unauthenticated.status_code == 401
    assert unauthenticated.json()["error"]["code"] == "UNAUTHENTICATED"
    assert request_unauthenticated.status_code == 401
    assert request_unauthenticated.json()["error"]["code"] == "UNAUTHENTICATED"

    async with factory() as session:
        run = await session.get(PortfolioDecisionRunRecord, run_id)
        output = await session.scalar(
            select(PortfolioDecisionOutputRecord).where(
                PortfolioDecisionOutputRecord.run_id == run_id
            )
        )
        assert run is not None and output is not None and output.intent is not None
        session.expunge(run)
        session.expunge(output)

    original_input_hash = run.input_hash
    run.input_hash = "f" * 64
    with pytest.raises(ValueError, match="authority"):
        _response(run, output)
    run.input_hash = original_input_hash

    output.intent = {**output.intent, "portfolio_id": str(UUID(int=1))}
    output.content_hash = canonical_portfolio_decision_hash(
        {
            "status": output.status,
            "scenario_result_hash": output.scenario_result_hash,
            "intent": output.intent,
            "details": output.details,
        }
    )
    with pytest.raises(ValueError, match="identity"):
        _response(run, output)

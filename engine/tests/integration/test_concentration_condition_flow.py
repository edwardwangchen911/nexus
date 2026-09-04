"""Normal diagnosis/research consumers evaluate frozen facts, not model assertions."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents import codex_staged
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.diagnostics.service import diagnose_and_persist
from nexus.ops.openai_local import prepare_database
from nexus.optimizer.formal_inputs import (
    FormalResearchOutputReference,
    freeze_formal_research,
)
from nexus.optimizer.research_context import _intent
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import select, text, update


async def setup_concentration(url, *, ratio="0.3000000000001", missing_price=False):
    settings = EngineSettings(database_url=url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, structured_synthesis=True)
    factory = session_factory(settings)
    async with factory.begin() as session:
        # Only source records are synthetic; no final D, R, output or evaluation
        # is inserted. The source has no price-history or full research authority.
        await seed_filing(session)
        account = Account(provider="fosun", status="ACTIVE", created_at=NOW)
        session.add(account)
        await session.flush()
        portfolio = Portfolio(
            account_id=account.id, name="synthetic-private-canary", created_at=NOW
        )
        session.add(portfolio)
        await session.flush()
        sync = BrokerSyncRun(
            account_id=account.id,
            portfolio_id=portfolio.id,
            trigger_kind="MANUAL",
            actor_id="test",
            route="test",
            idempotency_key="concentration",
            request_hash="a" * 64,
            status="READY",
            accepted_at=NOW,
            completed_at=NOW,
            cash_flow_quality="PASS",
            attempt_count=1,
            created_at=NOW,
        )
        session.add(sync)
        await session.flush()
        portfolio_snapshot = PortfolioSnapshotRecord(
            portfolio_id=portfolio.id,
            sync_run_id=sync.id,
            status="READY",
            as_of=NOW,
            position_hash_version="broker_position_hash_v1",
            position_hash="b" * 64,
            order_hash_version="broker_order_hash_v1",
            order_hash="c" * 64,
            source_request_ids=[],
            source_observations=[],
            created_at=NOW,
        )
        session.add(portfolio_snapshot)
        await session.flush()
        session.add(
            CashSnapshot(
                portfolio_snapshot_id=portfolio_snapshot.id,
                currency="USD",
                cash_balance=Decimal("100000000000000"),
                available_cash=Decimal("100000000000000"),
                provider_as_of=NOW,
                created_at=NOW,
            )
        )
        total = Decimal("10000000000000")
        held = total * Decimal(ratio)
        for index, value in enumerate([held, *[(total - held) / 10] * 10]):
            instrument_id = INSTRUMENT if index == 0 else UUID(int=1000 + index)
            if index:
                session.add(
                    Instrument(
                        id=instrument_id,
                        asset_type="equity",
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket="us_equity",
                        created_at=NOW,
                    )
                )
                await session.flush()
                session.add(
                    InstrumentAlias(
                        instrument_id=instrument_id,
                        alias_type="ticker",
                        symbol=f"SYN{index}",
                        valid_from=NOW.date(),
                        created_at=NOW,
                    )
                )
            session.add(
                PositionSnapshot(
                    portfolio_snapshot_id=portfolio_snapshot.id,
                    instrument_id=instrument_id,
                    currency="USD",
                    quantity=1,
                    available_quantity=1,
                    frozen_quantity=0,
                    current_price=None if missing_price and index == 10 else value,
                    provider_as_of=NOW,
                    created_at=NOW,
                )
            )
        await session.flush()
        report = await diagnose_and_persist(
            session,
            portfolio_id=portfolio.id,
            snapshot_id=portfolio_snapshot.id,
            artifact_reader=AsyncMock(side_effect=AssertionError("no market artifact is seeded")),
        )
        assert report.optimization_need == "RESEARCH_REQUIRED"
        cutoff = datetime.now(UTC)
        snapshot = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=cutoff,
            portfolio_snapshot_id=portfolio_snapshot.id,
        )
        assert snapshot is not None
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == "codex-sdk-chatgpt-v3"
            )
        )
        admission = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="observations",
            request_hash="d" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT,
                data_snapshot_id=snapshot.id,
                decision_time=cutoff,
            ),
            prompt_version_id=None,
            model_config_id=model.id,
            daily_limit_usd=Decimal("3"),
            now=cutoff,
        )
        return settings, factory, admission.run.id, portfolio_snapshot.id


async def sdk_response(payload, *, output_model):
    if payload["stage"] == "synthesis":
        response = {
            "synthesis_assessment": {
                "stance": "buy_candidate",
                "claims": [{"text": "Synthetic assertion.", "citations": [0]}],
                "reviews": [
                    {
                        "claim_id": item["claim_id"],
                        "status": "accepted",
                        "resolved": True,
                        "rationale": "Synthetic resolved review.",
                        "citations": [0],
                        "missing_evidence": [],
                        "prevented_conclusion": None,
                    }
                    for item in payload["previous_claims"]
                    if item["role"] in {"bear", "evidence_audit"}
                ],
                "review_conclusion": "sufficient",
                "uncertainties": [],
                "conflicting_facts": False,
            }
        }
    else:
        response = {
            "roles": [
                {
                    "role": role,
                    "vote": "support",
                    "conflicting_facts": False,
                    "claims": [{"text": "Synthetic source claim.", "citations": [0]}],
                    "reviewed_claim_ids": [] if payload["stage"] == "research" else ["bull:0"],
                }
                for role in payload["requested_roles"]
            ]
        }
    raw = json.dumps(response)
    output_model.model_validate_json(raw)
    return raw, UsageSnapshot(input_tokens=300, output_tokens=80)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ratio,expected,missing_price",
    [
        ("0.15", ["false", "false"], False),
        ("0.3000000000001", ["true", "true"], False),
        ("0.30", ["unknown", "unknown"], True),
    ],
)
async def test_normal_diagnose_to_v3_worker_exact_reader_and_http(
    migrated_database_url,
    monkeypatch,
    ratio,
    expected,
    missing_price,
):
    settings, factory, run_id, portfolio_id = await setup_concentration(
        migrated_database_url,
        ratio=ratio,
        missing_price=missing_price,
    )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        original_payload, original_hash = dict(snapshot.payload), snapshot.revision
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=False)
        result = await tools.execute(
            "get_portfolio_exposure",
            {
                "portfolio_snapshot_id": str(portfolio_id),
                "instrument_id": str(INSTRUMENT),
                "as_of": run.decision_time.isoformat(),
            },
        )
        context = result["items"][0]
        facts = context.get("concentration_facts")
        if missing_price:
            assert facts is None
        else:
            assert Decimal(facts["observed_ratio"]) == Decimal(ratio)
            assert facts["warning_limit"] == "0.15" and facts["critical_limit"] == "0.30"
            assert facts["weight_basis"] == "same_currency_securities_excluding_cash"
            assert facts["policy_evidence_refs"][0]["content_hash"]
        assert context["execution_locked"] is True
        assert "synthetic-private-canary" not in json.dumps(result)
    sdk = AsyncMock(side_effect=sdk_response)
    monkeypatch.setattr(codex_staged, "invoke_codex", sdk)
    assert await run_agent_worker_once(settings)
    assert sdk.await_count == 3
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        assert run.status == "completed"
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == run_id,
                AgentResearchOutputRecord.status == "completed",
            )
        )
        intent = await _intent(session, run, output, datetime.now(UTC))
        assert [item.result for item in intent.condition_evaluations] == expected
        assert intent.quality_status == intent.evidence_audit == "partial"
        assert intent.stance == "watch" and intent.exposure_action == "none"
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        assert snapshot.payload == original_payload and snapshot.revision == original_hash
        with pytest.raises(ValueError, match="OPTIMIZATION_FORMAL_RESEARCH_NOT_READY"):
            diagnostic = await session.scalar(select(PortfolioDiagnosticReportRecord))
            await freeze_formal_research(
                session,
                reference=FormalResearchOutputReference(run_id=run.id, output_id=output.id),
                portfolio_snapshot_id=portfolio_id,
                decision_cutoff_at=datetime.now(UTC),
                diagnostic_report_id=diagnostic.id,
                diagnostic_input_hash=diagnostic.diagnostic_input_hash,
                valuation_time=NOW,
            )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        path = f"/internal/v1/research-runs/{run_id}"
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        body = response.json()["data"]
        assert [item["result"] for item in body["intent"]["condition_evaluations"]] == expected
        assert body["inputs"]["portfolio_context"].get("concentration_facts") == facts
        assert body["inputs"]["filings"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        "context_hash",
        "context_removed",
        "facts_removed",
        "result",
        "refs",
        "deleted",
        "instrument_id",
        "data_snapshot_id",
        "decision_time",
    ],
)
async def test_frozen_observation_tampering_rejected_without_hiding_sec(
    migrated_database_url,
    monkeypatch,
    defect,
):
    settings, factory, run_id, _ = await setup_concentration(migrated_database_url)
    monkeypatch.setattr(codex_staged, "invoke_codex", AsyncMock(side_effect=sdk_response))
    assert await run_agent_worker_once(settings)
    async with factory.begin() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == run_id,
                AgentResearchOutputRecord.status == "completed",
            )
        )
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        payload = json.loads(json.dumps(snapshot.payload))
        if defect == "context_hash":
            payload["portfolio_context"]["content_hash"] = "0" * 64
        elif defect == "context_removed":
            payload.pop("portfolio_context")
        elif defect == "facts_removed":
            payload["portfolio_context"].pop("concentration_facts")
        broken_intent = json.loads(json.dumps(output.intent))
        if defect in {"instrument_id", "data_snapshot_id", "decision_time"}:
            broken_intent[defect] = (
                (run.decision_time + timedelta(seconds=1)).isoformat()
                if defect == "decision_time"
                else str(UUID(int=999))
            )
        elif defect == "result":
            broken_intent["condition_evaluations"][0]["result"] = "false"
        elif defect == "refs":
            original_ref = next(
                ref
                for ref in broken_intent["evidence_refs"]
                if ref["source"] != "nexus_portfolio_context"
            )
            broken_intent["condition_evaluations"][0]["condition"]["evidence_refs"] = [original_ref]
        else:
            assert broken_intent.pop("condition_evaluations")
        # Deliberate storage corruption only in this isolated throwaway DB.
        # Restore every trigger before readback; never relax a product guard.
        await session.execute(text("ALTER TABLE agent_research_outputs DISABLE TRIGGER USER"))
        await session.execute(text("ALTER TABLE data_snapshots DISABLE TRIGGER USER"))
        try:
            await session.execute(
                update(DataSnapshot)
                .where(DataSnapshot.id == snapshot.id)
                .values(
                    payload=payload,
                )
            )
            await session.execute(
                update(AgentResearchOutputRecord)
                .where(
                    AgentResearchOutputRecord.id == output.id,
                )
                .values(intent=broken_intent)
            )
        finally:
            await session.execute(text("ALTER TABLE data_snapshots ENABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE agent_research_outputs ENABLE TRIGGER USER"))
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        output = await session.get(AgentResearchOutputRecord, output.id)
        rejected = False
        try:
            await _intent(session, run, output, datetime.now(UTC))
        except ValueError as error:
            assert str(error) == "DRAFT_RESEARCH_OUTPUT_INVALID"
            rejected = True
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        path = f"/internal/v1/research-runs/{run_id}"
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        body = response.json()["data"]
        assert (rejected, body["intent"] is None) == (True, True)
        assert body["abstention_reason"] == (
            "evidence_authority_invalid"
            if defect in {"instrument_id", "data_snapshot_id", "decision_time"}
            else "condition_evaluation_invalid"
        )
        assert body["inputs"]["filings"]  # Keep independently valid SEC context.
        assert (body["inputs"]["portfolio_context"] is None) == (
            defect in {"context_hash", "context_removed", "facts_removed"}
        )

"""Normal native v3 uses synthetic SDK transport, never mocked synthesis or authority."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.research_snapshots import seed_filing
from nexus.agents import codex_sdk, codex_staged
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.optimizer.formal_inputs import FormalResearchOutputReference, freeze_formal_research
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.research.current_price_trends_storage import persist_current_price_trends
from nexus.settings import AgentRuntimeMode
from nexus.worker import run_agent_worker_once
from sqlalchemy import select, text
from unit.agents.test_codex_sdk import _restore_view

from .test_current_price_trends import INSTRUMENT, setup_trends


async def corrupt_trend(factory):
    async with factory.begin() as session:
        await session.execute(text("ALTER TABLE data_snapshots DISABLE TRIGGER USER"))
        await session.execute(
            text(
                "UPDATE data_snapshots SET payload=jsonb_set(payload, '{features,sma_20}', "
                "'\"123.000000000000\"'::jsonb) WHERE source='alpaca_current_trends'"
            )
        )
        await session.execute(text("ALTER TABLE data_snapshots ENABLE TRIGGER USER"))


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", [None, "before", "after"])
async def test_native_v3_trends_keep_partial_and_revalidate_before_and_after_model(
    corruption,
    migrated_database_url,
    tmp_path,
    monkeypatch,
):
    settings, factory, store, _, archive = await setup_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    settings = settings.model_copy(update={"agent_runtime_mode": AgentRuntimeMode.CODEX_SDK})
    await prepare_database(settings, structured_synthesis=True)
    async with factory.begin() as session:
        await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        trend_ref = await persist_current_price_trends(session, archive, store=store)
        trend_id = trend_ref.id
        cutoff = datetime.now(UTC)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=cutoff)
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == codex_sdk.STRUCTURED_CONFIG_VERSION
            )
        )
        admission = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="current-trends-v3",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=frozen.id, decision_time=cutoff
            ),
            prompt_version_id=None,
            model_config_id=model.id,
            daily_limit_usd=Decimal("3"),
            now=cutoff,
        )
        run_id = admission.run_id
    calls = []

    async def invoke(payload, *, output_model):
        calls.append(payload["stage"])
        inputs = _restore_view(payload["frozen_inputs"])
        observed = inputs["price_features"]["items"][0]
        assert observed["evidence_ref_id"] == str(trend_id)
        assert observed["features"]["sma_200"] == "100.500000000000"
        assert observed["market_price_eligible"] is False
        if payload["stage"] == "synthesis":
            if corruption == "after":
                await corrupt_trend(factory)
            response = {
                "synthesis_assessment": {
                    "stance": "watch",
                    "claims": [{"text": "Synthetic trend observation.", "citations": [0]}],
                    "reviews": [
                        {
                            "claim_id": item["claim_id"],
                            "status": "accepted",
                            "rationale": "Synthetic review.",
                            "citations": [0],
                            "resolved": True,
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
                        "claims": [{"text": "Synthetic claim.", "citations": [0]}],
                        "reviewed_claim_ids": [] if payload["stage"] == "research" else ["bull:0"],
                    }
                    for role in payload["requested_roles"]
                ]
            }
        raw = json.dumps(response)
        output_model.model_validate_json(raw)
        return raw, UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    if corruption == "before":
        await corrupt_trend(factory)
    assert await run_agent_worker_once(settings)
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .where(AgentResearchOutputRecord.run_id == run_id)
                .order_by(AgentResearchOutputRecord.sequence)
            )
        )
        final = outputs[-1]
        assert run.input_tokens == (0 if corruption == "before" else 900)
        assert run.output_tokens == (0 if corruption == "before" else 240)
        if corruption:
            assert run.status != "completed"
            assert all(output.intent is None for output in outputs)
            assert len(calls) == (0 if corruption == "before" else 3)
            assert await load_frozen_evidence(session, run, allow_fixture_evidence=False) == ()
        else:
            assert run.status == "completed" and len(calls) == 3
            assert final.intent["quality_status"] == "partial"
            assert final.intent["evidence_audit"] == "partial"
            assert final.intent["exposure_action"] == "none"
            with pytest.raises(ValueError, match="OPTIMIZATION_FORMAL_RESEARCH_NOT_READY"):
                await freeze_formal_research(
                    session,
                    FormalResearchOutputReference(run_id=run.id, output_id=final.id),
                    portfolio_snapshot_id=UUID(int=77),
                    diagnostic_report_id=UUID(int=88),
                    diagnostic_input_hash="0" * 64,
                    valuation_time=cutoff,
                    decision_cutoff_at=datetime.now(UTC),
                )

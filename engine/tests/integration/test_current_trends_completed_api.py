"""Completed native output is read only while its exact supplemental source remains valid."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import quote

import httpx
import pytest
from helpers.portfolio_api import app, headers
from helpers.research_snapshots import seed_filing
from nexus.agents import codex_sdk, codex_staged
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.research.current_price_trends_storage import persist_current_price_trends
from nexus.settings import AgentRuntimeMode
from nexus.worker import run_agent_worker_once
from sqlalchemy import select
from unit.agents.test_codex_sdk import _assessment_payload, _restore_view, _staged_batch

from .test_current_price_trends import INSTRUMENT, setup_trends
from .test_current_trends_worker import corrupt_trend


@pytest.mark.asyncio
@pytest.mark.parametrize("declares_trend", [True, False])
async def test_completed_native_api_revalidates_declared_trend_without_rewriting_history(
    migrated_database_url, tmp_path, monkeypatch, declares_trend
):
    settings, factory, store, _, archive = await setup_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    settings = settings.model_copy(update={"agent_runtime_mode": AgentRuntimeMode.CODEX_SDK})
    await prepare_database(settings, structured_synthesis=True)
    async with factory.begin() as session:
        await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        if declares_trend:
            await persist_current_price_trends(session, archive, store=store)
        cutoff = datetime.now(UTC)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=cutoff)
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == codex_sdk.STRUCTURED_CONFIG_VERSION
            )
        )
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="current-trends-completed-api",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=frozen.id, decision_time=cutoff
            ),
            prompt_version_id=None,
            model_config_id=model.id,
            daily_limit_usd=Decimal("3"),
            now=cutoff,
        )
        run_id = admitted.run_id
    calls = []

    async def invoke(payload, *, output_model):
        calls.append(payload["stage"])
        inputs = _restore_view(payload["frozen_inputs"])
        if declares_trend:
            assert inputs["price_features"]["items"][0]["source"] == "alpaca_current_trends"
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        raw = json.dumps(body)
        output_model.model_validate_json(raw)
        return raw, UsageSnapshot(input_tokens=300, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    assert await run_agent_worker_once(settings)
    assert calls == ["research", "critique", "synthesis"]
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        assert run.status == "completed"
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .where(AgentResearchOutputRecord.run_id == run_id)
                .order_by(AgentResearchOutputRecord.sequence)
            )
        )
        saved = [
            (row.id, row.content_hash, deepcopy(row.intent), deepcopy(row.payload))
            for row in outputs
        ]
        manifest, evidence_hash = deepcopy(run.evidence_manifest), run.evidence_hash
    paths = [
        f"/internal/v1/research-runs/{run_id}",
        f"/internal/v1/instruments/{INSTRUMENT}/research?as_of="
        + quote(cutoff.isoformat().replace("+00:00", "Z"), safe=""),
    ]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://test",
    ) as client:
        before = []
        for path in paths:
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            data = response.json()["data"]
            assert data["status"] == "completed" and data["intent"] is not None
            assert len(data["role_votes"]) == 8
            assert len(data["inputs"]["current_price_trends"]) == int(declares_trend)
            before.append(data)
        if not declares_trend:
            # New, unrelated optional data must not affect an older no-trend run.
            async with factory.begin() as session:
                await persist_current_price_trends(session, archive, store=store)
        await corrupt_trend(factory)
        for index, path in enumerate(paths):
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            data = response.json()["data"]
            assert len(data["inputs"]["filings"]) == 1
            assert data["inputs"]["current_price_trends"] == []
            if declares_trend:
                assert data["status"] == "abstained"
                assert data["abstention_reason"] == "evidence_authority_invalid"
                assert data["intent"] is None and data["quality_status"] is None
                assert (
                    data["evidence"] == data["role_votes"] == data["invalidation_conditions"] == []
                )
                assert data["disagreement"] is None
                assert data["stage_trace"]["status"] == "invalid"
                assert data["stage_trace"]["stages"] == []
            else:
                assert data == before[index]
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        assert run.status == "completed" and run.retry_count == 0
        assert run.evidence_manifest == manifest and run.evidence_hash == evidence_hash
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .where(AgentResearchOutputRecord.run_id == run_id)
                .order_by(AgentResearchOutputRecord.sequence)
            )
        )
        assert [(row.id, row.content_hash, row.intent, row.payload) for row in outputs] == saved
    assert calls == ["research", "critique", "synthesis"]

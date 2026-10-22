"""Stored safe observations remain readable without granting actionable authority."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.agents.committee import load_prompt_assets
from nexus.agents.synthesis import staged_intent_semantics_valid
from nexus.persistence.models.agents import (
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.portfolio.research import read_position_research
from nexus.settings import EngineSettings
from sqlalchemy import text

from engine.tests.integration.test_agent_execution_pg import (
    _INSTRUMENT_ID,
    _MODEL_ID,
    _NOW,
    _SNAPSHOT_ID,
    _seed_authority,
)
from engine.tests.unit.agents.test_execution import _synthesize

V10 = "codex-sdk-chatgpt-v10-flexible-stage-budget"
V11 = "codex-sdk-chatgpt-v11-bounded-stage-budget"


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def seed_observation(
    factory, version, defect=None, *, status="completed", usage=(None, None), sufficient=False
):
    prompt_ids = await _seed_authority(factory, model_version=version, include_base_evidence=False)
    prompts = load_prompt_assets()
    async with factory() as session, session.begin():
        evidence = ResearchEvidenceRef(
            source="nexus_portfolio_context" if defect == "context" else "sec",
            provider_object_id="synthetic-filing",
            revision="v1",
            available_at=_NOW - timedelta(days=1),
            location="memory://sha256/" + "e" * 64,
            content_hash="e" * 64,
            byte_length=1,
        )
        session.add(evidence)
        await session.flush()
        ref = {
            "source": evidence.source,
            "object_id": evidence.provider_object_id,
            "available_at": evidence.available_at.isoformat().replace("+00:00", "Z"),
            "location": evidence.location,
            "content_hash": evidence.content_hash,
        }
        manifest = [
            {
                **ref,
                "id": str(evidence.id),
                "provider_object_id": ref["object_id"],
                "revision": "v1",
            }
        ]
        del manifest[0]["object_id"]
        prompt_manifest = {
            role: {
                "id": str(prompt_ids[role]),
                "version": asset.version,
                "content_hash": asset.content_hash,
            }
            for role, asset in prompts.items()
        }
        intent = _synthesize(
            ("support",) * 6 + ("oppose", "abstain"), actionable=True, assessment_stance="watch"
        )
        snapshot_id = _SNAPSHOT_ID
        data_version = "v1"
        if sufficient:
            from nexus.persistence.models.data import DataSnapshot

            intent = _synthesize(
                ("support",) * 8,
                actionable=True,
                assessment_stance="watch",
                price_authority_valid=False,
            )
            if defect != "ordinary_snapshot":
                payload = {
                    "purpose": "research_only_v2",
                    "instrument_id": str(_INSTRUMENT_ID),
                    "as_of": _NOW.isoformat(),
                }
                if defect == "purpose":
                    payload["purpose"] = "formal"
                if defect == "snapshot_instrument":
                    payload["instrument_id"] = str(_MODEL_ID)
                revision = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
                if defect == "snapshot_hash":
                    revision = "f" * 64
                snapshot = DataSnapshot(
                    source="nexus_research_only",
                    provider_object_id=str(_INSTRUMENT_ID),
                    revision=revision,
                    observed_at=_NOW,
                    available_at=_NOW,
                    received_at=_NOW,
                    schema_version="research_only_v2",
                    payload=payload,
                )
                session.add(snapshot)
                await session.flush()
                snapshot_id, data_version = snapshot.id, snapshot.revision
        if defect == "complete":
            intent = _synthesize(("support",) * 8, actionable=True)
        doc = intent.model_dump(mode="json")

        def replace_refs(value):
            if isinstance(value, dict):
                return {
                    k: [ref] if k == "evidence_refs" else replace_refs(v) for k, v in value.items()
                }
            if isinstance(value, list):
                return [replace_refs(v) for v in value]
            return value

        doc = replace_refs(doc)
        doc.update(
            instrument_id=str(_INSTRUMENT_ID),
            data_snapshot_id=str(snapshot_id),
            decision_time=_NOW.isoformat().replace("+00:00", "Z"),
            model_version="fixture-model",
            prompt_version=prompts["orchestrator"].version,
            data_version=data_version,
        )
        if defect == "non_watch":
            doc["stance"] = "buy_candidate"
            doc["synthesis_assessment"]["stance"] = "buy_candidate"
        if defect == "review_gap":
            doc["synthesis_assessment"]["reviews"][0]["claim_id"] = "bear:2"
        if defect == "exposure":
            doc["exposure_action"] = "eligible_for_increase"
        if defect == "model_version":
            doc["model_version"] = "wrong-model"
        if defect == "prompt_version":
            doc["prompt_version"] = "wrong-prompt"
        if defect == "assessment":
            doc["synthesis_assessment"] = None
        if defect == "audit":
            doc["evidence_audit"] = "failed"
        if defect == "nested":
            doc["theses"][0]["evidence_refs"] = [{**ref, "content_hash": "f" * 64}]
        if defect == "identity":
            doc["data_snapshot_id"] = str(_MODEL_ID)
        run = AgentResearchRunRecord(
            instrument_id=_INSTRUMENT_ID,
            data_snapshot_id=snapshot_id,
            prompt_version_id=prompt_ids["orchestrator"],
            model_config_id=_MODEL_ID,
            decision_time=_NOW,
            status=status,
            input_tokens=usage[0],
            output_tokens=usage[1],
            actor_id="synthetic-readback",
            idempotency_key="one",
            schema_hash="a" * 64,
            model_hash="2" * 64,
            prompt_hash=digest(prompt_manifest),
            prompt_manifest=prompt_manifest,
            evidence_hash="0" * 64 if defect == "frozen_hash" else digest(manifest),
            evidence_manifest=manifest,
            tool_hash="b" * 64,
            input_hash="c" * 64,
            reserved_cost_usd=Decimal("1"),
            settled_cost_usd=None,
            pricing_version=version,
        )
        session.add(run)
        await session.flush()
        content = {
            "status": status,
            "role": None,
            "payload": {"quality_status": doc["quality_status"]}
            if status == "completed"
            else {"reason": "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN"}
            if status == "failed"
            else {"reason": "manual_unused_research_cancelled"},
            "intent": doc if status == "completed" else None,
        }
        output = AgentResearchOutputRecord(
            run_id=run.id,
            sequence=1,
            **content,
            content_hash="0" * 64 if defect == "hash" else digest(content),
        )
        if defect == "future_output":
            output.created_at = datetime.now(UTC) + timedelta(days=1)
        session.add(output)
        await session.flush()
        session.add(
            AgentResearchOutputEvidenceRecord(
                output_id=output.id,
                evidence_ref_id=evidence.id,
                source=evidence.source,
                provider_object_id=evidence.provider_object_id,
                revision="v1",
                available_at=evidence.available_at,
                location=evidence.location,
                content_hash=evidence.content_hash,
            )
        )
        await session.flush()
        if defect == "binding":
            # Simulate damaged historical storage only in this isolated test database.
            await session.execute(
                text("ALTER TABLE agent_research_output_evidence DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE agent_research_output_evidence SET content_hash=:digest "
                        "WHERE output_id=:output_id"
                    ),
                    {"digest": "0" * 64, "output_id": output.id},
                )
            finally:
                await session.execute(
                    text("ALTER TABLE agent_research_output_evidence ENABLE TRIGGER USER")
                )
    return run.id, doc


async def business_digest(factory):
    async with factory() as session:
        return [
            tuple(
                (
                    await session.execute(
                        text(
                            f"SELECT md5(row_to_json(t)::text) FROM {table} t "
                            "ORDER BY row_to_json(t)::text"
                        )
                    )
                ).scalars()
            )
            for table in (
                "agent_research_runs",
                "agent_research_outputs",
                "agent_research_output_evidence",
            )
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [V10, V11])
async def test_safe_observation_get_summary_and_brief_preserve_history(
    migrated_database_url, version
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, doc = await seed_observation(factory, version)
    before = await business_digest(factory)
    path = f"/internal/v1/research-runs/{run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://test"
    ) as client:
        for _ in range(2):
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            data = response.json()["data"]
            assert data["intent"] == doc
            assert data["quality_status"] == "partial" and data["abstention_reason"] is None
        brief_path = f"/internal/v1/briefs/{_NOW.date()}"
        brief = (await client.get(brief_path, headers=headers("GET", brief_path))).json()["data"]
        assert brief["candidates"] == [] and brief["holding_risks"] == []
        assert brief["evidence_completeness"]["partial"] == 1
        assert len(brief["disagreements"]) == len(brief["invalidations"]) == 1
    async with factory() as session:
        summary = (
            await read_position_research(session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC))
        )[_INSTRUMENT_ID]
        assert summary.summary == doc["theses"][0]["text"]
        assert summary.quality_status == "partial"
    from nexus.strategy.intents import ResearchIntent

    assert not staged_intent_semantics_valid(ResearchIntent.model_validate(doc), version)
    assert await business_digest(factory) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        "assessment",
        "audit",
        "nested",
        "identity",
        "hash",
        "binding",
        "model_version",
        "prompt_version",
        "frozen_hash",
        "context",
        "future_output",
    ],
)
async def test_invalid_observation_is_hidden_in_both_readers(migrated_database_url, defect):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, V11, defect)
    path = f"/internal/v1/research-runs/{run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        assert response.json()["data"]["intent"] is None
    async with factory() as session:
        result = (
            await read_position_research(session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC))
        )[_INSTRUMENT_ID]
        assert result.summary is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version", ["codex-sdk-chatgpt-v99-unknown", "codex-sdk-chatgpt-v4-actionable"]
)
async def test_unapproved_version_gets_no_new_observation_exception(migrated_database_url, version):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, version)
    path = f"/internal/v1/research-runs/{run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        assert response.json()["data"]["intent"] is None
    async with factory() as session:
        assert (
            await read_position_research(session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC))
        )[_INSTRUMENT_ID].summary is None


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [V10, V11])
async def test_existing_complete_readback_is_unchanged(migrated_database_url, version):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, doc = await seed_observation(factory, version, "complete")
    path = f"/internal/v1/research-runs/{run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://test"
    ) as client:
        data = (await client.get(path, headers=headers("GET", path))).json()["data"]
        assert data["intent"] == doc and data["quality_status"] == "complete"


@pytest.mark.asyncio
async def test_cached_authority_does_not_accept_another_frozen_hash(migrated_database_url):
    from nexus.agents.observation_readback import ObservationReadback
    from sqlalchemy import select

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, V11)
    before = await business_digest(factory)
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
        )
        bound = tuple(
            await session.scalars(
                select(AgentResearchOutputEvidenceRecord).where(
                    AgentResearchOutputEvidenceRecord.output_id == output.id
                )
            )
        )
        reader = ObservationReadback(session, read_at=datetime.now(UTC))
        with session.no_autoflush:
            assert await reader.verified(run, output, bound)
            old_prompt = run.prompt_hash
            run.prompt_hash = "0" * 64
            assert not await reader.verified(run, output, bound)
            run.prompt_hash = old_prompt
            run.evidence_hash = "0" * 64
            assert not await reader.verified(run, output, bound)
    assert await business_digest(factory) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version,expected_billing",
    [
        ("codex-sdk-chatgpt-v6-stage-time-budget", "chatgpt_subscription"),
        ("codex-sdk-chatgpt-v7-stage-capacity-candidate", "chatgpt_subscription"),
        ("codex-sdk-chatgpt-v8-stage-capacity", "chatgpt_subscription"),
        ("codex-sdk-chatgpt-v9-full-input-capacity", "chatgpt_subscription"),
        (V10, "chatgpt_subscription"),
        (V11, "chatgpt_subscription"),
        ("codex-sdk-chatgpt-v999-unrecognized", None),
        ("fixture-model-v1", None),
    ],
)
@pytest.mark.parametrize(
    "status,usage",
    [
        ("completed", (123, 45)),
        ("failed", (None, None)),
        ("abstained", (0, 0)),
    ],
)
async def test_research_readback_preserves_billing_and_usage(
    migrated_database_url, version, expected_billing, status, usage
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, version, status=status, usage=usage)
    before = await business_digest(factory)
    path = f"/internal/v1/research-runs/{run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["billing_mode"] == expected_billing
    assert (data["input_tokens"], data["output_tokens"]) == usage
    assert data["settled_cost_usd"] is None
    if status != "completed":
        assert data["status"] == status
        assert data["intent"] is None
        assert data["abstention_reason"] == (
            "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN"
            if status == "failed"
            else "manual_unused_research_cancelled"
        )
    assert await business_digest(factory) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        None,
        "ordinary_snapshot",
        "purpose",
        "snapshot_hash",
        "snapshot_instrument",
        "non_watch",
        "review_gap",
        "exposure",
        "nested",
        "hash",
        "binding",
        "frozen_hash",
        "audit",
    ],
)
async def test_sufficient_pure_observation_reads_without_formal_authority(
    migrated_database_url, defect
):
    from nexus.agents.actionable_research import (
        actionable_intent_shape_valid,
        safe_no_trade_intent_shape_valid,
    )
    from nexus.agents.research_only import require_formal_research
    from nexus.strategy.intents import ResearchIntent

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, doc = await seed_observation(factory, V11, defect, sufficient=True)
    before = await business_digest(factory)
    intent = ResearchIntent.model_validate(doc)
    assert intent.synthesis_assessment.review_conclusion == "sufficient"
    assert intent.quality_status == "partial"
    assert not actionable_intent_shape_valid(intent)
    assert not safe_no_trade_intent_shape_valid(intent)
    assert not staged_intent_semantics_valid(intent, V11)
    path = f"/internal/v1/research-runs/{run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://test"
    ) as client:
        for _ in range(2):
            data = (await client.get(path, headers=headers("GET", path))).json()["data"]
            assert data["intent"] == (doc if defect is None else None)
            if defect is None:
                assert data["quality_status"] == "partial"
                assert data["abstention_reason"] is None
    async with factory() as session:
        summary = (
            await read_position_research(session, (_INSTRUMENT_ID,), read_at=datetime.now(UTC))
        )[_INSTRUMENT_ID]
        assert summary.summary == (doc["theses"][0]["text"] if defect is None else None)
        if defect is None:
            run = await session.get(AgentResearchRunRecord, run_id)
            with pytest.raises(ValueError, match="RESEARCH_ONLY_FORMAL_CONSUMPTION_FORBIDDEN"):
                await require_formal_research(session, run)
    assert await business_digest(factory) == before

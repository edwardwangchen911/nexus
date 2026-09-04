"""An archived observation reaches the actual frozen tool without model calls."""

from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.codex_staged import StagedCodexCommittee
from nexus.agents.committee import load_prompt_assets
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import CorporateActionRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.alpaca_actions import SCHEMA, SOURCE
from nexus.research.alpaca_actions_archive import load_action_collection
from nexus.research.alpaca_actions_storage import (
    persist_action_observation,
    read_action_observations,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.research.test_alpaca_actions import archive


def synthetic_archive(tmp_path):
    receipt = NOW + timedelta(minutes=1)
    directory = archive(tmp_path, receipts={1: {"received_at": receipt.isoformat()}})
    return load_action_collection(directory, instrument_id=INSTRUMENT, symbol="TEST")


async def persist_originals(session, validated):
    document = validated.document
    for original in validated.originals:
        digest = sha256(original.content).hexdigest()
        session.add(
            DataArtifact(
                uri="memory://sha256/" + digest,
                sha256=digest,
                byte_size=len(original.content),
                mime_type="application/json",
                source="alpaca",
                schema_version=original.schema_version,
                observed_at=original.received_at,
                available_at=original.received_at,
                received_at=original.received_at,
            )
        )
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    location = "memory://sha256/" + digest
    session.add(
        DataArtifact(
            uri=location,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            source="alpaca",
            schema_version=SCHEMA,
            observed_at=document.available_at,
            available_at=document.available_at,
            received_at=document.available_at,
        )
    )
    await session.flush()
    return await persist_action_observation(session, document, location=location)


@pytest.mark.asyncio
async def test_current_action_reaches_frozen_tool_and_not_old_cutoff(
    migrated_database_url, tmp_path
):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    factory = session_factory(settings)
    validated = synthetic_archive(tmp_path)
    decision = NOW + timedelta(minutes=2)
    async with factory() as session, session.begin():
        await seed_filing(session)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        evidence = await persist_originals(session, validated)
        assert not await read_action_observations(session, instrument_id=INSTRUMENT, as_of=NOW)
        assert not await read_action_observations(
            session,
            instrument_id=UUID(int=99),
            as_of=decision,
        )
        assert not await read_action_observations(
            session,
            instrument_id=INSTRUMENT,
            as_of=decision,
            evidence_ids=(),
        )
        loaded = await read_action_observations(
            session,
            instrument_id=INSTRUMENT,
            as_of=decision,
        )
        assert loaded[evidence.id] == validated.document
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
        assert old is not None and frozen is not None and old.id != frozen.id
        assert str(evidence.id) not in old.payload["evidence_ids"]
        assert str(evidence.id) in frozen.payload["evidence_ids"]
        request = ResearchRequest(
            instrument_id=INSTRUMENT,
            data_snapshot_id=frozen.id,
            decision_time=decision,
        )
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="actions-frozen-tool",
            request_hash="b" * 64,
            request=request,
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=decision,
        )
        tools = await database_read_only_tools(session, admitted.run, allow_fixture_evidence=False)
        result = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(frozen.id),
                "as_of": decision.isoformat(),
            },
        )
        observations = [
            item["corporate_action_observation"]
            for item in result["items"]
            if "corporate_action_observation" in item
        ]
        assert len(observations) == 1
        observed = observations[0]
        assert observed["events"][0]["rate"] == "0.123456789012345678901"
        assert observed["events"][0]["currency"] == ""
        assert observed["events"][0]["process_date"] is None
        assert observed["available_at"] == "2026-09-02T04:01:00Z"
        assert observed["source_completeness"] == "unknown"
        assert observed["ledger_eligible"] is False
        assert observed["historical_backtest_eligible"] is False
        assert observed["execution_locked"] is True
        committee = StagedCodexCommittee(prompts=load_prompt_assets(), tools=tools)
        inputs = await committee._inputs(request)
        assert inputs["security_snapshot"] == result
        assert inputs["price_close"]["status"] == "unavailable"
        assert await session.scalar(select(func.count()).select_from(CorporateActionRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["bad_payload", "late_document", "missing_canonical"])
async def test_invalid_optional_action_does_not_hide_valid_sec(
    migrated_database_url,
    tmp_path,
    invalid,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    validated = synthetic_archive(tmp_path)
    document = validated.document
    decision = NOW + timedelta(minutes=2)
    async with factory() as session, session.begin():
        sec = await seed_filing(session)
        if invalid == "missing_canonical":
            content = document.canonical_bytes()
            digest = sha256(content).hexdigest()
            session.add(
                ResearchEvidenceRef(
                    source=SOURCE,
                    provider_object_id=document.provider_object_id,
                    revision=digest,
                    available_at=document.available_at,
                    content_hash=digest,
                    byte_length=len(content),
                    location="memory://sha256/" + digest,
                )
            )
            session.add(
                DataSnapshot(
                    source=SOURCE,
                    provider_object_id=document.provider_object_id,
                    revision=digest,
                    schema_version=SCHEMA,
                    payload=document.model_dump(mode="json"),
                    observed_at=document.available_at,
                    available_at=document.available_at,
                    received_at=document.available_at,
                )
            )
        else:
            # A separately imported later invalid observation cannot mask existing SEC facts.
            payload = document.model_dump(mode="json")
            if invalid == "bad_payload":
                payload["events"] = "invalid"
            session.add(
                DataSnapshot(
                    source=SOURCE,
                    provider_object_id=document.provider_object_id,
                    revision="e" * 64,
                    schema_version=SCHEMA,
                    payload=payload,
                    observed_at=document.available_at,
                    available_at=document.available_at,
                    received_at=document.available_at,
                    created_at=decision + timedelta(seconds=1)
                    if invalid == "late_document"
                    else NOW,
                )
            )
            session.add(
                ResearchEvidenceRef(
                    source=SOURCE,
                    provider_object_id=document.provider_object_id,
                    revision="e" * 64,
                    available_at=document.available_at,
                    content_hash="e" * 64,
                    byte_length=100,
                    location="memory://sha256/" + "e" * 64,
                )
            )
        await session.flush()
        assert not await read_action_observations(
            session,
            instrument_id=INSTRUMENT,
            as_of=decision,
        )
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
        assert frozen.payload["evidence_ids"] == [str(sec.id)]

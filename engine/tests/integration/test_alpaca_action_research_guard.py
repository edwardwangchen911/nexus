"""Current action observations acquire research ownership, never ledger authority."""

import asyncio
import hashlib
import json
from datetime import timedelta
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine


async def seed_observation(session, invalid=None):
    await seed_filing(session)
    receipt = NOW - timedelta(minutes=1)
    payload = {
        "schema_version": "alpaca_action_observation_v1",
        "instrument_id": str(INSTRUMENT),
        "symbol": "TEST",
        "query_start": "2026-08-01",
        "query_end": "2026-08-31",
        "available_at": receipt.isoformat(),
        "manifest_content_hash": "1" * 64,
        "manifest_byte_length": 20,
        "pages": [
            {
                "page_number": 1,
                "raw_content_hash": "2" * 64,
                "raw_byte_length": 30,
                "receipt_content_hash": "3" * 64,
                "receipt_byte_length": 40,
                "received_at": receipt.isoformat(),
            }
        ],
        "events": [],
        "total_event_count": 0,
        "unbound_event_count": 0,
        "source_completeness": "unknown",
        "historical_backtest_eligible": False,
        "ledger_eligible": False,
        "revision_semantics": "local_content_hash",
        "execution_locked": True,
        "pagination_exhausted": True,
    }
    if invalid == "wrong_instrument":
        payload["instrument_id"] = str(uuid4())
    if invalid == "ledger_eligible":
        payload["ledger_eligible"] = True
    if invalid == "historical_eligible":
        payload["historical_backtest_eligible"] = True
    if invalid == "empty_pages":
        payload["pages"] = []
    if invalid == "unknown_pagination":
        payload["pagination_exhausted"] = False
    if invalid == "receipt_after_available":
        payload["pages"][0]["received_at"] = NOW.isoformat()
    for kind, digest, size in (
        ("manifest", "1" * 64, 20),
        ("raw", "2" * 64, 30),
        ("receipt", "3" * 64, 40),
    ):
        if invalid == "missing_" + kind:
            continue
        arrival = NOW + timedelta(seconds=1) if invalid == "future_raw" else receipt
        session.add(
            DataArtifact(
                uri="memory://sha256/" + digest,
                sha256=digest,
                byte_size=size + (1 if invalid == "wrong_size" and kind == "raw" else 0),
                mime_type="application/json",
                schema_version=f"alpaca_actions_{kind}_v1",
                source="other" if invalid == "wrong_source" and kind == "raw" else "alpaca",
                observed_at=NOW + timedelta(seconds=1)
                if invalid == "future_observation"
                else arrival,
                available_at=arrival,
                received_at=arrival,
                created_at=NOW + timedelta(seconds=1) if invalid == "late_artifact" else receipt,
            )
        )
    content = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(content).hexdigest()
    if invalid != "missing_canonical":
        session.add(
            DataArtifact(
                uri="memory://sha256/" + digest,
                sha256=digest,
                byte_size=len(content),
                mime_type="application/json",
                schema_version="alpaca_action_observation_v1",
                source="alpaca",
                observed_at=receipt,
                available_at=receipt,
                received_at=receipt,
            )
        )
    common = dict(
        source="alpaca_action_observation",
        provider_object_id=f"actions:{INSTRUMENT}:2026-08-01:2026-08-31",
        revision=digest,
        available_at=receipt,
    )
    evidence = ResearchEvidenceRef(
        **common,
        location="memory://sha256/" + digest,
        content_hash=digest,
        byte_length=len(content),
    )
    session.add(evidence)
    session.add(
        DataSnapshot(
            **common,
            observed_at=receipt,
            received_at=receipt,
            schema_version="alpaca_action_observation_v1",
            payload=payload,
            created_at=NOW + timedelta(seconds=1) if invalid == "late_document" else receipt,
        )
    )
    await session.flush()
    parent = DataSnapshot(
        source="nexus_research",
        provider_object_id=str(INSTRUMENT),
        revision="a" * 64,
        observed_at=NOW,
        available_at=NOW,
        received_at=NOW,
        schema_version="research_filings_v1",
        payload={
            "instrument_id": str(INSTRUMENT),
            "as_of": NOW.isoformat(),
            "evidence_ids": [str(evidence.id)],
            "scope": "persisted_research_observations",
        },
    )
    session.add(parent)
    await session.flush()
    return parent, evidence


def binding(parent, evidence):
    return AgentDataSnapshotEvidenceRecord(
        data_snapshot_id=parent.id,
        instrument_id=INSTRUMENT,
        evidence_ref_id=evidence.id,
        authority_kind="research",
        selected_at=None,
        source=evidence.source,
        provider_object_id=evidence.provider_object_id,
        revision=evidence.revision,
        available_at=evidence.available_at,
        location=evidence.location,
        content_hash=evidence.content_hash,
    )


@pytest.mark.asyncio
async def test_complete_current_archive_can_bind_and_prevents_unsafe_downgrade(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        parent, evidence = await seed_observation(session)
        session.add(binding(parent, evidence))
        await session.flush()
    with pytest.raises(RuntimeError, match="frozen action observation"):
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0040_formal_decision_cutoff"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "missing_raw",
        "missing_receipt",
        "missing_manifest",
        "missing_canonical",
        "wrong_source",
        "wrong_size",
        "future_raw",
        "future_observation",
        "late_artifact",
        "late_document",
        "wrong_instrument",
        "ledger_eligible",
        "historical_eligible",
        "empty_pages",
        "unknown_pagination",
        "receipt_after_available",
    ],
)
async def test_guard_rejects_missing_provenance_or_promoted_current_observation(
    migrated_database_url,
    invalid,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        parent, evidence = await seed_observation(session, invalid)
        with pytest.raises(IntegrityError, match="not owned"):
            async with session.begin_nested():
                session.add(binding(parent, evidence))
                await session.flush()


@pytest.mark.asyncio
async def test_empty_migration_roundtrip_restores_exact_existing_guard(
    temporary_database_url,
    monkeypatch,
):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    engine = create_async_engine(temporary_database_url)
    query = text(
        "SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
    )
    try:
        await asyncio.to_thread(command.upgrade, config, "0040_formal_decision_cutoff")
        async with engine.connect() as conn:
            before = await conn.scalar(query)
        await asyncio.to_thread(command.upgrade, config, "head")
        async with engine.connect() as conn:
            assert await conn.scalar(query) != before
            assert await conn.scalar(
                text(
                    "SELECT proconfig FROM pg_proc WHERE oid="
                    "'public.nexus_validate_agent_snapshot_evidence()'::regprocedure"
                )
            ) == [
                "search_path=pg_catalog, public, pg_temp",
            ]
        await asyncio.to_thread(command.downgrade, config, "0040_formal_decision_cutoff")
        async with engine.connect() as conn:
            assert await conn.scalar(query) == before
        await asyncio.to_thread(command.upgrade, config, "head")
    finally:
        await engine.dispose()

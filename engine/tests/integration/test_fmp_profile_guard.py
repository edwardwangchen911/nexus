"""Profile provenance can bind as context without acquiring market qualifications."""

import asyncio
import hashlib
import json
from datetime import timedelta
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from .test_alpaca_action_research_guard import binding


async def seed_profile(session, invalid=None):
    await seed_filing(session)
    receipt = NOW - timedelta(minutes=1)
    payload = {
        "instrument_id": str(INSTRUMENT),
        "symbol": "TEST",
        "company_name": "Synthetic Test",
        "description": None,
        "cik": None,
        "isin": None,
        "cusip": None,
        "industry": "Synthetic industry",
        "sector": None,
        "source_url": "https://financialmodelingprep.com/stable/profile?symbol=TEST",
        "received_at": receipt.isoformat(),
        "available_at": receipt.isoformat(),
        "raw_content_hash": "1" * 64,
        "raw_byte_length": 30,
        "receipt_content_hash": "2" * 64,
        "receipt_byte_length": 40,
        "source_published_at": None,
        "valid_until": None,
        "sector_taxonomy": None,
        "revision_semantics": "local_content_hash",
        "sector_risk_eligible": False,
        "financial_fact_eligible": False,
        "price_authority_eligible": False,
        "historical_backtest_eligible": False,
        "execution_locked": True,
    }
    if invalid == "wrong_instrument":
        payload["instrument_id"] = str(uuid4())
    if invalid in {
        "sector_risk_eligible",
        "financial_fact_eligible",
        "price_authority_eligible",
        "historical_backtest_eligible",
    }:
        payload[invalid] = True
    if invalid == "execution_unlocked":
        payload["execution_locked"] = False
    if invalid == "claimed_source_time":
        payload["source_published_at"] = receipt.isoformat()
    if invalid == "wrong_receipt_clock":
        payload["received_at"] = NOW.isoformat()
    for kind, digest, size in (("raw", "1" * 64, 30), ("receipt", "2" * 64, 40)):
        if invalid == "missing_" + kind:
            continue
        future = NOW + timedelta(seconds=1)
        session.add(
            DataArtifact(
                uri="memory://sha256/" + digest,
                sha256=digest,
                byte_size=size + (1 if invalid == "wrong_size" and kind == "raw" else 0),
                mime_type="application/json",
                schema_version=f"fmp_profile_{kind}_v1",
                source="other" if invalid == "wrong_source" and kind == "raw" else "fmp",
                observed_at=future if invalid == "future_raw" else receipt,
                available_at=receipt,
                received_at=receipt,
                created_at=future if invalid == "late_artifact" else receipt,
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
                schema_version="fmp_company_profile_v1",
                source="fmp",
                observed_at=receipt,
                available_at=receipt,
                received_at=receipt,
            )
        )
    common = dict(
        source="fmp_company_profile",
        provider_object_id=f"profile:{INSTRUMENT}:TEST",
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
            schema_version="fmp_company_profile_v1",
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


@pytest.mark.asyncio
async def test_profile_with_complete_provenance_can_bind_and_blocks_unsafe_downgrade(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        parent, evidence = await seed_profile(session)
        session.add(binding(parent, evidence))
        await session.flush()
    with pytest.raises(RuntimeError, match="frozen FMP profile"):
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0041_action_observations"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "missing_raw",
        "missing_receipt",
        "missing_canonical",
        "wrong_source",
        "wrong_size",
        "future_raw",
        "late_artifact",
        "late_document",
        "wrong_instrument",
        "sector_risk_eligible",
        "financial_fact_eligible",
        "price_authority_eligible",
        "historical_backtest_eligible",
        "execution_unlocked",
        "claimed_source_time",
        "wrong_receipt_clock",
    ],
)
async def test_profile_guard_rejects_bad_provenance_and_promotion(migrated_database_url, invalid):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        parent, evidence = await seed_profile(session, invalid)
        with pytest.raises(IntegrityError, match="not owned"):
            async with session.begin_nested():
                session.add(binding(parent, evidence))
                await session.flush()


@pytest.mark.asyncio
async def test_empty_fmp_migration_roundtrip_preserves_exact_previous_guard(
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
        await asyncio.to_thread(command.upgrade, config, "0041_action_observations")
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
        await asyncio.to_thread(command.downgrade, config, "0041_action_observations")
        async with engine.connect() as conn:
            assert await conn.scalar(query) == before
        await asyncio.to_thread(command.upgrade, config, "head")
    finally:
        await engine.dispose()

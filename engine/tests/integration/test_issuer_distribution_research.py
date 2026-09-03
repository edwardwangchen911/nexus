"""Synthetic official-source shapes reach the existing frozen security tool."""

import asyncio
import hashlib
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from integration.test_research_snapshot_preparation import INSTRUMENT, NOW, seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.ssga_distributions import (
    parse_distribution,
    persist_distribution,
    read_distributions,
)
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from engine.tests.unit.research.test_ssga_distributions import identity, workbook


@pytest.mark.asyncio
async def test_issuer_fact_reaches_new_frozen_tool_without_leaking_into_old_cutoff(
    migrated_database_url,
):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    factory = session_factory(settings)
    receipt = NOW + timedelta(minutes=1)
    decision = NOW + timedelta(minutes=2)
    document = parse_distribution(
        raw=workbook(),
        identity_html=identity(),
        instrument_id=INSTRUMENT,
        symbol="TEST",
        identity_url="https://www.ssga.com/us/en/individual/etfs/synthetic-fund-test",
        received_at=receipt,
    )
    location = "memory://sha256/" + hashlib.sha256(document.canonical_bytes()).hexdigest()
    async with factory() as session, session.begin():
        await seed_filing(session)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        for digest, size in (
            (document.raw_content_hash, document.raw_byte_length),
            (document.identity_content_hash, document.identity_byte_length),
        ):
            session.add(
                DataArtifact(
                    uri="memory://sha256/" + digest,
                    sha256=digest,
                    byte_size=size,
                    mime_type="application/octet-stream",
                    schema_version="issuer_raw_v1",
                    source="ssga",
                    observed_at=receipt,
                    available_at=receipt,
                    received_at=receipt,
                )
            )
        await session.flush()
        evidence = await persist_distribution(session, document, location=location)
        replay = await persist_distribution(session, document, location=location)
        assert replay.id == evidence.id
        assert not await read_distributions(session, instrument_id=INSTRUMENT, as_of=NOW)
        assert not await read_distributions(session, instrument_id=UUID(int=99), as_of=decision)
        assert not await read_distributions(
            session,
            instrument_id=INSTRUMENT,
            as_of=decision,
            evidence_ids=(),
        )
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
        assert old is not None and frozen is not None and old.id != frozen.id
        assert old.payload["scope"] == "persisted_sec_filings"
        assert str(evidence.id) not in old.payload["evidence_ids"]
        assert str(evidence.id) in frozen.payload["evidence_ids"]
        bindings = list(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == frozen.id,
                )
            )
        )
        assert evidence.id in {row.evidence_ref_id for row in bindings}
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="issuer-current-research",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT,
                data_snapshot_id=frozen.id,
                decision_time=decision,
            ),
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
        facts = [
            item["issuer_distribution"] for item in result["items"] if "issuer_distribution" in item
        ]
        assert len(facts) == 1
        assert facts[0]["dividend_per_share"] == "0.125000"
        assert facts[0]["short_term_capital_gain"] is None
        assert facts[0]["historical_backtest_eligible"] is False
        assert facts[0]["available_at"] == "2026-09-02T04:01:00Z"
    with pytest.raises(RuntimeError, match="frozen issuer evidence"):
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0034_capability_versions"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["missing_raw", "future_raw", "wrong_instrument"])
async def test_database_guard_rejects_unbound_or_future_issuer_observations(
    migrated_database_url,
    invalid,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    document = parse_distribution(
        raw=workbook(),
        identity_html=identity(),
        instrument_id=INSTRUMENT,
        symbol="TEST",
        identity_url="https://www.ssga.com/us/en/individual/etfs/synthetic-fund-test",
        received_at=NOW,
    )
    async with factory() as session, session.begin():
        await seed_filing(session)
        parent = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        evidence = await persist_distribution(
            session,
            document,
            location="memory://sha256/" + hashlib.sha256(document.canonical_bytes()).hexdigest(),
        )
        if invalid != "missing_raw":
            for digest, size in (
                (document.raw_content_hash, document.raw_byte_length),
                (document.identity_content_hash, document.identity_byte_length),
            ):
                received = NOW + timedelta(seconds=1) if invalid == "future_raw" else NOW
                session.add(
                    DataArtifact(
                        uri="memory://sha256/" + digest,
                        sha256=digest,
                        byte_size=size,
                        mime_type="application/octet-stream",
                        schema_version="issuer_raw_v1",
                        source="ssga",
                        observed_at=received,
                        available_at=received,
                        received_at=received,
                    )
                )
        if invalid == "wrong_instrument":
            from nexus.persistence.models.market import Instrument

            session.add(
                Instrument(
                    id=UUID(int=99),
                    asset_type="equity",
                    exchange="XNAS",
                    currency="USD",
                    risk_bucket="us_equity",
                )
            )
        await session.flush()
        with pytest.raises(IntegrityError, match="not owned"):
            async with session.begin_nested():
                session.add(
                    AgentDataSnapshotEvidenceRecord(
                        data_snapshot_id=parent.id,
                        instrument_id=UUID(int=99) if invalid == "wrong_instrument" else INSTRUMENT,
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
                )
                await session.flush()


@pytest.mark.asyncio
async def test_migration_roundtrip_preserves_prior_guard_and_search_path(temporary_database_url):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = temporary_database_url
    config = Config("engine/alembic.ini")
    engine = create_async_engine(temporary_database_url)
    query = text(
        "SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
    )
    try:
        await asyncio.to_thread(command.upgrade, config, "0034_capability_versions")
        async with engine.connect() as connection:
            before = await connection.scalar(query)
        await asyncio.to_thread(command.upgrade, config, "head")
        async with engine.connect() as connection:
            assert await connection.scalar(
                text(
                    "SELECT proconfig FROM pg_proc WHERE "
                    "oid='public.nexus_validate_agent_snapshot_evidence()'::regprocedure"
                )
            ) == ["search_path=pg_catalog, public, pg_temp"]
        await asyncio.to_thread(command.downgrade, config, "0034_capability_versions")
        async with engine.connect() as connection:
            assert await connection.scalar(query) == before
    finally:
        await engine.dispose()
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", [None, "orphan", "hash", "uri", "schema"])
async def test_local_import_archives_originals_and_reads_same_instrument_without_model(
    migrated_database_url,
    monkeypatch,
    corruption,
):
    from nexus.ops import issuer_distribution_local
    from nexus.research.sec_disclosures import SecDisclosure, SecFundIdentity, persist_disclosure
    from nexus.research.ssga_distributions import SOURCE_URL

    class LocalObjectStore:
        def __init__(self):
            self.objects = {}

        async def put_if_absent(self, *, key, content):
            assert self.objects.get(key, content) == content
            self.objects[key] = content
            return f"s3://synthetic/provider/market/{key}?versionId=v1"

        async def read_raw(self, key, *, uri):
            assert uri == f"s3://synthetic/provider/market/{key}?versionId=v1"
            return self.objects[key]

    store = LocalObjectStore()
    monkeypatch.setattr(issuer_distribution_local, "S3ProviderEvidenceStore", lambda _: store)
    settings = EngineSettings(
        database_url=migrated_database_url, object_store_endpoint="http://localhost:1"
    )
    async with session_factory(settings)() as session, session.begin():
        await seed_filing(session)
        doc = SecDisclosure(
            instrument_id=INSTRUMENT,
            form="497K",
            accession_number="0000000001-26-000002",
            source_url="https://www.sec.gov/Archives/edgar/data/1/000000000126000002/synthetic.htm",
            filed_at=NOW,
            accepted_at=NOW,
            raw_content_hash="b" * 64,
            excerpt="Synthetic share-class identity document for TEST fund.",
            truncated=False,
            fund_identity=SecFundIdentity(
                cik="0000000001",
                series_id="S000000001",
                class_id="C000000001",
                symbol="TEST",
                filing_index_hash="c" * 64,
            ),
        )
        digest = hashlib.sha256(doc.canonical_bytes()).hexdigest()
        if corruption in {"orphan", "hash"}:
            payload = doc.model_dump(mode="json")
            payload["excerpt"] = "Synthetic altered excerpt without matching canonical hash."
            session.add(
                DataSnapshot(
                    source="sec",
                    provider_object_id=doc.accession_number,
                    revision=digest,
                    observed_at=NOW,
                    available_at=NOW,
                    received_at=NOW,
                    schema_version="sec_disclosure_v1",
                    payload=payload,
                )
            )
            if corruption == "hash":
                evidence = ResearchEvidenceRef(
                    source="sec",
                    provider_object_id=doc.accession_number,
                    revision=digest,
                    available_at=NOW,
                    content_hash=digest,
                    byte_length=len(doc.canonical_bytes()),
                    location="memory://sha256/" + digest,
                )
                session.add(evidence)
                await session.flush()
                session.add(
                    Filing(
                        instrument_id=INSTRUMENT,
                        evidence_ref_id=evidence.id,
                        source="sec",
                        provider_object_id=doc.accession_number,
                        revision=digest,
                        accession_number=doc.accession_number,
                        form="497K",
                        filed_at=NOW,
                        accepted_at=NOW,
                    )
                )
        else:
            await persist_disclosure(
                session,
                doc,
                location="memory://sha256/" + digest,
                received_at=NOW,
            )
    args = dict(
        instrument_id=INSTRUMENT,
        workbook=(workbook(), SOURCE_URL, NOW),
        identity=(
            identity(),
            "https://www.ssga.com/us/en/individual/etfs/synthetic-fund-test",
            NOW,
        ),
    )
    async with session_factory(settings)() as session, session.begin():
        if corruption in {"uri", "schema"}:
            raw = args["workbook"][0]
            digest = hashlib.sha256(raw).hexdigest()
            session.add(
                DataArtifact(
                    uri=(
                        "s3://synthetic/wrong?versionId=v1"
                        if corruption == "uri"
                        else f"s3://synthetic/provider/market/raw/ssga/distributions/{digest}.bin?versionId=v1"
                    ),
                    sha256=digest,
                    byte_size=len(raw),
                    mime_type="application/octet-stream",
                    schema_version="wrong_v1" if corruption == "schema" else "issuer_raw_v1",
                    source="ssga",
                    observed_at=NOW,
                    available_at=NOW,
                    received_at=NOW,
                )
            )
    if corruption:
        error = (
            "ISSUER_SEC_SHARE_IDENTITY_REQUIRED"
            if corruption in {"orphan", "hash"}
            else "ISSUER_ORIGINAL_BINDING_CONFLICT"
        )
        with pytest.raises(ValueError, match=error):
            await issuer_distribution_local.import_originals(settings, **args)
        return
    result = await issuer_distribution_local.import_originals(settings, **args)
    replay = await issuer_distribution_local.import_originals(settings, **args)
    assert result["evidence_id"] == replay["evidence_id"]
    assert result["frozen_readback_verified"] is True and result["model_called"] is False
    assert len(store.objects) == 3

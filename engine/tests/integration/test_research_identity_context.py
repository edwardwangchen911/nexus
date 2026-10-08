"""Synthetic normal identity import → frozen research → tool; no model/provider calls."""

import asyncio
import importlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from alembic import command
from alembic.config import Config
from helpers.current_identity import archives
from helpers.research_snapshots import seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.openai_local import prepare_database
from nexus.ops.research_identity_binding_local import register_identity
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from unit.market.test_object_store import FakeS3Client


async def seed(factory, symbol="AAPL"):
    identifier = uuid4()
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=identifier,
                asset_type="common_stock",
                exchange="XNAS",
                currency="USD",
                risk_bucket="EQUITY",
                tick_size=None,
            )
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=identifier,
                symbol=symbol,
                alias_type="ticker",
                valid_from=datetime.now(UTC).date() - timedelta(days=3),
                valid_to=None,
            )
        )
    return identifier


def source_archive(path):
    path.mkdir(mode=0o700)
    # Synthetic receipt evidence is comfortably before admission; do not test
    # contract correctness against host/container submillisecond clock skew.
    received = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
    documents = {
        "directory": (
            {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[123456, "Synthetic", "AAPL", "NYSE"]],
            },
            "https://www.sec.gov/files/company_tickers_exchange.json",
        ),
        "AAPL-submissions": (
            {"cik": "123456", "tickers": ["AAPL"], "exchanges": ["NYSE"]},
            "https://data.sec.gov/submissions/CIK0000123456.json",
        ),
    }
    for name, (document, url) in documents.items():
        raw = json.dumps(document).encode()
        for filename, content in (
            (name + ".json", raw),
            (
                name + "-receipt.json",
                json.dumps(
                    {
                        "schema": "sec_identity_source_receipt_v1",
                        "source_url": url,
                        "http_status": 200,
                        "received_at": received,
                        "sha256": sha256(raw).hexdigest(),
                        "byte_length": len(raw),
                    }
                ).encode(),
            ),
        ):
            saved = path / filename
            saved.write_bytes(content)
            saved.chmod(0o600)
    saved = path / "result.json"
    saved.write_text(
        json.dumps(
            {
                "status": "archived",
                "requests": 2,
                "authority_registered": False,
                "execution_locked": True,
            }
        )
    )
    saved.chmod(0o600)


@pytest.mark.asyncio
async def test_exact_new_frozen_identity_and_old_run_unchanged(migrated_database_url, tmp_path):
    module = importlib.import_module("nexus.research.current_identity_storage")
    settings = EngineSettings(
        database_url=migrated_database_url, agent_runtime_mode=AgentRuntimeMode.CODEX_SDK
    ).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(settings, client=client)
    try:
        instrument = await seed(factory)
        fm, sc = archives(tmp_path / "base")
        ops = importlib.import_module("nexus.ops.instrument_identity_local")
        base = await ops.revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        await prepare_database(settings)
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            await seed_filing(session, instrument_id=instrument, seed_instrument=False)
            old = await prepare_research_snapshot(session, instrument, as_of=datetime.now(UTC))
            old_id, old_payload, old_revision = old.id, dict(old.payload), old.revision
        source_archive(tmp_path / "current")
        registered = await register_identity(
            settings,
            identity_snapshot_id=base.snapshot_id,
            collection_dir=tmp_path / "current",
            store=store,
        )
        assert registered["status"] == "registered"
        assert registered["execution_locked"] is True
        async with factory() as session, session.begin():
            ref = await module.import_identity_binding(
                session,
                store,
                identity_snapshot_id=base.snapshot_id,
                collection_dir=tmp_path / "current",
            )
            replay = await module.import_identity_binding(
                session,
                store,
                identity_snapshot_id=base.snapshot_id,
                collection_dir=tmp_path / "current",
            )
            assert replay.id == ref.id
            assert registered["evidence_id"] == str(ref.id)
            ref_id = ref.id
        decision = datetime.now(UTC)
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            frozen = await prepare_research_snapshot(session, instrument, as_of=decision)
            assert str(ref_id) in frozen.payload["evidence_ids"]
            old = await session.get(DataSnapshot, old_id)
            assert (old.payload, old.revision) == (old_payload, old_revision)
            assert str(ref_id) not in old.payload["evidence_ids"]
            # Available current evidence and its immutable snapshot binding are
            # different readiness facts. Old snapshots must not become ready.
            for diagnostic_snapshot, expected_state in ((old, "missing"), (frozen, "ready")):
                diagnostic = await module.inspect_identity_binding_snapshot(
                    session,
                    instrument_id=instrument,
                    snapshot=diagnostic_snapshot,
                    as_of=decision,
                    decision_session=decision.astimezone(ZoneInfo("America/New_York")).date(),
                )
                assert diagnostic["evidence_status"] == "ready"
                assert diagnostic["snapshot_status"] == expected_state
            tomorrow = await module.inspect_identity_binding_snapshot(
                session,
                instrument_id=instrument,
                snapshot=frozen,
                as_of=decision + timedelta(days=1),
                decision_session=(decision + timedelta(days=1))
                .astimezone(ZoneInfo("America/New_York"))
                .date(),
            )
            assert tomorrow["evidence_status"] == "unknown"
            assert tomorrow["snapshot_status"] == "missing"
            next_session = await module.inspect_identity_binding_snapshot(
                session,
                instrument_id=instrument,
                snapshot=frozen,
                as_of=decision,
                decision_session=(decision + timedelta(days=1))
                .astimezone(ZoneInfo("America/New_York"))
                .date(),
            )
            assert next_session["evidence_status"] == "unknown"
            assert next_session["snapshot_status"] == "missing"
            for snapshot, expected in ((old, 0), (frozen, 1)):
                cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
                admission = await admit_research_run(
                    session,
                    actor_id="test",
                    route="/internal/v1/research-runs",
                    idempotency_key=f"identity-{expected}",
                    request_hash=str(expected) * 64,
                    request=ResearchRequest(
                        instrument_id=instrument, data_snapshot_id=snapshot.id, decision_time=cutoff
                    ),
                    prompt_version_id=None,
                    model_config_id=None,
                    daily_limit_usd=Decimal("3"),
                    now=cutoff,
                )
                tools = await database_read_only_tools(
                    session, admission.run, allow_fixture_evidence=True
                )
                result = await tools.execute(
                    "get_security_snapshot",
                    {
                        "instrument_id": str(instrument),
                        "data_snapshot_id": str(snapshot.id),
                        "as_of": cutoff.isoformat(),
                    },
                )
                bindings = [
                    row["research_identity_binding"]
                    for row in result["items"]
                    if "research_identity_binding" in row
                ]
                assert len(bindings) == expected
                if bindings:
                    assert bindings[0]["issuer_cik"] == "0000123456"
                    assert bindings[0]["execution_locked"] is True
            assert await load_frozen_evidence(session, admission.run, allow_fixture_evidence=False)
            assert not await module.read_identity_bindings(
                session,
                instrument_id=instrument,
                as_of=cutoff,
                evidence_ids=(uuid4(),),
            )
            assert not await module.read_identity_bindings(
                session,
                instrument_id=instrument,
                as_of=datetime.fromisoformat(old.payload["as_of"]),
                evidence_ids=(ref_id,),
            )
            assert not await module.read_identity_bindings(
                session,
                instrument_id=instrument,
                as_of=decision + timedelta(days=1),
                evidence_ids=(ref_id,),
            )
            row = await session.scalar(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == frozen.id,
                    AgentDataSnapshotEvidenceRecord.evidence_ref_id == ref_id,
                )
            )
            copied = {column.name: getattr(row, column.name) for column in row.__table__.columns}
            foreign = await seed(factory, "OTHER")
            for override in (
                {"instrument_id": foreign},
                {"data_snapshot_id": old.id},
                {"instrument_id": foreign, "authority_kind": "price", "selected_at": decision},
            ):
                with pytest.raises(IntegrityError):
                    async with session.begin_nested():
                        session.add(AgentDataSnapshotEvidenceRecord(**(copied | override)))
                        await session.flush()
        # Exact frozen readback cannot silently fall back after source corruption.
        config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with pytest.raises(RuntimeError, match="cannot remove ownership support"):
            await asyncio.to_thread(command.downgrade, config, "0065_portfolio_management")
        async with factory() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            session.info["sec_exhibit_store"] = store
            assert (
                await module.inspect_identity_binding_snapshot(
                    session,
                    instrument_id=instrument,
                    snapshot=frozen,
                    as_of=decision,
                    decision_session=decision.astimezone(ZoneInfo("America/New_York")).date(),
                )
            )["snapshot_status"] == "ready"
            verified = await module.read_identity_bindings(
                session, instrument_id=instrument, as_of=decision, evidence_ids=(ref_id,)
            )
            document = verified[ref_id]
            digests = {
                artifact.sha256
                for artifact in document.identity_artifacts + document.source_artifacts
            } | {ref.content_hash}
            assert len(digests) == 9
            keys = [next(key for key in client.objects if digest in key) for digest in digests]
            for key in keys:
                original = client.objects[key]
                try:
                    client.objects[key] = original + b" "
                    diagnostic = await module.inspect_identity_binding_snapshot(
                        session,
                        instrument_id=instrument,
                        snapshot=frozen,
                        as_of=decision,
                        decision_session=decision.astimezone(ZoneInfo("America/New_York")).date(),
                    )
                    assert diagnostic["evidence_status"] == "unknown"
                    assert diagnostic["snapshot_status"] == "missing"
                    assert not await module.read_identity_bindings(
                        session, instrument_id=instrument, as_of=decision, evidence_ids=(ref_id,)
                    )
                    assert not await load_frozen_evidence(
                        session, admission.run, allow_fixture_evidence=False
                    )
                    tools = await database_read_only_tools(
                        session, admission.run, allow_fixture_evidence=False
                    )
                    rejected = await tools.execute(
                        "get_security_snapshot",
                        {
                            "instrument_id": str(instrument),
                            "data_snapshot_id": str(frozen.id),
                            "as_of": decision.isoformat(),
                        },
                    )
                    assert rejected["status"] == "unavailable"
                    assert rejected["reason"] == "invalid_frozen_evidence_authority"
                finally:
                    client.objects[key] = original
                # Restoring only this dependency recovers the exact original ref;
                # rejection must not be due to accumulated corruption or alias changes.
                assert await load_frozen_evidence(
                    session, admission.run, allow_fixture_evidence=False
                )

    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["identity_only", "capacity", "ambiguous"])
async def test_identity_remains_supplemental_and_bounded(migrated_database_url, tmp_path, case):
    from nexus.persistence.models.research import Filing, ResearchEvidenceRef
    from nexus.research.current_identity_storage import (
        import_identity_binding,
        read_identity_bindings,
    )

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    try:
        instrument = await seed(factory)
        fm, sc = archives(tmp_path / "base")
        ops = importlib.import_module("nexus.ops.instrument_identity_local")
        base = await ops.revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        source_archive(tmp_path / "current")
        result = await register_identity(
            settings,
            identity_snapshot_id=base.snapshot_id,
            collection_dir=tmp_path / "current",
            store=store,
        )
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            if case != "identity_only":
                await seed_filing(session, instrument_id=instrument, seed_instrument=False)
            if case == "capacity":
                stamp = datetime.now(UTC) - timedelta(days=1)
                for index in range(97):
                    digest = sha256(str(index).encode()).hexdigest()
                    ref = ResearchEvidenceRef(
                        source="sec",
                        provider_object_id=f"capacity:{index}",
                        revision="v1",
                        available_at=stamp,
                        created_at=stamp,
                        content_hash=digest,
                        location="memory://sha256/" + digest,
                        byte_length=10,
                    )
                    session.add(ref)
                    await session.flush()
                    session.add(
                        Filing(
                            instrument_id=instrument,
                            evidence_ref_id=ref.id,
                            source="sec",
                            provider_object_id=ref.provider_object_id,
                            revision=ref.revision,
                            accession_number=f"0000000001-26-{index + 2:06d}",
                            form="10-Q",
                            filed_at=stamp,
                            accepted_at=stamp,
                            created_at=stamp,
                        )
                    )
                await session.flush()
            if case == "ambiguous":
                source_archive(tmp_path / "another")
                second = await import_identity_binding(
                    session,
                    store,
                    identity_snapshot_id=base.snapshot_id,
                    collection_dir=tmp_path / "another",
                )
                assert str(second.id) != result["evidence_id"]
            cutoff = datetime.now(UTC)
            identities = await read_identity_bindings(
                session, instrument_id=instrument, as_of=cutoff
            )
            assert bool(identities) is (case != "ambiguous")
            frozen = await prepare_research_snapshot(session, instrument, as_of=cutoff)
            if case == "identity_only":
                assert frozen is None
            elif case == "capacity":
                assert len(frozen.payload["evidence_ids"]) == 98
                assert result["evidence_id"] not in frozen.payload["evidence_ids"]
                assert frozen.payload["research_identity_diagnostic"]["status"] == "unknown"
            else:
                assert result["evidence_id"] not in frozen.payload["evidence_ids"]
                assert str(second.id) not in frozen.payload["evidence_ids"]
                # Exact frozen identity remains readable; no latest-version fallback.
                assert await read_identity_bindings(
                    session,
                    instrument_id=instrument,
                    as_of=cutoff,
                    evidence_ids=(second.id,),
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_import_refuses_old_identity_artifact_registered_in_the_future(
    migrated_database_url, tmp_path
):
    from nexus.persistence.models.data import DataArtifact

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    try:
        await seed(factory)
        fm, sc = archives(tmp_path / "base")
        # Construct a late-registration authority row via an ordinary INSERT;
        # keep immutable triggers and the original source receipt unchanged.
        raw = (fm / "profile.json").read_bytes()
        digest = sha256(raw).hexdigest()
        uri = await store.put_if_absent(key=f"raw/synthetic/{digest}.json", content=raw)
        receipt = json.loads((fm / "receipt.json").read_text())
        received = datetime.fromisoformat(receipt["received_at"])
        async with factory() as session, session.begin():
            session.add(
                DataArtifact(
                    uri=uri,
                    sha256=digest,
                    byte_size=len(raw),
                    mime_type="application/json",
                    source="fmp",
                    schema_version="fmp_profile_raw_v1",
                    observed_at=received,
                    received_at=received,
                    available_at=received,
                    created_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
        ops = importlib.import_module("nexus.ops.instrument_identity_local")
        base = await ops.revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        source_archive(tmp_path / "current")
        with pytest.raises(ValueError, match="RESEARCH_IDENTITY_ARTIFACT_INVALID"):
            await register_identity(
                settings,
                identity_snapshot_id=base.snapshot_id,
                collection_dir=tmp_path / "current",
                store=store,
            )
        from nexus.persistence.models.research import ResearchEvidenceRef
        from nexus.research.current_identity_storage import SOURCE

        async with factory() as session:
            assert (
                await session.scalar(
                    select(ResearchEvidenceRef).where(ResearchEvidenceRef.source == SOURCE)
                )
                is None
            )
            assert (
                await session.scalar(select(DataSnapshot).where(DataSnapshot.source == SOURCE))
                is None
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_full_capacity_identity_never_removes_unresolved_amendment(
    migrated_database_url, tmp_path
):
    from nexus.agents.trailing_valuation import derive_trailing_valuation
    from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
    from nexus.research.sec_amendment_storage import persist_amendment
    from nexus.research.sec_amendments import SecAmendmentDisclosure
    from nexus.research.sec_disclosures import disclosure_excerpt

    settings = EngineSettings(
        database_url=migrated_database_url, agent_runtime_mode=AgentRuntimeMode.CODEX_SDK
    ).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    try:
        instrument = await seed(factory)
        fm, sc = archives(tmp_path / "base")
        ops = importlib.import_module("nexus.ops.instrument_identity_local")
        base = await ops.revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        await prepare_database(settings)
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            fact_ref = await seed_filing(session, instrument_id=instrument, seed_instrument=False)
            filing = await session.scalar(
                select(Filing).where(Filing.evidence_ref_id == fact_ref.id)
            )
            stamp = datetime.now(UTC) - timedelta(days=1)
            for index in range(96):
                digest = sha256(str(index).encode()).hexdigest()
                ref = ResearchEvidenceRef(
                    source="sec",
                    provider_object_id=f"full:{index}",
                    revision="v1",
                    available_at=stamp,
                    created_at=stamp,
                    content_hash=digest,
                    location="memory://sha256/" + digest,
                    byte_length=10,
                )
                session.add(ref)
                await session.flush()
                session.add(
                    Filing(
                        instrument_id=instrument,
                        evidence_ref_id=ref.id,
                        source="sec",
                        provider_object_id=ref.provider_object_id,
                        revision=ref.revision,
                        accession_number=f"0000000001-26-{index + 2:06d}",
                        form="10-Q",
                        filed_at=stamp,
                        accepted_at=stamp,
                        created_at=stamp,
                    )
                )
            raw = b"<html><p>Synthetic unresolved amendment disclosure.</p></html>"
            excerpt, truncated = disclosure_excerpt(raw)
            accepted = datetime(2026, 9, 1, 20, tzinfo=UTC)
            doc = SecAmendmentDisclosure(
                instrument_id=instrument,
                form="8-K/A",
                issuer_cik="0000123456",
                accession_number="0000123456-26-000099",
                source_url="https://www.sec.gov/Archives/edgar/data/123456/000012345626000099/doc.htm",
                filed_at=accepted.replace(hour=0),
                accepted_at=accepted,
                received_at=accepted,
                raw_byte_length=len(raw),
                http_status=200,
                raw_content_hash=sha256(raw).hexdigest(),
                excerpt=excerpt,
                truncated=truncated,
            )
            assert await persist_amendment(session, store, doc, raw=raw)
            amendment = await session.scalar(
                select(ResearchEvidenceRef).where(
                    ResearchEvidenceRef.provider_object_id == doc.accession_number,
                )
            )
            session.add(
                FinancialFactRecord(
                    filing_id=filing.id,
                    evidence_ref_id=fact_ref.id,
                    instrument_id=instrument,
                    canonical_fact="diluted_eps",
                    raw_taxonomy="us-gaap",
                    raw_tag="EarningsPerShareDiluted",
                    unit="USD/shares",
                    value=Decimal("2.02"),
                    period_kind="duration",
                    period_start=accepted.date() - timedelta(days=90),
                    period_end=accepted.date(),
                    fiscal_year=2026,
                    fiscal_period="Q3",
                    form=filing.form,
                    filed_at=filing.filed_at,
                    accepted_at=filing.accepted_at,
                    accession_number=filing.accession_number,
                    frame=None,
                    revision=filing.revision,
                    created_at=filing.created_at,
                )
            )
            await session.flush()
            old = await prepare_research_snapshot(session, instrument, as_of=datetime.now(UTC))
            assert len(old.payload["evidence_ids"]) == 98
            assert old.payload["evidence_ids"][-1] == str(amendment.id)
            old_id, old_payload, old_revision = old.id, dict(old.payload), old.revision
        source_archive(tmp_path / "current")
        registered = await register_identity(
            settings,
            identity_snapshot_id=base.snapshot_id,
            collection_dir=tmp_path / "current",
            store=store,
        )
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            frozen = await prepare_research_snapshot(session, instrument, as_of=datetime.now(UTC))
            old = await session.get(DataSnapshot, old_id)
            assert (old.payload, old.revision) == (old_payload, old_revision)
            for index, snapshot in enumerate((old, frozen)):
                assert str(amendment.id) in snapshot.payload["evidence_ids"]
                assert len(snapshot.payload["evidence_ids"]) == 98
                assert registered["evidence_id"] not in snapshot.payload["evidence_ids"]
                cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
                admission = await admit_research_run(
                    session,
                    actor_id="test",
                    route="/internal/v1/research-runs",
                    idempotency_key=f"capacity-amendment-{index}",
                    request_hash=str(index) * 64,
                    request=ResearchRequest(
                        instrument_id=instrument, data_snapshot_id=snapshot.id, decision_time=cutoff
                    ),
                    prompt_version_id=None,
                    model_config_id=None,
                    daily_limit_usd=Decimal("3"),
                    now=cutoff,
                )
                tools = await database_read_only_tools(
                    session, admission.run, allow_fixture_evidence=False
                )
                payload = {
                    "instrument_id": str(instrument),
                    "data_snapshot_id": str(snapshot.id),
                    "as_of": cutoff.isoformat(),
                }
                facts = await tools.execute(
                    "get_financial_facts", payload | {"fields": ["diluted_eps"]}
                )
                assert facts["status"] == "ok" and len(facts["items"]) == 1
                assert facts["items"][0]["amendment_review_status"] == "unverified"
                assert facts["items"][0]["amendment_evidence_ref_ids"] == [str(amendment.id)]
                security = await tools.execute("get_security_snapshot", payload)
                if index == 1:
                    readiness_module = importlib.import_module(
                        "nexus.research.current_identity_storage"
                    )
                    diagnostic = await readiness_module.inspect_identity_binding_snapshot(
                        session,
                        instrument_id=instrument,
                        snapshot=snapshot,
                        as_of=cutoff,
                        decision_session=cutoff.astimezone(ZoneInfo("America/New_York")).date(),
                    )
                    assert diagnostic == {
                        "evidence_status": "ready",
                        "snapshot_status": "missing",
                        "reason": "research_identity_evidence_capacity_exhausted",
                    }
                    assert security["research_identity_diagnostic"] == {
                        "status": "unknown",
                        "reason": "research_identity_evidence_capacity_exhausted",
                    }
                valuation = derive_trailing_valuation(security, facts)
                assert valuation["status"] == "unavailable"
                assert valuation["reason"] == "sec_amendment_accounting_review_unverified"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_host_snapshot_binds_registered_identity_with_configured_store(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.agents.persistence import select_authoritative_snapshot
    from nexus.ops.agent_paper_loop import _prepare_current_trends_research_snapshot
    from nexus.research import sec_exhibit_storage

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    configured_reads = []

    def configured_transport(actual):
        assert actual == settings
        configured_reads.append(True)
        return store

    monkeypatch.setattr(sec_exhibit_storage, "S3ProviderEvidenceStore", configured_transport)
    try:
        instrument = await seed(factory)
        fm, sc = archives(tmp_path / "base")
        base = await importlib.import_module("nexus.ops.instrument_identity_local").revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        async with factory() as session, session.begin():
            await seed_filing(session, instrument_id=instrument, seed_instrument=False)
        source_archive(tmp_path / "current")
        registered = await register_identity(
            settings,
            identity_snapshot_id=base.snapshot_id,
            collection_dir=tmp_path / "current",
            store=store,
        )
        # Production binding metadata is written after the evidence cutoff.
        # The generic PG fixture freezes defaults in the past, hiding this ordering.
        async with factory() as session, session.begin():
            await session.execute(text(
                "ALTER TABLE agent_data_snapshot_evidence ALTER COLUMN created_at "
                "SET DEFAULT clock_timestamp()"
            ))
        # Exercise the same snapshot producer used by the normal Mac host.
        # No session.info override or mock of the identity reader is supplied.
        await _prepare_current_trends_research_snapshot(
            settings, instrument_id=instrument, now=datetime.now(UTC)
        )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            snapshot = await select_authoritative_snapshot(
                session, instrument_id=instrument, as_of=datetime.now(UTC)
            )
            assert snapshot is not None
            assert registered["evidence_id"] in snapshot.payload["evidence_ids"]
            assert configured_reads
            diagnostic = await importlib.import_module(
                "nexus.research.current_identity_storage"
            ).inspect_identity_binding_snapshot(
                session,
                instrument_id=instrument,
                snapshot=snapshot,
                as_of=datetime.now(UTC),
                decision_session=datetime.now(UTC).astimezone(ZoneInfo("America/New_York")).date(),
            )
            assert diagnostic["snapshot_status"] == "ready"
    finally:
        await factory.kw["bind"].dispose()

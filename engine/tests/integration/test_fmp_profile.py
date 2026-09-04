"""Only HTTP/S3 transport is synthetic; profile import uses real PG and canonical reader."""

import importlib
import json
import sys
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fmp_profile_local import collect_profile
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client

INSTRUMENT = UUID(int=17)


async def setup_profile(url, directory, *, alias_created_at=None, identifiers=(), raw=None):
    settings = EngineSettings(database_url=url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic-profile",
            "object_store_prefix": "synthetic-profile",
            "object_store_access_key_id": SecretStr("synthetic-access"),
            "object_store_secret_access_key": SecretStr("synthetic-secret"),
        }
    )
    factory = session_factory(settings)
    async with factory.begin() as session:
        session.add(
            Instrument(
                id=INSTRUMENT,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
                **({"created_at": alias_created_at} if alias_created_at else {}),
            )
        )
        for kind, symbol in identifiers:
            session.add(
                InstrumentAlias(
                    instrument_id=INSTRUMENT,
                    symbol=symbol,
                    alias_type=kind,
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                )
            )
    raw = raw or (
        b'[{"symbol":"AAPL","companyName":"Synthetic","industry":"Vendor Industry","price":1.25}]'
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=raw),
        )
    ) as client:
        await collect_profile(
            symbol="AAPL", key=SecretStr("synthetic-secret"), output_dir=directory, client=client
        )
    client = FakeS3Client()
    return settings, factory, S3ProviderEvidenceStore(settings, client=client), client, raw


@pytest.mark.asyncio
async def test_normal_profile_import_replays_exact_evidence_without_financial_authority(
    migrated_database_url,
    tmp_path,
):
    entry = importlib.import_module("nexus.ops.fmp_profile_local")
    directory = tmp_path / "profile"
    settings, factory, store, client, raw = await setup_profile(migrated_database_url, directory)
    before = datetime.now(UTC)
    first = await entry.import_profile(
        settings, instrument_id=INSTRUMENT, symbol="AAPL", collection_dir=directory, store=store
    )
    second = await entry.import_profile(
        settings, instrument_id=INSTRUMENT, symbol="AAPL", collection_dir=directory, store=store
    )
    assert first["status"] == "stored_context" and first["evidence_id"] == second["evidence_id"]
    assert first["execution_locked"] and not first["model_called"]
    assert len(client.objects) == 3 and raw in client.objects.values()
    reader = importlib.import_module("nexus.research.fmp_profile_storage").read_company_profiles
    evidence_id = UUID(first["evidence_id"])
    async with factory() as session:
        docs = await reader(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC), evidence_ids=(evidence_id,)
        )
        assert docs[evidence_id].industry == "Vendor Industry"
        assert not docs[evidence_id].financial_fact_eligible
        assert not docs[evidence_id].sector_risk_eligible
        assert (
            await reader(
                session,
                instrument_id=UUID(int=999),
                as_of=datetime.now(UTC),
                evidence_ids=(evidence_id,),
            )
            == {}
        )
        assert (
            await reader(
                session,
                instrument_id=INSTRUMENT,
                as_of=docs[evidence_id].received_at - timedelta(microseconds=1),
            )
            == {}
        )
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 3
        snapshot = await session.scalar(select(DataSnapshot))
        assert snapshot.created_at >= before and snapshot.source == "fmp_company_profile"


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["unknown_instrument", "late_alias"])
async def test_import_rejects_unproven_identity_before_object_or_evidence_writes(
    problem, migrated_database_url, tmp_path
):
    entry = importlib.import_module("nexus.ops.fmp_profile_local")
    directory = tmp_path / "profile"
    settings, factory, store, client, _ = await setup_profile(
        migrated_database_url,
        directory,
        alias_created_at=datetime.now(UTC) + timedelta(minutes=1)
        if problem == "late_alias"
        else None,
    )
    target = UUID(int=999) if problem == "unknown_instrument" else INSTRUMENT
    with pytest.raises(ValueError, match="FMP_PROFILE_IDENTITY_INVALID"):
        await entry.import_profile(
            settings, instrument_id=target, symbol="AAPL", collection_dir=directory, store=store
        )
    assert not client.objects
    async with factory() as session:
        for model in (ResearchEvidenceRef, DataSnapshot, DataArtifact):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


@pytest.mark.asyncio
async def test_profile_canonical_without_original_artifacts_is_rejected(
    migrated_database_url, tmp_path
):
    from nexus.research.fmp_profile_archive import load_profile_archive
    from nexus.research.fmp_profile_storage import persist_company_profile

    directory = tmp_path / "profile"
    _, factory, _, client, _ = await setup_profile(migrated_database_url, directory)
    document = load_profile_archive(directory, instrument_id=INSTRUMENT, symbol="AAPL").document
    with pytest.raises(ValueError, match="FMP_PROFILE_ARTIFACT_INVALID"):
        async with factory.begin() as session:
            await persist_company_profile(session, document, location="s3://synthetic/missing")
    assert not client.objects
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0


@pytest.mark.asyncio
async def test_import_readback_failure_rolls_back_all_database_rows_not_private_objects(
    migrated_database_url, tmp_path, monkeypatch
):
    entry = importlib.import_module("nexus.ops.fmp_profile_local")
    directory = tmp_path / "profile"
    settings, factory, store, client, _ = await setup_profile(migrated_database_url, directory)

    async def failed_readback(*args, **kwargs):
        raise ValueError("synthetic readback failure")

    monkeypatch.setattr(entry, "read_company_profiles", failed_readback)
    with pytest.raises(ValueError, match="synthetic readback failure"):
        await entry.import_profile(
            settings, instrument_id=INSTRUMENT, symbol="AAPL", collection_dir=directory, store=store
        )
    assert len(client.objects) == 3
    async with factory() as session:
        for model in (ResearchEvidenceRef, DataSnapshot, DataArtifact):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


def test_actual_import_cli_uses_normal_storage_and_emits_only_safe_summary(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    import asyncio

    entry = importlib.import_module("nexus.ops.fmp_profile_local")
    directory = tmp_path / "profile"
    settings, _, store, _, _ = asyncio.run(setup_profile(migrated_database_url, directory))
    output = tmp_path / "result.json"
    monkeypatch.setattr(entry, "load_local_environment", lambda *args: settings.model_dump())
    monkeypatch.setattr(entry, "S3ProviderEvidenceStore", lambda *args, **kwargs: store)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fmp-profile",
            "import",
            "--symbol",
            "AAPL",
            "--instrument-id",
            str(INSTRUMENT),
            "--collection-dir",
            str(directory),
            "--output",
            str(output),
        ],
    )
    entry.main()
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "status": "stored_context",
        "model_called": False,
        "research_created": False,
    }
    result = json.loads(output.read_bytes())
    assert UUID(result["evidence_id"]) and result["execution_locked"]
    assert output.stat().st_mode & 0o077 == 0
    assert "AAPL" not in captured.out and "secret" not in captured.out


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,known,reported",
    [
        ("cik", "0000000001", "0000000002"),
        ("isin", "US0000000001", "US0000000002"),
        ("cusip", "000000001", "000000002"),
    ],
)
async def test_import_identifier_conflict_rejected_before_s3(
    kind, known, reported, migrated_database_url, tmp_path
):
    entry = importlib.import_module("nexus.ops.fmp_profile_local")
    directory = tmp_path / "profile"
    settings, factory, store, client, _ = await setup_profile(
        migrated_database_url,
        directory,
        identifiers=((kind, known),),
        raw=json.dumps([{"symbol": "AAPL", kind: reported}]).encode(),
    )
    with pytest.raises(ValueError, match="FMP_PROFILE_IDENTITY_INVALID"):
        await entry.import_profile(
            settings, instrument_id=INSTRUMENT, symbol="AAPL", collection_dir=directory, store=store
        )
    assert not client.objects
    async with factory() as session:
        for model in (ResearchEvidenceRef, DataSnapshot, DataArtifact):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("reported", [None, "1", "0000000001"])
async def test_missing_or_equivalent_cik_does_not_invent_an_identity_conflict(
    reported, migrated_database_url, tmp_path
):
    entry = importlib.import_module("nexus.ops.fmp_profile_local")
    directory = tmp_path / "profile"
    settings, _, store, _, _ = await setup_profile(
        migrated_database_url,
        directory,
        identifiers=(("cik", "0000000001"),),
        raw=json.dumps([{"symbol": "AAPL", "cik": reported}]).encode(),
    )
    result = await entry.import_profile(
        settings, instrument_id=INSTRUMENT, symbol="AAPL", collection_dir=directory, store=store
    )
    assert result["status"] == "stored_context"


@pytest.mark.asyncio
@pytest.mark.parametrize("known_before_receipt", [True, False])
async def test_reader_rechecks_legacy_identifier_conflicts_only_at_original_receipt(
    known_before_receipt, migrated_database_url, tmp_path
):
    """Legacy rows from the old ticker-only writer remain stored, never requalified."""
    from hashlib import sha256

    from nexus.research.fmp_profile import SCHEMA, SOURCE
    from nexus.research.fmp_profile_archive import load_profile_archive
    from nexus.research.fmp_profile_storage import read_company_profiles, save_profile_artifact

    directory = tmp_path / "profile"
    _, factory, store, _, _ = await setup_profile(
        migrated_database_url,
        directory,
        identifiers=(("cik", "0000000001"),) if known_before_receipt else (),
        raw=b'[{"symbol":"AAPL","cik":"0000000002"}]',
    )
    archive = load_profile_archive(directory, instrument_id=INSTRUMENT, symbol="AAPL")
    document = archive.document
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    async with factory.begin() as session:
        if not known_before_receipt:
            session.add(
                InstrumentAlias(
                    instrument_id=INSTRUMENT,
                    symbol="0000000001",
                    alias_type="cik",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                    created_at=datetime.now(UTC),
                )
            )
        for original in archive.originals:
            await save_profile_artifact(
                session,
                store,
                content=original.content,
                schema=original.schema_version,
                received_at=original.received_at,
            )
        location = await save_profile_artifact(
            session,
            store,
            content=content,
            schema=SCHEMA,
            received_at=document.received_at,
        )
        evidence = ResearchEvidenceRef(
            source=SOURCE,
            provider_object_id=document.provider_object_id,
            revision=digest,
            available_at=document.available_at,
            location=location,
            content_hash=digest,
            byte_length=len(content),
        )
        session.add(evidence)
        await session.flush()
        session.add(
            DataSnapshot(
                source=SOURCE,
                provider_object_id=document.provider_object_id,
                revision=digest,
                observed_at=document.received_at,
                received_at=document.received_at,
                available_at=document.available_at,
                schema_version=SCHEMA,
                payload=document.model_dump(mode="json"),
            )
        )
        evidence_id = evidence.id
    async with factory() as session:
        from nexus.research.fmp_profile_storage import require_profile_identity

        if not known_before_receipt:
            late_alias = await session.scalar(
                select(InstrumentAlias).where(InstrumentAlias.alias_type == "cik")
            )
            assert late_alias.created_at > document.received_at
            await require_profile_identity(session, document)
        result = await read_company_profiles(
            session,
            instrument_id=INSTRUMENT,
            as_of=datetime.now(UTC),
            evidence_ids=(evidence_id,),
        )
        assert (evidence_id in result) is not known_before_receipt
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1

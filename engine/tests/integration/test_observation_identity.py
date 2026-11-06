"""Stored original → observation → exact historical readback, isolated PostgreSQL."""

import importlib
import json
from datetime import timedelta
from hashlib import sha256
from urllib.parse import urlsplit

import pytest
from helpers.current_identity import archives
from integration.test_research_identity_context import seed, source_archive
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from nexus.research.current_identity_storage import import_identity_binding
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from unit.market.test_object_store import FakeS3Client


async def fixture(factory, settings, store, tmp_path):
    instrument = await seed(factory)
    fm, sc = archives(tmp_path / "base")
    base = await importlib.import_module("nexus.ops.instrument_identity_local").revise_listing(
        settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
    )
    source_archive(tmp_path / "source")
    async with factory() as session, session.begin():
        original = await import_identity_binding(
            session,
            store,
            identity_snapshot_id=base.snapshot_id,
            collection_dir=tmp_path / "source",
        )
        return instrument, original.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "corruption",
        "future_cutoff",
        "before_registration",
        "mapping_conflict",
        "missing_dependency",
        "new_source_conflict",
        "new_source_same_mapping",
        "new_receipt_same_bytes",
    ],
)
async def test_register_observation_exact_readback_and_fail_closed(
    migrated_database_url, tmp_path, fault
):
    module = importlib.import_module("nexus.research.observation_identity_storage")
    settings = EngineSettings(database_url=migrated_database_url).model_copy(
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
        instrument, original = await fixture(factory, settings, store, tmp_path)
        async with factory() as session:
            before = await session.scalar(select(func.clock_timestamp()))
        async with factory() as session, session.begin():
            ref = await module.register_observation_identity_binding(
                session, instrument_id=instrument, store=store
            )
            repeat = await module.register_observation_identity_binding(
                session, instrument_id=instrument, store=store
            )
            assert repeat.id == ref.id
            assert ref.source == "research_observation_identity"
        async with factory() as session:
            cutoff = await session.scalar(select(func.clock_timestamp()))
        client.get_calls.clear()
        async with factory() as session, session.begin():
            result = await module.read_observation_identity_bindings(
                session, instrument_id=instrument, as_of=cutoff, store=store
            )
            assert set(result) == {ref.id}
            doc = result[ref.id]
            assert doc.original_evidence_id == original
            # The original, derived wrapper and conflict scan share this immutable listing base.
            base_key = urlsplit(doc.identity_artifacts[0].uri).path.lstrip("/")
            assert sum(call["Key"] == base_key for call in client.get_calls) == 1
            assert doc.source_effective_at is None and doc.remote_latest_status == "unverified"
            assert doc.execution_locked is True
            if fault == "corruption":
                key = next(
                    key for key in client.objects if "research-identity" in key and "/raw/" in key
                )
                client.objects[key] = b"corrupt"
            if fault == "future_cutoff":
                cutoff += timedelta(days=1)
            if fault == "before_registration":
                cutoff = before
            if fault in {
                "new_source_conflict",
                "new_source_same_mapping",
                "new_receipt_same_bytes",
            }:
                from nexus.research.current_identity_archive import RAW_SCHEMA, RECEIPT_SCHEMA
                from nexus.research.current_identity_storage import _save

                raw = json.dumps(
                    {
                        "fields": ["cik", "name", "ticker", "exchange"],
                        "data": [
                            [
                                999999 if fault == "new_source_conflict" else 123456,
                                "Changed issuer",
                                "AAPL",
                                "NYSE",
                            ]
                        ],
                    }
                ).encode()
                if fault == "new_receipt_same_bytes":
                    from nexus.research.current_identity_storage import _read_artifact

                    raw = await _read_artifact(session, store, doc.source_artifacts[0], cutoff)
                receipt = json.dumps(
                    {
                        "schema": RECEIPT_SCHEMA,
                        "source_url": "https://www.sec.gov/files/company_tickers_exchange.json",
                        "http_status": 200,
                        "received_at": cutoff.isoformat(),
                        "sha256": sha256(raw).hexdigest(),
                        "byte_length": len(raw),
                    }
                ).encode()
                await _save(session, store, raw, RAW_SCHEMA, cutoff)
                await _save(session, store, receipt, RECEIPT_SCHEMA, cutoff)
                cutoff = await session.scalar(select(func.clock_timestamp()))
            if fault in {"mapping_conflict", "missing_dependency"}:
                base = (
                    await session.get(DataSnapshot, doc.identity_snapshot_id)
                    if fault == "mapping_conflict"
                    else await session.scalar(
                        select(DataSnapshot).where(
                            DataSnapshot.source == "research_current_identity"
                        )
                    )
                )
                changed = dict(base.payload)
                if fault == "mapping_conflict":
                    changed["identity"] = dict(changed["identity"], cik="0000999999")
                session.add(
                    DataSnapshot(
                        source=base.source,
                        provider_object_id=base.provider_object_id,
                        revision="c" * 64,
                        schema_version=base.schema_version,
                        payload=changed,
                        observed_at=base.observed_at,
                        received_at=base.received_at,
                        available_at=base.available_at,
                    )
                )
                await session.flush()
            read = await module.read_observation_identity_bindings(
                session, instrument_id=instrument, as_of=cutoff, evidence_ids=(ref.id,), store=store
            )
            assert bool(read) is (
                fault in {None, "new_source_same_mapping", "new_receipt_same_bytes"}
            )
            if fault in {"new_source_same_mapping", "new_receipt_same_bytes"}:
                assert read[ref.id].source_artifacts == doc.source_artifacts
                assert read[ref.id].source_received_at == doc.source_received_at
                assert read[ref.id].remote_latest_status == "unverified"
            if fault is None:
                snapshot = DataSnapshot(
                    source="nexus_research_only",
                    provider_object_id=str(instrument),
                    revision="a" * 64,
                    schema_version="research_only_v2",
                    observed_at=cutoff,
                    available_at=cutoff,
                    received_at=cutoff,
                    payload={"as_of": cutoff.isoformat()},
                )
                session.add(snapshot)
                await session.flush()
                values = dict(
                    data_snapshot_id=snapshot.id,
                    instrument_id=instrument,
                    evidence_ref_id=ref.id,
                    source=ref.source,
                    provider_object_id=ref.provider_object_id,
                    revision=ref.revision,
                    available_at=ref.available_at,
                    location=ref.location,
                    content_hash=ref.content_hash,
                )
                session.add(
                    AgentDataSnapshotEvidenceRecord(
                        **values, authority_kind="research", selected_at=None
                    )
                )
                await session.flush()
                price_snapshot = DataSnapshot(
                    source="nexus_research_only",
                    provider_object_id=str(instrument),
                    revision="b" * 64,
                    schema_version="research_only_v2",
                    observed_at=cutoff,
                    available_at=cutoff,
                    received_at=cutoff,
                    payload={"as_of": cutoff.isoformat()},
                )
                session.add(price_snapshot)
                await session.flush()
                values["data_snapshot_id"] = price_snapshot.id
                with pytest.raises(IntegrityError):
                    async with session.begin_nested():
                        session.add(
                            AgentDataSnapshotEvidenceRecord(
                                **values, authority_kind="price", selected_at=cutoff
                            )
                        )
                        await session.flush()
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_known_receipts_batch_registration_reads_without_skipping_originals(
    migrated_database_url, tmp_path
):
    from nexus.research.observation_identity_storage import (
        ResearchIdentityBinding,
        _check_known_sources,
    )
    from sqlalchemy import event

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(settings, client=client)
    engine = factory.kw["bind"]
    reads = []

    def record(_conn, _cursor, statement, _params, _context, _many):
        if "FROM data_artifacts" in statement:
            reads.append(statement)

    try:
        await fixture(factory, settings, store, tmp_path)
        async with factory() as session:
            snapshot = await session.scalar(
                select(DataSnapshot).where(DataSnapshot.source == "research_current_identity")
            )
            document = ResearchIdentityBinding.model_validate(snapshot.payload)
            cutoff = await session.scalar(select(func.clock_timestamp()))
            client.get_calls.clear()
            event.listen(engine.sync_engine, "before_cursor_execute", record)
            await _check_known_sources(session, store, document, cutoff)
            event.remove(engine.sync_engine, "before_cursor_execute", record)
            # Receipts plus their exact originals, not one registration query per object.
            assert len(reads) == 2
            assert len(client.get_calls) == 4
            # Already-read bytes are never treated as authority on the next invocation.
            raw_key = urlsplit(document.source_artifacts[0].uri).path.lstrip("/")
            client.objects[raw_key] = b"corrupt"
            with pytest.raises(ValueError):
                await _check_known_sources(session, store, document, cutoff)
    finally:
        if event.contains(engine.sync_engine, "before_cursor_execute", record):
            event.remove(engine.sync_engine, "before_cursor_execute", record)
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["delete", "metadata"])
async def test_batched_original_registration_is_immutable_during_read(
    migrated_database_url, tmp_path, change
):
    from nexus.research.observation_identity_storage import (
        ResearchIdentityBinding,
        _check_known_sources,
    )
    from sqlalchemy import text

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    changed = False
    try:
        await fixture(factory, settings, store, tmp_path)
        async with factory() as session:
            snapshot = await session.scalar(
                select(DataSnapshot).where(DataSnapshot.source == "research_current_identity")
            )
            document = ResearchIdentityBinding.model_validate(snapshot.payload)
            cutoff = await session.scalar(select(func.clock_timestamp()))

            class ConcurrentStore:
                async def read_raw(self, key, *, uri):
                    nonlocal changed
                    value = await store.read_raw(key, uri=uri)
                    if not changed and uri in {
                        document.source_artifacts[0].uri,
                        document.source_artifacts[2].uri,
                    }:
                        other = document.source_artifacts[
                            2 if uri == document.source_artifacts[0].uri else 0
                        ]
                        with pytest.raises(
                            IntegrityError, match="diagnostic authority record is immutable"
                        ):
                            async with factory.begin() as writer:
                                sql = (
                                    "DELETE FROM data_artifacts WHERE sha256=:hash"
                                    if change == "delete"
                                    else "UPDATE data_artifacts SET byte_size=byte_size+1 "
                                    "WHERE sha256=:hash"
                                )
                                await writer.execute(text(sql), {"hash": other.sha256})
                        changed = True
                    return value

            await _check_known_sources(session, ConcurrentStore(), document, cutoff)
            assert changed
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_scoped_registration_still_checks_cutoff_binding_and_original(
    migrated_database_url, tmp_path
):
    from nexus.research.artifact_read_scope import artifact_read_scope
    from nexus.research.current_identity_storage import _read_artifact
    from nexus.research.observation_identity_storage import ResearchIdentityBinding
    from sqlalchemy import event

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(settings, client=client)
    engine = factory.kw["bind"]
    reads = []

    def record(_conn, _cursor, statement, _params, _context, _many):
        if "FROM data_artifacts" in statement:
            reads.append(statement)

    try:
        await fixture(factory, settings, store, tmp_path)
        async with factory() as session:
            snapshot = await session.scalar(
                select(DataSnapshot).where(DataSnapshot.source == "research_current_identity")
            )
            document = ResearchIdentityBinding.model_validate(snapshot.payload)
            binding = document.source_artifacts[0]
            cutoff = await session.scalar(select(func.clock_timestamp()))
            event.listen(engine.sync_engine, "before_cursor_execute", record)
            with artifact_read_scope(session):
                original = await _read_artifact(session, store, binding, cutoff)
                assert await _read_artifact(session, store, binding, cutoff) == original
                assert len(reads) == 1
                with pytest.raises(ValueError, match="RESEARCH_IDENTITY_ARTIFACT_INVALID"):
                    await _read_artifact(
                        session, store, binding, snapshot.created_at - timedelta(days=1)
                    )
                with pytest.raises(ValueError, match="RESEARCH_IDENTITY_ARTIFACT_INVALID"):
                    await _read_artifact(
                        session, store, binding.model_copy(update={"source": "wrong"}), cutoff
                    )
                client.objects[urlsplit(binding.uri).path.lstrip("/")] = b"corrupt"
                with pytest.raises(ValueError, match="RESEARCH_IDENTITY_ARTIFACT_INVALID"):
                    await _read_artifact(session, store, binding, cutoff)
                assert len(reads) == 1
            with artifact_read_scope(session), pytest.raises(ValueError):
                await _read_artifact(session, store, binding, cutoff)
            assert len(reads) == 2
    finally:
        if event.contains(engine.sync_engine, "before_cursor_execute", record):
            event.remove(engine.sync_engine, "before_cursor_execute", record)
        await engine.dispose()


@pytest.mark.asyncio
async def test_scoped_registration_preserves_real_dirty_session_autoflush(
    migrated_database_url, tmp_path
):
    from nexus.persistence.models.data import DataArtifact
    from nexus.research.artifact_read_scope import artifact_read_scope, read_artifact
    from nexus.research.observation_identity_storage import ResearchIdentityBinding

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
        await fixture(factory, settings, store, tmp_path)
        async with factory() as session:
            snapshot = await session.scalar(
                select(DataSnapshot).where(DataSnapshot.source == "research_current_identity")
            )
            digest = (
                ResearchIdentityBinding.model_validate(snapshot.payload).source_artifacts[0].sha256
            )
            with artifact_read_scope(session):
                identity = (await read_artifact(session, digest)).id
                frozen = await read_artifact(session, digest)
                row = await session.get(DataArtifact, identity)
                row.source = "wrong"
                with pytest.raises(
                    IntegrityError, match="diagnostic authority record is immutable"
                ):
                    await read_artifact(session, digest)
                await session.rollback()
            assert (await read_artifact(session, digest)).source == frozen.source
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_scoped_registration_does_not_survive_savepoint_rollback(migrated_database_url):
    from datetime import UTC, datetime

    from nexus.persistence.models.data import DataArtifact
    from nexus.research.artifact_read_scope import artifact_read_scope, read_artifact

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    digest = "e" * 64
    try:
        async with factory() as session, session.begin():
            with artifact_read_scope(session):
                async with session.begin_nested() as savepoint:
                    now = datetime.now(UTC)
                    session.add(
                        DataArtifact(
                            uri="s3://synthetic/raw/one?versionId=one",
                            sha256=digest,
                            byte_size=1,
                            mime_type="application/json",
                            schema_version="test_v1",
                            source="test",
                            observed_at=now,
                            available_at=now,
                            received_at=now,
                        )
                    )
                    await session.flush()
                    session.expunge_all()
                    assert await read_artifact(session, digest) is not None
                    assert await read_artifact(session, digest) is not None
                    await savepoint.rollback()
                assert await read_artifact(session, digest) is None
    finally:
        await factory.kw["bind"].dispose()

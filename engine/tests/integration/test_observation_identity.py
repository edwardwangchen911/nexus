"""Stored original → observation → exact historical readback, isolated PostgreSQL."""

import importlib
import json
from datetime import timedelta
from hashlib import sha256

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
        async with factory() as session, session.begin():
            result = await module.read_observation_identity_bindings(
                session, instrument_id=instrument, as_of=cutoff, store=store
            )
            assert set(result) == {ref.id}
            doc = result[ref.id]
            assert doc.original_evidence_id == original
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

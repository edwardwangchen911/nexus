"""Archived amendment main text is read only through exact frozen evidence."""

from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from helpers.current_identity import MemoryStore
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.research import FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.sec_amendments import SecAmendmentDisclosure
from nexus.research.sec_disclosures import disclosure_excerpt, read_disclosures
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from test_sec_local import INSTRUMENT, seed_instrument

RAW = b"<html><body><p>This amendment adds a limited disclosure for investors.</p></body></html>"


class Store(MemoryStore):
    async def put_if_absent(self, *, key, content):
        uri = await super().put_if_absent(key=key, content=content)
        return f"memory://sha256/{key.rsplit('/', 1)[-1]}" if key.startswith("evidence/") else uri

    async def read_evidence(self, key, *, uri):
        assert uri == f"memory://sha256/{key.rsplit('/', 1)[-1]}"
        return self.objects[key]


def document():
    accepted = datetime(2026, 9, 1, 20, tzinfo=UTC)
    excerpt, truncated = disclosure_excerpt(RAW)
    return SecAmendmentDisclosure(
        instrument_id=INSTRUMENT,
        form="8-K/A",
        issuer_cik="0000320193",
        accession_number="0000320193-26-000002",
        source_url="https://www.sec.gov/Archives/edgar/data/320193/000032019326000002/doc.htm",
        filed_at=accepted.replace(hour=0),
        accepted_at=accepted,
        received_at=accepted + timedelta(days=1),
        raw_byte_length=len(RAW),
        http_status=200,
        raw_content_hash=sha256(RAW).hexdigest(),
        excerpt=excerpt,
        truncated=truncated,
    )


@pytest.mark.asyncio
async def test_amendment_persistence_readback_receipt_cutoff_and_replay(migrated_database_url):
    from nexus.research.sec_amendment_storage import persist_amendment

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    store = Store()
    doc = document()
    async with session_factory(settings)() as session, session.begin():
        session.info["sec_exhibit_store"] = store
        assert await persist_amendment(session, store, doc, raw=RAW)
        assert not await persist_amendment(session, store, doc, raw=RAW)
        evidence = await session.scalar(select(ResearchEvidenceRef))
        now = await session.scalar(select(func.clock_timestamp()))
        assert evidence.available_at == doc.received_at
        assert not await read_disclosures(
            session,
            instrument_id=INSTRUMENT,
            evidence_ids=(evidence.id,),
            as_of=doc.received_at - timedelta(seconds=1),
        )
        found = await read_disclosures(
            session, instrument_id=INSTRUMENT, evidence_ids=(evidence.id,), as_of=now
        )
        assert found[evidence.id].canonical_bytes() == doc.canonical_bytes()
        assert found[evidence.id].relation_status == "unknown"
        assert not await read_disclosures(
            session, instrument_id=INSTRUMENT, evidence_ids=(), as_of=now
        )
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["raw", "canonical", "missing", "owner", "receipt"])
async def test_tampered_archive_or_receipt_never_reaches_reader(migrated_database_url, failure):
    from nexus.research.sec_amendment_storage import persist_amendment

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    doc, store = document(), Store()
    async with session_factory(settings)() as session, session.begin():
        session.info["sec_exhibit_store"] = store
        await persist_amendment(session, store, doc, raw=RAW)
        evidence = await session.scalar(select(ResearchEvidenceRef))
        now = await session.scalar(select(func.clock_timestamp()))
        raw_key = next(key for key in store.objects if key.startswith("raw/"))
        canonical_key = next(key for key in store.objects if key.startswith("evidence/"))
        instrument = INSTRUMENT
        if failure == "raw":
            store.objects[raw_key] = b"wrong original"
        elif failure == "canonical":
            store.objects[canonical_key] = b"{}"
        elif failure == "missing":
            del store.objects[raw_key]
        elif failure == "owner":
            from uuid import uuid4

            instrument = uuid4()
        else:
            artifact = await session.scalar(select(DataArtifact))
            with pytest.raises(IntegrityError, match="immutable"):
                async with session.begin_nested():
                    artifact.received_at = doc.received_at - timedelta(seconds=1)
                    await session.flush()
            await session.refresh(artifact)
            assert artifact.received_at == doc.received_at
            assert await read_disclosures(
                session, instrument_id=instrument, evidence_ids=(evidence.id,), as_of=now
            )
            return
        assert not await read_disclosures(
            session, instrument_id=instrument, evidence_ids=(evidence.id,), as_of=now
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["hash", "excerpt", "truncated", "length", "future"])
async def test_bad_intake_has_no_business_evidence(migrated_database_url, failure):
    from nexus.research.sec_amendment_storage import persist_amendment

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    doc, store = document(), Store()
    updates = {
        "hash": {"raw_content_hash": "b" * 64},
        "excerpt": {"excerpt": "Unsupported text not contained in this original."},
        "truncated": {"truncated": True},
        "length": {"raw_byte_length": len(RAW) + 1},
        "future": {"received_at": datetime.now(UTC) + timedelta(days=1)},
    }
    invalid = doc.model_copy(update=updates[failure])
    async with session_factory(settings)() as session, session.begin():
        with pytest.raises(ValueError):
            await persist_amendment(session, store, invalid, raw=RAW)
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 0
        assert store.objects == {}


@pytest.mark.asyncio
async def test_accession_conflict_does_not_replace_original(migrated_database_url):
    from nexus.research.sec_amendment_storage import persist_amendment

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    doc, store = document(), Store()
    async with session_factory(settings)() as session, session.begin():
        session.info["sec_exhibit_store"] = store
        await persist_amendment(session, store, doc, raw=RAW)
        changed_raw = RAW.replace(b"limited", b"different")
        excerpt, truncated = disclosure_excerpt(changed_raw)
        changed = doc.model_copy(
            update={
                "raw_content_hash": sha256(changed_raw).hexdigest(),
                "raw_byte_length": len(changed_raw),
                "excerpt": excerpt,
                "truncated": truncated,
            }
        )
        before = dict(store.objects)
        with pytest.raises(ValueError, match="ACCESSION_CONFLICT"):
            await persist_amendment(session, store, changed, raw=changed_raw)
        assert store.objects == before
        evidence = await session.scalar(select(ResearchEvidenceRef))
        now = await session.scalar(select(func.clock_timestamp()))
        found = await read_disclosures(
            session, instrument_id=INSTRUMENT, evidence_ids=(evidence.id,), as_of=now
        )
        assert found[evidence.id].canonical_bytes() == doc.canonical_bytes()
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1


@pytest.mark.asyncio
async def test_later_observation_reuses_original_receipt_without_rebinding(migrated_database_url):
    from nexus.research.sec_amendment_storage import persist_amendment

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    doc, store = document(), Store()
    async with session_factory(settings)() as session, session.begin():
        session.info["sec_exhibit_store"] = store
        await persist_amendment(session, store, doc, raw=RAW)
        before = dict(store.objects)
        later = doc.model_copy(update={"received_at": doc.received_at + timedelta(hours=1)})
        assert not await persist_amendment(session, store, later, raw=RAW)
        assert store.objects == before
        evidence = await session.scalar(select(ResearchEvidenceRef))
        now = await session.scalar(select(func.clock_timestamp()))
        found = await read_disclosures(
            session, instrument_id=INSTRUMENT, evidence_ids=(evidence.id,), as_of=now
        )
        assert found[evidence.id].received_at == doc.received_at
        assert evidence.available_at == doc.received_at


@pytest.mark.asyncio
async def test_distinct_accessions_share_bytes_but_keep_their_own_receipts(migrated_database_url):
    from nexus.research.sec_amendment_storage import persist_amendment

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    first, store = document(), Store()
    second = first.model_copy(
        update={
            "accession_number": "0000320193-26-000003",
            "source_url": first.source_url.replace("000032019326000002", "000032019326000003"),
            "received_at": first.received_at + timedelta(hours=1),
        }
    )
    async with session_factory(settings)() as session, session.begin():
        session.info["sec_exhibit_store"] = store
        assert await persist_amendment(session, store, first, raw=RAW)
        assert await persist_amendment(session, store, second, raw=RAW)
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        evidence = tuple(await session.scalars(select(ResearchEvidenceRef)))
        assert len(evidence) == 2
        now = await session.scalar(select(func.clock_timestamp()))
        readback = await read_disclosures(
            session,
            instrument_id=INSTRUMENT,
            evidence_ids=tuple(ref.id for ref in evidence),
            as_of=now,
        )
        assert {doc.accession_number: doc.received_at for doc in readback.values()} == {
            first.accession_number: first.received_at,
            second.accession_number: second.received_at,
        }

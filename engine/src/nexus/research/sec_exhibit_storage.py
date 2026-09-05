"""Store and revalidate every original in one explicitly imported 8-K exhibit bundle."""

from collections.abc import Sequence
from datetime import datetime
from hashlib import sha256
from typing import Protocol, cast
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.current_identity_storage import IdentityStore, read_current_identity
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.research.sec_exhibits import (
    SCHEMA_VERSION,
    SecExhibitDisclosure,
    build_exhibit_disclosure,
)
from nexus.settings import EngineSettings


class ExhibitStore(IdentityStore, Protocol):
    async def read_evidence(self, key: str, *, uri: str) -> bytes: ...


def configured_exhibit_store(session: AsyncSession) -> ExhibitStore:
    override = session.info.get("sec_exhibit_store")
    if override is not None:
        return cast(ExhibitStore, override)
    settings = session.info.get("engine_settings")
    if not isinstance(settings, EngineSettings):
        raise ValueError("SEC_EXHIBIT_STORE_UNAVAILABLE")
    return S3ProviderEvidenceStore(settings)


def _schema(index: int, receipt: bool) -> str:
    return f"sec_exhibit_{index}_{'receipt' if receipt else 'raw'}_v2"


async def _read_original(
    session: AsyncSession,
    store: ExhibitStore,
    *,
    digest: str,
    schema: str,
    received_at: datetime,
    as_of: datetime,
) -> bytes:
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if (
        artifact is None
        or artifact.source != "sec"
        or artifact.schema_version != schema
        or artifact.received_at != received_at
        or artifact.available_at != received_at
        or artifact.observed_at != received_at
        or artifact.created_at > as_of
        or received_at > as_of
    ):
        raise ValueError("SEC_EXHIBIT_ARTIFACT_INVALID")
    path = urlsplit(artifact.uri).path
    if "/raw/" not in path:
        raise ValueError("SEC_EXHIBIT_ARTIFACT_INVALID")
    key = "raw/" + path.split("/raw/", 1)[1]
    raw = await store.read_raw(key, uri=artifact.uri)
    if len(raw) != artifact.byte_size or sha256(raw).hexdigest() != digest:
        raise ValueError("SEC_EXHIBIT_ARTIFACT_INVALID")
    return raw


async def verify_exhibit_originals(
    session: AsyncSession,
    store: ExhibitStore,
    document: SecExhibitDisclosure,
    *,
    as_of: datetime,
) -> None:
    proof = await read_current_identity(session, store, snapshot_id=document.identity_snapshot_id)
    if (
        proof.instrument_id != document.instrument_id
        or proof.identity.cik != document.issuer_cik
        or proof.checked_at > as_of
    ):
        raise ValueError("SEC_EXHIBIT_IDENTITY_INVALID")
    archives = []
    for index, receipt in enumerate(document.originals):
        raw = await _read_original(
            session,
            store,
            digest=receipt.sha256,
            schema=_schema(index, False),
            received_at=receipt.received_at,
            as_of=as_of,
        )
        receipt_raw = await _read_original(
            session,
            store,
            digest=document.receipt_hashes[index],
            schema=_schema(index, True),
            received_at=receipt.received_at,
            as_of=as_of,
        )
        archives.append((raw, receipt_raw))
    rebuilt = build_exhibit_disclosure(
        instrument_id=proof.instrument_id,
        identity_snapshot_id=document.identity_snapshot_id,
        issuer_cik=proof.identity.cik,
        archives=archives,
        now=as_of,
    )
    if rebuilt.canonical_bytes() != document.canonical_bytes():
        raise ValueError("SEC_EXHIBIT_FROZEN_ORIGINAL_MISMATCH")


async def persist_exhibit_archive(
    session: AsyncSession,
    store: ExhibitStore,
    *,
    identity_snapshot_id: UUID,
    archives: Sequence[tuple[bytes, bytes]],
    now: datetime,
) -> dict[str, str]:
    # Serialize imports sharing this identity without changing its immutable proof.
    identity = await session.scalar(
        select(DataSnapshot).where(DataSnapshot.id == identity_snapshot_id).with_for_update()
    )
    if identity is None or identity.created_at > now:
        raise ValueError("SEC_EXHIBIT_IDENTITY_INVALID")
    proof = await read_current_identity(session, store, snapshot_id=identity_snapshot_id)
    document = build_exhibit_disclosure(
        instrument_id=proof.instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=proof.identity.cik,
        archives=archives,
        now=now,
    )
    if proof.checked_at > now or document.filed_at > document.available_at:
        raise ValueError("SEC_EXHIBIT_RECEIPT_INVALID")
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    existing = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == "sec",
            DataSnapshot.provider_object_id == document.accession_number,
            DataSnapshot.revision == digest,
        )
    )
    if existing is not None:
        ref = await session.scalar(
            select(ResearchEvidenceRef).where(
                ResearchEvidenceRef.source == "sec",
                ResearchEvidenceRef.provider_object_id == document.accession_number,
                ResearchEvidenceRef.revision == digest,
            )
        )
        if ref is None or existing.payload != document.model_dump(mode="json"):
            raise ValueError("SEC_EXHIBIT_EXISTING_CONFLICT")
        from nexus.research.sec_disclosures import read_disclosures

        session.info["sec_exhibit_store"] = store
        verified = await read_disclosures(
            session, instrument_id=proof.instrument_id, evidence_ids=(ref.id,), as_of=now
        )
        if ref.id not in verified or verified[ref.id].canonical_bytes() != content:
            raise ValueError("SEC_EXHIBIT_EXISTING_CONFLICT")
        return {
            "status": "existing",
            "snapshot_id": str(existing.id),
            "evidence_ref_id": str(ref.id),
        }
    for index, pair in enumerate(archives):
        for receipt, raw in enumerate(pair):
            original_hash = sha256(raw).hexdigest()
            schema = _schema(index, bool(receipt))
            key = f"raw/sec/exhibits/{schema}/{original_hash}"
            uri = await store.put_if_absent(key=key, content=raw)
            clock = document.originals[index].received_at
            await session.execute(
                insert(DataArtifact)
                .values(
                    source="sec",
                    sha256=original_hash,
                    uri=uri,
                    byte_size=len(raw),
                    schema_version=schema,
                    mime_type="application/json" if receipt or index == 0 else "text/html",
                    observed_at=clock,
                    received_at=clock,
                    available_at=clock,
                    created_at=func.clock_timestamp(),
                )
                .on_conflict_do_nothing(index_elements=["sha256"])
            )
    # The DB clock may advance during source reads; new rows must be readable now.
    verified_at = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(verified_at, datetime)
    await verify_exhibit_originals(session, store, document, as_of=verified_at)
    location = await store.put_if_absent(
        key=f"evidence/sec/{document.accession_number}/{digest}.json",
        content=content,
    )
    ref = ResearchEvidenceRef(
        source="sec",
        provider_object_id=document.accession_number,
        revision=digest,
        available_at=document.available_at,
        location=location,
        content_hash=digest,
        byte_length=len(content),
        created_at=func.clock_timestamp(),
    )
    session.add(ref)
    await session.flush()
    session.add(
        Filing(
            instrument_id=proof.instrument_id,
            evidence_ref_id=ref.id,
            source="sec",
            provider_object_id=document.accession_number,
            revision=digest,
            accession_number=document.accession_number,
            form="8-K",
            filed_at=document.filed_at,
            accepted_at=document.accepted_at,
            created_at=func.clock_timestamp(),
        )
    )
    snapshot = DataSnapshot(
        source="sec",
        provider_object_id=document.accession_number,
        revision=digest,
        observed_at=document.accepted_at,
        received_at=document.available_at,
        available_at=document.available_at,
        schema_version=SCHEMA_VERSION,
        payload=document.model_dump(mode="json"),
        created_at=func.clock_timestamp(),
    )
    session.add(snapshot)
    await session.flush()
    return {"status": "created", "snapshot_id": str(snapshot.id), "evidence_ref_id": str(ref.id)}

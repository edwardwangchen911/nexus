"""Append-only SEC amendment main text with archive verification on every read."""

from datetime import datetime
from hashlib import sha256
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.research.sec_amendments import SCHEMA_VERSION, SecAmendmentDisclosure
from nexus.research.sec_disclosures import disclosure_excerpt

RAW_SCHEMA = "sec_amendment_raw_v1"


class AmendmentStore(Protocol):
    async def put_if_absent(self, *, key: str, content: bytes) -> str: ...
    async def read_raw(self, key: str, *, uri: str) -> bytes: ...
    async def read_evidence(self, key: str, *, uri: str) -> bytes: ...


def _raw_key(document: SecAmendmentDisclosure) -> str:
    return f"raw/sec/amendments/{document.raw_content_hash}.html"


def _validate_raw(document: SecAmendmentDisclosure, raw: bytes) -> None:
    if sha256(raw).hexdigest() != document.raw_content_hash or len(raw) != document.raw_byte_length:
        raise ValueError("SEC_AMENDMENT_ORIGINAL_INVALID")
    excerpt, truncated = disclosure_excerpt(raw)
    if excerpt != document.excerpt or truncated != document.truncated:
        raise ValueError("SEC_AMENDMENT_TEXT_MISMATCH")


async def verify_amendment_archive(
    session: AsyncSession,
    store: AmendmentStore,
    document: SecAmendmentDisclosure,
    *,
    canonical_uri: str,
    as_of: datetime,
) -> None:
    artifact = await session.scalar(
        select(DataArtifact).where(DataArtifact.sha256 == document.raw_content_hash)
    )
    if (
        artifact is None
        or artifact.source != "sec"
        or artifact.schema_version != RAW_SCHEMA
        or artifact.byte_size != document.raw_byte_length
        or artifact.received_at > document.received_at
        or artifact.available_at != artifact.received_at
        or artifact.observed_at != artifact.received_at
        or artifact.created_at > as_of
        or document.received_at > as_of
    ):
        raise ValueError("SEC_AMENDMENT_ARTIFACT_INVALID")
    raw = await store.read_raw(_raw_key(document), uri=artifact.uri)
    _validate_raw(document, raw)
    digest = sha256(document.canonical_bytes()).hexdigest()
    canonical = await store.read_evidence(
        f"evidence/sec/{document.accession_number}/{digest}.json", uri=canonical_uri
    )
    if canonical != document.canonical_bytes():
        raise ValueError("SEC_AMENDMENT_CANONICAL_INVALID")


async def persist_amendment(
    session: AsyncSession,
    store: AmendmentStore,
    document: SecAmendmentDisclosure,
    *,
    raw: bytes,
) -> bool:
    # Revalidate even model_copy/unchecked construction before writes or archive IO.
    document = SecAmendmentDisclosure.model_validate(document.model_dump(mode="json"))
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or document.received_at > now:
        raise ValueError("SEC_AMENDMENT_RECEIPT_TIME_INVALID")
    _validate_raw(document, raw)
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    existing = await session.scalar(
        select(ResearchEvidenceRef)
        .where(
            ResearchEvidenceRef.source == "sec",
            ResearchEvidenceRef.provider_object_id == document.accession_number,
        )
        .limit(1)
    )
    if existing is not None:
        filing = await session.scalar(select(Filing).where(Filing.evidence_ref_id == existing.id))
        snapshot = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == "sec",
                DataSnapshot.provider_object_id == document.accession_number,
                DataSnapshot.revision == existing.revision,
                DataSnapshot.schema_version == SCHEMA_VERSION,
            )
        )
        if snapshot is None:
            raise ValueError("SEC_AMENDMENT_ACCESSION_CONFLICT")
        saved = SecAmendmentDisclosure.model_validate(snapshot.payload)
        saved_content = saved.canonical_bytes()
        saved_digest = sha256(saved_content).hexdigest()
        if (
            document.received_at < saved.received_at
            or saved.model_dump(mode="json", exclude={"received_at"})
            != document.model_dump(mode="json", exclude={"received_at"})
            or filing is None
            or filing.instrument_id != document.instrument_id
            or existing.content_hash != saved_digest
            or existing.revision != saved_digest
            or existing.available_at != saved.received_at
            or existing.byte_length != len(saved_content)
            or filing.source != "sec"
            or filing.provider_object_id != saved.accession_number
            or filing.revision != saved_digest
            or filing.form != document.form
            or filing.accession_number != document.accession_number
            or filing.accepted_at != document.accepted_at
            or filing.filed_at != document.filed_at
            or snapshot.received_at != saved.received_at
            or snapshot.available_at != saved.received_at
            or snapshot.observed_at != document.accepted_at
        ):
            raise ValueError("SEC_AMENDMENT_ACCESSION_CONFLICT")
        await verify_amendment_archive(
            session, store, saved, canonical_uri=existing.location, as_of=now
        )
        return False
    raw_uri = await store.put_if_absent(key=_raw_key(document), content=raw)
    artifact = await session.scalar(
        select(DataArtifact).where(DataArtifact.sha256 == document.raw_content_hash)
    )
    if artifact is None:
        session.add(
            DataArtifact(
                source="sec",
                sha256=document.raw_content_hash,
                uri=raw_uri,
                byte_size=len(raw),
                mime_type="text/html",
                schema_version=RAW_SCHEMA,
                observed_at=document.received_at,
                received_at=document.received_at,
                available_at=document.received_at,
                created_at=func.clock_timestamp(),
            )
        )
    elif (
        artifact.uri != raw_uri
        or artifact.schema_version != RAW_SCHEMA
        or artifact.source != "sec"
        or artifact.byte_size != len(raw)
        or artifact.received_at > document.received_at
        or artifact.available_at != artifact.received_at
        or artifact.observed_at != artifact.received_at
    ):
        raise ValueError("SEC_AMENDMENT_ARTIFACT_CONFLICT")
    location = await store.put_if_absent(
        key=f"evidence/sec/{document.accession_number}/{digest}.json", content=content
    )
    await session.flush()
    verified_at = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(verified_at, datetime)
    await verify_amendment_archive(
        session, store, document, canonical_uri=location, as_of=verified_at
    )
    evidence = ResearchEvidenceRef(
        source="sec",
        provider_object_id=document.accession_number,
        revision=digest,
        available_at=document.received_at,
        location=location,
        content_hash=digest,
        byte_length=len(content),
        created_at=func.clock_timestamp(),
    )
    session.add(evidence)
    await session.flush()
    session.add(
        Filing(
            instrument_id=document.instrument_id,
            evidence_ref_id=evidence.id,
            source="sec",
            provider_object_id=document.accession_number,
            revision=digest,
            accession_number=document.accession_number,
            form=document.form,
            filed_at=document.filed_at,
            accepted_at=document.accepted_at,
            created_at=func.clock_timestamp(),
        )
    )
    session.add(
        DataSnapshot(
            source="sec",
            provider_object_id=document.accession_number,
            revision=digest,
            observed_at=document.accepted_at,
            available_at=document.received_at,
            received_at=document.received_at,
            schema_version=SCHEMA_VERSION,
            payload=document.model_dump(mode="json"),
            created_at=func.clock_timestamp(),
        )
    )
    await session.flush()
    return True

"""Immutable storage and point-in-time reads for Fosun classification hints."""

from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.fosun_security_classification import (
    RAW_SCHEMA,
    SCHEMA,
    SOURCE,
    FosunSecurityClassificationDocument,
)


async def save_classification_artifact(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    document: FosunSecurityClassificationDocument,
) -> str:
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    key = f"evidence/fosun/security-classification/{digest}.json"
    uri = await store.put_if_absent(key=key, content=content)
    if await store.read_evidence(key, uri=uri) != content:
        raise ValueError("FOSUN_CLASSIFICATION_OBJECT_READBACK_FAILED")
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            source=SOURCE,
            schema_version=SCHEMA,
            observed_at=document.received_at,
            received_at=document.received_at,
            available_at=document.available_at,
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["sha256"])
    )
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is None or (
        artifact.uri,
        artifact.byte_size,
        artifact.mime_type,
        artifact.source,
        artifact.schema_version,
        artifact.observed_at,
        artifact.received_at,
        artifact.available_at,
    ) != (
        uri,
        len(content),
        "application/json",
        SOURCE,
        SCHEMA,
        document.received_at,
        document.received_at,
        document.available_at,
    ):
        raise ValueError("FOSUN_CLASSIFICATION_ARTIFACT_CONFLICT")
    return uri


async def _artifacts_valid(
    session: AsyncSession,
    document: FosunSecurityClassificationDocument,
    *,
    location: str,
    as_of: datetime,
) -> bool:
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    artifacts = {
        item.sha256: item
        for item in await session.scalars(
            select(DataArtifact).where(
                DataArtifact.sha256.in_((digest, document.raw_quote.artifact_sha256))
            )
        )
    }
    canonical = artifacts.get(digest)
    raw = artifacts.get(document.raw_quote.artifact_sha256)
    return bool(
        canonical is not None
        and canonical.uri == location
        and canonical.source == SOURCE
        and canonical.schema_version == SCHEMA
        and canonical.byte_size == len(content)
        and raw is not None
        and raw.uri == document.raw_quote.artifact_uri
        and raw.source == "fosun"
        and raw.schema_version == RAW_SCHEMA
        and raw.byte_size == document.raw_quote.artifact_byte_length
        and canonical.created_at <= as_of
        and raw.created_at <= as_of
        and max(canonical.observed_at, canonical.received_at, canonical.available_at)
        <= min(document.available_at, as_of)
        and max(raw.observed_at, raw.received_at, raw.available_at)
        <= min(document.available_at, as_of)
    )


async def persist_security_classification(
    session: AsyncSession,
    document: FosunSecurityClassificationDocument,
    *,
    location: str,
) -> ResearchEvidenceRef:
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    now = datetime.now(UTC)
    if document.available_at > now or not await _artifacts_valid(
        session, document, location=location, as_of=now
    ):
        raise ValueError("FOSUN_CLASSIFICATION_ARTIFACT_INVALID")
    identity = {
        "source": SOURCE,
        "provider_object_id": document.provider_object_id,
        "revision": digest,
    }
    async with session.begin_nested():
        await session.execute(
            insert(ResearchEvidenceRef)
            .values(
                **identity,
                available_at=document.available_at,
                location=location,
                content_hash=digest,
                byte_length=len(content),
                created_at=func.clock_timestamp(),
            )
            .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
        )
        evidence = await session.scalar(
            select(ResearchEvidenceRef).where(
                ResearchEvidenceRef.source == SOURCE,
                ResearchEvidenceRef.provider_object_id == document.provider_object_id,
                ResearchEvidenceRef.revision == digest,
            )
        )
        if evidence is None or (
            evidence.available_at,
            evidence.location,
            evidence.content_hash,
            evidence.byte_length,
        ) != (document.available_at, location, digest, len(content)):
            raise ValueError("FOSUN_CLASSIFICATION_EVIDENCE_CONFLICT")
        await session.execute(
            insert(DataSnapshot)
            .values(
                **identity,
                observed_at=document.received_at,
                received_at=document.received_at,
                available_at=document.available_at,
                schema_version=SCHEMA,
                payload=document.model_dump(mode="json"),
                created_at=func.clock_timestamp(),
            )
            .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
        )
        snapshot = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == document.provider_object_id,
                DataSnapshot.revision == digest,
            )
        )
        if snapshot is None or (
            snapshot.schema_version,
            snapshot.observed_at,
            snapshot.received_at,
            snapshot.available_at,
            snapshot.payload,
        ) != (
            SCHEMA,
            document.received_at,
            document.received_at,
            document.available_at,
            document.model_dump(mode="json"),
        ):
            raise ValueError("FOSUN_CLASSIFICATION_SNAPSHOT_CONFLICT")
    return evidence


async def read_security_classifications(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, FosunSecurityClassificationDocument]:
    as_of = require_utc(as_of)
    query = (
        select(DataSnapshot, ResearchEvidenceRef)
        .join(
            ResearchEvidenceRef,
            (ResearchEvidenceRef.source == DataSnapshot.source)
            & (ResearchEvidenceRef.provider_object_id == DataSnapshot.provider_object_id)
            & (ResearchEvidenceRef.revision == DataSnapshot.revision),
        )
        .where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.schema_version == SCHEMA,
            DataSnapshot.payload["instrument_id"].astext == str(instrument_id),
            DataSnapshot.created_at <= as_of,
            DataSnapshot.observed_at <= as_of,
            DataSnapshot.received_at <= as_of,
            DataSnapshot.available_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
            ResearchEvidenceRef.available_at <= as_of,
        )
        .order_by(
            DataSnapshot.available_at.desc(),
            DataSnapshot.received_at.desc(),
            ResearchEvidenceRef.id,
        )
    )
    if evidence_ids is not None:
        query = query.where(ResearchEvidenceRef.id.in_(evidence_ids))
    valid: list[tuple[ResearchEvidenceRef, FosunSecurityClassificationDocument]] = []
    with session.no_autoflush:
        for snapshot, evidence in await session.execute(query):
            try:
                document = FosunSecurityClassificationDocument.model_validate(snapshot.payload)
                content = document.canonical_bytes()
                digest = sha256(content).hexdigest()
                if (
                    document.instrument_id == instrument_id
                    and document.provider_object_id
                    == snapshot.provider_object_id
                    == evidence.provider_object_id
                    and digest
                    == snapshot.revision
                    == evidence.revision
                    == evidence.content_hash
                    and len(content) == evidence.byte_length
                    and document.received_at
                    == snapshot.observed_at
                    == snapshot.received_at
                    == snapshot.available_at
                    == evidence.available_at
                    and await _artifacts_valid(
                        session, document, location=evidence.location, as_of=as_of
                    )
                ):
                    valid.append((evidence, document))
            except (TypeError, ValueError):
                continue
    if evidence_ids is not None:
        return {evidence.id: document for evidence, document in valid}
    if not valid:
        return {}
    newest = max((item[1].available_at, item[1].received_at) for item in valid)
    latest = [item for item in valid if (item[1].available_at, item[1].received_at) == newest]
    facts = {item[1].canonical_bytes() for item in latest}
    if len(facts) != 1:
        return {}
    evidence, document = min(latest, key=lambda item: str(item[0].id))
    return {evidence.id: document}

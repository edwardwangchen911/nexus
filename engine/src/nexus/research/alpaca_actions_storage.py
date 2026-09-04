"""Exact, receipt-bound current action observations in existing evidence tables."""

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.alpaca_actions import SCHEMA, SOURCE, AlpacaActionObservationDocument


async def _artifacts_valid(
    session: AsyncSession,
    document: AlpacaActionObservationDocument,
    *,
    as_of: datetime,
    location: str,
) -> bool:
    content = document.canonical_bytes()
    canonical_hash = hashlib.sha256(content).hexdigest()
    required = [
        (
            document.manifest_content_hash,
            document.manifest_byte_length,
            "alpaca_actions_manifest_v1",
            document.available_at,
        ),
        (canonical_hash, len(content), SCHEMA, document.available_at),
    ]
    for page in document.pages:
        required.extend(
            (
                (
                    page.raw_content_hash,
                    page.raw_byte_length,
                    "alpaca_actions_raw_v1",
                    page.received_at,
                ),
                (
                    page.receipt_content_hash,
                    page.receipt_byte_length,
                    "alpaca_actions_receipt_v1",
                    page.received_at,
                ),
            )
        )
    artifacts = {
        row.sha256: row
        for row in await session.scalars(
            select(DataArtifact).where(DataArtifact.sha256.in_([item[0] for item in required]))
        )
    }
    for digest, length, schema, receipt in required:
        artifact = artifacts.get(digest)
        if artifact is None or (
            artifact.source != "alpaca"
            or artifact.schema_version != schema
            or artifact.byte_size != length
            or artifact.created_at > as_of
            or artifact.available_at > min(receipt, as_of)
            or artifact.received_at > min(receipt, as_of)
            or artifact.observed_at > min(receipt, as_of)
            or (digest == canonical_hash and artifact.uri != location)
        ):
            return False
    return True


async def persist_action_observation(
    session: AsyncSession, document: AlpacaActionObservationDocument, *, location: str
) -> ResearchEvidenceRef:
    content = document.canonical_bytes()
    digest = hashlib.sha256(content).hexdigest()
    now = datetime.now(UTC)
    if document.available_at > now or not await _artifacts_valid(
        session, document, as_of=now, location=location
    ):
        raise ValueError("ALPACA_ACTIONS_ARTIFACT_INVALID")
    identity = dict(source=SOURCE, provider_object_id=document.provider_object_id, revision=digest)
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(
            **identity,
            available_at=document.available_at,
            location=location,
            content_hash=digest,
            byte_length=len(content),
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
        evidence.content_hash,
        evidence.byte_length,
        evidence.available_at,
        evidence.location,
    ) != (digest, len(content), document.available_at, location):
        raise ValueError("ALPACA_ACTIONS_EVIDENCE_CONFLICT")
    await session.execute(
        insert(DataSnapshot)
        .values(
            **identity,
            observed_at=document.available_at,
            received_at=document.available_at,
            available_at=document.available_at,
            schema_version=SCHEMA,
            payload=document.model_dump(mode="json"),
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
        document.available_at,
        document.available_at,
        document.available_at,
        document.model_dump(mode="json"),
    ):
        raise ValueError("ALPACA_ACTIONS_SNAPSHOT_CONFLICT")
    return evidence


async def read_action_observations(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, AlpacaActionObservationDocument]:
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
            DataSnapshot.received_at <= as_of,
            DataSnapshot.available_at <= as_of,
            DataSnapshot.created_at <= as_of,
            ResearchEvidenceRef.available_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
        )
        .order_by(DataSnapshot.available_at.desc(), DataSnapshot.id)
        .limit(1)
    )
    if evidence_ids is not None:
        query = query.where(ResearchEvidenceRef.id.in_(evidence_ids))
    result = {}
    with session.no_autoflush:
        for snapshot, evidence in await session.execute(query):
            try:
                document = AlpacaActionObservationDocument.model_validate(snapshot.payload)
                content = document.canonical_bytes()
                if (
                    document.instrument_id == instrument_id
                    and document.provider_object_id
                    == snapshot.provider_object_id
                    == evidence.provider_object_id
                    and hashlib.sha256(content).hexdigest()
                    == snapshot.revision
                    == evidence.revision
                    == evidence.content_hash
                    and len(content) == evidence.byte_length
                    and document.available_at
                    == snapshot.observed_at
                    == snapshot.available_at
                    == snapshot.received_at
                    == evidence.available_at
                    and await _artifacts_valid(
                        session, document, as_of=as_of, location=evidence.location
                    )
                ):
                    result[evidence.id] = document
            except (ValueError, TypeError, KeyError):
                continue  # Optional corrupt action evidence must not hide valid SEC facts.
    return result


def action_tool_payload(document: AlpacaActionObservationDocument) -> dict[str, object]:
    payload = document.model_dump(mode="json", exclude={"events", "pages"})
    payload.update(
        events=[event.model_dump(mode="json") for event in document.events[:32]],
        page_count=len(document.pages),
        bound_event_count=len(document.events),
        truncated=len(document.events) > 32,
        available_at_semantics="maximum_page_receipt_not_historical_availability",
    )
    return payload

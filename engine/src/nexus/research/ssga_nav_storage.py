"""Persist and read exact frozen issuer NAV documents, separate from market bars."""

import hashlib
from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.ssga_nav_history import SCHEMA, SOURCE, IssuerNavHistory


async def persist_nav_history(
    session: AsyncSession, document: IssuerNavHistory, *, location: str
) -> ResearchEvidenceRef:
    content = document.canonical_bytes()
    digest = hashlib.sha256(content).hexdigest()
    object_id = (
        f"nav-history:{document.instrument_id}:{max(row.date for row in document.observations)}"
    )
    values = dict(
        source=SOURCE,
        provider_object_id=object_id,
        revision=digest,
        available_at=document.available_at,
    )
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(**values, location=location, content_hash=digest, byte_length=len(content))
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
            ResearchEvidenceRef.revision == digest,
        )
    )
    if evidence is None or (
        evidence.content_hash,
        evidence.byte_length,
        evidence.available_at,
        evidence.location,
    ) != (digest, len(content), document.available_at, location):
        raise ValueError("ISSUER_NAV_EVIDENCE_CONFLICT")
    await session.execute(
        insert(DataSnapshot)
        .values(
            **values,
            observed_at=document.available_at,
            received_at=document.available_at,
            schema_version=SCHEMA,
            payload=document.model_dump(mode="json"),
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    return evidence


async def read_nav_histories(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, IssuerNavHistory]:
    require_utc(as_of)
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
                document = IssuerNavHistory.model_validate(snapshot.payload)
            except ValidationError:
                continue  # Invalid optional issuer data must not hide valid frozen SEC facts.
            content = document.canonical_bytes()
            if (
                hashlib.sha256(content).hexdigest()
                == snapshot.revision
                == evidence.revision
                == evidence.content_hash
                and len(content) == evidence.byte_length
                and document.available_at
                == snapshot.received_at
                == snapshot.available_at
                == evidence.available_at
            ):
                result[evidence.id] = document
    return result


def nav_tool_payload(document: IssuerNavHistory) -> dict[str, object]:
    """Bound the model view, retaining full coverage and receipt semantics."""
    rows = sorted(document.observations, key=lambda row: row.date)
    payload = document.model_dump(mode="json", exclude={"observations"})
    payload.update(
        observations=[row.model_dump(mode="json") for row in rows[-32:]],
        total_observations=len(rows),
        truncated=len(rows) > 32,
        coverage_start=rows[0].date.isoformat(),
        coverage_end=rows[-1].date.isoformat(),
        date_semantics="issuer_observation_date_not_verified_trading_session",
    )
    return payload

"""Immutable per-date FRED research rows and self-contained, verified originals."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
from typing import Any, cast
from uuid import UUID

import exchange_calendars as xcals  # type: ignore[import-untyped]
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.calendar_policy import load_xnys_calendar_adoption
from nexus.domain.time import require_utc
from nexus.jobs.provider_collection import RawEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot, MacroObservation
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.fred_collection import capture_receipt, json_bytes
from nexus.research.fred_observations import (
    SCHEMA,
    FredCapture,
    FredVintageCollection,
    FredVintageRequest,
    decode,
    parse_collected_vintage,
)


@dataclass(frozen=True)
class FredResearchObservation:
    observation_id: UUID
    evidence_ref_id: UUID
    calendar_evidence_ref_id: UUID
    series: str
    observed_on: date
    value: Decimal
    unit: str
    vintage: date
    available_at: datetime
    received_at: datetime
    raw_sha256: str
    canonical_sha256: str


def _midnight(value: date) -> datetime:
    return datetime.combine(value, datetime.min.time(), UTC)


async def _calendar(
    session: AsyncSession, vintage: date, known_at: datetime
) -> tuple[dict[str, object], datetime, datetime, str]:
    adoption = await load_xnys_calendar_adoption(session, known_at)
    if version("exchange-calendars") != adoption.payload.package_version:
        raise ValueError("FRED_CALENDAR_VERSION_MISMATCH")
    calendar = xcals.get_calendar("XNYS")
    day = calendar.date_to_session((vintage + timedelta(days=1)).isoformat(), direction="next")
    next_open = calendar.session_open(day).to_pydatetime().astimezone(UTC)
    payload = {
        "adoption": adoption.canonical_input(),
        "vintage": vintage.isoformat(),
        "next_session_open": next_open.isoformat(),
    }
    return payload, adoption.created_at, next_open, f"calendar:XNYS:{adoption.row_id}:{vintage}"


async def _artifact(
    session: AsyncSession,
    *,
    store: RawEvidenceStore,
    raw: bytes,
    schema: str,
    received_at: datetime,
    available_at: datetime,
) -> DataArtifact:
    digest = sha256(raw).hexdigest()
    existing = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if existing is not None:
        if (
            existing.source != "fred"
            or existing.schema_version != schema
            or existing.byte_size != len(raw)
            or existing.received_at > received_at
            or existing.available_at > available_at
        ):
            raise ValueError("FRED_ARTIFACT_CONFLICT")
        return existing
    uri = await store.put_if_absent(key=f"evidence/fred/sha256/{digest}.json", content=raw)
    row = DataArtifact(
        created_at=func.clock_timestamp(),
        uri=uri,
        sha256=digest,
        byte_size=len(raw),
        mime_type="application/json",
        schema_version=schema,
        source="fred",
        observed_at=received_at,
        available_at=available_at,
        received_at=received_at,
    )
    session.add(row)
    await session.flush()
    return row


async def import_fred_vintage(
    session: AsyncSession, *, collection: FredVintageCollection, store: RawEvidenceStore
) -> tuple[UUID, ...]:
    """Caller owns the transaction. No HTTP; failed imports leave private originals intact."""
    checked = parse_collected_vintage(collection.request, collection.metadata, collection.pages)
    if checked != collection:
        raise ValueError("FRED_COLLECTION_MISMATCH")
    request = collection.request
    materialized_at = datetime.now(UTC)
    if collection.received_at > materialized_at:
        raise ValueError("FRED_COLLECTION_FUTURE")
    calendar_payload, calendar_at, next_open, calendar_object = await _calendar(
        session, request.vintage, materialized_at
    )
    available = max(calendar_at, next_open, collection.received_at)
    calendar_raw = json_bytes(calendar_payload)
    calendar_hash = sha256(calendar_raw).hexdigest()
    calendar_ref = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == "exchange_calendars",
            ResearchEvidenceRef.provider_object_id == calendar_object,
            ResearchEvidenceRef.revision == calendar_hash,
        )
    )
    if calendar_ref is None:
        uri = await store.put_if_absent(
            key=f"evidence/calendar/sha256/{calendar_hash}.json", content=calendar_raw
        )
        calendar_ref = ResearchEvidenceRef(
            created_at=func.clock_timestamp(),
            source="exchange_calendars",
            provider_object_id=calendar_object,
            revision=calendar_hash,
            available_at=calendar_at,
            location=uri,
            content_hash=calendar_hash,
            byte_length=len(calendar_raw),
        )
        session.add(calendar_ref)
        await session.flush()
    elif (
        calendar_ref.content_hash != calendar_hash
        or calendar_ref.available_at != calendar_at
        or calendar_ref.byte_length != len(calendar_raw)
    ):
        raise ValueError("FRED_CALENDAR_CONFLICT")
    captures = []
    for capture in (collection.metadata, *collection.pages):
        artifact = await _artifact(
            session,
            store=store,
            raw=capture.raw_payload,
            schema="fred_http_json_v1",
            received_at=capture.received_at,
            available_at=capture.received_at,
        )
        captures.append(
            {
                "receipt": capture_receipt(request, capture),
                "raw_text": capture.raw_payload.decode("utf-8"),
                "artifact_id": str(artifact.id),
            }
        )
    canonical = {
        "schema": SCHEMA,
        "request": request.model_dump(mode="json"),
        "captures": captures,
        "calendar": calendar_payload,
        "calendar_evidence_ref_id": str(calendar_ref.id),
        "available_at": available.isoformat(),
    }
    content = json_bytes(canonical)
    digest = sha256(content).hexdigest()
    artifact = await _artifact(
        session,
        store=store,
        raw=content,
        schema=SCHEMA,
        received_at=collection.received_at,
        available_at=available,
    )
    snapshot = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == "fred",
            DataSnapshot.provider_object_id == f"vintage:{digest}",
            DataSnapshot.revision == digest,
        )
    )
    if snapshot is None:
        snapshot = DataSnapshot(
            created_at=func.clock_timestamp(),
            source="fred",
            provider_object_id=f"vintage:{digest}",
            revision=digest,
            schema_version=SCHEMA,
            payload=canonical,
            observed_at=_midnight(request.observation_end),
            available_at=available,
            received_at=collection.received_at,
        )
        session.add(snapshot)
        await session.flush()
    elif snapshot.payload != canonical:
        raise ValueError("FRED_CANONICAL_CONFLICT")
    ids: list[UUID] = []
    for observed, value in collection.observations:
        revision = f"local-{request.vintage}:{observed}:{digest}"
        object_id = f"{request.series}:{revision}"
        existing = await session.scalar(
            select(MacroObservation).where(
                MacroObservation.source == "fred",
                MacroObservation.provider_object_id == object_id,
                MacroObservation.revision == revision,
            )
        )
        if existing is not None:
            if existing.evidence_ref_id is None:
                raise ValueError("FRED_OBSERVATION_CONFLICT")
            existing_ref = await session.get(ResearchEvidenceRef, existing.evidence_ref_id)
            if existing_ref is None:
                raise ValueError("FRED_OBSERVATION_CONFLICT")
            # Replay verifies identity at the actual current time, not an old source cutoff.
            await _read_one(
                session, existing, existing_ref, snapshot, datetime.now(UTC), require_visible=False
            )
            ids.append(existing.id)
            continue
        ref = ResearchEvidenceRef(
            created_at=func.clock_timestamp(),
            source="fred",
            provider_object_id=object_id,
            revision=revision,
            available_at=available,
            location=artifact.uri,
            content_hash=digest,
            byte_length=len(content),
        )
        session.add(ref)
        await session.flush()
        row = MacroObservation(
            created_at=func.clock_timestamp(),
            source="fred",
            series=request.series,
            observed_at=_midnight(observed),
            available_at=available,
            revision=revision,
            value=value,
            unit=collection.unit,
            provider_object_id=object_id,
            authority_version="research_macro_v1",
            real_time_start=request.vintage,
            real_time_end=request.vintage,
            evidence_ref_id=ref.id,
            calendar_evidence_ref_id=calendar_ref.id,
            calendar_source=calendar_ref.source,
            calendar_provider_object_id=calendar_ref.provider_object_id,
            calendar_revision=calendar_ref.revision,
            display_sanity_only=request.series == "DEXHKUS",
        )
        session.add(row)
        await session.flush()
        ids.append(row.id)
    return tuple(ids)


async def _read_one(
    session: AsyncSession,
    row: MacroObservation,
    ref: ResearchEvidenceRef,
    snapshot: DataSnapshot,
    as_of: datetime,
    *,
    require_visible: bool = True,
) -> FredResearchObservation:
    canonical: dict[str, Any] = snapshot.payload
    content = json_bytes(canonical)
    digest = sha256(content).hexdigest()
    if (
        any(item.created_at > as_of for item in (row, ref, snapshot))
        or snapshot.schema_version != SCHEMA
        or snapshot.source != "fred"
        or row.authority_version != "research_macro_v1"
        or row.source != "fred"
        or canonical["schema"] != SCHEMA
        or snapshot.revision != digest
        or ref.content_hash != digest
        or snapshot.provider_object_id != f"vintage:{digest}"
        or ref.byte_length != len(content)
    ):
        raise ValueError("FRED_CANONICAL_INVALID")
    request = FredVintageRequest.model_validate(canonical["request"])
    originals = canonical["captures"]
    if not isinstance(originals, list) or not 2 <= len(originals) <= 5:
        raise ValueError("FRED_ORIGINAL_INVALID")
    captures = []
    for index, entry in enumerate(originals):
        raw = entry["raw_text"].encode("utf-8")
        receipt = entry["receipt"]
        capture = FredCapture(
            "series" if index == 0 else "observations",
            receipt["offset"],
            raw,
            datetime.fromisoformat(receipt["received_at"]),
        )
        artifact = await session.get(DataArtifact, UUID(entry["artifact_id"]))
        if (
            receipt != capture_receipt(request, capture)
            or artifact is None
            or artifact.source != "fred"
            or artifact.schema_version != "fred_http_json_v1"
            or artifact.sha256 != sha256(raw).hexdigest()
            or artifact.byte_size != len(raw)
            or artifact.received_at > capture.received_at
            or artifact.available_at > capture.received_at
            or artifact.created_at > as_of
        ):
            raise ValueError("FRED_ORIGINAL_INVALID")
        captures.append(capture)
    collection = parse_collected_vintage(request, captures[0], tuple(captures[1:]))
    calendar_payload, calendar_at, next_open, calendar_object = await _calendar(
        session, request.vintage, as_of
    )
    calendar_ref = await session.get(
        ResearchEvidenceRef, UUID(canonical["calendar_evidence_ref_id"])
    )
    calendar_raw = json_bytes(calendar_payload)
    calendar_hash = sha256(calendar_raw).hexdigest()
    available = max(calendar_at, next_open, collection.received_at)
    if (
        canonical["calendar"] != calendar_payload
        or calendar_ref is None
        or calendar_ref.source != "exchange_calendars"
        or calendar_ref.provider_object_id != calendar_object
        or calendar_ref.revision != calendar_hash
        or calendar_ref.content_hash != calendar_hash
        or calendar_ref.byte_length != len(calendar_raw)
        or calendar_ref.available_at != calendar_at
        or calendar_ref.created_at > as_of
        or canonical["available_at"] != available.isoformat()
        or (require_visible and available > as_of)
        or row.calendar_evidence_ref_id != calendar_ref.id
        or row.calendar_source != calendar_ref.source
        or row.calendar_provider_object_id != calendar_object
        or row.calendar_revision != calendar_hash
    ):
        raise ValueError("FRED_CALENDAR_INVALID")
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    observed = row.observed_at.date()
    revision = f"local-{request.vintage}:{observed}:{digest}"
    if (
        artifact is None
        or artifact.source != "fred"
        or artifact.schema_version != SCHEMA
        or artifact.uri != ref.location
        or artifact.byte_size != len(content)
        or artifact.received_at != collection.received_at
        or artifact.available_at != available
        or artifact.created_at > as_of
        or snapshot.received_at != collection.received_at
        or snapshot.available_at != available
        or snapshot.observed_at != _midnight(request.observation_end)
        or row.available_at != available
        or ref.available_at != available
        or row.unit != collection.unit
        or row.series != request.series
        or row.observed_at != _midnight(observed)
        or row.value != dict(collection.observations).get(observed)
        or row.revision != revision
        or row.provider_object_id != f"{request.series}:{revision}"
        or ref.revision != revision
        or ref.provider_object_id != row.provider_object_id
        or ref.source != "fred"
        or row.real_time_start != request.vintage
        or row.real_time_end != request.vintage
        or row.display_sanity_only != (request.series == "DEXHKUS")
    ):
        raise ValueError("FRED_OBSERVATION_INVALID")
    page_hash = next(
        sha256(page.raw_payload).hexdigest()
        for page in collection.pages
        if any(
            item["date"] == observed.isoformat()
            for item in cast(list[dict[str, object]], decode(page.raw_payload)["observations"])
        )
    )
    return FredResearchObservation(
        row.id,
        ref.id,
        calendar_ref.id,
        row.series,
        observed,
        row.value,
        row.unit,
        request.vintage,
        available,
        collection.received_at,
        page_hash,
        digest,
    )


async def read_fred_observations(
    session: AsyncSession,
    *,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
    observation_ids: tuple[UUID, ...] | None = None,
    series: frozenset[str] | None = None,
    vintage: date | None = None,
) -> dict[UUID, FredResearchObservation]:
    """Exact optional evidence scope; invalid optional originals never hide unrelated research."""
    require_utc(as_of)
    if evidence_ids == () or observation_ids == () or series == frozenset():
        return {}
    query = (
        select(MacroObservation, ResearchEvidenceRef, DataSnapshot)
        .join(ResearchEvidenceRef, MacroObservation.evidence_ref_id == ResearchEvidenceRef.id)
        .join(DataSnapshot, DataSnapshot.revision == ResearchEvidenceRef.content_hash)
        .where(
            MacroObservation.source == "fred",
            MacroObservation.authority_version == "research_macro_v1",
            MacroObservation.available_at <= as_of,
            MacroObservation.created_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
            DataSnapshot.created_at <= as_of,
            DataSnapshot.source == "fred",
            DataSnapshot.schema_version == SCHEMA,
        )
        .order_by(
            MacroObservation.observed_at.desc(),
            MacroObservation.available_at.desc(),
            MacroObservation.id,
        )
    )
    if evidence_ids is not None:
        query = query.where(ResearchEvidenceRef.id.in_(evidence_ids))
    if observation_ids is not None:
        query = query.where(MacroObservation.id.in_(observation_ids))
    if series is not None:
        query = query.where(MacroObservation.series.in_(series))
    if vintage is not None:
        query = query.where(
            MacroObservation.real_time_start == vintage,
            MacroObservation.real_time_end == vintage,
        )
    rows = (await session.execute(query.limit(512))).all()
    result = {}
    for row, ref, snapshot in rows:
        try:
            result[ref.id] = await _read_one(session, row, ref, snapshot, as_of)
        except (ValueError, TypeError, KeyError, AttributeError, StopIteration, OverflowError):
            continue
    return result

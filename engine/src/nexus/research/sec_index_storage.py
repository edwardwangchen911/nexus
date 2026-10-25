"""Append and revalidate complete SEC index captures, without investment inference."""

import json
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.research.identity_sources import (
    directory_binding,
    parse_identity_source,
    require_submissions_binding,
)
from nexus.research.sec_disclosure_index import SecDisclosureIndex, build_disclosure_index
from nexus.research.sec_exhibit_storage import ExhibitStore, _read_identity

SOURCE = "sec_disclosure_index"
SCHEMA: Literal["sec_disclosure_index_v1"] = "sec_disclosure_index_v1"
DIRECTORY_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


class IndexArchiveBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    artifact_id: UUID
    uri: str = Field(min_length=1, max_length=2048)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_size: int = Field(ge=1, le=8 * 1024 * 1024)


class IndexCapture(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["sec_disclosure_index_v1"] = SCHEMA
    index: SecDisclosureIndex
    originals: tuple[
        IndexArchiveBinding, IndexArchiveBinding, IndexArchiveBinding, IndexArchiveBinding
    ]

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def _receipt(raw: bytes, receipt: bytes, *, source_url: str, now: datetime) -> datetime:
    if len(raw) > 8 * 1024 * 1024 or len(receipt) > 65536:
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    value = parse_identity_source(receipt)
    required = {"source_url", "received_at", "sha256", "byte_length", "http_status"}
    if set(value) not in (required, required | {"schema"}) or (
        "schema" in value and value["schema"] != "sec_identity_source_receipt_v1"
    ):
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    if (
        value["source_url"] != source_url
        or type(value["http_status"]) is not int
        or value["http_status"] != 200
        or type(value["byte_length"]) is not int
        or value["byte_length"] != len(raw)
        or value["sha256"] != sha256(raw).hexdigest()
        or not isinstance(value["received_at"], str)
    ):
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    received = require_utc(datetime.fromisoformat(value["received_at"]))
    if received > now:
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    return received


async def _original(store: ExhibitStore, binding: IndexArchiveBinding) -> bytes:
    path = urlsplit(binding.uri).path
    if "/raw/" not in path:
        raise ValueError("SEC_INDEX_ARTIFACT_INVALID")
    raw = await store.read_raw("raw/" + path.split("/raw/", 1)[1], uri=binding.uri)
    if len(raw) != binding.byte_size or sha256(raw).hexdigest() != binding.sha256:
        raise ValueError("SEC_INDEX_ARTIFACT_INVALID")
    return raw


async def _archive(
    session: AsyncSession,
    store: ExhibitStore,
    raw: bytes,
    *,
    now: datetime,
    received: datetime,
    receipt: bool,
) -> IndexArchiveBinding:
    digest = sha256(raw).hexdigest()
    key = f"raw/sec/disclosure-index/{digest}.json"
    uri = await store.put_if_absent(key=key, content=raw)
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(raw),
            mime_type="application/json",
            schema_version="sec_index_receipt_v1" if receipt else "sec_index_raw_v1",
            source="sec",
            observed_at=received,
            available_at=now,
            received_at=now,
        )
        .on_conflict_do_nothing(index_elements=[DataArtifact.sha256])
    )
    row = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if (
        row is None
        or row.source != "sec"
        or row.byte_size != len(raw)
        or max(row.created_at, row.available_at, row.received_at, row.observed_at) > now
    ):
        raise ValueError("SEC_INDEX_ARTIFACT_INVALID")
    # SHA-256 is globally unique. An unchanged source can reuse its first archived
    # bytes; the separate new HTTP receipt proves this capture's actual fetch time.
    binding = IndexArchiveBinding(
        artifact_id=row.id, uri=row.uri, sha256=digest, byte_size=len(raw)
    )
    await _original(store, binding)
    return binding


async def persist_index_archive(
    session: AsyncSession,
    store: ExhibitStore,
    *,
    identity_snapshot_id: UUID,
    directory: tuple[bytes, bytes],
    submissions: tuple[bytes, bytes],
    now: datetime,
) -> tuple[DataSnapshot, bool]:
    now = require_utc(now)
    identity = await _read_identity(session, store, snapshot_id=identity_snapshot_id, as_of=now)
    received = _receipt(*directory, source_url=DIRECTORY_URL, now=now)
    mapping = parse_identity_source(directory[0])
    cik, exchange = directory_binding(mapping, identity.identity.symbol)
    if cik != identity.identity.cik or exchange != identity.identity.exchange:
        raise ValueError("SEC_INDEX_IDENTITY_INVALID")
    require_submissions_binding(
        parse_identity_source(submissions[0]), identity.identity.symbol, cik, exchange
    )
    document = build_disclosure_index(
        *submissions,
        instrument_id=identity.instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=cik,
    )
    if document.received_at > now:
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    capture_key = f"{identity.instrument_id}:{sha256(submissions[1]).hexdigest()}"
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": f"sec-index:{capture_key}"},
    )
    originals = []
    for pair, captured in ((directory, received), (submissions, document.received_at)):
        for position, raw in enumerate(pair):
            originals.append(
                await _archive(
                    session, store, raw, now=now, received=captured, receipt=position == 1
                )
            )
    published_at = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(published_at, datetime) or published_at < now:
        raise ValueError("SEC_INDEX_CLOCK_UNAVAILABLE")
    now = published_at
    capture = IndexCapture(
        index=document, originals=(originals[0], originals[1], originals[2], originals[3])
    )
    digest = sha256(capture.canonical()).hexdigest()
    existing = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == capture_key,
            DataSnapshot.revision == digest,
        )
    )
    if existing is not None:
        await read_index_capture(session, store, snapshot_id=existing.id, as_of=now)
        return existing, False
    row = DataSnapshot(
        source=SOURCE,
        provider_object_id=capture_key,
        revision=digest,
        schema_version=SCHEMA,
        observed_at=document.received_at,
        available_at=now,
        received_at=now,
        payload=capture.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="sec-index",
        action="research.monitor.sec_index_received",
        schema_version=SCHEMA,
        occurred_at=now,
        payload={
            "snapshot_id": str(row.id),
            "instrument_id": str(identity.instrument_id),
            "raw_hash": document.raw_content_hash,
        },
    )
    return row, True


async def read_index_capture(
    session: AsyncSession,
    store: ExhibitStore,
    *,
    snapshot_id: UUID,
    as_of: datetime,
) -> tuple[DataSnapshot, SecDisclosureIndex]:
    as_of = require_utc(as_of)
    row = await session.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or max(row.created_at, row.observed_at, row.available_at, row.received_at) > as_of
    ):
        raise ValueError("SEC_INDEX_CAPTURE_UNAVAILABLE")
    capture = IndexCapture.model_validate(row.payload)
    if sha256(capture.canonical()).hexdigest() != row.revision:
        raise ValueError("SEC_INDEX_CAPTURE_INVALID")
    originals = []
    # These rows are immutable in storage. Fetch only scalars so the fast path
    # does not populate the caller's identity map or change get/autoflush behavior.
    artifact_values: dict[
        UUID, Row[tuple[UUID, str, str, str, int, datetime, datetime, datetime]]
    ] = {}
    keys = [
        session.identity_key(DataArtifact, binding.artifact_id) for binding in capture.originals
    ]
    if not (
        session.new
        or session.dirty
        or session.deleted
        or any(key in session.identity_map for key in keys)
    ):
        artifact_values = {
            value.id: value
            for value in await session.execute(
                select(
                    DataArtifact.id,
                    DataArtifact.source,
                    DataArtifact.uri,
                    DataArtifact.sha256,
                    DataArtifact.byte_size,
                    DataArtifact.created_at,
                    DataArtifact.available_at,
                    DataArtifact.received_at,
                ).where(DataArtifact.id.in_([binding.artifact_id for binding in capture.originals]))
            )
        }
    for binding in capture.originals:
        artifact: (
            DataArtifact | Row[tuple[UUID, str, str, str, int, datetime, datetime, datetime]] | None
        )
        # A store read awaits caller code; re-check state on every iteration.
        if (
            session.new
            or session.dirty
            or session.deleted
            or session.identity_key(DataArtifact, binding.artifact_id) in session.identity_map
            or binding.artifact_id not in artifact_values
        ):
            artifact = await session.get(DataArtifact, binding.artifact_id)
        else:
            artifact = artifact_values[binding.artifact_id]
        if (
            artifact is None
            or artifact.source != "sec"
            or (artifact.uri, artifact.sha256, artifact.byte_size)
            != (binding.uri, binding.sha256, binding.byte_size)
            or max(artifact.created_at, artifact.available_at, artifact.received_at)
            > row.available_at
        ):
            raise ValueError("SEC_INDEX_ARTIFACT_INVALID")
        originals.append(await _original(store, binding))
    identity = await _read_identity(
        session, store, snapshot_id=capture.index.identity_snapshot_id, as_of=row.available_at
    )
    _receipt(originals[0], originals[1], source_url=DIRECTORY_URL, now=row.available_at)
    cik, exchange = directory_binding(parse_identity_source(originals[0]), identity.identity.symbol)
    if (cik, exchange) != (identity.identity.cik, identity.identity.exchange):
        raise ValueError("SEC_INDEX_IDENTITY_INVALID")
    require_submissions_binding(
        parse_identity_source(originals[2]), identity.identity.symbol, cik, exchange
    )
    rebuilt = build_disclosure_index(
        originals[2],
        originals[3],
        instrument_id=identity.instrument_id,
        identity_snapshot_id=capture.index.identity_snapshot_id,
        issuer_cik=cik,
    )
    if (
        rebuilt != capture.index
        or row.observed_at != rebuilt.received_at
        or (
            row.provider_object_id != f"{identity.instrument_id}:{sha256(originals[3]).hexdigest()}"
        )
    ):
        raise ValueError("SEC_INDEX_CAPTURE_INVALID")
    return row, rebuilt


async def compare_index_captures(
    session: AsyncSession,
    store: ExhibitStore,
    *,
    baseline_id: UUID,
    current_id: UUID,
    baseline_as_of: datetime,
    now: datetime,
) -> dict[str, object]:
    now, baseline_as_of = require_utc(now), require_utc(baseline_as_of)
    if baseline_as_of > now:
        raise ValueError("SEC_INDEX_BASELINE_INVALID")
    baseline_row, baseline = await read_index_capture(
        session, store, snapshot_id=baseline_id, as_of=baseline_as_of
    )
    current_row, current = await read_index_capture(
        session, store, snapshot_id=current_id, as_of=now
    )
    if (baseline.instrument_id, baseline.issuer_cik) != (current.instrument_id, current.issuer_cik):
        raise ValueError("SEC_INDEX_IDENTITY_INVALID")
    result: dict[str, object] = {
        "result": "unknown",
        "reason": "SEC_INDEX_COVERAGE_UNAVAILABLE",
        "event_ids": [],
        "execution_locked": True,
        "model_requests": 0,
        "provider_requests": 0,
    }
    if current.received_at < baseline.received_at or now - current.received_at > timedelta(
        minutes=5
    ):
        return {**result, "reason": "SEC_INDEX_STALE"}
    previous = {entry.accession_number: entry for entry in baseline.entries}
    for entry in current.entries:
        if entry.accession_number in previous and previous[entry.accession_number] != entry:
            return {**result, "reason": "SEC_INDEX_EVENT_CONFLICT"}
    added = [entry for entry in current.entries if entry.accession_number not in previous]
    # Recent rows prove positives, but not absence in unread historical shards.
    complete = current.history_complete
    return {
        **result,
        "result": "true" if added else "false" if complete else "unknown",
        "reason": None if added or complete else result["reason"],
        "event_ids": [f"sec:{current.issuer_cik}:{entry.accession_number}" for entry in added],
        "history_coverage": "complete" if complete else "partial",
        # Research work capacity cannot erase an established disclosure fact.
        "automatic_review_capacity": "blocked" if len(added) > 32 else "within_limit",
        "impact_review": "not_performed",
        "baseline_snapshot_id": str(baseline_row.id),
        "snapshot_id": str(current_row.id),
    }

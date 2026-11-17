"""Archive and revalidate durable SEC metadata without discarding independent rows."""

import json
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.research.durable_sec_index import DurableIndexEvidence, build_durable_index
from nexus.research.identity_sources import (
    directory_binding,
    directory_binding_from_raw,
    parse_identity_source,
    require_submissions_binding,
    require_submissions_from_raw,
)
from nexus.research.sec_exhibit_storage import ExhibitStore, _read_identity
from nexus.research.sec_index_storage import (
    IndexArchiveBinding,
    _archive,
    _original,
    _receipt,
)

SOURCE = "sec_durable_disclosure_index"
SCHEMA: Literal["sec_durable_index_v1"] = "sec_durable_index_v1"
DIRECTORY_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


class DurableIndexCapture(DurableIndexEvidence):
    schema_version: Literal["sec_durable_index_v1"] = SCHEMA
    originals: tuple[
        IndexArchiveBinding, IndexArchiveBinding, IndexArchiveBinding, IndexArchiveBinding
    ]

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


async def persist_durable_index_archive(
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
    document = build_durable_index(
        *submissions,
        instrument_id=identity.instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=cik,
        now=now,
    )
    if document.index.received_at > now:
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    capture_key = f"{identity.instrument_id}:{sha256(submissions[1]).hexdigest()}"
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": f"sec-index-current:{identity.instrument_id}"},
    )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": f"sec-durable-index:{capture_key}"},
    )
    originals = []
    for pair, captured in ((directory, received), (submissions, document.index.received_at)):
        for position, raw in enumerate(pair):
            originals.append(
                await _archive(
                    session, store, raw, now=now, received=captured, receipt=position == 1
                )
            )
    published_at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
    if not isinstance(published_at, datetime) or published_at < now:
        raise ValueError("SEC_INDEX_CLOCK_UNAVAILABLE")
    now = published_at
    capture = DurableIndexCapture(
        **document.model_dump(), originals=(originals[0], originals[1], originals[2], originals[3])
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
        await read_durable_index_capture(session, store, snapshot_id=existing.id, as_of=now)
        return existing, False
    row = DataSnapshot(
        source=SOURCE,
        provider_object_id=capture_key,
        revision=digest,
        schema_version=SCHEMA,
        observed_at=document.index.received_at,
        available_at=now,
        received_at=now,
        payload=capture.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="sec-durable-index",
        action="research.monitor.sec_durable_index_received",
        schema_version=SCHEMA,
        occurred_at=now,
        payload={
            "snapshot_id": str(row.id),
            "instrument_id": str(identity.instrument_id),
            "raw_hash": document.raw_content_hash,
        },
    )
    return row, True


async def read_durable_index_capture(
    session: AsyncSession,
    store: ExhibitStore,
    *,
    snapshot_id: UUID,
    as_of: datetime,
) -> tuple[DataSnapshot, DurableIndexCapture]:
    as_of = require_utc(as_of)
    row = await session.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or max(row.created_at, row.observed_at, row.available_at, row.received_at) > as_of
    ):
        raise ValueError("SEC_INDEX_CAPTURE_UNAVAILABLE")
    capture = DurableIndexCapture.model_validate(row.payload)
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
    cik, exchange = directory_binding_from_raw(originals[0], identity.identity.symbol)
    if (cik, exchange) != (identity.identity.cik, identity.identity.exchange):
        raise ValueError("SEC_INDEX_IDENTITY_INVALID")
    require_submissions_from_raw(originals[2], identity.identity.symbol, cik, exchange)
    rebuilt = build_durable_index(
        originals[2],
        originals[3],
        instrument_id=identity.instrument_id,
        identity_snapshot_id=capture.index.identity_snapshot_id,
        issuer_cik=cik,
        now=row.available_at,
    )
    if (
        rebuilt
        != DurableIndexEvidence.model_validate(
            capture.model_dump(exclude={"originals", "schema_version"})
        )
        or row.observed_at != rebuilt.index.received_at
        or (
            row.provider_object_id != f"{identity.instrument_id}:{sha256(originals[3]).hexdigest()}"
        )
    ):
        raise ValueError("SEC_INDEX_CAPTURE_INVALID")
    return row, capture

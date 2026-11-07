"""Explicit new-source custody for listed history; v1 plans cannot consume it implicitly."""

import json
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.research.identity_sources import (
    directory_binding,
    directory_binding_from_raw,
    parse_identity_source,
    require_submissions_binding,
    require_submissions_from_raw,
)
from nexus.research.sec_disclosure_index import MAX_RAW_BYTES
from nexus.research.sec_exhibit_storage import ExhibitStore, _read_identity
from nexus.research.sec_index_storage import (
    DIRECTORY_URL,
    IndexArchiveBinding,
    _archive,
    _original,
    _receipt,
)
from nexus.research.sec_listed_history import ListedHistoryIndex, build_listed_index

SOURCE = "sec_disclosure_listed_index"
SCHEMA = "sec_disclosure_index_v2"


class ListedCapture(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["sec_disclosure_index_v2"] = "sec_disclosure_index_v2"
    index: ListedHistoryIndex
    history_names: tuple[str, ...]
    originals: tuple[IndexArchiveBinding, ...]

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def capture_key(instrument_id: UUID, originals: list[bytes]) -> str:
    # Bind every receipt: the same parent with changed history is a different capture.
    hashes = [sha256(x).hexdigest() for x in originals]
    return f"{instrument_id}:v2:{sha256(json.dumps(hashes).encode()).hexdigest()}"


async def persist_listed_archive(
    session: AsyncSession,
    store: ExhibitStore,
    *,
    identity_snapshot_id: UUID,
    directory: tuple[bytes, bytes],
    submissions: tuple[bytes, bytes],
    closing: tuple[bytes, bytes],
    history: tuple[tuple[str, bytes, bytes], ...],
    now: datetime,
) -> tuple[DataSnapshot, bool]:
    now = require_utc(now)
    identity = await _read_identity(session, store, snapshot_id=identity_snapshot_id, as_of=now)
    directory_received = _receipt(*directory, source_url=DIRECTORY_URL, now=now)
    cik, exchange = directory_binding(parse_identity_source(directory[0]), identity.identity.symbol)
    if (cik, exchange) != (identity.identity.cik, identity.identity.exchange):
        raise ValueError("SEC_INDEX_IDENTITY_INVALID")
    require_submissions_binding(
        parse_identity_source(submissions[0]), identity.identity.symbol, cik, exchange
    )
    index = build_listed_index(
        parent=submissions,
        closing=closing,
        history=history,
        instrument_id=identity.instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=cik,
    )
    if index.received_at > now:
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    pairs = [directory, submissions, closing, *((raw, proof) for _, raw, proof in history)]
    raw_originals = [raw for pair in pairs for raw in pair]
    if sum(map(len, raw_originals)) > MAX_RAW_BYTES:
        raise ValueError("SEC_INDEX_HISTORY_LIMIT")
    key = capture_key(identity.instrument_id, raw_originals)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"), {"key": f"sec-index:{key}"}
    )
    originals = []
    for i, pair in enumerate(pairs):
        received = (
            directory_received
            if i == 0
            else require_utc(datetime.fromisoformat(parse_identity_source(pair[1])["received_at"]))
        )
        for position, raw in enumerate(pair):
            originals.append(
                await _archive(
                    session, store, raw, now=now, received=received, receipt=position == 1
                )
            )
    published = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(published, datetime) or published < now:
        raise ValueError("SEC_INDEX_CLOCK_UNAVAILABLE")
    capture = ListedCapture(
        index=index, history_names=tuple(x[0] for x in history), originals=tuple(originals)
    )
    digest = sha256(capture.canonical()).hexdigest()
    previous = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == key,
            DataSnapshot.revision == digest,
        )
    )
    if previous is not None:
        await read_listed_capture(session, store, snapshot_id=previous.id, as_of=published)
        return previous, False
    row = DataSnapshot(
        source=SOURCE,
        provider_object_id=key,
        revision=digest,
        schema_version=SCHEMA,
        observed_at=index.received_at,
        available_at=published,
        received_at=published,
        payload=capture.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="sec-index",
        action="research.monitor.sec_index_received",
        schema_version=SCHEMA,
        occurred_at=published,
        payload={
            "snapshot_id": str(row.id),
            "instrument_id": str(identity.instrument_id),
            "raw_hash": index.raw_content_hash,
        },
    )
    return row, True


async def read_listed_capture(
    session: AsyncSession, store: ExhibitStore, *, snapshot_id: UUID, as_of: datetime
) -> tuple[DataSnapshot, ListedHistoryIndex]:
    as_of = require_utc(as_of)
    row = await session.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or max(row.created_at, row.observed_at, row.available_at, row.received_at) > as_of
    ):
        raise ValueError("SEC_INDEX_CAPTURE_UNAVAILABLE")
    capture = ListedCapture.model_validate(row.payload)
    if (
        sha256(capture.canonical()).hexdigest() != row.revision
        or len(capture.history_names) > 1
        or len(capture.originals) != 6 + 2 * len(capture.history_names)
    ):
        raise ValueError("SEC_INDEX_CAPTURE_INVALID")
    if sum(x.byte_size for x in capture.originals) > MAX_RAW_BYTES:
        raise ValueError("SEC_INDEX_HISTORY_LIMIT")
    originals = []
    for binding in capture.originals:
        artifact = await session.get(DataArtifact, binding.artifact_id)
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
    rebuilt = build_listed_index(
        parent=(originals[2], originals[3]),
        closing=(originals[4], originals[5]),
        history=tuple(
            (name, originals[6 + 2 * i], originals[7 + 2 * i])
            for i, name in enumerate(capture.history_names)
        ),
        instrument_id=identity.instrument_id,
        identity_snapshot_id=capture.index.identity_snapshot_id,
        issuer_cik=cik,
    )
    if (
        rebuilt != capture.index
        or row.observed_at != rebuilt.received_at
        or row.provider_object_id != capture_key(identity.instrument_id, originals)
    ):
        raise ValueError("SEC_INDEX_CAPTURE_INVALID")
    return row, rebuilt

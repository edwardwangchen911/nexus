"""Exact original custody with an explicit, non-consumable GRAPHIC evidence gap."""

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.filing_archive import _save_original
from nexus.monitoring.filing_content import (
    MAX_ARCHIVE_BYTES,
    _Inventory,
    archival_document_inventory,
    filing_root,
)
from nexus.monitoring.filing_transport import prove_document
from nexus.monitoring.sec_index_reader import read_monitor_index
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.research.sec_index_storage import IndexArchiveBinding, _original, _receipt

SOURCE = "nexus_sec_uninterpreted_filing"
SCHEMA: Literal["sec_uninterpreted_filing_v1"] = "sec_uninterpreted_filing_v1"


class OriginalArchive(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["sec_uninterpreted_filing_v1"] = SCHEMA
    index_snapshot_id: UUID
    index_hash: str
    issuer_cik: str
    instrument_id: UUID
    entry: SecIndexEntry
    originals: dict[str, tuple[IndexArchiveBinding, IndexArchiveBinding]]
    declarations: list[dict[str, object]]
    research_qualified: Literal[False] = False
    nontext_content_status: Literal["uninterpreted"] = "uninterpreted"

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def verify_originals(
    *,
    entry: SecIndexEntry,
    issuer_cik: str,
    originals: dict[str, tuple[bytes, bytes]],
    now: datetime,
) -> list[dict[str, object]]:
    now = require_utc(now)
    inventory_name = entry.accession_number + "-index.html"
    submission_name = entry.accession_number + ".txt"
    if inventory_name not in originals or submission_name not in originals:
        raise ValueError("SEC_MONITOR_DOCUMENT_SET_INVALID")
    rows = archival_document_inventory(
        originals[inventory_name][0], entry=entry, issuer_cik=issuer_cik
    )
    if not any(kind == "GRAPHIC" for _, kind, _ in rows) or set(originals) != {
        inventory_name,
        submission_name,
        *(name for name, _, _ in rows),
    }:
        raise ValueError("SEC_MONITOR_DOCUMENT_SET_INVALID")
    if sum(len(pair[0]) for pair in originals.values()) > MAX_ARCHIVE_BYTES:
        raise ValueError("SEC_MONITOR_CONTENT_CAPACITY")
    root = filing_root(issuer_cik, entry.accession_number)
    received = {}
    for name, (raw, receipt) in originals.items():
        received[name] = _receipt(raw, receipt, source_url=root + name, now=now)
        if received[name] < max(entry.accepted_at, entry.filed_at):
            raise ValueError("SEC_MONITOR_DOCUMENT_TIME_INVALID")
    parser = _Inventory()
    parser.feed(originals[inventory_name][0].decode("utf-8-sig"))
    parser.close()
    sequences = {row[2][2][0]: int(row[0][0]) for row in parser.rows if row[0][0]}
    result = []
    for name, kind, size in rows:
        raw, receipt = originals[name]
        if not 0 < size <= MAX_ARCHIVE_BYTES:
            raise ValueError("SEC_MONITOR_DOCUMENT_SIZE_INVALID")
        if kind == "GRAPHIC":
            signatures = {
                ".jpg": (b"\xff\xd8",),
                ".jpeg": (b"\xff\xd8",),
                ".png": (b"\x89PNG\r\n\x1a\n",),
                ".gif": (b"GIF87a", b"GIF89a"),
                ".webp": (b"RIFF",),
            }
            suffix = Path(name).suffix
            if (
                len(raw) != size
                or not raw.startswith(signatures[suffix])
                or (suffix == ".webp" and raw[8:12] != b"WEBP")
            ):
                raise ValueError("SEC_MONITOR_DOCUMENT_FORMAT_UNSUPPORTED")
            relation = "unverified"
        else:
            prove_document(
                originals[submission_name][0],
                raw,
                accession=entry.accession_number,
                issuer_cik=issuer_cik,
                form=entry.form,
                filename=name,
                kind=kind,
                declared_size=size,
            )
            relation = "verified"
        result.append(
            dict(
                sequence=sequences[name],
                filename=name,
                document_type=kind,
                source_url=root + name,
                declared_size=size,
                raw_hash=sha256(raw).hexdigest(),
                receipt_hash=sha256(receipt).hexdigest(),
                received_at=received[name].isoformat(),
                submission_relation=relation,
                content_qualification="uninterpreted" if kind == "GRAPHIC" else "not_assessed",
            )
        )
    return result


def _local_pair(directory: Path, name: str) -> tuple[bytes, bytes]:
    paths = (directory / name, directory / (name + ".receipt.json"))
    if (
        any(
            p.is_symlink() or not p.is_file() or not 0 < p.stat().st_size <= MAX_ARCHIVE_BYTES
            for p in paths
        )
        or paths[1].stat().st_size > 65536
    ):
        raise ValueError("SEC_MONITOR_ARCHIVE_UNAVAILABLE")
    return paths[0].read_bytes(), paths[1].read_bytes()


async def import_originals(
    session: AsyncSession,
    *,
    index_snapshot_id: UUID,
    accession: str,
    archive_dir: Path,
    now: datetime,
) -> DataSnapshot:
    store = configured_exhibit_store(session)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,0))"),
        {"scope": f"sec-monitor-content:{index_snapshot_id}:{accession}"},
    )
    source, index = await read_monitor_index(
        session, store, snapshot_id=index_snapshot_id, as_of=now
    )
    matches = [e for e in index.entries if e.accession_number == accession]
    if len(matches) != 1:
        raise ValueError("SEC_MONITOR_ENTRY_INVALID")
    entry = matches[0]
    inventory_name = accession + "-index.html"
    inventory = _local_pair(archive_dir, inventory_name)
    rows = archival_document_inventory(inventory[0], entry=entry, issuer_cik=index.issuer_cik)
    pairs = {
        inventory_name: inventory,
        accession + ".txt": _local_pair(archive_dir, accession + ".txt"),
        **{name: _local_pair(archive_dir, name) for name, _, _ in rows},
    }
    declarations = verify_originals(
        entry=entry, issuer_cik=index.issuer_cik, originals=pairs, now=now
    )
    originals = {}
    for name, (raw, receipt) in pairs.items():
        received = _receipt(
            raw, receipt, source_url=filing_root(index.issuer_cik, accession) + name, now=now
        )
        originals[name] = (
            await _save_original(session, store, raw, received=received, now=now),
            await _save_original(session, store, receipt, received=received, now=now),
        )
    document = OriginalArchive(
        index_snapshot_id=source.id,
        index_hash=source.revision,
        issuer_cik=index.issuer_cik,
        instrument_id=index.instrument_id,
        entry=entry,
        originals=originals,
        declarations=declarations,
    )
    digest = sha256(document.canonical()).hexdigest()
    prior = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == f"{index.issuer_cik}:{accession}",
            DataSnapshot.revision == digest,
        )
    )
    if prior is not None:
        await read_originals(
            session,
            snapshot_id=prior.id,
            as_of=await session.scalar(text("SELECT clock_timestamp()")),
        )
        return prior
    at = await session.scalar(text("SELECT clock_timestamp()"))
    if not isinstance(at, datetime) or at < now:
        raise ValueError("SEC_MONITOR_CLOCK_UNAVAILABLE")
    row = DataSnapshot(
        source=SOURCE,
        schema_version=SCHEMA,
        provider_object_id=f"{index.issuer_cik}:{accession}",
        revision=digest,
        payload=document.model_dump(mode="json"),
        created_at=at,
        observed_at=entry.accepted_at,
        available_at=at,
        received_at=at,
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.uninterpreted_originals_received",
        schema_version=SCHEMA,
        occurred_at=at,
        payload={
            "snapshot_id": str(row.id),
            "canonical_hash": digest,
            "index_snapshot_id": str(source.id),
            "research_qualified": False,
        },
    )
    return row


async def read_originals(
    session: AsyncSession, *, snapshot_id: UUID, as_of: datetime
) -> OriginalArchive:
    as_of = require_utc(as_of)
    row = await session.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or max(row.created_at, row.observed_at, row.received_at, row.available_at) > as_of
    ):
        raise ValueError("SEC_MONITOR_ARCHIVE_UNAVAILABLE")
    document = OriginalArchive.model_validate(row.payload)
    if (
        sha256(document.canonical()).hexdigest() != row.revision
        or row.provider_object_id != f"{document.issuer_cik}:{document.entry.accession_number}"
        or row.observed_at != document.entry.accepted_at
    ):
        raise ValueError("SEC_MONITOR_ARCHIVE_INVALID")
    store = configured_exhibit_store(session)
    source, index = await read_monitor_index(
        session, store, snapshot_id=document.index_snapshot_id, as_of=as_of
    )
    if (
        source.revision != document.index_hash
        or index.instrument_id != document.instrument_id
        or index.issuer_cik != document.issuer_cik
        or document.entry not in index.entries
    ):
        raise ValueError("SEC_MONITOR_ARCHIVE_INVALID")
    pairs = {}
    for name, bindings in document.originals.items():
        raw = []
        for b in bindings:
            a = await session.get(DataArtifact, b.artifact_id)
            if (
                a is None
                or (a.uri, a.sha256, a.byte_size) != (b.uri, b.sha256, b.byte_size)
                or a.source != "sec"
                or max(a.created_at, a.observed_at, a.received_at, a.available_at) > as_of
            ):
                raise ValueError("SEC_MONITOR_ARTIFACT_INVALID")
            raw.append(await _original(store, b))
        pairs[name] = (raw[0], raw[1])
    if (
        verify_originals(
            entry=document.entry, issuer_cik=document.issuer_cik, originals=pairs, now=as_of
        )
        != document.declarations
    ):
        raise ValueError("SEC_MONITOR_ARCHIVE_INVALID")
    return document

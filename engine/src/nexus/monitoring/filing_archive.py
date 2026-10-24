"""Normal local custody for exact monitor filing documents, separate from accounting facts."""

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.filing_content import (
    FilingDocument,
    document_inventory,
    filing_root,
    verify_filing_documents,
)
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_exhibit_storage import ExhibitStore, configured_exhibit_store
from nexus.research.sec_index_storage import (
    IndexArchiveBinding,
    _original,
    _receipt,
    read_index_capture,
)

SOURCE = "nexus_sec_monitor_filing"
SCHEMA: Literal["sec_monitor_filing_v1"] = "sec_monitor_filing_v1"


class FilingArchive(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["sec_monitor_filing_v1"] = SCHEMA
    instrument_id: UUID
    issuer_cik: str
    index_snapshot_id: UUID
    index_hash: str
    entry: SecIndexEntry
    accounting_revision_status: Literal["unverified"] = "unverified"
    amendment_relation_status: Literal["not_assessed"] = "not_assessed"
    originals: dict[str, tuple[IndexArchiveBinding, IndexArchiveBinding]]
    documents: tuple[FilingDocument, ...]

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class VerifiedFilingContent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    snapshot_id: UUID
    evidence_ref_id: UUID
    canonical_hash: str
    archive: FilingArchive


async def _save_original(
    session: AsyncSession, store: ExhibitStore, raw: bytes, *, received: datetime, now: datetime
) -> IndexArchiveBinding:
    digest = sha256(raw).hexdigest()
    uri = await store.put_if_absent(key=f"raw/sec/monitor-filing/{digest}", content=raw)
    archived = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(archived, datetime) or archived < now:
        raise ValueError("SEC_MONITOR_CLOCK_UNAVAILABLE")
    now = archived
    await session.execute(
        insert(DataArtifact)
        .values(
            created_at=now,
            uri=uri,
            sha256=digest,
            byte_size=len(raw),
            mime_type="application/octet-stream",
            schema_version="sec_monitor_original_v1",
            source="sec",
            observed_at=received,
            available_at=now,
            received_at=received,
        )
        .on_conflict_do_nothing(index_elements=[DataArtifact.sha256])
    )
    row = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if (
        row is None
        or row.source != "sec"
        or row.byte_size != len(raw)
        or max(row.observed_at, row.available_at, row.received_at, row.created_at) > now
    ):
        raise ValueError("SEC_MONITOR_ARTIFACT_INVALID")
    binding = IndexArchiveBinding(
        artifact_id=row.id, uri=row.uri, sha256=digest, byte_size=row.byte_size
    )
    await _original(store, binding)
    return binding


async def import_filing_content(
    session: AsyncSession,
    *,
    index_snapshot_id: UUID,
    accession: str,
    archive_dir: Path,
    now: datetime,
) -> tuple[VerifiedFilingContent, bool]:
    now = require_utc(now)
    store = configured_exhibit_store(session)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,0))"),
        {"scope": f"sec-monitor-content:{index_snapshot_id}:{accession}"},
    )
    clock = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(clock, datetime) or clock < now:
        raise ValueError("SEC_MONITOR_CLOCK_UNAVAILABLE")
    now = clock
    source, index = await read_index_capture(
        session, store, snapshot_id=index_snapshot_id, as_of=now
    )
    matches = [row for row in index.entries if row.accession_number == accession]
    if len(matches) != 1:
        raise ValueError("SEC_MONITOR_ENTRY_UNAVAILABLE")
    entry = matches[0]
    root = filing_root(index.issuer_cik, entry.accession_number)
    inventory_name = entry.accession_number + "-index.html"

    def read(name: str) -> tuple[bytes, bytes]:
        paths = (archive_dir / name, archive_dir / (name + ".receipt.json"))
        if any(
            path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024 * 1024
            for path in paths
        ):
            raise ValueError("SEC_MONITOR_ARCHIVE_UNAVAILABLE")
        if paths[1].stat().st_size > 65536:
            raise ValueError("SEC_MONITOR_ARCHIVE_UNAVAILABLE")
        return paths[0].read_bytes(), paths[1].read_bytes()

    inventory = read(inventory_name)
    rows = document_inventory(inventory[0], entry=entry, issuer_cik=index.issuer_cik)
    bodies = {name: read(name) for name, _, _ in rows}
    documents = verify_filing_documents(
        entry=entry, issuer_cik=index.issuer_cik, inventory=inventory, documents=bodies, now=now
    )
    originals = {}
    for name, (raw, receipt) in {inventory_name: inventory, **bodies}.items():
        received = _receipt(raw, receipt, source_url=root + name, now=now)
        originals[name] = (
            await _save_original(session, store, raw, received=received, now=now),
            await _save_original(session, store, receipt, received=received, now=now),
        )
    document = FilingArchive(
        instrument_id=index.instrument_id,
        issuer_cik=index.issuer_cik,
        index_snapshot_id=source.id,
        index_hash=source.revision,
        entry=entry,
        originals=originals,
        documents=documents,
    )
    raw = document.canonical()
    digest = sha256(raw).hexdigest()
    prior = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == f"{index.issuer_cik}:{accession}",
            DataSnapshot.revision == digest,
        )
    )
    if prior is not None:
        verified = await read_filing_content(session, snapshot_id=prior.id, as_of=now)
        if verified is None:
            raise ValueError("SEC_MONITOR_ARCHIVE_INVALID")
        return verified, False
    uri = await store.put_if_absent(
        key=f"evidence/monitor-filing/{index.issuer_cik}/{accession}/sha256/{digest}.json",
        content=raw,
    )
    published = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(published, datetime) or published < now:
        raise ValueError("SEC_MONITOR_CLOCK_UNAVAILABLE")
    now = published
    evidence = ResearchEvidenceRef(
        created_at=now,
        source=SOURCE,
        provider_object_id=f"{index.issuer_cik}:{accession}",
        revision=digest,
        available_at=now,
        content_hash=digest,
        byte_length=len(raw),
        location=uri,
    )
    snapshot = DataSnapshot(
        created_at=now,
        source=SOURCE,
        provider_object_id=evidence.provider_object_id,
        revision=digest,
        observed_at=entry.accepted_at,
        available_at=now,
        received_at=now,
        schema_version=SCHEMA,
        payload=document.model_dump(mode="json"),
    )
    session.add_all((evidence, snapshot))
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.filing_content_received",
        schema_version=SCHEMA,
        occurred_at=now,
        payload={
            "snapshot_id": str(snapshot.id),
            "canonical_hash": digest,
            "index_snapshot_id": str(source.id),
        },
    )
    result = await read_filing_content(session, snapshot_id=snapshot.id, as_of=now)
    if result is None:
        raise ValueError("SEC_MONITOR_ARCHIVE_INVALID")
    return result, True


async def read_filing_content(
    session: AsyncSession, *, snapshot_id: UUID, as_of: datetime
) -> VerifiedFilingContent | None:
    as_of = require_utc(as_of)
    row = await session.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or max(row.created_at, row.observed_at, row.available_at, row.received_at) > as_of
    ):
        return None
    try:
        document = FilingArchive.model_validate_json(json.dumps(row.payload))
        raw = document.canonical()
        digest = sha256(raw).hexdigest()
        if (
            row.revision != digest
            or row.provider_object_id != f"{document.issuer_cik}:{document.entry.accession_number}"
            or row.observed_at != document.entry.accepted_at
        ):
            return None
        evidence = await session.scalar(
            select(ResearchEvidenceRef).where(
                ResearchEvidenceRef.source == SOURCE,
                ResearchEvidenceRef.provider_object_id == row.provider_object_id,
                ResearchEvidenceRef.revision == digest,
            )
        )
        if (
            evidence is None
            or evidence.content_hash != digest
            or evidence.byte_length != len(raw)
            or evidence.available_at != row.available_at
            or evidence.created_at > as_of
        ):
            return None
        store = configured_exhibit_store(session)
        source, index = await read_index_capture(
            session, store, snapshot_id=document.index_snapshot_id, as_of=as_of
        )
        if (
            source.revision != document.index_hash
            or index.instrument_id != document.instrument_id
            or index.issuer_cik != document.issuer_cik
            or document.entry not in index.entries
        ):
            return None
        originals = {}
        for name, bindings in document.originals.items():
            pair = []
            for binding in bindings:
                artifact = await session.get(DataArtifact, binding.artifact_id)
                if (
                    artifact is None
                    or (artifact.uri, artifact.sha256, artifact.byte_size)
                    != (binding.uri, binding.sha256, binding.byte_size)
                    or artifact.source != "sec"
                    or max(
                        artifact.created_at,
                        artifact.observed_at,
                        artifact.available_at,
                        artifact.received_at,
                    )
                    > as_of
                ):
                    return None
                pair.append(await _original(store, binding))
            originals[name] = (pair[0], pair[1])
        name = document.entry.accession_number + "-index.html"
        inventory = originals.pop(name)
        rebuilt = verify_filing_documents(
            entry=document.entry,
            issuer_cik=document.issuer_cik,
            inventory=inventory,
            documents=originals,
            now=as_of,
        )
        canonical = await store.read_evidence(
            f"evidence/monitor-filing/{document.issuer_cik}/{document.entry.accession_number}/sha256/{digest}.json",
            uri=evidence.location,
        )
        if rebuilt != document.documents or canonical != raw:
            return None
        return VerifiedFilingContent(
            snapshot_id=row.id, evidence_ref_id=evidence.id, canonical_hash=digest, archive=document
        )
    except (ValidationError, ValueError, KeyError, TypeError, OSError, BotoCoreError, ClientError):
        return None

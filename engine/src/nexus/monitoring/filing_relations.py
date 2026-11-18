"""Append exact binary relations without relabeling old custody or content eligibility."""

import json
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.filing_originals import _read_pairs, read_originals
from nexus.monitoring.filing_transport import prove_graphic
from nexus.persistence.models.data import DataSnapshot

SOURCE = "nexus_sec_original_relations"
SCHEMA = "sec_original_relations_v1"


class OriginalRelations(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["sec_original_relations_v1"] = "sec_original_relations_v1"
    original_snapshot_id: UUID
    original_hash: str
    declarations: list[dict[str, object]]
    research_qualified: Literal[False] = False
    nontext_content_status: Literal["uninterpreted"] = "uninterpreted"

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


async def _prove(session: AsyncSession, *, snapshot_id: UUID, as_of: datetime) -> OriginalRelations:
    document = await read_originals(session, snapshot_id=snapshot_id, as_of=as_of)
    original = await session.get(DataSnapshot, snapshot_id)
    assert original is not None
    pairs = await _read_pairs(session, document=document, as_of=as_of)
    source = pairs[document.entry.accession_number + ".txt"][0]
    declarations = []
    for declaration in document.declarations:
        if declaration["document_type"] != "GRAPHIC":
            continue
        name = str(declaration["filename"])
        relation = prove_graphic(
            source,
            pairs[name][0],
            accession=document.entry.accession_number,
            issuer_cik=document.issuer_cik,
            form=document.entry.form,
            filename=name,
            sequence=int(str(declaration["sequence"])),
            declared_size=int(str(declaration["declared_size"])),
        )
        declarations.append({**declaration, **relation})
    if not declarations:
        raise ValueError("SEC_MONITOR_DOCUMENT_SET_INVALID")
    return OriginalRelations(
        original_snapshot_id=snapshot_id,
        original_hash=original.revision,
        declarations=declarations,
    )


async def read_relations(
    session: AsyncSession, *, snapshot_id: UUID, as_of: datetime
) -> OriginalRelations:
    as_of = require_utc(as_of)
    row = await session.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or max(row.created_at, row.observed_at, row.received_at, row.available_at) > as_of
    ):
        raise ValueError("SEC_MONITOR_ARCHIVE_UNAVAILABLE")
    proof = OriginalRelations.model_validate(row.payload)
    if (
        row.revision != sha256(proof.canonical()).hexdigest()
        or row.provider_object_id != str(proof.original_snapshot_id)
        or proof != await _prove(session, snapshot_id=proof.original_snapshot_id, as_of=as_of)
    ):
        raise ValueError("SEC_MONITOR_ARCHIVE_INVALID")
    return proof


async def prove_relations(
    session: AsyncSession, *, snapshot_id: UUID, now: datetime
) -> DataSnapshot:
    now = require_utc(now)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,0))"),
        {"scope": f"sec-original-relations:{snapshot_id}"},
    )
    proof = await _prove(session, snapshot_id=snapshot_id, as_of=now)
    digest = sha256(proof.canonical()).hexdigest()
    prior = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == str(snapshot_id),
            DataSnapshot.revision == digest,
        )
    )
    at = await session.scalar(text("SELECT clock_timestamp()"))
    if not isinstance(at, datetime) or at < now:
        raise ValueError("SEC_MONITOR_CLOCK_UNAVAILABLE")
    if prior is not None:
        await read_relations(session, snapshot_id=prior.id, as_of=at)
        return prior
    row = DataSnapshot(
        source=SOURCE,
        schema_version=SCHEMA,
        provider_object_id=str(snapshot_id),
        revision=digest,
        payload=proof.model_dump(mode="json"),
        created_at=at,
        observed_at=at,
        received_at=at,
        available_at=at,
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.original_relations_proved",
        schema_version=SCHEMA,
        occurred_at=at,
        payload={
            "snapshot_id": str(row.id),
            "canonical_hash": digest,
            "original_snapshot_id": str(snapshot_id),
            "research_qualified": False,
        },
    )
    return row

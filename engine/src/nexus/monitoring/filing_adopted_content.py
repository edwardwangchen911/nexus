"""Combine exact custody, text proofs and the single authorized image judgment.

Only Document Format Files are necessary filing bodies here. XBRL schema links
are separately proved format metadata; no semantic review of all Data Files or
aggregate members is claimed. No authority, event, notification or job is minted.
"""

import re
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataSnapshot

if TYPE_CHECKING:
    from nexus.monitoring.filing_archive import FilingArchive, VerifiedFilingContent

from nexus.domain.time import require_utc
from nexus.monitoring.filing_adoption import GraphicDesignation, canonical, validate_designation
from nexus.monitoring.filing_content import (
    FilingDocument,
    archival_document_inventory,
    verify_text_document,
)
from nexus.monitoring.filing_material import build_material
from nexus.monitoring.filing_originals import OriginalArchive, verify_originals
from nexus.monitoring.filing_relations import prove_original_relations
from nexus.monitoring.filing_review import observe_html_dependencies
from nexus.monitoring.filing_transport import prove_document

ERROR = "SEC_ADOPTED_CONTENT_INVALID"
GRAPHIC_CONTEXT = "q3fy27cfocommentary.htm"


def _schema_reference(
    source: bytes, name: str, *, accession: str, issuer_cik: str
) -> dict[str, object]:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*\.xsd", name):
        raise ValueError(ERROR)
    members = [
        m
        for m in re.findall(rb"(?ms)^<DOCUMENT>\r?\n.*?^</DOCUMENT>(?:\r?\n|$)", source)
        if name.encode() in re.findall(rb"(?m)^<FILENAME>([^\r\n]+)", m.split(b"<TEXT>", 1)[0])
    ]
    if len(members) != 1:
        raise ValueError(ERROR)
    member = members[0]
    head = member.split(b"<TEXT>", 1)[0]
    sequences = re.findall(rb"(?m)^<SEQUENCE>([^\r\n]+)", head)
    bodies = re.findall(rb"(?ms)^<TEXT>\r?\n(.*?)\r?\n</TEXT>\r?\n</DOCUMENT>", member)
    if len(sequences) != 1 or not sequences[0].isdigit() or len(bodies) != 1:
        raise ValueError(ERROR)
    body = bodies[0]
    prove_document(
        source,
        body,
        accession=accession,
        issuer_cik=issuer_cik,
        form="8-K",
        filename=name,
        kind="EX-101.SCH",
        declared_size=len(body),
    )
    return {
        "filename": name,
        "sequence": int(sequences[0]),
        "member_sha256": sha256(member).hexdigest(),
        "body_sha256": sha256(body).hexdigest(),
        "purpose": "xbrl_format_metadata",
        "semantic_review_performed": False,
    }


def verify_dependency_closure(
    contents: dict[str, bytes], *, graphic: str, source: bytes, accession: str, issuer_cik: str
) -> dict[str, object]:
    """Conservative static closure for this exact frozen, designated context."""
    images = 0
    metadata: list[dict[str, object]] = []
    for name, body in contents.items():
        observation = observe_html_dependencies(body)
        if observation["event_attribute_count"] or observation["inline_script_characters"]:
            raise ValueError(ERROR)
        css = observation["css_observations"]
        if not isinstance(css, dict) or any(css.values()):
            raise ValueError(ERROR)
        refs = observation["references"]
        if not isinstance(refs, list):
            raise ValueError(ERROR)
        for ref in refs:
            if ref["duplicate_attributes"]:
                raise ValueError(ERROR)
            tag, attrs = ref["tag"], ref["attributes"]
            if tag == "a" and set(attrs) <= {"href"}:
                continue  # Citation/navigation, not a rendered body dependency.
            if tag == "img" and attrs == {"src": graphic} and name == GRAPHIC_CONTEXT:
                images += 1
            elif tag == "link:schemaref" and set(attrs) == {"xlink:href"}:
                metadata.append(
                    _schema_reference(
                        source, attrs["xlink:href"], accession=accession, issuer_cik=issuer_cik
                    )
                )
            else:
                raise ValueError(ERROR)
    if images != 1:
        raise ValueError(ERROR)
    return {
        "profile": "exact_owner_designated_graphic_v1",
        "graphic_references": images,
        "graphic_context": GRAPHIC_CONTEXT,
        "schema_references": metadata,
        "all_aggregate_members_reviewed": False,
    }


def verify_adopted_documents(
    *,
    designation: GraphicDesignation,
    original: OriginalArchive,
    original_id: UUID,
    original_hash: str,
    pairs: dict[str, tuple[bytes, bytes]],
    now: datetime,
) -> tuple[tuple[FilingDocument, ...], dict[str, object]]:
    designation = validate_designation(designation.model_dump(mode="json"))
    material = designation.material
    prepared = require_utc(datetime.fromisoformat(str(material["prepared_at"])))
    if prepared > now or str(original_id) != material["original_snapshot_id"]:
        raise ValueError(ERROR)
    if (
        sha256(original.canonical()).hexdigest() != original_hash
        or material["original_hash"] != original_hash
    ):
        raise ValueError(ERROR)
    if set(pairs) != set(original.originals):
        raise ValueError(ERROR)
    # Preserve the original descriptor order, which belongs to the frozen export.
    pairs = {name: pairs[name] for name in original.originals}
    if (
        verify_originals(
            entry=original.entry, issuer_cik=original.issuer_cik, originals=pairs, now=prepared
        )
        != original.declarations
    ):
        raise ValueError(ERROR)
    relations = prove_original_relations(
        document=original, snapshot_id=original_id, original_hash=original_hash, pairs=pairs
    )
    rebuilt = build_material(
        document=original,
        snapshot_id=original_id,
        pairs=pairs,
        relations=relations,
        prepared_at=prepared,
        schema=str(material["database_schema"]),
    )
    if canonical(rebuilt) != canonical(material):
        raise ValueError(ERROR)
    graphics = [d for d in original.declarations if d["document_type"] == "GRAPHIC"]
    if len(graphics) != 1 or graphics[0]["filename"] != designation.graphic_filename:
        raise ValueError(ERROR)
    if sha256(pairs[designation.graphic_filename][0]).hexdigest() != designation.graphic_sha256:
        raise ValueError(ERROR)
    entry = original.entry
    inventory = pairs[entry.accession_number + "-index.html"]
    submission = pairs[entry.accession_number + ".txt"]
    rows = archival_document_inventory(inventory[0], entry=entry, issuer_cik=original.issuer_cik)
    documents = []
    contents = {}
    for name, kind, size in rows:
        if kind == "GRAPHIC":
            continue  # Exact decoded bytes and designated content were proved above.
        raw, receipt = pairs[name]
        documents.append(
            verify_text_document(
                entry=entry,
                issuer_cik=original.issuer_cik,
                filename=name,
                kind=kind,
                size=size,
                raw=raw,
                receipt=receipt,
                now=prepared,
                submission=submission,
            )
        )
        contents[name] = prove_document(
            submission[0],
            raw,
            accession=entry.accession_number,
            issuer_cik=original.issuer_cik,
            form=entry.form,
            filename=name,
            kind=kind,
            declared_size=size,
        )
    closure = verify_dependency_closure(
        contents,
        graphic=designation.graphic_filename,
        source=submission[0],
        accession=entry.accession_number,
        issuer_cik=original.issuer_cik,
    )
    return tuple(documents), closure


async def read_adopted_binding(
    session: AsyncSession, *, document: "FilingArchive", available_at: datetime, as_of: datetime
) -> tuple[GraphicDesignation, OriginalArchive, DataSnapshot, datetime]:
    """Prove the v3 authority/identity without reading each immutable object twice."""
    from nexus.monitoring.filing_adoption import read_adoption_record
    from nexus.monitoring.filing_originals import SCHEMA as ORIGINAL_SCHEMA
    from nexus.monitoring.filing_originals import SOURCE as ORIGINAL_SOURCE
    from nexus.persistence.models.data import DataSnapshot

    if document.graphic_adoption_id is None:
        raise ValueError(ERROR)
    designation = await read_adoption_record(
        session, snapshot_id=document.graphic_adoption_id, as_of=as_of
    )
    adopted = await session.get(DataSnapshot, document.graphic_adoption_id)
    assert adopted is not None
    material = designation.material
    prepared = require_utc(datetime.fromisoformat(str(material["prepared_at"])))
    original_row = await session.get(DataSnapshot, UUID(str(material["original_snapshot_id"])))
    if (
        original_row is None
        or original_row.source != ORIGINAL_SOURCE
        or original_row.schema_version != ORIGINAL_SCHEMA
        or not prepared <= adopted.created_at <= available_at <= as_of
        or max(
            original_row.created_at,
            original_row.observed_at,
            original_row.available_at,
            original_row.received_at,
        )
        > prepared
    ):
        raise ValueError(ERROR)
    original = OriginalArchive.model_validate(original_row.payload)
    if (
        original_row.revision != material["original_hash"]
        or sha256(original.canonical()).hexdigest() != original_row.revision
        or original_row.provider_object_id
        != f"{original.issuer_cik}:{original.entry.accession_number}"
        or original_row.observed_at != original.entry.accepted_at
        or any(
            getattr(document, key) != getattr(original, key)
            for key in (
                "instrument_id",
                "issuer_cik",
                "index_snapshot_id",
                "index_hash",
                "entry",
                "originals",
            )
        )
    ):
        raise ValueError(ERROR)
    return designation, original, original_row, prepared


async def build_adopted_archive(
    session: AsyncSession, *, adoption_id: UUID, account_id: UUID, now: datetime
) -> "FilingArchive":
    from nexus.monitoring.filing_adoption import read_adoption
    from nexus.monitoring.filing_archive import ADOPTED_SCHEMA, FilingArchive
    from nexus.monitoring.filing_originals import _read_pairs, read_originals
    from nexus.persistence.models.data import DataSnapshot

    designation = await read_adoption(
        session, snapshot_id=adoption_id, account_id=account_id, as_of=now
    )
    original_id = UUID(str(designation.material["original_snapshot_id"]))
    original = await read_originals(session, snapshot_id=original_id, as_of=now)
    row = await session.get(DataSnapshot, original_id)
    assert row is not None
    pairs = await _read_pairs(session, document=original, as_of=now)
    documents, closure = verify_adopted_documents(
        designation=designation,
        original=original,
        original_id=original_id,
        original_hash=row.revision,
        pairs=pairs,
        now=now,
    )
    return FilingArchive(
        schema_version=ADOPTED_SCHEMA,
        instrument_id=original.instrument_id,
        issuer_cik=original.issuer_cik,
        index_snapshot_id=original.index_snapshot_id,
        index_hash=original.index_hash,
        entry=original.entry,
        originals=original.originals,
        documents=documents,
        graphic_adoption_id=adoption_id,
        dependency_proof=closure,
    )


async def publish_adopted_content(
    session: AsyncSession, *, adoption_id: UUID, account_id: UUID, now: datetime
) -> tuple["VerifiedFilingContent", bool]:
    from sqlalchemy import text

    from nexus.monitoring.filing_archive import _persist_archive

    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": f"sec-adopted-content:{adoption_id}"},
    )
    document = await build_adopted_archive(
        session, adoption_id=adoption_id, account_id=account_id, now=now
    )
    return await _persist_archive(session, document=document, now=now)

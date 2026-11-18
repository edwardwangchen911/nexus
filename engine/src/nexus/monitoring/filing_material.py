"""Rebuild a frozen review export from verified immutable originals."""

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.monitoring.filing_originals import OriginalArchive, _read_pairs, read_originals
from nexus.monitoring.filing_relations import OriginalRelations, _prove
from nexus.monitoring.filing_review import ERROR, observe_html_dependencies


async def rebuild_material(
    db: AsyncSession, *, snapshot_id: UUID, prepared_at: datetime, schema: str
) -> tuple[dict[str, object], dict[str, tuple[bytes, bytes]]]:
    document = await read_originals(db, snapshot_id=snapshot_id, as_of=prepared_at)
    pairs = await _read_pairs(db, document=document, as_of=prepared_at)
    relations = await _prove(db, snapshot_id=snapshot_id, as_of=prepared_at)
    return build_material(
        document=document,
        snapshot_id=snapshot_id,
        pairs=pairs,
        relations=relations,
        prepared_at=prepared_at,
        schema=schema,
    ), pairs


def build_material(
    *,
    document: OriginalArchive,
    snapshot_id: UUID,
    pairs: dict[str, tuple[bytes, bytes]],
    relations: OriginalRelations,
    prepared_at: datetime,
    schema: str,
) -> dict[str, object]:
    descriptions = []
    html = {}
    for name, (raw, receipt) in pairs.items():
        if Path(name).name != name or name in {"material.json", "complete.json"}:
            raise ValueError(ERROR)
        proof = json.loads(receipt)
        descriptions.append(
            {
                "filename": name,
                "sha256": sha256(raw).hexdigest(),
                "bytes": len(raw),
                "receipt_sha256": sha256(receipt).hexdigest(),
                "source_url": proof["source_url"],
                "received_at": proof["received_at"],
            }
        )
        if any(
            d["filename"] == name and d["document_type"] != "GRAPHIC" for d in document.declarations
        ):
            html[name] = observe_html_dependencies(raw)
    material = {
        "schema_version": "sec_content_review_material_v1",
        "original_snapshot_id": str(snapshot_id),
        "original_hash": relations.original_hash,
        "source_schema": document.schema_version,
        "prepared_at": prepared_at.isoformat(),
        "database_schema": schema,
        "entry": document.entry.model_dump(mode="json"),
        "issuer_cik": document.issuer_cik,
        "originals": descriptions,
        "original_declarations": document.declarations,
        "derived_relations": relations.model_dump(mode="json"),
        "html_dependency_observations": html,
        "authority_status": "unverified",
        "reviewer_status": "not_designated",
        "research_qualified": False,
        "nontext_content_status": "uninterpreted",
    }
    return material

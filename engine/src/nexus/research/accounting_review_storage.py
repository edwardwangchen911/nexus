"""Append-only receipt of opinions, separate from research evidence and adoption.

This source is deliberately absent from research selectors. An author's claimed
identity and conclusions are retained without creating permission to consume them.
"""

from datetime import datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.research.accounting_review import (
    ReviewMaterial,
    ReviewSubmission,
    canonical_bytes,
    validate_submission,
)

SOURCE = "nexus_accounting_review_receipt"
SCHEMA = "accounting_receipt_v1"


async def verify_database_scope(session: AsyncSession, material: ReviewMaterial) -> None:
    """Check immutable database identities without granting accounting authority."""
    for item in material.facts:
        fact = await session.get(FinancialFactRecord, item.fact_id)
        if fact is None or fact.instrument_id != material.instrument_id:
            raise ValueError("ACCOUNTING_DATABASE_SCOPE_INVALID")
        ref = await session.get(ResearchEvidenceRef, fact.evidence_ref_id)
        if ref is None or ref.source != "sec":
            raise ValueError("ACCOUNTING_DATABASE_SCOPE_INVALID")
        expected: dict[str, str | None] = {
            name: str(getattr(fact, name))
            for name in (
                "instrument_id",
                "canonical_fact",
                "raw_taxonomy",
                "raw_tag",
                "period_kind",
                "unit",
                "accession_number",
                "revision",
                "evidence_ref_id",
            )
        }
        expected.update(
            period_start=fact.period_start.isoformat() if fact.period_start else None,
            period_end=fact.period_end.isoformat(),
            value=format(fact.value, "f"),
            fact_created_at=fact.created_at.isoformat(),
            evidence_content_hash=ref.content_hash,
            evidence_revision=ref.revision,
            evidence_available_at=ref.available_at.isoformat(),
        )
        if any(item.binding.get(name) != value for name, value in expected.items()):
            raise ValueError("ACCOUNTING_DATABASE_SCOPE_INVALID")
    for amendment in material.amendments:
        found = await session.scalar(
            select(Filing).where(
                Filing.evidence_ref_id == amendment.evidence_ref_id,
                Filing.instrument_id == material.instrument_id,
                Filing.form == "8-K/A",
                Filing.source == "sec",
                Filing.accession_number == amendment.accession_number,
            )
        )
        if found is None:
            raise ValueError("ACCOUNTING_DATABASE_SCOPE_INVALID")


async def read_receipt(session: AsyncSession, submission_id: UUID) -> DataSnapshot | None:
    record = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == str(submission_id),
            DataSnapshot.revision == "v1",
            DataSnapshot.schema_version == SCHEMA,
        )
    )
    if record is not None:
        opinion = ReviewSubmission.model_validate(record.payload["submission"])
        material = ReviewMaterial.model_validate(record.payload["material"])
        if (
            record.payload.get("status") != "received_unverified"
            or opinion.submission_id != submission_id
            or record.payload.get("submission_sha256")
            != sha256(canonical_bytes(opinion)).hexdigest()
            or opinion.material_sha256 != sha256(canonical_bytes(material)).hexdigest()
            or record.observed_at != opinion.reviewed_at
            or not record.observed_at
            <= record.received_at
            == record.available_at
            == record.created_at
        ):
            raise ValueError("ACCOUNTING_RECEIPT_INVALID")
    return record


async def receive_submission(
    session: AsyncSession,
    material: ReviewMaterial,
    opinion: ReviewSubmission,
    *,
    source_root: Path,
) -> DataSnapshot:
    # Serialize same-key contenders, including the first insert when no row exists.
    key = int.from_bytes(sha256(str(opinion.submission_id).encode()).digest()[:8], signed=True)
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("ACCOUNTING_RECEIPT_TIME_INVALID")
    opinion = validate_submission(
        opinion.model_dump(mode="json"), package=material, source_root=source_root, now=now
    )
    payload: dict[str, object] = {
        "status": "received_unverified",
        "material": material.model_dump(mode="json"),
        "submission": opinion.model_dump(mode="json"),
        "submission_sha256": sha256(canonical_bytes(opinion)).hexdigest(),
    }
    existing = await read_receipt(session, opinion.submission_id)
    if existing is not None:
        if existing.payload != payload:
            raise ValueError("ACCOUNTING_SUBMISSION_CONFLICT")
        return existing
    record = DataSnapshot(
        source=SOURCE,
        provider_object_id=str(opinion.submission_id),
        revision="v1",
        schema_version=SCHEMA,
        observed_at=opinion.reviewed_at,
        received_at=now,
        available_at=now,
        created_at=now,
        payload=payload,
    )
    session.add(record)
    await session.flush()
    return record

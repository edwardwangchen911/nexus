"""Append-only planning for newly recognized facts in existing SEC evidence."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.providers.sec import is_canonical_sec_accession_number, normalize_saved_accession_facts
from nexus.research.financial_facts import FinancialFact

_KEY_FIELDS = (
    "instrument_id",
    "canonical_fact",
    "period_kind",
    "period_start",
    "period_end",
    "unit",
    "accession_number",
    "revision",
)


def missing_fact_projections(
    facts: Sequence[FinancialFact],
    existing: Sequence[FinancialFactRecord],
) -> list[dict[str, object]]:
    """Validate every row before returning additions; never supply a historic clock."""

    def key(data: Mapping[str, object]) -> tuple[object, ...]:
        return tuple(data[field] for field in _KEY_FIELDS)

    incoming: dict[tuple[object, ...], dict[str, object]] = {}
    for fact in facts:
        data = fact.model_dump(exclude={"evidence_ref"})
        row_key = key(data)
        prior = incoming.setdefault(row_key, data)
        if prior != data:
            raise ValueError("SEC_FACT_PROJECTION_CONFLICT")
    present = set()
    for record in existing:
        row_key = tuple(getattr(record, field) for field in _KEY_FIELDS)
        expected = incoming.get(row_key)
        if expected is None or any(
            getattr(record, field) != value for field, value in expected.items()
        ):
            raise ValueError("SEC_FACT_PROJECTION_CONFLICT")
        present.add(row_key)
    return [data for row_key, data in incoming.items() if row_key not in present]


class SavedSecEvidenceReader(Protocol):
    async def read_evidence(self, key: str, *, uri: str) -> bytes: ...


async def append_saved_sec_facts(
    session: AsyncSession,
    filing_id: UUID,
    store: SavedSecEvidenceReader,
) -> int:
    """Append missing projections atomically under the existing filing's row lock."""
    filing = await session.scalar(select(Filing).where(Filing.id == filing_id).with_for_update())
    if filing is None or filing.source != "sec" or filing.form not in {"10-K", "10-Q"}:
        raise ValueError("SEC_FACT_PROJECTION_OWNER_INVALID")
    ref = await session.get(ResearchEvidenceRef, filing.evidence_ref_id)
    instrument = await session.get(Instrument, filing.instrument_id)
    now = await session.scalar(select(func.clock_timestamp()))
    if (
        ref is None
        or instrument is None
        or not isinstance(now, datetime)
        or not is_canonical_sec_accession_number(filing.accession_number)
        or filing.provider_object_id != filing.accession_number
        or ref.source != "sec"
        or ref.provider_object_id != filing.accession_number
        or ref.revision != filing.revision
        or filing.revision != f"{filing.accession_number}@{filing.accepted_at.isoformat()}"
        or max(
            filing.created_at,
            ref.created_at,
            instrument.created_at,
            filing.accepted_at,
            filing.filed_at,
            ref.available_at,
        )
        > now
        or ref.available_at < filing.accepted_at
    ):
        raise ValueError("SEC_FACT_PROJECTION_OWNER_INVALID")
    key = f"evidence/sec/{filing.accession_number}/{ref.content_hash}.json"
    raw = await store.read_evidence(key, uri=ref.location)
    if len(raw) != ref.byte_length:
        raise ValueError("SEC_EVIDENCE_CONFLICT")
    facts = normalize_saved_accession_facts(
        raw,
        instrument_id=filing.instrument_id,
        accession=filing.accession_number,
        accepted_at=filing.accepted_at,
        form=filing.form,
        evidence_ref=EvidenceRef(
            source=ref.source,
            object_id=ref.provider_object_id,
            available_at=ref.available_at,
            location=ref.location,
            content_hash=ref.content_hash,
        ),
    )
    if any(fact.filed_at != filing.filed_at or fact.revision != filing.revision for fact in facts):
        raise ValueError("SEC_FACT_PROJECTION_OWNER_INVALID")
    existing = tuple(
        await session.scalars(
            select(FinancialFactRecord).where(
                or_(
                    FinancialFactRecord.filing_id == filing.id,
                    FinancialFactRecord.evidence_ref_id == ref.id,
                )
            )
        )
    )
    if any(
        row.filing_id != filing.id
        or row.evidence_ref_id != ref.id
        or row.instrument_id != filing.instrument_id
        or row.accession_number != filing.accession_number
        or row.revision != filing.revision
        for row in existing
    ):
        raise ValueError("SEC_FACT_PROJECTION_OWNER_INVALID")
    pending = missing_fact_projections(facts, existing)
    session.add_all(
        FinancialFactRecord(
            **data,
            filing_id=filing.id,
            evidence_ref_id=ref.id,
            created_at=func.clock_timestamp(),
        )
        for data in pending
    )
    await session.flush()
    return len(pending)

"""Freeze existing SEC filing evidence without network access or model execution."""

import hashlib
import json
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution_authority import MAX_FROZEN_EVIDENCE
from nexus.domain.time import require_utc
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import Filing, ResearchEvidenceRef


async def prepare_research_snapshot(
    session: AsyncSession, instrument_id: UUID, *, as_of: datetime
) -> DataSnapshot | None:
    """Bind real filing references; the caller owns the transaction.

    The first local slice deliberately uses the existing SEC filing store only.
    Display-only price manifests cannot satisfy research ownership. Price authority
    is selected separately by the existing admission/worker code, never promoted here.
    """
    require_utc(as_of)
    if as_of > datetime.now(UTC):
        raise ValueError("OPENAI_RESEARCH_TIME_IN_FUTURE")
    evidence = list(
        await session.scalars(
            select(ResearchEvidenceRef)
            .where(
                ResearchEvidenceRef.source == "sec",
                ResearchEvidenceRef.available_at <= as_of,
                ResearchEvidenceRef.created_at <= as_of,
                select(Filing.id)
                .where(
                    Filing.evidence_ref_id == ResearchEvidenceRef.id,
                    Filing.instrument_id == instrument_id,
                    Filing.source == "sec",
                    Filing.form.in_(("10-K", "10-Q", "8-K")),
                    Filing.filed_at <= as_of,
                    Filing.accepted_at <= as_of,
                    Filing.created_at <= as_of,
                )
                .exists(),
            )
            .order_by(ResearchEvidenceRef.available_at.desc(), ResearchEvidenceRef.id)
            # Admission can add one price reference to the same bounded manifest.
            .limit(MAX_FROZEN_EVIDENCE - 1)
        )
    )
    if not evidence:
        return None
    payload = {
        "instrument_id": str(instrument_id),
        "as_of": as_of.isoformat(),
        "evidence_ids": [str(item.id) for item in evidence],
        "scope": "persisted_sec_filings",
    }
    revision = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    snapshot_id = await session.scalar(
        insert(DataSnapshot)
        .values(
            source="nexus_research",
            provider_object_id=str(instrument_id),
            revision=revision,
            observed_at=as_of,
            available_at=as_of,
            received_at=as_of,
            schema_version="research_filings_v1",
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
        .returning(DataSnapshot.id)
    )
    if snapshot_id is None:
        return cast(
            DataSnapshot | None,
            await session.scalar(
                select(DataSnapshot).where(
                    DataSnapshot.source == "nexus_research",
                    DataSnapshot.provider_object_id == str(instrument_id),
                    DataSnapshot.revision == revision,
                )
            ),
        )
    session.add_all(
        AgentDataSnapshotEvidenceRecord(
            data_snapshot_id=snapshot_id,
            instrument_id=instrument_id,
            evidence_ref_id=item.id,
            authority_kind="research",
            selected_at=None,
            source=item.source,
            provider_object_id=item.provider_object_id,
            revision=item.revision,
            available_at=item.available_at,
            location=item.location,
            content_hash=item.content_hash,
        )
        for item in evidence
    )
    await session.flush()
    return await session.get(DataSnapshot, snapshot_id)

"""Freeze existing filings and verified issuer facts without external calls."""

import hashlib
import json
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution_authority import MAX_FROZEN_EVIDENCE
from nexus.agents.portfolio_context import freeze_portfolio_context
from nexus.domain.time import require_utc
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.research.sec_disclosures import SEC_RESEARCH_FORMS
from nexus.research.ssga_distributions import read_distributions
from nexus.research.ssga_fund_observations import read_fund_observations


async def prepare_research_snapshot(
    session: AsyncSession, instrument_id: UUID, *, as_of: datetime,
    portfolio_snapshot_id: UUID | None = None,
) -> DataSnapshot | None:
    """Bind real filing references; the caller owns the transaction.

    Issuer observations retain receipt time and do not become historical PIT data.
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
                    Filing.form.in_(SEC_RESEARCH_FORMS),
                    Filing.filed_at <= as_of,
                    Filing.accepted_at <= as_of,
                    Filing.created_at <= as_of,
                )
                .exists(),
            )
            .order_by(ResearchEvidenceRef.available_at.desc(), ResearchEvidenceRef.id)
            # Leave room for one price reference and one separate portfolio context.
            .limit(MAX_FROZEN_EVIDENCE - 2)
        )
    )
    distributions = await read_distributions(session, instrument_id=instrument_id, as_of=as_of)
    if distributions:
        issuer_refs = list(
            await session.scalars(
                select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(distributions))
            )
        )
        evidence = evidence[: MAX_FROZEN_EVIDENCE - 3] + issuer_refs
    observations = await read_fund_observations(session, instrument_id=instrument_id, as_of=as_of)
    if observations:
        observation_refs = list(
            await session.scalars(
                select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(observations))
            )
        )
        # Preserve both issuer kinds and reserve price/portfolio slots.
        retained_issuer = [item for item in evidence if item.id in distributions]
        sec_refs = [item for item in evidence if item.id not in distributions]
        keep = MAX_FROZEN_EVIDENCE - 2 - len(observation_refs) - len(retained_issuer)
        evidence = sec_refs[:keep] + retained_issuer + observation_refs
    if not evidence:
        return None
    context = await freeze_portfolio_context(
        session, instrument_id, as_of, portfolio_snapshot_id=portfolio_snapshot_id,
    )
    if portfolio_snapshot_id is not None and context is None:
        raise ValueError("OPENAI_PORTFOLIO_CONTEXT_UNAVAILABLE")
    payload: dict[str, object] = {
        "instrument_id": str(instrument_id),
        "as_of": as_of.isoformat(),
        "evidence_ids": [str(item.id) for item in evidence],
        "scope": "persisted_sec_filings_and_issuer_distributions"
        if distributions
        else "persisted_sec_filings",
    }
    if observations:
        payload["scope"] = "persisted_sec_filings_and_issuer_observations"
    if context is not None:
        payload["portfolio_context"] = context[0].model_dump(mode="json")
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

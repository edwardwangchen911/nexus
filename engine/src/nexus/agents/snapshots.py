"""Freeze existing filings and verified issuer facts without external calls."""

import hashlib
import json
from datetime import UTC, date, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution_authority import MAX_FROZEN_EVIDENCE
from nexus.agents.portfolio_context import freeze_portfolio_context
from nexus.domain.time import require_utc
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.research.alpaca_actions_storage import read_action_observations
from nexus.research.current_identity_storage import read_identity_bindings
from nexus.research.current_price_trends_storage import read_current_price_trends
from nexus.research.fmp_profile_storage import read_company_profiles
from nexus.research.fosun_security_classification_storage import (
    read_security_classifications,
)
from nexus.research.fred_storage import FredResearchObservation, read_fred_observations
from nexus.research.news_evidence import read_news_evidence
from nexus.research.sec_disclosures import SEC_RESEARCH_FORMS
from nexus.research.ssga_distributions import read_distributions
from nexus.research.ssga_fund_observations import read_fund_observations
from nexus.research.ssga_nav_storage import read_nav_histories


async def prepare_research_snapshot(
    session: AsyncSession,
    instrument_id: UUID,
    *,
    as_of: datetime,
    portfolio_snapshot_id: UUID | None = None,
    news_evidence_id: UUID | None = None,
    macro_series: frozenset[str] | None = None,
    macro_vintage: date | None = None,
) -> DataSnapshot | None:
    """Bind real filing references; the caller owns the transaction.

    Issuer observations retain receipt time and do not become historical PIT data.
    Display-only price manifests cannot satisfy research ownership. Price authority
    is selected separately by the existing admission/worker code, never promoted here.
    """
    require_utc(as_of)
    if as_of > datetime.now(UTC):
        raise ValueError("OPENAI_RESEARCH_TIME_IN_FUTURE")
    # Select one revision per filing before applying the evidence cap. Cutoff
    # filters run first so a later import cannot change an older frozen request.
    revisions = (
        select(
            ResearchEvidenceRef.id.label("evidence_id"),
            func.row_number()
            .over(
                partition_by=(Filing.instrument_id, Filing.source, Filing.accession_number),
                order_by=(
                    ResearchEvidenceRef.available_at.desc(),
                    ResearchEvidenceRef.created_at.desc(),
                    ResearchEvidenceRef.id.desc(),
                ),
            )
            .label("revision_rank"),
        )
        .join(Filing, Filing.evidence_ref_id == ResearchEvidenceRef.id)
        .where(
            ResearchEvidenceRef.source == "sec",
            ResearchEvidenceRef.available_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
            Filing.instrument_id == instrument_id,
            Filing.source == "sec",
            Filing.form.in_(SEC_RESEARCH_FORMS),
            Filing.filed_at <= as_of,
            Filing.accepted_at <= as_of,
            Filing.created_at <= as_of,
        )
        .subquery()
    )
    evidence = list(
        await session.scalars(
            select(ResearchEvidenceRef)
            .join(revisions, revisions.c.evidence_id == ResearchEvidenceRef.id)
            .where(revisions.c.revision_rank == 1)
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
    nav = await read_nav_histories(session, instrument_id=instrument_id, as_of=as_of)
    if nav:
        nav_refs = list(
            await session.scalars(
                select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(nav))
            )
        )
        retained_issuer = [
            item for item in evidence if item.id in distributions or item.id in observations
        ]
        sec_refs = [
            item
            for item in evidence
            if item.id not in distributions and item.id not in observations
        ]
        keep = MAX_FROZEN_EVIDENCE - 2 - len(nav_refs) - len(retained_issuer)
        evidence = sec_refs[:keep] + retained_issuer + nav_refs
    actions = await read_action_observations(session, instrument_id=instrument_id, as_of=as_of)
    if actions:
        action_refs = list(
            await session.scalars(
                select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(actions))
            )
        )
        retained_ids = distributions.keys() | observations.keys() | nav.keys()
        retained = [item for item in evidence if item.id in retained_ids]
        sec_refs = [item for item in evidence if item.id not in retained_ids]
        keep = MAX_FROZEN_EVIDENCE - 2 - len(action_refs) - len(retained)
        evidence = sec_refs[:keep] + retained + action_refs
    if not evidence:
        return None
    trends = await read_current_price_trends(session, instrument_id=instrument_id, as_of=as_of)
    if trends:
        trend_refs = list(
            await session.scalars(
                select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(trends))
            )
        )
        retained = [item for item in evidence if item.source != "sec"]
        sec_refs = [item for item in evidence if item.source == "sec"]
        keep = MAX_FROZEN_EVIDENCE - 2 - len(trend_refs) - len(retained)
        evidence = sec_refs[:keep] + retained + trend_refs
    # A current vendor profile supplements research; it cannot create its base.
    profiles = await read_company_profiles(session, instrument_id=instrument_id, as_of=as_of)
    if profiles:
        profile_refs = list(
            await session.scalars(
                select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(profiles))
            )
        )
        retained = [item for item in evidence if item.source != "sec"]
        sec_refs = [item for item in evidence if item.source == "sec"]
        keep = MAX_FROZEN_EVIDENCE - 2 - len(profile_refs) - len(retained)
        evidence = sec_refs[:keep] + retained + profile_refs
    macro_ids = _macro_context_ids(
        await read_fred_observations(
            session,
            as_of=as_of,
            series=macro_series,
            vintage=macro_vintage,
        )
    )
    if macro_ids:
        macro_refs = list(
            await session.scalars(
                select(ResearchEvidenceRef)
                .where(ResearchEvidenceRef.id.in_(macro_ids))
                .order_by(ResearchEvidenceRef.id)
            )
        )
        retained = [item for item in evidence if item.source != "sec"]
        sec_refs = [item for item in evidence if item.source == "sec"]
        keep = MAX_FROZEN_EVIDENCE - 2 - len(macro_refs) - len(retained)
        evidence = sec_refs[:keep] + retained + macro_refs
    if news_evidence_id is not None:
        news = await read_news_evidence(
            session,
            instrument_id=instrument_id,
            as_of=as_of,
            evidence_ids=(news_evidence_id,),
        )
        if news_evidence_id not in news or len(evidence) >= MAX_FROZEN_EVIDENCE - 2:
            raise ValueError("OPENAI_NEWS_EVIDENCE_UNAVAILABLE")
        news_ref = await session.get(ResearchEvidenceRef, news_evidence_id)
        if news_ref is None:
            raise ValueError("OPENAI_NEWS_EVIDENCE_UNAVAILABLE")
        evidence.append(news_ref)
    classifications = await read_security_classifications(
        session, instrument_id=instrument_id, as_of=as_of
    )
    if classifications:
        classification_id = next(iter(classifications))
        classification_ref = await session.get(ResearchEvidenceRef, classification_id)
        if classification_ref is not None:
            research_capacity = MAX_FROZEN_EVIDENCE - 2
            if len(evidence) >= research_capacity:
                oldest_sec = next(
                    (
                        index
                        for index in range(len(evidence) - 1, -1, -1)
                        if evidence[index].source == "sec"
                    ),
                    None,
                )
                if oldest_sec is not None:
                    evidence.pop(oldest_sec)
            if len(evidence) < research_capacity:
                evidence.append(classification_ref)
    identities = await read_identity_bindings(session, instrument_id=instrument_id, as_of=as_of)
    if identities:
        identity_ref = await session.get(ResearchEvidenceRef, next(iter(identities)))
        if identity_ref is not None:
            capacity = MAX_FROZEN_EVIDENCE - 2
            if len(evidence) >= capacity:
                sec_indexes = [index for index, ref in enumerate(evidence) if ref.source == "sec"]
                if len(sec_indexes) > 1:
                    evidence.pop(sec_indexes[-1])
            if len(evidence) < capacity:
                evidence.append(identity_ref)
    context = await freeze_portfolio_context(
        session,
        instrument_id,
        as_of,
        portfolio_snapshot_id=portfolio_snapshot_id,
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
    if nav:
        payload["scope"] = "persisted_sec_filings_and_issuer_nav_history"
    if (
        actions
        or profiles
        or macro_ids
        or trends
        or classifications
        or news_evidence_id is not None
    ):
        payload["scope"] = "persisted_research_observations"
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


def _macro_context_ids(observations: dict[UUID, FredResearchObservation]) -> set[UUID]:
    """Latest visible economic period/vintage/receipt, never UUID as source recency."""
    result: set[UUID] = set()
    for series in sorted({row.series for row in observations.values()}):
        candidates = [row for row in observations.values() if row.series == series]
        rank = max((row.observed_on, row.vintage, row.received_at) for row in candidates)
        latest = [
            row for row in candidates if (row.observed_on, row.vintage, row.received_at) == rank
        ]
        if len({(row.value, row.unit) for row in latest}) != 1:
            # Conflicting values at the same source clocks are unknown, not a vote.
            continue
        selected = min(latest, key=lambda row: (row.canonical_sha256, str(row.evidence_ref_id)))
        result.update((selected.evidence_ref_id, selected.calendar_evidence_ref_id))
    return result

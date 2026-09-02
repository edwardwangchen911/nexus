"""Read-only display of the SEC facts already bound to one research snapshot."""

from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_context import read_portfolio_context
from nexus.api.schemas.agents import ResearchInputFact, ResearchInputFiling, ResearchInputsSummary
from nexus.domain.decimal import decimal_to_string
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord as Frozen
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef


async def read_research_inputs(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID, as_of: datetime
) -> ResearchInputsSummary | None:
    """No discovery, model call, inferred ratios, or promotion to execution authority."""
    rows = list(
        await session.execute(
            select(Filing, ResearchEvidenceRef)
            .join(ResearchEvidenceRef, Filing.evidence_ref_id == ResearchEvidenceRef.id)
            .join(Frozen, Frozen.evidence_ref_id == ResearchEvidenceRef.id)
            .where(
                Frozen.data_snapshot_id == snapshot_id,
                Frozen.instrument_id == instrument_id,
                Frozen.authority_kind == "research",
                Frozen.source == ResearchEvidenceRef.source,
                Frozen.provider_object_id == ResearchEvidenceRef.provider_object_id,
                Frozen.revision == ResearchEvidenceRef.revision,
                Frozen.available_at == ResearchEvidenceRef.available_at,
                Frozen.content_hash == ResearchEvidenceRef.content_hash,
                Frozen.location == ResearchEvidenceRef.location,
                ResearchEvidenceRef.source == "sec",
                ResearchEvidenceRef.available_at <= as_of,
                ResearchEvidenceRef.created_at <= as_of,
                Filing.instrument_id == instrument_id,
                Filing.accepted_at <= as_of,
                Filing.filed_at <= as_of,
                Filing.created_at <= as_of,
            )
            .order_by(Filing.accepted_at.desc(), Filing.id)
            .limit(21)
        )
    )
    if not rows:
        return None
    visible = rows[:20]
    query = select(FinancialFactRecord).where(
        FinancialFactRecord.filing_id.in_([filing.id for filing, _ in visible]),
        FinancialFactRecord.instrument_id == instrument_id,
        FinancialFactRecord.accepted_at <= as_of,
        FinancialFactRecord.filed_at <= as_of,
        FinancialFactRecord.created_at <= as_of,
        FinancialFactRecord.period_end <= as_of.date(),
    )
    total = int(await session.scalar(select(func.count()).select_from(query.subquery())) or 0)
    facts = list(
        await session.scalars(
            query.order_by(
                FinancialFactRecord.canonical_fact,
                FinancialFactRecord.period_end.desc(),
                FinancialFactRecord.period_start.desc(),
                FinancialFactRecord.id,
            ).limit(100)
        )
    )
    symbol = await session.scalar(
        select(InstrumentAlias.symbol).where(
            InstrumentAlias.instrument_id == instrument_id,
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.valid_from <= as_of.date(),
            or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= as_of.date()),
        )
    )
    return ResearchInputsSummary(
        portfolio_context=await read_portfolio_context(session, snapshot_id, instrument_id, as_of),
        display_symbol=symbol,
        filings=tuple(
            ResearchInputFiling(
                evidence_ref_id=evidence.id,
                form=filing.form,
                accession_number=filing.accession_number,
                accepted_at=filing.accepted_at,
                received_at=evidence.created_at,
                revision=evidence.revision,
                content_hash=evidence.content_hash,
            )
            for filing, evidence in visible
        ),
        facts=tuple(
            ResearchInputFact(
                id=fact.id,
                evidence_ref_id=fact.evidence_ref_id,
                fact=fact.canonical_fact,
                raw_tag=fact.raw_tag,
                value=decimal_to_string(fact.value),
                unit=fact.unit,
                period_kind=cast(Literal["instant", "duration"], fact.period_kind),
                period_start=fact.period_start,
                period_end=fact.period_end,
            )
            for fact in facts
        ),
        total_facts=total,
        truncated=len(rows) > 20 or total > len(facts),
    )

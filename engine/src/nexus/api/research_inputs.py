"""Read-only display of facts already bound to one research snapshot."""

from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_context import read_portfolio_context
from nexus.api.schemas.agents import (
    ResearchInputFact,
    ResearchInputFiling,
    ResearchInputFundHolding,
    ResearchInputFundIndustry,
    ResearchInputFundObservation,
    ResearchInputIssuerDistribution,
    ResearchInputsSummary,
)
from nexus.domain.decimal import decimal_to_string
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord as Frozen
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.research.sec_disclosures import read_disclosures
from nexus.research.ssga_distributions import SOURCE, read_distributions
from nexus.research.ssga_fund_observations import SOURCE as FUND_SOURCE
from nexus.research.ssga_fund_observations import IssuerFundObservation, read_fund_observations


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
    issuer_distributions = await _read_issuer_inputs(
        session, snapshot_id=snapshot_id, instrument_id=instrument_id, as_of=as_of
    )
    fund_observations = await _read_fund_inputs(
        session, snapshot_id=snapshot_id, instrument_id=instrument_id, as_of=as_of
    )
    if not rows and not issuer_distributions and not fund_observations:
        return None
    visible = rows[:20]
    disclosures = await read_disclosures(
        session,
        instrument_id=instrument_id,
        evidence_ids=tuple(evidence.id for _, evidence in visible),
        as_of=as_of,
    )
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
        issuer_distributions=issuer_distributions,
        issuer_fund_observations=fund_observations,
        portfolio_context=await read_portfolio_context(session, snapshot_id, instrument_id, as_of),
        display_symbol=symbol,
        filings=tuple(
            ResearchInputFiling(
                evidence_ref_id=evidence.id,
                form=filing.form,
                accession_number=filing.accession_number,
                accepted_at=filing.accepted_at,
                received_at=evidence.available_at
                if evidence.id in disclosures and disclosures[evidence.id].form == "8-K"
                else evidence.created_at,
                revision=evidence.revision,
                content_hash=evidence.content_hash,
                disclosure_excerpt=disclosures[evidence.id].excerpt
                if evidence.id in disclosures
                else None,
                disclosure_truncated=disclosures[evidence.id].truncated
                if evidence.id in disclosures
                else False,
                source_url=disclosures[evidence.id].source_url
                if evidence.id in disclosures
                else None,
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


async def _frozen_issuer_references(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID,
    as_of: datetime, source: str,
) -> dict[UUID, ResearchEvidenceRef]:
    return {
        evidence.id: evidence
        for evidence in await session.scalars(
            select(ResearchEvidenceRef)
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
                ResearchEvidenceRef.source == source,
                ResearchEvidenceRef.available_at <= as_of,
                ResearchEvidenceRef.created_at <= as_of,
            )
        )
    }


async def _read_issuer_inputs(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID, as_of: datetime
) -> tuple[ResearchInputIssuerDistribution, ...]:
    references = await _frozen_issuer_references(
        session, snapshot_id=snapshot_id, instrument_id=instrument_id, as_of=as_of, source=SOURCE
    )
    if not references:
        return ()
    try:
        documents = await read_distributions(
            session, instrument_id=instrument_id, as_of=as_of, evidence_ids=tuple(references)
        )
    except ValidationError:
        # A malformed optional observation must not hide valid frozen SEC facts.
        return ()
    return tuple(
        ResearchInputIssuerDistribution(
            evidence_ref_id=evidence_id,
            source="ssga_distribution",
            revision=references[evidence_id].revision,
            content_hash=references[evidence_id].content_hash,
            ex_date=document.ex_date,
            record_date=document.record_date,
            payable_date=document.payable_date,
            currency=document.currency,
            amount_basis=document.amount_basis,
            dividend_per_share=decimal_to_string(document.dividend_per_share),
            short_term_capital_gain=(
                None
                if document.short_term_capital_gain is None
                else decimal_to_string(document.short_term_capital_gain)
            ),
            long_term_capital_gain=(
                None
                if document.long_term_capital_gain is None
                else decimal_to_string(document.long_term_capital_gain)
            ),
            available_at=document.available_at,
            # The canonical reader verifies received_at == document.available_at.
            received_at=document.available_at,
            source_url=document.source_url,
            historical_backtest_eligible=document.historical_backtest_eligible,
        )
        for evidence_id, document in documents.items()
    )


async def _read_fund_inputs(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID, as_of: datetime
) -> tuple[ResearchInputFundObservation, ...]:
    references = await _frozen_issuer_references(
        session, snapshot_id=snapshot_id, instrument_id=instrument_id, as_of=as_of,
        source=FUND_SOURCE,
    )
    if not references:
        return ()
    documents = await read_fund_observations(
        session, instrument_id=instrument_id, as_of=as_of, evidence_ids=tuple(references)
    )
    result = []
    for evidence_id, document in documents.items():
        try:
            result.append(_fund_observation_response(document, references[evidence_id]))
        except ValueError:
            # Decimal wire limits and schema validation must not obscure valid SEC inputs.
            continue
    return tuple(result)


def _fund_observation_response(
    document: IssuerFundObservation, evidence: ResearchEvidenceRef,
) -> ResearchInputFundObservation:
    return ResearchInputFundObservation(
        evidence_ref_id=evidence.id, source="ssga_fund_observation",
        revision=evidence.revision, content_hash=evidence.content_hash,
        **document.model_dump(include={
            "as_of", "classification_level", "weight_unit", "html_url", "workbook_url",
            "html_content_hash", "workbook_content_hash", "html_received_at",
            "workbook_received_at", "available_at", "source_published_at", "valid_until",
            "denominator", "residual_classification", "historical_backtest_eligible",
            "sector_risk_eligible",
        }),
        industries=tuple(ResearchInputFundIndustry(
            label=row.label, weight_percent=decimal_to_string(row.weight_percent)
        ) for row in document.industries[:50]),
        holdings=tuple(ResearchInputFundHolding(
            name=row.name, ticker=row.ticker, classification=row.classification,
            weight_percent=decimal_to_string(row.weight_percent), local_currency=row.local_currency,
        ) for row in document.holdings[:100]),
        total_industries=len(document.industries), total_holdings=len(document.holdings),
        truncated=len(document.industries) > 50 or len(document.holdings) > 100,
    )

"""Immutable ORM mappings for point-in-time research evidence."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    ForeignKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class ResearchEvidenceRef(ImmutableCreatedAt, Base):
    __tablename__ = "research_evidence_refs"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "provider_object_id",
            "revision",
            name="uq_research_evidence_refs_object",
        ),
        UniqueConstraint(
            "id",
            "source",
            "provider_object_id",
            "revision",
            name="uq_research_evidence_refs_owner_identity",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        CheckConstraint("byte_length >= 0", name="byte_length_nonnegative"),
        CheckConstraint(
            "(location LIKE 'memory://sha256/' || content_hash || '%') OR "
            "(location LIKE 'memory://normalized/market/%/' || content_hash || '.parquet') OR "
            "(location LIKE 'package://%' AND location LIKE '%@sha256:' || content_hash || '%') OR "
            "(location LIKE 's3://%' AND (location LIKE '%/sha256/' || content_hash || '%' "
            "OR location ~ '[?&]versionId=[^&#]+')) OR "
            "(location ~ ('^db://price-bar-set/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
            "[0-9a-f]{4}-[0-9a-f]{12}@sha256:' || content_hash || '$'))",
            name="location_content_addressed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    location: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))
    byte_length: Mapped[int] = mapped_column(BigInteger)


class Filing(ImmutableCreatedAt, Base):
    __tablename__ = "filings"
    __table_args__ = (
        ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_filings_instrument_id_instruments"
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_filings_evidence_owner",
        ),
        UniqueConstraint(
            "source",
            "accession_number",
            "revision",
            name="uq_filings_accession_revision",
        ),
        UniqueConstraint(
            "id",
            "instrument_id",
            "accession_number",
            "revision",
            "evidence_ref_id",
            name="uq_filings_fact_owner",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    instrument_id: Mapped[UUID]
    evidence_ref_id: Mapped[UUID]
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    accession_number: Mapped[str] = mapped_column(String(32))
    form: Mapped[str] = mapped_column(String(32))
    filed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    accepted_at: Mapped[datetime] = mapped_column(UTCDateTime())
    revision: Mapped[str] = mapped_column(String(255))


class FinancialFactRecord(ImmutableCreatedAt, Base):
    __tablename__ = "financial_facts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["filing_id"], ["filings.id"], name="fk_financial_facts_filing_id_filings"
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_financial_facts_evidence_ref",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_financial_facts_instrument_id_instruments",
        ),
        ForeignKeyConstraint(
            ["filing_id", "instrument_id", "accession_number", "revision", "evidence_ref_id"],
            [
                "filings.id",
                "filings.instrument_id",
                "filings.accession_number",
                "filings.revision",
                "filings.evidence_ref_id",
            ],
            name="fk_financial_facts_filing_identity",
        ),
        UniqueConstraint(
            "instrument_id",
            "canonical_fact",
            "period_kind",
            "period_start",
            "period_end",
            "unit",
            "accession_number",
            "revision",
            name="uq_financial_facts_point_in_time",
            postgresql_nulls_not_distinct=True,
        ),
        CheckConstraint(
            "canonical_fact IN ('revenue', 'gross_profit', 'operating_income', 'net_income', "
            "'operating_cash_flow', 'capital_expenditure', 'total_assets', 'total_liabilities', "
            "'stockholders_equity', 'diluted_shares', 'diluted_eps')",
            name="canonical_fact_allowed",
        ),
        CheckConstraint("period_kind IN ('instant', 'duration')", name="period_kind_allowed"),
        CheckConstraint(
            "(period_kind = 'instant' AND period_start IS NULL) OR "
            "(period_kind = 'duration' AND period_start IS NOT NULL "
            "AND period_start <= period_end)",
            name="period_shape_allowed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    filing_id: Mapped[UUID]
    evidence_ref_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    canonical_fact: Mapped[str] = mapped_column(String(64))
    raw_taxonomy: Mapped[str] = mapped_column(String(100))
    raw_tag: Mapped[str] = mapped_column(String(255))
    unit: Mapped[str] = mapped_column(String(100))
    value: Mapped[Decimal]
    period_kind: Mapped[str] = mapped_column(String(16))
    period_start: Mapped[date | None] = mapped_column(Date)
    period_end: Mapped[date] = mapped_column(Date)
    fiscal_year: Mapped[int | None]
    fiscal_period: Mapped[str | None] = mapped_column(String(16))
    form: Mapped[str] = mapped_column(String(32))
    filed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    accepted_at: Mapped[datetime] = mapped_column(UTCDateTime())
    accession_number: Mapped[str] = mapped_column(String(32))
    frame: Mapped[str | None] = mapped_column(String(100))
    revision: Mapped[str] = mapped_column(String(255))


class CorporateActionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "corporate_actions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_corporate_actions_instrument_id_instruments",
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_corporate_actions_evidence_ref",
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_event_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_corporate_actions_evidence_owner",
        ),
        UniqueConstraint(
            "source",
            "provider_event_id",
            "revision",
            name="uq_corporate_actions_event_revision",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    evidence_ref_id: Mapped[UUID]
    instrument_id: Mapped[UUID | None]
    source: Mapped[str] = mapped_column(String(100))
    provider_event_id: Mapped[str] = mapped_column(String(255))
    action_type: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(50))
    process_date: Mapped[date] = mapped_column(Date)
    ex_date: Mapped[date | None] = mapped_column(Date)
    record_date: Mapped[date | None] = mapped_column(Date)
    payable_date: Mapped[date | None] = mapped_column(Date)
    rate: Mapped[Decimal | None]
    revision: Mapped[str] = mapped_column(String(255))

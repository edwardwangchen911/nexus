"""Versioned data-source, evidence, and analytics metadata mappings."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class ProviderRun(ImmutableCreatedAt, Base):
    __tablename__ = "provider_runs"
    __table_args__ = (
        UniqueConstraint(
            "provider", "provider_object_id", "revision", name="uq_provider_runs_object"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    provider: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class ProviderCapability(ImmutableCreatedAt, Base):
    __tablename__ = "provider_capabilities"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    provider: Mapped[str] = mapped_column(String(100))
    capability: Mapped[str] = mapped_column(String(100))
    contract_hash: Mapped[str] = mapped_column(String(64))
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


Index(
    "uq_provider_capabilities_contract_scope",
    ProviderCapability.provider,
    ProviderCapability.capability,
    ProviderCapability.contract_hash,
    text("COALESCE(payload ->> 'operation', '')"),
    text("COALESCE(payload ->> 'market_scope', '')"),
    unique=True,
)


class SourcePolicy(ImmutableCreatedAt, Base):
    __tablename__ = "source_policies"
    __table_args__ = (UniqueConstraint("key", "version", name="uq_source_policies_key_version"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    key: Mapped[str] = mapped_column(String(100))
    version: Mapped[str] = mapped_column(String(100))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class DataSnapshot(ImmutableCreatedAt, Base):
    __tablename__ = "data_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "provider_object_id",
            "revision",
            name="uq_data_snapshots_source_object_revision",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    received_at: Mapped[datetime] = mapped_column(UTCDateTime())
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class QualityReport(ImmutableCreatedAt, Base):
    __tablename__ = "quality_reports"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    data_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey("data_snapshots.id", name="fk_quality_reports_data_snapshot_id_data_snapshots")
    )
    status: Mapped[str] = mapped_column(String(50))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class DataArtifact(ImmutableCreatedAt, Base):
    __tablename__ = "data_artifacts"
    __table_args__ = (UniqueConstraint("sha256", name="uq_data_artifacts_sha256"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    uri: Mapped[str] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64))
    byte_size: Mapped[int] = mapped_column(BigInteger)
    mime_type: Mapped[str] = mapped_column(String(255))
    schema_version: Mapped[str] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(100))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    received_at: Mapped[datetime] = mapped_column(UTCDateTime())


class PriceSeriesManifest(ImmutableCreatedAt, Base):
    __tablename__ = "price_series_manifests"
    __table_args__ = (
        CheckConstraint(
            "delivery_mode IN ('real_time', 'delayed', 'fixture', 'unknown')",
            name="delivery_mode_allowed",
        ),
        CheckConstraint(
            "source <> 'fixture' OR delivery_mode = 'fixture'",
            name="fixture_delivery_mode",
        ),
        CheckConstraint(
            "source <> 'fosun' OR delivery_mode <> 'fixture'",
            name="fosun_delivery_mode",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    data_artifact_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "data_artifacts.id",
            name="fk_price_series_manifests_data_artifact_id_data_artifacts",
        )
    )
    instrument: Mapped[str] = mapped_column(String(100))
    source: Mapped[str] = mapped_column(String(100))
    feed: Mapped[str] = mapped_column(String(100))
    delivery_mode: Mapped[str] = mapped_column(String(20))
    adjustment: Mapped[str] = mapped_column(String(100))
    start_at: Mapped[datetime] = mapped_column(UTCDateTime())
    end_at: Mapped[datetime] = mapped_column(UTCDateTime())
    row_count: Mapped[int] = mapped_column(BigInteger)


class MacroObservation(ImmutableCreatedAt, Base):
    __tablename__ = "macro_observations"
    __table_args__ = (
        UniqueConstraint(
            "source", "series", "observed_at", "revision", name="uq_macro_observations_revision"
        ),
        UniqueConstraint(
            "source",
            "series",
            "provider_object_id",
            "real_time_start",
            "revision",
            name="uq_macro_observations_provider_vintage",
            postgresql_nulls_not_distinct=True,
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_macro_observations_evidence_ref",
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_macro_observations_evidence_owner",
        ),
        ForeignKeyConstraint(
            ["calendar_evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_macro_observations_calendar_evidence_ref",
        ),
        ForeignKeyConstraint(
            [
                "calendar_evidence_ref_id",
                "calendar_source",
                "calendar_provider_object_id",
                "calendar_revision",
            ],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_macro_observations_calendar_evidence_owner",
        ),
        CheckConstraint(
            "series IN ('DFF', 'DGS2', 'DGS10', 'CPIAUCSL', "
            "'UNRATE', 'INDPRO', 'VIXCLS', 'DEXHKUS')",
            name="series_allowed",
        ),
        CheckConstraint(
            "real_time_start IS NULL OR real_time_end IS NOT NULL AND "
            "real_time_end >= real_time_start",
            name="real_time_period_allowed",
        ),
        CheckConstraint(
            "source <> 'fred' OR (provider_object_id IS NOT NULL AND "
            "real_time_start IS NOT NULL AND real_time_end IS NOT NULL AND "
            "evidence_ref_id IS NOT NULL AND calendar_evidence_ref_id IS NOT NULL AND "
            "calendar_source IS NOT NULL AND calendar_provider_object_id IS NOT NULL AND "
            "calendar_revision IS NOT NULL)",
            name="macro_fred_authority_required",
        ),
        CheckConstraint(
            "(series = 'DEXHKUS') = display_sanity_only",
            name="dexhkus_display_sanity_only",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    source: Mapped[str] = mapped_column(String(100))
    series: Mapped[str] = mapped_column(String(255))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    revision: Mapped[str] = mapped_column(String(255))
    value: Mapped[Decimal]
    unit: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str | None] = mapped_column(String(255))
    real_time_start: Mapped[date | None] = mapped_column(Date)
    real_time_end: Mapped[date | None] = mapped_column(Date)
    evidence_ref_id: Mapped[UUID | None]
    calendar_evidence_ref_id: Mapped[UUID | None]
    calendar_source: Mapped[str | None] = mapped_column(String(100))
    calendar_provider_object_id: Mapped[str | None] = mapped_column(String(255))
    calendar_revision: Mapped[str | None] = mapped_column(String(255))
    display_sanity_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

"""Versioned data-source, evidence, and analytics metadata mappings."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt


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
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProviderCapability(ImmutableCreatedAt, Base):
    __tablename__ = "provider_capabilities"
    __table_args__ = (
        UniqueConstraint(
            "provider", "capability", "contract_hash", name="uq_provider_capabilities_contract"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    provider: Mapped[str] = mapped_column(String(100))
    capability: Mapped[str] = mapped_column(String(100))
    contract_hash: Mapped[str] = mapped_column(String(64))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


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
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class QualityReport(ImmutableCreatedAt, Base):
    __tablename__ = "quality_reports"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
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
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class PriceSeriesManifest(ImmutableCreatedAt, Base):
    __tablename__ = "price_series_manifests"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    data_artifact_id: Mapped[UUID] = mapped_column(ForeignKey("data_artifacts.id"))
    instrument: Mapped[str] = mapped_column(String(100))
    source: Mapped[str] = mapped_column(String(100))
    feed: Mapped[str] = mapped_column(String(100))
    adjustment: Mapped[str] = mapped_column(String(100))
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    row_count: Mapped[int] = mapped_column(BigInteger)


class MacroObservation(ImmutableCreatedAt, Base):
    __tablename__ = "macro_observations"
    __table_args__ = (
        UniqueConstraint(
            "source", "series", "observed_at", "revision", name="uq_macro_observations_revision"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    source: Mapped[str] = mapped_column(String(100))
    series: Mapped[str] = mapped_column(String(255))
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revision: Mapped[str] = mapped_column(String(255))
    value: Mapped[Decimal]
    unit: Mapped[str] = mapped_column(String(100))

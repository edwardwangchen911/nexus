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
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.market.limits import FOSUN_DAILY_MAX_INSTRUMENTS
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
    ProviderCapability.created_at,
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


class FosunDailyBarStaging(ImmutableCreatedAt, Base):
    """Append-only reservation and fetched-raw records for one daily shard."""

    __tablename__ = "fosun_daily_bar_staging"
    __table_args__ = (
        UniqueConstraint(
            "fetch_job_id",
            "record_kind",
            name="uq_fosun_daily_staging_fetch_kind",
        ),
        UniqueConstraint(
            "root_job_id",
            "instrument_id",
            "record_kind",
            name="uq_fosun_daily_staging_root_instrument_kind",
        ),
        CheckConstraint(
            "record_kind IN ('reservation','fetched')",
            name="record_kind_allowed",
        ),
        CheckConstraint(
            "schema_version = 'fosun_daily_staging_v1'",
            name="schema_version_allowed",
        ),
        CheckConstraint(
            "child_input_hash ~ '^[0-9a-f]{64}$' AND universe_content_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        CheckConstraint(
            "ordinal >= 0 AND total_instruments BETWEEN 1 AND "
            f"{FOSUN_DAILY_MAX_INSTRUMENTS} AND ordinal < total_instruments",
            name="shard_bounds",
        ),
        CheckConstraint(
            "request_start_at < request_end_at",
            name="request_window_order",
        ),
        CheckConstraint(
            "(record_kind = 'reservation' AND "
            "num_nonnulls(raw_artifact_id,raw_artifact_identity_sha256,gateway_request_id,request_body_sha256,"
            "sent_at,received_at,provider_capability_id,provider_status_id,"
            "provider_status_version,provider_contract_hash,provider_authorized_at,"
            "delivery_mode) = 0) OR "
            "(record_kind = 'fetched' AND "
            "num_nonnulls(raw_artifact_id,raw_artifact_identity_sha256,gateway_request_id,request_body_sha256,"
            "sent_at,received_at,provider_capability_id,provider_status_id,"
            "provider_status_version,provider_contract_hash,provider_authorized_at,"
            "delivery_mode) = 12)",
            name="record_shape",
        ),
        CheckConstraint(
            "raw_artifact_identity_sha256 IS NULL OR "
            "raw_artifact_identity_sha256 ~ '^[0-9a-f]{64}$'",
            name="raw_artifact_identity_hash_canonical",
        ),
        CheckConstraint(
            "request_body_sha256 IS NULL OR request_body_sha256 ~ '^[0-9a-f]{64}$'",
            name="request_hash_canonical",
        ),
        CheckConstraint(
            "provider_contract_hash IS NULL OR provider_contract_hash ~ '^[0-9a-f]{64}$'",
            name="contract_hash_canonical",
        ),
        CheckConstraint(
            "provider_status_version IS NULL OR provider_status_version > 0",
            name="status_version_positive",
        ),
        CheckConstraint(
            "delivery_mode IS NULL OR delivery_mode IN ('unknown','real_time','delayed')",
            name="delivery_mode_allowed",
        ),
        CheckConstraint(
            "sent_at IS NULL OR (received_at >= sent_at AND provider_authorized_at >= received_at)",
            name="transport_time_order",
        ),
        Index("ix_fosun_daily_staging_root", "root_job_id", "record_kind"),
        Index("ix_fosun_daily_staging_artifact", "raw_artifact_id"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    root_job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.id", name="fk_fosun_daily_staging_root_job")
    )
    # Deliberately not an FK: the worker holds a row lock on its Job while the
    # independently committed reservation is inserted. The RLS policy and
    # handler validate this identity against jobs without taking an FK key lock.
    fetch_job_id: Mapped[UUID]
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_fosun_daily_staging_instrument")
    )
    record_kind: Mapped[str] = mapped_column(String(20))
    schema_version: Mapped[str] = mapped_column(String(32))
    child_input_hash: Mapped[str] = mapped_column(String(64))
    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "exchange_calendar_authorities.id",
            name="fk_fosun_daily_staging_calendar",
        )
    )
    universe_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "universe_snapshots.id",
            name="fk_fosun_daily_staging_universe",
        )
    )
    universe_content_hash: Mapped[str] = mapped_column(String(64))
    ordinal: Mapped[int] = mapped_column(Integer)
    total_instruments: Mapped[int] = mapped_column(Integer)
    recorded_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, server_default=func.clock_timestamp()
    )
    request_start_at: Mapped[datetime] = mapped_column(UTCDateTime())
    request_end_at: Mapped[datetime] = mapped_column(UTCDateTime())
    raw_artifact_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("data_artifacts.id", name="fk_fosun_daily_staging_raw_artifact")
    )
    raw_artifact_identity_sha256: Mapped[str | None] = mapped_column(String(64))
    gateway_request_id: Mapped[UUID | None]
    request_body_sha256: Mapped[str | None] = mapped_column(String(64))
    sent_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    received_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    provider_capability_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "provider_capabilities.id",
            name="fk_fosun_daily_staging_provider_capability",
        )
    )
    provider_status_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "integration_status.id",
            name="fk_fosun_daily_staging_provider_status",
        )
    )
    provider_status_version: Mapped[int | None] = mapped_column(Integer)
    provider_contract_hash: Mapped[str | None] = mapped_column(String(64))
    provider_authorized_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    delivery_mode: Mapped[str | None] = mapped_column(String(20))


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
        CheckConstraint("interval IN ('1Min', '1Day')", name="interval_allowed"),
        CheckConstraint(
            "num_nonnulls(provider_capability_id,provider_status_id,"
            "provider_status_version,provider_contract_hash,provider_authorized_at) "
            "IN (0,5)",
            name="provider_binding_complete",
        ),
        CheckConstraint(
            "source = 'fosun' OR num_nonnulls(provider_capability_id,provider_status_id,"
            "provider_status_version,provider_contract_hash,provider_authorized_at) = 0",
            name="provider_binding_fosun_only",
        ),
        CheckConstraint(
            "provider_status_version IS NULL OR provider_status_version > 0",
            name="provider_status_version_positive",
        ),
        CheckConstraint(
            "provider_contract_hash IS NULL OR provider_contract_hash ~ '^[0-9a-f]{64}$'",
            name="provider_contract_hash_canonical",
        ),
        CheckConstraint(
            "(source = 'fosun' AND gateway_transport_artifact_id IS NOT NULL AND "
            "((interval = '1Day' AND daily_calendar_authority_id IS NOT NULL AND "
            "gateway_request_start_at IS NOT NULL AND gateway_request_end_at IS NOT NULL) OR "
            "(interval = '1Min' AND daily_calendar_authority_id IS NULL AND "
            "gateway_request_start_at IS NULL AND gateway_request_end_at IS NULL))) OR "
            "(source <> 'fosun' AND gateway_transport_artifact_id IS NULL AND "
            "daily_calendar_authority_id IS NULL AND gateway_request_start_at IS NULL AND "
            "gateway_request_end_at IS NULL)",
            name="fosun_transport_lineage_complete",
        ),
        Index(
            "ix_price_series_manifests_provider_binding",
            "provider_capability_id",
            "provider_status_id",
            "provider_authorized_at",
        ),
        Index(
            "ix_price_series_manifests_gateway_transport",
            "gateway_transport_artifact_id",
        ),
        Index(
            "ix_price_series_manifests_daily_calendar",
            "daily_calendar_authority_id",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    evidence_ref_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("research_evidence_refs.id", name="fk_price_series_evidence_ref"),
        unique=True,
    )
    data_artifact_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "data_artifacts.id",
            name="fk_price_series_manifests_data_artifact_id_data_artifacts",
        )
    )
    gateway_transport_artifact_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "data_artifacts.id",
            name="fk_price_series_manifests_gateway_transport_artifact",
        )
    )
    daily_calendar_authority_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "exchange_calendar_authorities.id",
            name="fk_price_series_manifests_daily_calendar_authority",
        )
    )
    gateway_request_start_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    gateway_request_end_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    instrument: Mapped[str] = mapped_column(String(100))
    source: Mapped[str] = mapped_column(String(100))
    feed: Mapped[str] = mapped_column(String(100))
    delivery_mode: Mapped[str] = mapped_column(String(20))
    adjustment: Mapped[str] = mapped_column(String(100))
    interval: Mapped[str] = mapped_column(String(8), default="1Day")
    start_at: Mapped[datetime] = mapped_column(UTCDateTime())
    end_at: Mapped[datetime] = mapped_column(UTCDateTime())
    row_count: Mapped[int] = mapped_column(BigInteger)
    provider_capability_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "provider_capabilities.id",
            name="fk_price_series_manifests_provider_capability",
        )
    )
    provider_status_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "integration_status.id",
            name="fk_price_series_manifests_provider_status",
        )
    )
    provider_status_version: Mapped[int | None] = mapped_column(Integer)
    provider_contract_hash: Mapped[str | None] = mapped_column(String(64))
    provider_authorized_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class PriceBarRecord(ImmutableCreatedAt, Base):
    """Database-resident normalized bars for zero-network frozen research features."""

    __tablename__ = "price_bar_records"
    __table_args__ = (
        UniqueConstraint(
            "price_series_manifest_id",
            "observed_at",
            "revision",
            name="uq_price_bar_records_manifest_observation_revision",
        ),
        CheckConstraint("high >= open AND high >= close AND high >= low", name="high_valid"),
        CheckConstraint("low <= open AND low <= close AND low <= high", name="low_valid"),
        CheckConstraint("open > 0 AND high > 0 AND low > 0 AND close > 0", name="price_positive"),
        CheckConstraint("volume >= 0", name="volume_nonnegative"),
        CheckConstraint("available_at >= observed_at", name="available_after_observed"),
        CheckConstraint("received_at >= available_at", name="received_after_available"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    price_series_manifest_id: Mapped[UUID] = mapped_column(
        ForeignKey("price_series_manifests.id", name="fk_price_bar_records_manifest")
    )
    instrument_id: Mapped[UUID]
    session: Mapped[date] = mapped_column(Date)
    open: Mapped[Decimal]
    high: Mapped[Decimal]
    low: Mapped[Decimal]
    close: Mapped[Decimal]
    volume: Mapped[Decimal]
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    received_at: Mapped[datetime] = mapped_column(UTCDateTime())
    revision: Mapped[str] = mapped_column(String(255))
    quality_flags: Mapped[list[str]] = mapped_column(JSONB)


class PriceBarSetAuthority(ImmutableCreatedAt, Base):
    """Immutable content authority for one complete normalized Bar manifest."""

    __tablename__ = "price_bar_set_authorities"
    __table_args__ = (
        ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_price_bar_set_authorities_evidence_owner",
        ),
        UniqueConstraint("price_series_manifest_id", name="uq_price_bar_set_authority_manifest"),
        UniqueConstraint("evidence_ref_id", name="uq_price_bar_set_authority_evidence"),
        UniqueConstraint(
            "id",
            "source",
            "content_hash",
            name="uq_price_bar_set_authorities_id_source_hash",
        ),
        CheckConstraint(
            "(hash_version = 'price_bar_set_sha256_v1' "
            "AND policy_version = 'agent_price_authority_v1') OR "
            "(hash_version = 'price_bar_set_sha256_v2' "
            "AND policy_version = 'agent_price_authority_v2')",
            name="hash_policy_version_tuple",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        CheckConstraint("byte_length > 0", name="byte_length_positive"),
        CheckConstraint("bar_count BETWEEN 1 AND 10000", name="bar_count_bounded"),
        CheckConstraint("first_session <= last_session", name="session_ordered"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    price_series_manifest_id: Mapped[UUID] = mapped_column(
        ForeignKey("price_series_manifests.id", name="fk_price_bar_set_authorities_manifest")
    )
    evidence_ref_id: Mapped[UUID]
    source_policy_id: Mapped[UUID] = mapped_column(
        ForeignKey("source_policies.id", name="fk_price_bar_set_authorities_source_policy")
    )
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    hash_version: Mapped[str] = mapped_column(String(64), default="price_bar_set_sha256_v1")
    policy_version: Mapped[str] = mapped_column(String(64), default="agent_price_authority_v1")
    content_hash: Mapped[str] = mapped_column(String(64))
    byte_length: Mapped[int] = mapped_column(BigInteger)
    bar_count: Mapped[int] = mapped_column(Integer)
    first_session: Mapped[date] = mapped_column(Date)
    last_session: Mapped[date] = mapped_column(Date)
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())


class IntradayBarSetAuthority(ImmutableCreatedAt, Base):
    """Immutable display-only authority for one normalized 1Min manifest."""

    __tablename__ = "intraday_bar_set_authorities"
    __table_args__ = (
        ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_intraday_bar_set_authorities_evidence_owner",
        ),
        UniqueConstraint("price_series_manifest_id", name="uq_intraday_bar_set_authority_manifest"),
        UniqueConstraint("evidence_ref_id", name="uq_intraday_bar_set_authority_evidence"),
        UniqueConstraint(
            "id",
            "source",
            "content_hash",
            name="uq_intraday_bar_set_authorities_id_source_hash",
        ),
        CheckConstraint(
            "(hash_version = 'intraday_bar_set_sha256_v1' "
            "AND policy_version = 'intraday_monitor_authority_v1') OR "
            "(hash_version = 'intraday_bar_set_sha256_v2' "
            "AND policy_version = 'intraday_monitor_authority_v2')",
            name="hash_policy_version_tuple",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        CheckConstraint("byte_length > 0", name="byte_length_positive"),
        CheckConstraint("bar_count BETWEEN 1 AND 90", name="bar_count_bounded"),
        CheckConstraint("first_observed_at <= last_observed_at", name="observation_time_ordered"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    price_series_manifest_id: Mapped[UUID] = mapped_column(
        ForeignKey("price_series_manifests.id", name="fk_intraday_bar_set_authorities_manifest")
    )
    evidence_ref_id: Mapped[UUID]
    source_policy_id: Mapped[UUID] = mapped_column(
        ForeignKey("source_policies.id", name="fk_intraday_bar_set_authorities_source_policy")
    )
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    hash_version: Mapped[str] = mapped_column(String(64), default="intraday_bar_set_sha256_v1")
    policy_version: Mapped[str] = mapped_column(String(64), default="intraday_monitor_authority_v1")
    content_hash: Mapped[str] = mapped_column(String(64))
    byte_length: Mapped[int] = mapped_column(BigInteger)
    bar_count: Mapped[int] = mapped_column(Integer)
    first_observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    last_observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())


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
            "authority_version IS NULL OR authority_version IN "
            "('legacy_task3', 'research_macro_v1')",
            name="macro_authority_version_allowed",
        ),
        CheckConstraint(
            "COALESCE((authority_version = 'legacy_task3' "
            "AND lower(source) IN ('fred', 'alfred')) OR "
            "(authority_version = 'research_macro_v1' AND source IN ('fred', 'alfred') "
            "AND provider_object_id IS NOT NULL AND real_time_start IS NOT NULL "
            "AND real_time_end IS NOT NULL AND evidence_ref_id IS NOT NULL "
            "AND calendar_evidence_ref_id IS NOT NULL AND calendar_source IS NOT NULL "
            "AND calendar_provider_object_id IS NOT NULL "
            "AND calendar_revision IS NOT NULL) OR "
            "(authority_version IS NULL AND lower(source) NOT IN ('fred', 'alfred')), FALSE)",
            name="macro_research_v1_authority_required",
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
    authority_version: Mapped[str | None] = mapped_column(String(32))
    real_time_start: Mapped[date | None] = mapped_column(Date)
    real_time_end: Mapped[date | None] = mapped_column(Date)
    evidence_ref_id: Mapped[UUID | None]
    calendar_evidence_ref_id: Mapped[UUID | None]
    calendar_source: Mapped[str | None] = mapped_column(String(100))
    calendar_provider_object_id: Mapped[str | None] = mapped_column(String(255))
    calendar_revision: Mapped[str | None] = mapped_column(String(255))
    display_sanity_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

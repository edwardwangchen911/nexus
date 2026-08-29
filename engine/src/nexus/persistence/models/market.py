"""Instrument master, universe, quote, and watchlist mappings."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class Instrument(ImmutableCreatedAt, Base):
    __tablename__ = "instruments"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    asset_type: Mapped[str] = mapped_column(String(50))
    exchange: Mapped[str] = mapped_column(String(20))
    currency: Mapped[str] = mapped_column(String(3))
    tick_size: Mapped[Decimal | None]
    risk_bucket: Mapped[str] = mapped_column(String(100))


class InstrumentAlias(ImmutableCreatedAt, Base):
    __tablename__ = "instrument_aliases"
    __table_args__ = (
        CheckConstraint(
            "valid_to IS NULL OR valid_to >= valid_from",
            name="instrument_alias_valid_range",
        ),
        UniqueConstraint("symbol", "valid_from", name="uq_instrument_aliases_symbol_valid_from"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_instrument_aliases_instrument_id_instruments")
    )
    symbol: Mapped[str] = mapped_column(String(32))
    alias_type: Mapped[str] = mapped_column(String(32), default="ticker")
    valid_from: Mapped[date] = mapped_column(Date)
    valid_to: Mapped[date | None] = mapped_column(Date)


class UniverseSnapshot(ImmutableCreatedAt, Base):
    __tablename__ = "universe_snapshots"
    __table_args__ = (
        UniqueConstraint("key", "version", name="uq_universe_snapshots_key_version"),
        UniqueConstraint("content_hash", name="uq_universe_snapshots_content_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(100))
    version: Mapped[str] = mapped_column(String(100))
    effective_from: Mapped[date] = mapped_column(Date)
    source: Mapped[str] = mapped_column(String(255))
    content_hash: Mapped[str] = mapped_column(String(64))
    published_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class UniverseMember(ImmutableCreatedAt, Base):
    __tablename__ = "universe_members"
    __table_args__ = (
        UniqueConstraint(
            "universe_snapshot_id",
            "instrument_id",
            name="uq_universe_members_snapshot_instrument",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    universe_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "universe_snapshots.id",
            name="fk_universe_members_universe_snapshot_id_universe_snapshots",
        )
    )
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_universe_members_instrument_id_instruments")
    )
    inclusion_reason: Mapped[str] = mapped_column(String(255))


class SectorExposure(ImmutableCreatedAt, Base):
    __tablename__ = "sector_exposures"
    __table_args__ = (
        CheckConstraint("weight >= 0 AND weight <= 1", name="sector_exposure_weight"),
        UniqueConstraint(
            "instrument_id",
            "sector_code",
            "as_of",
            "content_hash",
            name="uq_sector_exposures_instrument_sector_as_of_hash",
        ),
        UniqueConstraint(
            "id",
            "instrument_id",
            "sector_code",
            "weight",
            "content_hash",
            name="uq_sector_exposures_id_instrument_sector_weight_hash",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_sector_exposures_instrument_id_instruments")
    )
    sector_code: Mapped[str] = mapped_column(String(100))
    weight: Mapped[Decimal]
    as_of: Mapped[date] = mapped_column(Date)
    source_url: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))
    synthetic: Mapped[bool] = mapped_column(Boolean)
    reviewed: Mapped[bool] = mapped_column(Boolean)


class LatestQuote(ImmutableCreatedAt, Base):
    __tablename__ = "latest_quotes"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id",
            "source",
            "feed",
            "event_kind",
            "observed_at",
            "content_hash",
            name="uq_latest_quotes_observation",
        ),
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
        CheckConstraint(
            "event_kind NOT IN ('trade','quote') OR "
            "(source = 'alpaca' AND evidence_status = 'verified' AND "
            "(((feed = 'iex' OR feed = 'sip') AND delivery_mode = 'real_time') OR "
            "(feed = 'delayed_sip' AND delivery_mode = 'delayed'))) OR "
            "(source = 'fixture' AND evidence_status = 'fixture' "
            "AND feed = 'fixture' AND delivery_mode = 'fixture') OR "
            "(source NOT IN ('alpaca','fixture') AND evidence_status = 'verified')",
            name="intraday_delivery_pair",
        ),
        CheckConstraint(
            "event_kind IN ('trade','quote','snapshot','fixture')",
            name="event_kind_allowed",
        ),
        CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$'",
            name="content_hash_canonical",
        ),
        CheckConstraint(
            "evidence_status IN ('verified','legacy_unverified','fixture')",
            name="evidence_status_allowed",
        ),
        CheckConstraint(
            "available_at >= observed_at AND received_at >= available_at",
            name="time_order",
        ),
        CheckConstraint("bid IS NULL OR bid > 0", name="bid_positive"),
        CheckConstraint("ask IS NULL OR ask > 0", name="ask_positive"),
        CheckConstraint("last IS NULL OR last > 0", name="last_positive"),
        CheckConstraint("bid IS NULL OR ask IS NULL OR bid <= ask", name="spread_order"),
        CheckConstraint(
            "(event_kind = 'trade' AND last IS NOT NULL AND bid IS NULL AND ask IS NULL) OR "
            "(event_kind = 'quote' AND bid IS NOT NULL AND ask IS NOT NULL AND last IS NULL) "
            "OR (event_kind IN ('snapshot','fixture') AND "
            "(bid IS NOT NULL OR ask IS NOT NULL OR last IS NOT NULL))",
            name="event_shape",
        ),
        CheckConstraint(
            "(source = 'fixture' AND evidence_status = 'fixture' "
            "AND data_artifact_id IS NULL) OR "
            "(source <> 'fixture' AND evidence_status = 'verified' "
            "AND data_artifact_id IS NOT NULL) OR "
            "(source <> 'fixture' AND evidence_status = 'legacy_unverified' "
            "AND data_artifact_id IS NULL)",
            name="evidence_state_consistent",
        ),
        Index(
            "ix_latest_quotes_observation_lookup",
            "instrument_id",
            "source",
            "feed",
            "event_kind",
            "observed_at",
            "available_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_latest_quotes_instrument_id_instruments")
    )
    source: Mapped[str] = mapped_column(String(100))
    feed: Mapped[str] = mapped_column(String(100))
    delivery_mode: Mapped[str] = mapped_column(String(20))
    bid: Mapped[Decimal | None]
    ask: Mapped[Decimal | None]
    last: Mapped[Decimal | None]
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    received_at: Mapped[datetime] = mapped_column(UTCDateTime())
    revision: Mapped[str] = mapped_column(String(255))
    quality_flags: Mapped[list[str]] = mapped_column(JSONB)
    event_kind: Mapped[str] = mapped_column(String(16))
    content_hash: Mapped[str] = mapped_column(String(64))
    evidence_status: Mapped[str] = mapped_column(String(24))
    data_artifact_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("data_artifacts.id", name="fk_latest_quotes_data_artifact")
    )


class Watchlist(ImmutableCreatedAt, Base):
    __tablename__ = "watchlists"
    __table_args__ = (UniqueConstraint("name", name="uq_watchlists_name"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    name: Mapped[str] = mapped_column(String(100))


class WatchlistMember(ImmutableCreatedAt, Base):
    __tablename__ = "watchlist_members"
    __table_args__ = (
        UniqueConstraint(
            "watchlist_id",
            "instrument_id",
            name="uq_watchlist_members_watchlist_instrument",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    watchlist_id: Mapped[UUID] = mapped_column(
        ForeignKey("watchlists.id", name="fk_watchlist_members_watchlist_id_watchlists")
    )
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_watchlist_members_instrument_id_instruments")
    )

"""Instrument master, universe, quote, and watchlist mappings."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, Date, ForeignKey, String, Text, UniqueConstraint
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
            "instrument_id", "source", "feed", name="uq_latest_quotes_instrument_source_feed"
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

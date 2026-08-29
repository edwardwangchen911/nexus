"""Closed 15-second schedule authority for the display-only intraday monitor."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.jobs.market_authority import (
    FrozenFosunMarketAuthority,
    load_frozen_fosun_market_authority,
)
from nexus.jobs.models import (
    JobKeyV1,
    JobType,
    QueueName,
    RetryPolicy,
    job_input_hash,
)
from nexus.jobs.repository import enqueue_job
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    Watchlist,
    WatchlistMember,
)
from nexus.persistence.models.runtime import Job
from nexus.providers.settings import IntradayMonitorMode, MarketProvider, ProviderSettings

_SCHEMA = "intraday_snapshot_job_v2"
_DEFAULT_WATCHLIST_ID = UUID("8c7cc45f-9420-5ea5-a75e-7df8a8a63b58")
_DEFAULT_WATCHLIST_NAME = "default_private"
_US_EXCHANGES = frozenset({"AMEX", "ARCA", "NASDAQ", "NYSE", "NYSEARCA", "XNYS"})
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
_CONTRACT_DOCUMENT = {
    "schema": _SCHEMA,
    "bucket_seconds": 15,
    "source": "fosun",
    "max_symbols": 10,
    "exchange": "XNYS",
    "watchlist_id": str(_DEFAULT_WATCHLIST_ID),
    "fields": (
        "calendar_authority_id",
        "session",
        "bucket_start",
        "bucket_end",
        "instruments",
        "symbol_set_hash",
        "source",
        "feed",
        "delivery_mode",
        "include_minute_bars",
        "quote_authority",
        "bars_authority",
        "contract_hash",
    ),
}
SNAPSHOT_CONTRACT_HASH = hashlib.sha256(
    canonical_json(_CONTRACT_DOCUMENT).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True)
class IntradayBucket:
    start: datetime
    end: datetime


@dataclass(frozen=True, slots=True)
class IntradayDatabaseAuthority:
    calendar_authority_id: UUID
    session: date
    instruments: tuple[tuple[UUID, str], ...]


class IntradaySnapshotInstrumentV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    symbol: str

    @field_validator("symbol")
    @classmethod
    def _symbol_is_safe(cls, value: str) -> str:
        if _SYMBOL.fullmatch(value) is None:
            raise ValueError("intraday symbol is invalid")
        return value


class IntradaySnapshotJobPayloadV2(BaseModel):
    """The complete immutable authority consumed by one collection Job."""

    model_config = ConfigDict(frozen=True, extra="forbid", serialize_by_alias=True)

    schema_version: Literal["intraday_snapshot_job_v2"] = Field(
        default="intraday_snapshot_job_v2",
        validation_alias="schema",
        serialization_alias="schema",
    )
    calendar_authority_id: UUID
    session: date
    bucket_start: str
    bucket_end: str
    instruments: tuple[IntradaySnapshotInstrumentV1, ...]
    symbol_set_hash: str
    source: Literal["fosun"] = "fosun"
    feed: Literal["l1"] = "l1"
    delivery_mode: Literal["unknown", "real_time", "delayed"]
    include_minute_bars: bool
    quote_authority: FrozenFosunMarketAuthority
    bars_authority: FrozenFosunMarketAuthority | None
    contract_hash: str

    @field_validator("bucket_start", "bucket_end")
    @classmethod
    def _canonical_time(cls, value: str) -> str:
        parsed = _parse_utc(value)
        if value != _utc_text(parsed):
            raise ValueError("intraday bucket timestamp is not canonical")
        return value

    @field_validator("symbol_set_hash", "contract_hash")
    @classmethod
    def _canonical_hash(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("intraday hash is invalid")
        return value

    @model_validator(mode="after")
    def _authority_is_consistent(self) -> Self:
        if self.contract_hash != SNAPSHOT_CONTRACT_HASH:
            raise ValueError("intraday contract hash mismatch")
        start = _parse_utc(self.bucket_start)
        end = _parse_utc(self.bucket_end)
        if end - start != timedelta(seconds=15):
            raise ValueError("intraday bucket must span 15 seconds")
        if bucket_for(start).start != start:
            raise ValueError("intraday bucket must align to 15 seconds")
        if not 1 <= len(self.instruments) <= 10:
            raise ValueError("intraday instruments must contain 1 to 10 items")
        identities = tuple((item.instrument_id, item.symbol) for item in self.instruments)
        if identities != tuple(sorted(identities, key=lambda item: (item[1], str(item[0])))):
            raise ValueError("intraday instruments must be canonically sorted")
        ids = tuple(item[0] for item in identities)
        symbols = tuple(item[1] for item in identities)
        if len(set(ids)) != len(ids) or len(set(symbols)) != len(symbols):
            raise ValueError("intraday instruments must be unique")
        if self.symbol_set_hash != _symbol_set_hash(identities):
            raise ValueError("intraday symbol set hash mismatch")
        minute_boundary = start.second == 0
        if self.include_minute_bars != minute_boundary:
            raise ValueError("intraday minute-bar intent contradicts bucket")
        if (
            self.quote_authority.operation != "fosun_get_quotes"
            or self.quote_authority.delivery_mode != self.delivery_mode
        ):
            raise ValueError("intraday quote authority mismatch")
        if self.include_minute_bars:
            if (
                self.bars_authority is None
                or self.bars_authority.operation != "fosun_get_bars"
                or self.bars_authority.delivery_mode != self.delivery_mode
            ):
                raise ValueError("intraday bars authority mismatch")
        elif self.bars_authority is not None:
            raise ValueError("intraday bars authority is not allowed off boundary")
        return self


IntradaySnapshotJobPayloadV1 = IntradaySnapshotJobPayloadV2


def bucket_for(now: datetime) -> IntradayBucket:
    """Return the exact half-open UTC bucket containing ``now``."""
    current = _require_utc(now, field="now")
    floored_second = current.second - current.second % 15
    start = current.replace(second=floored_second, microsecond=0)
    return IntradayBucket(start=start, end=start + timedelta(seconds=15))


def build_intraday_payload(
    *,
    calendar_authority_id: UUID,
    session: date | str,
    bucket: IntradayBucket,
    instruments: tuple[tuple[UUID, str], ...],
    quote_authority: FrozenFosunMarketAuthority,
    bars_authority: FrozenFosunMarketAuthority | None,
) -> IntradaySnapshotJobPayloadV2:
    ordered = tuple(sorted(instruments, key=lambda item: (item[1], str(item[0]))))
    return IntradaySnapshotJobPayloadV2(
        calendar_authority_id=calendar_authority_id,
        session=date.fromisoformat(session) if isinstance(session, str) else session,
        bucket_start=_utc_text(bucket.start),
        bucket_end=_utc_text(bucket.end),
        instruments=tuple(
            IntradaySnapshotInstrumentV1(instrument_id=instrument_id, symbol=symbol)
            for instrument_id, symbol in ordered
        ),
        symbol_set_hash=_symbol_set_hash(ordered),
        delivery_mode=quote_authority.delivery_mode,
        include_minute_bars=bucket.start.second == 0,
        quote_authority=quote_authority,
        bars_authority=bars_authority,
        contract_hash=SNAPSHOT_CONTRACT_HASH,
    )


async def schedule_due_intraday_snapshot(
    session: AsyncSession,
    *,
    now: datetime,
    settings: ProviderSettings,
) -> Job | None:
    """Enqueue one canonical bucket only when every database authority is complete."""
    scheduled_at = _require_utc(now, field="now")
    if settings.intraday_monitor_mode is not IntradayMonitorMode.PRODUCTION:
        return None
    if settings.market_provider is not MarketProvider.FOSUN:
        return None
    authority = await _load_current_intraday_authority(
        session,
        effective_at=scheduled_at,
        max_symbols=settings.intraday_max_symbols,
    )
    if authority is None:
        return None
    bucket = bucket_for(scheduled_at)
    quote_authority = await load_frozen_fosun_market_authority(
        session,
        check_time=scheduled_at,
        operation="fosun_get_quotes",
        delivery_mode=settings.fosun_market_delivery_mode,
    )
    if quote_authority is None:
        return None
    bars_authority = None
    if bucket.start.second == 0:
        bars_authority = await load_frozen_fosun_market_authority(
            session,
            check_time=scheduled_at,
            operation="fosun_get_bars",
            delivery_mode=settings.fosun_market_delivery_mode,
        )
        if bars_authority is None:
            return None
    try:
        payload = build_intraday_payload(
            calendar_authority_id=authority.calendar_authority_id,
            session=authority.session,
            bucket=bucket,
            instruments=authority.instruments,
            quote_authority=quote_authority,
            bars_authority=bars_authority,
        )
    except ValueError:
        return None
    encoded = payload.model_dump(mode="json")
    return await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=JobType.COLLECT_INTRADAY_SNAPSHOT,
            scope_kind="provider",
            scope_id="fosun-intraday",
            effective_at=bucket.start,
            input_hash=job_input_hash(encoded),
        ),
        schema_version=_SCHEMA,
        payload=encoded,
        queue=QueueName.PROVIDER,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=3,
        available_at=bucket.start,
    )


async def revalidate_intraday_payload_authority(
    session: AsyncSession,
    payload: IntradaySnapshotJobPayloadV2,
    *,
    effective_at: datetime,
    settings: ProviderSettings,
) -> bool:
    """Recheck every mutable/database authority before any external request."""
    bucket_start = _parse_utc(payload.bucket_start)
    bucket_end = _parse_utc(payload.bucket_end)
    check_time = _require_utc(effective_at, field="effective_at")
    if (
        settings.intraday_monitor_mode is not IntradayMonitorMode.PRODUCTION
        or not bucket_start <= check_time < bucket_end + timedelta(seconds=45)
        or settings.market_provider is not MarketProvider.FOSUN
        or settings.fosun_market_delivery_mode != payload.delivery_mode
    ):
        return False
    quote_authority = await load_frozen_fosun_market_authority(
        session,
        check_time=check_time,
        operation="fosun_get_quotes",
        delivery_mode=payload.delivery_mode,
    )
    if quote_authority != payload.quote_authority:
        return False
    if payload.include_minute_bars:
        bars_authority = await load_frozen_fosun_market_authority(
            session,
            check_time=check_time,
            operation="fosun_get_bars",
            delivery_mode=payload.delivery_mode,
        )
        if bars_authority != payload.bars_authority:
            return False
    authority = await _load_current_intraday_authority(
        session,
        effective_at=check_time,
        max_symbols=settings.intraday_max_symbols,
    )
    if authority is None:
        return False
    payload_identities = tuple(
        (item.instrument_id, item.symbol) for item in payload.instruments
    )
    return (
        authority.calendar_authority_id == payload.calendar_authority_id
        and authority.session == payload.session
        and authority.instruments == payload_identities
    )


async def _load_current_intraday_authority(
    session: AsyncSession,
    *,
    effective_at: datetime,
    max_symbols: int,
) -> IntradayDatabaseAuthority | None:
    authority_at = _require_utc(effective_at, field="effective_at")
    bucket_end = bucket_for(authority_at).end
    calendar_row = (
        await session.execute(
            select(
                ExchangeCalendarSessionRecord.calendar_authority_id,
                ExchangeCalendarSessionRecord.session,
            )
            .join(
                ExchangeCalendarAuthorityRecord,
                ExchangeCalendarAuthorityRecord.id
                == ExchangeCalendarSessionRecord.calendar_authority_id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= authority_at,
                ExchangeCalendarSessionRecord.market_open_at <= authority_at,
                bucket_end <= ExchangeCalendarSessionRecord.market_close_at,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(2)
        )
    ).all()
    if len(calendar_row) != 1:
        return None
    calendar_authority_id, session_date = calendar_row[0]
    watchlist = await session.get(Watchlist, _DEFAULT_WATCHLIST_ID)
    named_watchlist_id = await session.scalar(
        select(Watchlist.id).where(Watchlist.name == _DEFAULT_WATCHLIST_NAME)
    )
    if (
        watchlist is None
        or watchlist.name != _DEFAULT_WATCHLIST_NAME
        or named_watchlist_id != _DEFAULT_WATCHLIST_ID
    ):
        return None
    member_count = await session.scalar(
        select(func.count())
        .select_from(WatchlistMember)
        .where(WatchlistMember.watchlist_id == _DEFAULT_WATCHLIST_ID)
    )
    if member_count is None or not 1 <= member_count <= max_symbols:
        return None
    rows = (
        await session.execute(
            select(
                Instrument.id,
                InstrumentAlias.symbol,
                Instrument.exchange,
                Instrument.currency,
            )
            .select_from(WatchlistMember)
            .join(Instrument, Instrument.id == WatchlistMember.instrument_id)
            .join(InstrumentAlias, InstrumentAlias.instrument_id == Instrument.id)
            .where(
                WatchlistMember.watchlist_id == _DEFAULT_WATCHLIST_ID,
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= session_date,
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= session_date,
                ),
            )
        )
    ).all()
    if len(rows) != member_count:
        return None
    if any(
        exchange.upper() not in _US_EXCHANGES or currency.upper() != "USD"
        for _instrument_id, _symbol, exchange, currency in rows
    ):
        return None
    identities = tuple(
        sorted(
            ((instrument_id, symbol) for instrument_id, symbol, _, _ in rows),
            key=lambda item: (item[1], str(item[0])),
        )
    )
    return IntradayDatabaseAuthority(
        calendar_authority_id=calendar_authority_id,
        session=session_date,
        instruments=identities,
    )


def _symbol_set_hash(instruments: tuple[tuple[UUID, str], ...]) -> str:
    document = [
        {"instrument_id": str(instrument_id), "symbol": symbol}
        for instrument_id, symbol in instruments
    ]
    return hashlib.sha256(canonical_json(document).encode("utf-8")).hexdigest()


def _require_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must use UTC")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return _require_utc(value, field="timestamp").strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError("intraday timestamp is invalid") from error
    return parsed

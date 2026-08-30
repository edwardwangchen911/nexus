"""Strict payload authorities shared by daily scheduling and local adapters."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from json import dumps

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from nexus.market.limits import FOSUN_DAILY_MAX_INSTRUMENTS

ROOT_SCHEMA = "daily_pipeline_root_v1"
STAGE_SCHEMA = "daily_pipeline_stage_v1"
FOSUN_DAILY_INSTRUMENT_SCHEMA = "fosun_daily_instrument_v2"
FOSUN_DAILY_PUBLISH_SCHEMA = "fosun_daily_publish_v2"
PROFILE_PATTERN = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_SESSION = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_UTC_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
_HASH64 = re.compile(r"^[0-9a-f]{64}$")
_US_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")


def is_canonical_us_ticker_symbol(value: str) -> bool:
    """Match the exact Provider payload contract without case normalization."""
    return _US_SYMBOL.fullmatch(value) is not None


def instrument_alias_identity_sha256(
    *,
    alias_id: str,
    instrument_id: str,
    symbol: str,
    alias_type: str,
    valid_from: date,
    valid_to: date | None,
    created_at: datetime,
) -> str:
    """Canonical identity for the mutable instrument-alias row frozen into a Job."""
    payload = dumps(
        {
            "alias_id": alias_id,
            "alias_type": alias_type,
            "created_at": format_utc(created_at),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "valid_from": valid_from.isoformat(),
            "valid_to": None if valid_to is None else valid_to.isoformat(),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return sha256(payload).hexdigest()


def format_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("daily pipeline timestamps must use UTC")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_utc(value: str) -> datetime:
    if _UTC_TIMESTAMP.fullmatch(value) is None:
        raise ValueError("daily pipeline timestamp must be canonical UTC")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


class DailyPipelinePayload(BaseModel):
    """Frozen calendar and capability profile for one session root."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    calendar_authority_id: str = Field(pattern=_UUID.pattern)
    session: str = Field(pattern=_SESSION.pattern)
    market_close_at: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    planned_at: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    quality_cutoff_at: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    next_session: str | None = Field(default=None, pattern=_SESSION.pattern)
    next_paper_execution_at: str | None = Field(
        default=None,
        pattern=_UTC_TIMESTAMP.pattern,
    )
    last_session_of_week: bool
    profile_id: str = Field(pattern=PROFILE_PATTERN.pattern)
    universe_snapshot_id: str | None = Field(default=None, pattern=_UUID.pattern)
    universe_content_hash: str | None = Field(default=None, pattern=_HASH64.pattern)
    market_source: str | None = Field(default=None, pattern=r"^(fosun|alpaca)$")
    market_authority: FrozenFosunMarketAuthority | None = None

    @field_validator("session", "next_session")
    @classmethod
    def _valid_session(cls, value: str | None) -> str | None:
        if value is not None:
            date.fromisoformat(value)
        return value

    @field_validator(
        "market_close_at",
        "planned_at",
        "quality_cutoff_at",
        "next_paper_execution_at",
    )
    @classmethod
    def _valid_timestamp(cls, value: str | None) -> str | None:
        if value is not None:
            parse_utc(value)
        return value

    @model_validator(mode="after")
    def _next_session_binding(self) -> DailyPipelinePayload:
        if (self.next_session is None) != (self.next_paper_execution_at is None):
            raise ValueError("next session and execution time must be present together")
        if self.last_session_of_week and self.next_session is None:
            raise ValueError("week boundary requires the next authoritative session")
        if (self.universe_snapshot_id is None) != (self.universe_content_hash is None):
            raise ValueError("universe snapshot identity and hash must be present together")
        _validate_market_authority(self.market_source, self.market_authority)
        return self


class DailyStagePayload(BaseModel):
    """Closed payload shared by the orchestration-only stage adapters."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    root_job_id: str = Field(pattern=_UUID.pattern)
    calendar_authority_id: str = Field(pattern=_UUID.pattern)
    session: str = Field(pattern=_SESSION.pattern)
    market_close_at: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    scheduled_for: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    quality_cutoff_at: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    profile_id: str = Field(pattern=PROFILE_PATTERN.pattern)
    target_id: str | None = Field(default=None, pattern=_UUID.pattern)
    universe_snapshot_id: str | None = Field(default=None, pattern=_UUID.pattern)
    universe_content_hash: str | None = Field(default=None, pattern=_HASH64.pattern)
    market_source: str | None = Field(default=None, pattern=r"^(fosun|alpaca)$")
    market_authority: FrozenFosunMarketAuthority | None = None

    @field_validator("session")
    @classmethod
    def _valid_session(cls, value: str) -> str:
        date.fromisoformat(value)
        return value

    @field_validator("market_close_at", "scheduled_for", "quality_cutoff_at")
    @classmethod
    def _valid_timestamp(cls, value: str) -> str:
        parse_utc(value)
        return value

    @model_validator(mode="after")
    def _universe_binding(self) -> DailyStagePayload:
        if (self.universe_snapshot_id is None) != (self.universe_content_hash is None):
            raise ValueError("universe snapshot identity and hash must be present together")
        _validate_market_authority(self.market_source, self.market_authority)
        return self


class FosunDailyInstrumentPayload(BaseModel):
    """One immutable, single-request Fosun daily-bar shard authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    root_job_id: str = Field(pattern=_UUID.pattern)
    calendar_authority_id: str = Field(pattern=_UUID.pattern)
    session: str = Field(pattern=_SESSION.pattern)
    market_close_at: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    scheduled_for: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    profile_id: str = Field(pattern=PROFILE_PATTERN.pattern)
    universe_snapshot_id: str = Field(pattern=_UUID.pattern)
    universe_content_hash: str = Field(pattern=_HASH64.pattern)
    instrument_id: str = Field(pattern=_UUID.pattern)
    instrument_alias_id: str = Field(pattern=_UUID.pattern)
    instrument_symbol: str = Field(pattern=_US_SYMBOL.pattern)
    instrument_alias_identity_sha256: str = Field(pattern=_HASH64.pattern)
    ordinal: int = Field(ge=0, lt=FOSUN_DAILY_MAX_INSTRUMENTS)
    total_instruments: int = Field(ge=1, le=FOSUN_DAILY_MAX_INSTRUMENTS)
    market_authority: FrozenFosunMarketAuthority

    @field_validator("session")
    @classmethod
    def _valid_session(cls, value: str) -> str:
        date.fromisoformat(value)
        return value

    @field_validator("market_close_at", "scheduled_for")
    @classmethod
    def _valid_timestamp(cls, value: str) -> str:
        parse_utc(value)
        return value

    @model_validator(mode="after")
    def _valid_shard(self) -> FosunDailyInstrumentPayload:
        if self.ordinal >= self.total_instruments:
            raise ValueError("Fosun daily shard ordinal is outside the frozen set")
        if self.market_authority.operation != "fosun_get_bars":
            raise ValueError("daily market authority operation mismatch")
        return self


class FosunDailyChildRef(BaseModel):
    """Exact durable child identity admitted by the aggregate publisher."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str = Field(pattern=_UUID.pattern)
    instrument_id: str = Field(pattern=_UUID.pattern)
    instrument_alias_id: str = Field(pattern=_UUID.pattern)
    instrument_symbol: str = Field(pattern=_US_SYMBOL.pattern)
    instrument_alias_identity_sha256: str = Field(pattern=_HASH64.pattern)
    input_hash: str = Field(pattern=_HASH64.pattern)
    ordinal: int = Field(ge=0, lt=FOSUN_DAILY_MAX_INSTRUMENTS)


class FosunDailyPublishPayload(BaseModel):
    """Closed aggregate authority for one all-or-nothing daily publication."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    root_job_id: str = Field(pattern=_UUID.pattern)
    calendar_authority_id: str = Field(pattern=_UUID.pattern)
    session: str = Field(pattern=_SESSION.pattern)
    market_close_at: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    scheduled_for: str = Field(pattern=_UTC_TIMESTAMP.pattern)
    profile_id: str = Field(pattern=PROFILE_PATTERN.pattern)
    universe_snapshot_id: str = Field(pattern=_UUID.pattern)
    universe_content_hash: str = Field(pattern=_HASH64.pattern)
    market_authority: FrozenFosunMarketAuthority
    children: tuple[FosunDailyChildRef, ...] = Field(
        min_length=1,
        max_length=FOSUN_DAILY_MAX_INSTRUMENTS,
    )

    @field_validator("session")
    @classmethod
    def _valid_session(cls, value: str) -> str:
        date.fromisoformat(value)
        return value

    @field_validator("market_close_at", "scheduled_for")
    @classmethod
    def _valid_timestamp(cls, value: str) -> str:
        parse_utc(value)
        return value

    @model_validator(mode="after")
    def _valid_children(self) -> FosunDailyPublishPayload:
        if self.market_authority.operation != "fosun_get_bars":
            raise ValueError("daily market authority operation mismatch")
        expected = tuple(range(len(self.children)))
        actual = tuple(child.ordinal for child in self.children)
        if actual != expected:
            raise ValueError("Fosun daily children must be canonically ordered")
        if len({child.job_id for child in self.children}) != len(self.children):
            raise ValueError("Fosun daily child Job identity is duplicated")
        if len({child.instrument_id for child in self.children}) != len(self.children):
            raise ValueError("Fosun daily instrument identity is duplicated")
        if len({child.instrument_alias_id for child in self.children}) != len(self.children):
            raise ValueError("Fosun daily alias identity is duplicated")
        if len({child.instrument_symbol for child in self.children}) != len(self.children):
            raise ValueError("Fosun daily provider symbol is duplicated")
        return self


def _validate_market_authority(
    source: str | None,
    authority: FrozenFosunMarketAuthority | None,
) -> None:
    if source == "fosun":
        if authority is not None and authority.operation != "fosun_get_bars":
            raise ValueError("daily market authority operation mismatch")
        return
    if authority is not None:
        raise ValueError("daily Fosun authority requires Fosun source")

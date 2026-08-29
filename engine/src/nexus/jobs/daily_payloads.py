"""Strict payload authorities shared by daily scheduling and local adapters."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.jobs.market_authority import FrozenFosunMarketAuthority

ROOT_SCHEMA = "daily_pipeline_root_v1"
STAGE_SCHEMA = "daily_pipeline_stage_v1"
PROFILE_PATTERN = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_SESSION = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_UTC_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
_HASH64 = re.compile(r"^[0-9a-f]{64}$")


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
        if (self.universe_snapshot_id is None) != (
            self.universe_content_hash is None
        ):
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
        if (self.universe_snapshot_id is None) != (
            self.universe_content_hash is None
        ):
            raise ValueError("universe snapshot identity and hash must be present together")
        _validate_market_authority(self.market_source, self.market_authority)
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

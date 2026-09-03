"""Shared exact-decimal feature facts and PIT selectors; no backtest execution dependency."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.market.universe import EQUITY_SECTOR_CODES
from nexus.simulation.canonical import canonical_decimal


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BacktestBar(_FrozenModel):
    instrument_id: UUID
    session: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    observed_at: datetime
    available_at: datetime
    received_at: datetime
    revision: str = "1"
    quality_flags: tuple[str, ...] = ()

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def _reject_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("backtest values must not contain binary floats")
        return value

    @field_validator("open", "high", "low", "close", "volume", mode="after")
    @classmethod
    def _canonical_values(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @field_validator("observed_at", "available_at", "received_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _valid_bar(self) -> BacktestBar:
        if self.open <= 0 or self.high <= 0 or self.low <= 0 or self.close <= 0 or self.volume < 0:
            raise ValueError("backtest prices must be positive and volume nonnegative")
        if self.high < max(self.open, self.close, self.low) or self.low > min(
            self.open, self.close, self.high
        ):
            raise ValueError("backtest OHLC range is invalid")
        if not self.observed_at <= self.available_at <= self.received_at:
            raise ValueError("backtest bar availability timestamps are invalid")
        if self.quality_flags:
            raise ValueError("backtest bars with quality flags are not eligible")
        return self


class SectorClassification(_FrozenModel):
    authority_id: UUID
    instrument_id: UUID
    sector: str = Field(min_length=1, max_length=100)
    taxonomy: Literal["NEXUS_SECTOR_V1"] = "NEXUS_SECTOR_V1"
    effective_session: date
    published_at: datetime
    source_uri: str = Field(min_length=1, max_length=2048)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    weight: Decimal = Decimal(1)
    synthetic: bool = False
    reviewed: bool
    quality: Literal["PASS"] = "PASS"

    @field_validator("sector")
    @classmethod
    def _canonical_sector(cls, value: str) -> str:
        if value not in EQUITY_SECTOR_CODES:
            raise ValueError("sector must use the canonical NEXUS_SECTOR_V1 code")
        return value

    @field_validator("published_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("weight", mode="before")
    @classmethod
    def _reject_float_weight(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("sector weights must not contain binary floats")
        return value

    @field_validator("weight", mode="after")
    @classmethod
    def _canonical_weight(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @model_validator(mode="after")
    def _valid_weight(self) -> SectorClassification:
        if not Decimal(0) < self.weight <= Decimal(1):
            raise ValueError("sector weights must be positive and no greater than one")
        return self


def newer_bar(prior: BacktestBar | None, candidate: BacktestBar) -> BacktestBar:
    if prior is None:
        return candidate
    prior_key = (
        prior.available_at,
        prior.received_at,
        prior.revision,
        _json(prior.model_dump(mode="json")),
    )
    candidate_key = (
        candidate.available_at,
        candidate.received_at,
        candidate.revision,
        _json(candidate.model_dump(mode="json")),
    )
    return candidate if candidate_key > prior_key else prior


def select_sector_exposures(
    *,
    classifications: tuple[SectorClassification, ...],
    instrument_id: UUID,
    asset_type: str,
    sector_source: str,
    session: date,
    cutoff: datetime,
) -> tuple[SectorClassification, ...]:
    if asset_type == "non_equity_etf":
        return ()
    eligible = tuple(
        item
        for item in classifications
        if item.instrument_id == instrument_id
        and item.effective_session <= session
        and item.published_at <= cutoff
        and (item.reviewed or (sector_source == "fixture" and item.synthetic))
    )
    if not eligible:
        return ()
    latest_effective = max(item.effective_session for item in eligible)
    effective = tuple(item for item in eligible if item.effective_session == latest_effective)
    latest_published = max(item.published_at for item in effective)
    published = tuple(item for item in effective if item.published_at == latest_published)
    snapshot_identities = {
        (
            item.source_uri,
            item.content_hash,
            item.synthetic,
            item.reviewed,
        )
        for item in published
    }
    if len(snapshot_identities) != 1:
        return ()
    sectors = [item.sector for item in published]
    if len(sectors) != len(set(sectors)):
        return ()
    return tuple(sorted(published, key=lambda item: (item.sector, item.authority_id.int)))


def sector_exposures_complete(
    asset_type: Literal["stock", "equity_etf", "non_equity_etf"],
    exposures: tuple[SectorClassification, ...],
) -> bool:
    total = sum((item.weight for item in exposures), start=Decimal(0))
    if asset_type == "stock":
        return len(exposures) == 1 and total == Decimal(1)
    if asset_type == "non_equity_etf":
        return not exposures
    return bool(exposures) and Decimal("0.995") <= total <= Decimal("1.005")


def normalized_sector_weights(
    exposures: tuple[SectorClassification, ...],
) -> tuple[tuple[str, Decimal], ...]:
    total = sum((item.weight for item in exposures), start=Decimal(0))
    if total <= 0:
        return ()
    return tuple((item.sector, item.weight / total) for item in exposures)


def align_feature_window(
    sessions: tuple[date, ...],
    decision_session: date,
    raw_state: Mapping[date, BacktestBar],
    adjusted_state: Mapping[date, BacktestBar],
) -> tuple[tuple[BacktestBar, ...], tuple[BacktestBar, ...]]:
    aligned: list[date] = []
    for session in reversed(sessions):
        if session > decision_session:
            continue
        if session in raw_state and session in adjusted_state:
            aligned.append(session)
            if len(aligned) == 200:
                break
    aligned.reverse()
    return (tuple(raw_state[day] for day in aligned), tuple(adjusted_state[day] for day in aligned))


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))

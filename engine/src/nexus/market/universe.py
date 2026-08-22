"""Deterministic instrument identifiers and ETF exposure admission."""

from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, field_validator

from nexus.providers.base import InstrumentId

INSTRUMENT_NAMESPACE = UUID("7c775295-cd38-5baf-a6fb-816717215638")
EQUITY_SECTOR_CODES = (
    "communication_services",
    "consumer_discretionary",
    "consumer_staples",
    "energy",
    "financials",
    "health_care",
    "industrials",
    "information_technology",
    "materials",
    "real_estate",
    "utilities",
)


def stable_instrument_id(symbol: str) -> InstrumentId:
    """Derive a stable internal ID from the versioned U.S./XNYS namespace."""
    normalized = symbol.strip().upper()
    if not normalized or normalized != symbol:
        raise ValueError("symbol must already be uppercase and trimmed")
    return InstrumentId(uuid5(INSTRUMENT_NAMESPACE, f"US:XNYS:{normalized}"))


class SectorExposureRecord(BaseModel):
    """Controlled issuer or explicitly synthetic ETF exposure input."""

    model_config = ConfigDict(frozen=True)

    instrument_id: InstrumentId
    sector_code: str
    weight: Decimal
    as_of: date
    source_url: str
    content_hash: str
    synthetic: bool
    reviewed: bool

    @field_validator("weight", mode="before")
    @classmethod
    def _reject_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("exposure weight must not be a binary float")
        return value


class PositionEligibility(BaseModel):
    """Whether the instrument may become a new position."""

    model_config = ConfigDict(frozen=True)

    eligible: bool
    quality: Literal["PASS", "PARTIAL"]
    reasons: tuple[str, ...]


def etf_new_position_eligibility(
    *,
    asset_type: Literal["equity_etf", "non_equity_etf"],
    mode: Literal["fixture", "production"],
    exposures: tuple[SectorExposureRecord, ...],
    decision_date: date,
) -> PositionEligibility:
    """Fail closed when equity ETF look-through evidence is absent or stale."""
    if asset_type == "non_equity_etf":
        return PositionEligibility(eligible=True, quality="PASS", reasons=())
    if not exposures:
        return _ineligible()
    weight = sum((record.weight for record in exposures), start=Decimal(0))
    if not Decimal("0.995") <= weight <= Decimal("1.005"):
        return _ineligible("ETF_EXPOSURE_WEIGHT_INVALID")
    if any(record.as_of > decision_date for record in exposures):
        return _ineligible("ETF_EXPOSURE_FROM_FUTURE")
    if mode == "fixture":
        if not all(record.synthetic for record in exposures):
            return _ineligible("ETF_FIXTURE_EXPOSURE_NOT_SYNTHETIC")
        return PositionEligibility(eligible=True, quality="PASS", reasons=())
    if any(record.synthetic or not record.reviewed for record in exposures):
        return _ineligible()
    if any((decision_date - record.as_of).days > 30 for record in exposures):
        return _ineligible("ETF_EXPOSURE_STALE")
    return PositionEligibility(eligible=True, quality="PASS", reasons=())


def _ineligible(reason: str = "ETF_EXPOSURE_REVIEW_REQUIRED") -> PositionEligibility:
    return PositionEligibility(eligible=False, quality="PARTIAL", reasons=(reason,))

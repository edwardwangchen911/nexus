"""Deterministic instrument identifiers and reviewed sector-exposure admission."""

import csv
import io
import re
from datetime import date
from decimal import Decimal
from typing import Literal
from urllib.parse import urlsplit
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
_REVIEWED_SECTOR_FIELDS = (
    "instrument_id",
    "sector_code",
    "weight",
    "as_of",
    "source_url",
    "content_hash",
)
_CONTENT_HASH = re.compile(r"^[0-9a-f]{64}$")
_MAX_REVIEWED_SECTOR_BYTES = 1024 * 1024
_MAX_REVIEWED_SECTOR_ROWS = 512


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


def parse_reviewed_sector_csv(raw: bytes) -> tuple[SectorExposureRecord, ...]:
    """Parse one bounded, operator-reviewed sector snapshot without inferring authority."""
    try:
        if not 0 < len(raw) <= _MAX_REVIEWED_SECTOR_BYTES:
            raise ValueError
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""))
        if tuple(reader.fieldnames or ()) != _REVIEWED_SECTOR_FIELDS:
            raise ValueError
        rows = list(reader)
        if not 0 < len(rows) <= _MAX_REVIEWED_SECTOR_ROWS:
            raise ValueError
        records: list[SectorExposureRecord] = []
        identities: set[tuple[UUID, str]] = set()
        snapshots: dict[UUID, tuple[date, str, str]] = {}
        for row in rows:
            if set(row) != set(_REVIEWED_SECTOR_FIELDS) or any(
                value is None for value in row.values()
            ):
                raise ValueError
            instrument_id = UUID(row["instrument_id"])
            sector_code = row["sector_code"]
            weight = Decimal(row["weight"])
            as_of = date.fromisoformat(row["as_of"])
            source_url = row["source_url"]
            content_hash = row["content_hash"]
            source = urlsplit(source_url)
            identity = (instrument_id, sector_code)
            snapshot = (as_of, source_url, content_hash)
            if (
                str(instrument_id) != row["instrument_id"]
                or sector_code not in EQUITY_SECTOR_CODES
                or not Decimal(0) <= weight <= Decimal(1)
                or source.scheme != "https"
                or not source.hostname
                or source.username is not None
                or source.password is not None
                or source.fragment
                or _CONTENT_HASH.fullmatch(content_hash) is None
                or identity in identities
                or (instrument_id in snapshots and snapshots[instrument_id] != snapshot)
            ):
                raise ValueError
            identities.add(identity)
            snapshots[instrument_id] = snapshot
            records.append(
                SectorExposureRecord(
                    instrument_id=InstrumentId(instrument_id),
                    sector_code=sector_code,
                    weight=weight,
                    as_of=as_of,
                    source_url=source_url,
                    content_hash=content_hash,
                    synthetic=False,
                    reviewed=True,
                )
            )
        return tuple(sorted(records, key=lambda item: (item.instrument_id.int, item.sector_code)))
    except (
        UnicodeError,
        csv.Error,
        KeyError,
        TypeError,
        ValueError,
        ArithmeticError,
    ):
        raise ValueError("SECTOR_EXPOSURE_CSV_INVALID") from None


class PositionEligibility(BaseModel):
    """Whether the instrument may become a new position."""

    model_config = ConfigDict(frozen=True)

    eligible: bool
    quality: Literal["PASS", "PARTIAL"]
    reasons: tuple[str, ...]


def etf_new_position_eligibility(
    *,
    instrument_id: InstrumentId,
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
    if any(record.instrument_id != instrument_id for record in exposures):
        return _ineligible("ETF_EXPOSURE_INSTRUMENT_MISMATCH")
    if any(record.sector_code not in EQUITY_SECTOR_CODES for record in exposures):
        return _ineligible("ETF_EXPOSURE_SECTOR_INVALID")
    sector_codes = [record.sector_code for record in exposures]
    if len(sector_codes) != len(set(sector_codes)):
        return _ineligible("ETF_EXPOSURE_SECTOR_DUPLICATE")
    if any(not Decimal(0) <= record.weight <= Decimal(1) for record in exposures):
        return _ineligible("ETF_EXPOSURE_WEIGHT_OUT_OF_RANGE")
    snapshots = {
        (
            record.as_of,
            record.source_url,
            record.content_hash,
            record.synthetic,
            record.reviewed,
        )
        for record in exposures
    }
    if len(snapshots) != 1:
        return _ineligible("ETF_EXPOSURE_SNAPSHOT_MISMATCH")
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

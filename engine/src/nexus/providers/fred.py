"""Offline-testable FRED/ALFRED macro evidence normalization."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc

FRED_SERIES = frozenset(
    {"DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "INDPRO", "VIXCLS", "DEXHKUS"}
)


class FredTransport(Protocol):
    """Credential-free raw-byte transport supplied by composition or fixtures."""

    async def get(self, path: str) -> bytes: ...


class FredValidationError(ValueError):
    """A macro value is outside the small, point-in-time authority."""


@dataclass(frozen=True)
class FredRawDocument:
    raw_payload: bytes
    received_at: datetime


@dataclass(frozen=True)
class AlfredObservation:
    series: str
    observed_on: date
    real_time_start: date
    real_time_end: date
    revision: str
    value: Decimal
    unit: str
    available_at: datetime
    evidence_ref: EvidenceRef
    calendar_evidence_ref: EvidenceRef
    display_sanity_only: bool


class FredSeriesClient:
    def __init__(self, transport: FredTransport, *, now: Callable[[], datetime]) -> None:
        self._transport = transport
        self._now = now

    async def fetch_series(self, series: str) -> FredRawDocument:
        if series not in FRED_SERIES:
            raise FredValidationError("UNAPPROVED_SERIES")
        payload = await self._transport.get(f"/fred/series/observations?series_id={series}")
        if not payload:
            raise FredValidationError("EMPTY_PAYLOAD")
        return FredRawDocument(raw_payload=payload, received_at=require_utc(self._now()))

    async def fetch_alfred_series(
        self, series: str, real_time_start: str, real_time_end: str
    ) -> FredRawDocument:
        """Retrieve one explicit ALFRED vintage interval through the same injected seam."""
        if series not in FRED_SERIES:
            raise FredValidationError("UNAPPROVED_SERIES")
        try:
            start = date.fromisoformat(real_time_start)
            end = date.fromisoformat(real_time_end)
        except ValueError as error:
            raise FredValidationError("INVALID_REAL_TIME_PERIOD") from error
        if end < start:
            raise FredValidationError("INVALID_REAL_TIME_PERIOD")
        payload = await self._transport.get(
            "/alfred/series/observations?"
            f"series_id={series}&realtime_start={start.isoformat()}&realtime_end={end.isoformat()}"
        )
        if not payload:
            raise FredValidationError("EMPTY_PAYLOAD")
        return FredRawDocument(raw_payload=payload, received_at=require_utc(self._now()))


def conservative_next_xnys_availability(
    observed_on: date,
    *,
    release_time: datetime | None,
    next_session_close: Callable[[date], datetime],
) -> datetime:
    """Use reliable release time or wait through the next XNYS session close."""
    if release_time is not None:
        return require_utc(release_time)
    return require_utc(next_session_close(observed_on))


def normalize_alfred_observation(
    raw: dict[str, object],
    *,
    series: str,
    unit: str,
    available_at: datetime,
    evidence_ref: EvidenceRef,
    calendar_evidence_ref: EvidenceRef,
) -> AlfredObservation:
    """Preserve ALFRED real-time vintage boundaries without parsing binary floats."""
    if series not in FRED_SERIES:
        raise FredValidationError("UNAPPROVED_SERIES")
    value = raw.get("value")
    if not isinstance(value, str):
        raise FredValidationError("VALUE_MUST_BE_DECIMAL_STRING")
    try:
        decimal_value = Decimal(value)
    except Exception as error:
        raise FredValidationError("VALUE_MUST_BE_DECIMAL_STRING") from error
    if not decimal_value.is_finite():
        raise FredValidationError("VALUE_MUST_BE_FINITE")
    try:
        observed_on = date.fromisoformat(_required_string(raw, "date"))
        real_time_start = date.fromisoformat(_required_string(raw, "realtime_start"))
        real_time_end = date.fromisoformat(_required_string(raw, "realtime_end"))
    except ValueError as error:
        raise FredValidationError("INVALID_ALFRED_DATE") from error
    if real_time_end < real_time_start:
        raise FredValidationError("INVALID_REAL_TIME_PERIOD")
    revision = raw.get("revision", real_time_start.isoformat())
    if not isinstance(revision, str) or not revision:
        raise FredValidationError("INVALID_REVISION")
    return AlfredObservation(
        series=series,
        observed_on=observed_on,
        real_time_start=real_time_start,
        real_time_end=real_time_end,
        revision=revision,
        value=decimal_value,
        unit=unit,
        available_at=require_utc(available_at),
        evidence_ref=evidence_ref,
        calendar_evidence_ref=calendar_evidence_ref,
        display_sanity_only=series == "DEXHKUS",
    )


def _required_string(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str):
        raise FredValidationError(f"MISSING_{key.upper()}")
    return value

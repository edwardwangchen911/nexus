"""Offline-testable FRED/ALFRED evidence normalization with conservative vintages."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Protocol

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

    def __post_init__(self) -> None:
        object.__setattr__(self, "received_at", require_utc(self.received_at))


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
    authority_version: Literal["research_macro_v1", "legacy_task3"] = "research_macro_v1"


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
        """Retrieve one explicit ALFRED vintage interval through the injected seam."""
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
    *,
    release_date: date | None,
    real_time_start: date,
    next_session_open: Callable[[date], datetime],
) -> datetime:
    """Move an unknown-time release past its release/vintage day to the next open.

    The observation period is never an availability proxy: for an unknown release
    time, a documented release date wins, otherwise the ALFRED vintage start date is
    the only defensible bound.  ``next_session_open`` must return an actual XNYS
    session strictly after the supplied base date.
    """
    base_date = release_date or real_time_start
    available_at = require_utc(next_session_open(base_date))
    if available_at.date() <= base_date:
        raise FredValidationError("XNYS_OPEN_MUST_FOLLOW_RELEASE_DATE")
    return available_at


def normalize_alfred_observation(
    raw: Mapping[str, object],
    *,
    series: str,
    unit: str,
    evidence_ref: EvidenceRef,
    calendar_evidence_ref: EvidenceRef,
    next_session_open: Callable[[date], datetime],
) -> AlfredObservation:
    """Preserve ALFRED vintages and derive their conservative decision availability."""
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
    release_date = _optional_date(raw, "release_date")
    _validate_evidence_owners(
        series=series,
        revision=revision,
        evidence_ref=evidence_ref,
        calendar_evidence_ref=calendar_evidence_ref,
    )
    return AlfredObservation(
        series=series,
        observed_on=observed_on,
        real_time_start=real_time_start,
        real_time_end=real_time_end,
        revision=revision,
        value=decimal_value,
        unit=unit,
        available_at=conservative_next_xnys_availability(
            release_date=release_date,
            real_time_start=real_time_start,
            next_session_open=next_session_open,
        ),
        evidence_ref=evidence_ref,
        calendar_evidence_ref=calendar_evidence_ref,
        display_sanity_only=series == "DEXHKUS",
    )


def select_alfred_observation(
    observations: Sequence[AlfredObservation],
    series: str,
    observed_on: date,
    decision_time: datetime,
) -> AlfredObservation | None:
    """Select the latest v1 vintage within one series and economic observation period."""
    decision_at = require_utc(decision_time)
    visible = [
        observation
        for observation in observations
        if (
            observation.series == series
            and observation.observed_on == observed_on
            and observation.authority_version == "research_macro_v1"
            and observation.available_at <= decision_at
        )
    ]
    if not visible:
        return None
    latest_start = max(observation.real_time_start for observation in visible)
    latest = [observation for observation in visible if observation.real_time_start == latest_start]
    if len(latest) != 1:
        raise FredValidationError("AMBIGUOUS_VINTAGE")
    return latest[0]


def _validate_evidence_owners(
    *,
    series: str,
    revision: str,
    evidence_ref: EvidenceRef,
    calendar_evidence_ref: EvidenceRef,
) -> None:
    if evidence_ref.source != "fred" or evidence_ref.object_id != f"{series}:{revision}":
        raise FredValidationError("EVIDENCE_OWNER_MISMATCH")
    if (
        calendar_evidence_ref.source != "exchange_calendars"
        or not calendar_evidence_ref.object_id.startswith("calendar:XNYS:")
    ):
        raise FredValidationError("CALENDAR_EVIDENCE_OWNER_MISMATCH")


def _required_string(raw: Mapping[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str):
        raise FredValidationError(f"MISSING_{key.upper()}")
    return value


def _optional_date(raw: Mapping[str, object], key: str) -> date | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise FredValidationError(f"INVALID_{key.upper()}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise FredValidationError(f"INVALID_{key.upper()}") from error

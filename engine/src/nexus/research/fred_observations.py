"""Bounded original FRED vintage observations, with explicit source units."""

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

from nexus.domain.time import require_utc
from nexus.providers.fred import FRED_SERIES

SCHEMA = "fred_vintage_json_v1"
PAGE_SIZE = 16
MAX_PAGES = 4


class FredVintageRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    series: str
    observation_start: date
    observation_end: date
    vintage: date

    @model_validator(mode="after")
    def _bounded(self) -> "FredVintageRequest":
        if self.series not in FRED_SERIES or not (
            self.observation_start <= self.observation_end <= self.vintage
            and (self.observation_end - self.observation_start).days <= 31
        ):
            raise ValueError("FRED_REQUEST_INVALID")
        return self


@dataclass(frozen=True)
class FredCapture:
    kind: Literal["series", "observations"]
    offset: int | None
    raw_payload: bytes
    received_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.received_at)
        if not 0 < len(self.raw_payload) <= 1024 * 1024:
            raise ValueError("FRED_ORIGINAL_LIMIT")


@dataclass(frozen=True)
class FredVintageCollection:
    request: FredVintageRequest
    metadata: FredCapture
    pages: tuple[FredCapture, ...]
    unit: str
    observations: tuple[tuple[date, Decimal], ...]
    missing_dates: tuple[date, ...]
    received_at: datetime


def public_query(
    request: FredVintageRequest, capture_kind: str, offset: int | None
) -> dict[str, str]:
    query = {
        "series_id": request.series,
        "file_type": "json",
        "realtime_start": request.vintage.isoformat(),
        "realtime_end": request.vintage.isoformat(),
    }
    if capture_kind == "observations":
        if type(offset) is not int or not 0 <= offset <= 32:
            raise ValueError("FRED_OFFSET_INVALID")
        query.update(
            observation_start=request.observation_start.isoformat(),
            observation_end=request.observation_end.isoformat(),
            units="lin",
            output_type="1",
            sort_order="asc",
            limit=str(PAGE_SIZE),
            offset=str(offset),
        )
    elif capture_kind != "series" or offset is not None:
        raise ValueError("FRED_REQUEST_INVALID")
    return query


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("FRED_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def decode(raw: bytes) -> dict[str, object]:
    def reject(value: str) -> object:
        raise ValueError("FRED_JSON_CONSTANT_INVALID")

    try:
        value = json.loads(raw, object_pairs_hook=_unique, parse_constant=reject)
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ValueError("FRED_VINTAGE_INVALID") from error


def page_count(raw: bytes, *, expected_offset: int) -> tuple[int, int]:
    value = decode(raw)
    count, offset, limit, observations = (
        value.get(name) for name in ("count", "offset", "limit", "observations")
    )
    if (
        type(count) is not int
        or not 0 <= count <= 32
        or type(offset) is not int
        or offset != expected_offset
        or type(limit) is not int
        or limit != PAGE_SIZE
        or not isinstance(observations, list)
        or len(observations) > PAGE_SIZE
        or offset + len(observations) > count
        or (offset < count and not observations)
    ):
        raise ValueError("FRED_VINTAGE_INVALID")
    return count, len(observations)


def metadata_unit(request: FredVintageRequest, metadata: FredCapture) -> str:
    """Validate the first response before spending another HTTP request."""
    try:
        meta = decode(metadata.raw_payload)
        series = meta.get("seriess")
        if (
            metadata.kind != "series"
            or metadata.offset is not None
            or not isinstance(series, list)
            or len(series) != 1
            or not isinstance(series[0], dict)
            or series[0].get("id") != request.series
            or meta.get("realtime_start") != request.vintage.isoformat()
            or meta.get("realtime_end") != request.vintage.isoformat()
            or series[0].get("realtime_start") != request.vintage.isoformat()
            or series[0].get("realtime_end") != request.vintage.isoformat()
            or request.vintage > metadata.received_at.date()
        ):
            raise ValueError
        unit = series[0].get("units")
        if not isinstance(unit, str) or not unit.strip() or len(unit) > 100:
            raise ValueError
        return unit
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("FRED_VINTAGE_INVALID") from error


def parse_collected_vintage(
    request: FredVintageRequest,
    metadata: FredCapture,
    pages: tuple[FredCapture, ...],
) -> FredVintageCollection:
    return _parse(request, metadata, pages, require_complete=True)


def validate_captured_pages(
    request: FredVintageRequest, metadata: FredCapture, pages: tuple[FredCapture, ...]
) -> None:
    """A partial prefix may be valid but is never returned as a complete collection."""
    _parse(request, metadata, pages, require_complete=False)


def _parse(
    request: FredVintageRequest,
    metadata: FredCapture,
    pages: tuple[FredCapture, ...],
    *,
    require_complete: bool,
) -> FredVintageCollection:
    try:
        unit = metadata_unit(request, metadata)
        if not 1 <= len(pages) <= MAX_PAGES:
            raise ValueError
        offset, total = 0, None
        previous_time = metadata.received_at
        values: list[tuple[date, Decimal]] = []
        missing: list[date] = []
        previous_date: date | None = None
        for page in pages:
            if (
                page.kind != "observations"
                or page.offset != offset
                or page.received_at < previous_time
                or request.vintage > page.received_at.date()
            ):
                raise ValueError
            previous_time = page.received_at
            count, length = page_count(page.raw_payload, expected_offset=offset)
            if total is not None and (count != total or offset == total):
                raise ValueError
            total = count
            raw = decode(page.raw_payload)
            expected = {
                "realtime_start": request.vintage.isoformat(),
                "realtime_end": request.vintage.isoformat(),
                "observation_start": request.observation_start.isoformat(),
                "observation_end": request.observation_end.isoformat(),
                "units": "lin",
                "output_type": 1,
                "order_by": "observation_date",
                "sort_order": "asc",
            }
            if type(raw.get("output_type")) is not int or any(
                raw.get(key) != value for key, value in expected.items()
            ):
                raise ValueError
            observations = raw["observations"]
            assert isinstance(observations, list)
            for item in observations:
                if not isinstance(item, dict) or any(
                    item.get(name) != request.vintage.isoformat()
                    for name in ("realtime_start", "realtime_end")
                ):
                    raise ValueError
                observed = date.fromisoformat(item["date"])
                if not request.observation_start <= observed <= request.observation_end or (
                    previous_date is not None and observed <= previous_date
                ):
                    raise ValueError
                previous_date = observed
                text = item["value"]
                if text == ".":
                    missing.append(observed)
                    continue
                if not isinstance(text, str) or len(text) > 80:
                    raise ValueError
                number = Decimal(text)
                if not number.is_finite() or number.adjusted() >= 26:
                    raise ValueError
                digits, exponent = list(number.as_tuple().digits), number.as_tuple().exponent
                assert isinstance(exponent, int)
                while digits and digits[-1] == 0:
                    digits.pop()
                    exponent += 1
                if digits and exponent < -12:
                    raise ValueError
                values.append((observed, number))
            offset += length
        if require_complete and offset != total:
            raise ValueError
        return FredVintageCollection(
            request, metadata, pages, unit, tuple(values), tuple(missing), previous_time
        )
    except (ValueError, TypeError, KeyError, InvalidOperation) as error:
        raise ValueError("FRED_VINTAGE_INVALID") from error

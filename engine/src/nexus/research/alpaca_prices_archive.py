"""Exact private price originals, not a price authority or total-return series."""

import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars  # type: ignore[import-untyped]

from nexus.domain.time import require_utc
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_http import _bars_query, _validated_us_symbol, load_exact_json

MAX_PAGES = 10
MAX_PAGE_BYTES = 8 * 1024 * 1024
ORIGIN = "https://data.alpaca.markets"
_COMMON_KEYS = {
    "source",
    "origin",
    "path",
    "instrument_id",
    "resolved_symbol",
    "identity_asof",
    "identity_resolved_at",
    "calendar",
}
UNKNOWN_QUALIFICATION = {
    "source_completeness": "unknown",
    "trading_session_completeness": "unknown",
    "research_eligible": False,
    "historical_backtest_eligible": False,
    "execution_locked": True,
}


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def collection_window(
    instrument_id: UUID, start: date, end: date, adjustment: str, *, now: datetime
) -> tuple[BarsRequest, dict[str, str]]:
    """Only completed XNYS sessions; the buffer does not prove vendor finality."""
    clock = require_utc(now)
    calendar = exchange_calendars.get_calendar("XNYS")
    if (
        adjustment not in {"raw", "split"}
        or not 0 <= (end - start).days <= 366
        or start < calendar.first_session.date()
        or end > calendar.last_session.date()
        or not calendar.is_session(start)
        or not calendar.is_session(end)
    ):
        raise ValueError("ALPACA_PRICES_REQUEST_INVALID")
    close = calendar.session_close(end).to_pydatetime()
    end_at = close + timedelta(minutes=30)
    if end_at > clock:
        raise ValueError("ALPACA_PRICES_SESSION_NOT_COMPLETED")
    request = BarsRequest(
        instrument_id=InstrumentId(instrument_id),
        start=datetime.combine(start, time(), ZoneInfo("America/New_York")).astimezone(UTC),
        end=end_at,
        interval="1Day",
        adjustment=cast(Literal["raw", "split"], adjustment),
    )
    return request, {
        "source": "exchange_calendars",
        "version": version("exchange-calendars"),
        "exchange": "XNYS",
        "start_session": start.isoformat(),
        "end_session": end.isoformat(),
        "end_close_at": close.isoformat(),
    }


def price_page_cursor(payload: Mapping[str, object]) -> str | None:
    """Validate archive structure only, not OHLC economics or session completeness."""
    bars = payload.get("bars")
    if (
        not isinstance(bars, list)
        or len(bars) > 10000
        or any(not isinstance(bar, dict) for bar in bars)
    ):
        raise ValueError("ALPACA_PRICES_PAGE_INVALID")
    if "next_page_token" not in payload:
        raise ValueError("ALPACA_PRICES_PAGE_INVALID")
    cursor = payload["next_page_token"]
    if cursor is not None and (
        not isinstance(cursor, str) or re.fullmatch(r"[A-Za-z0-9._~=-]{1,512}", cursor) is None
    ):
        raise ValueError("ALPACA_PRICES_PAGE_INVALID")
    return cursor


@dataclass(frozen=True)
class PriceOriginal:
    name: str
    content: bytes
    received_at: datetime


@dataclass(frozen=True)
class ValidatedPriceArchive:
    request: BarsRequest
    symbol: str
    identity_asof: date
    received_at: datetime
    manifest_content_hash: str
    originals: tuple[PriceOriginal, ...]


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise ValueError


def _metadata(raw: bytes) -> dict[str, Any]:
    value = json.loads(
        raw, object_pairs_hook=_unique, parse_float=_invalid_number, parse_constant=_invalid_number
    )
    if not isinstance(value, dict):
        raise ValueError
    return value


def _read(directory: int, name: str, limit: int) -> bytes:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 0 < info.st_size <= limit:
            raise ValueError
        content = source.read(limit + 1)
        if len(content) != info.st_size:
            raise ValueError
        return content


def load_price_collection(
    directory: Path, *, instrument_id: UUID, now: datetime | None = None
) -> ValidatedPriceArchive:
    """Recheck every byte, clock and page binding, without choosing latest data."""
    return _read_collection(directory, instrument_id, now, "manifest.json")


def _verify_pending_collection(directory: Path, *, instrument_id: UUID) -> ValidatedPriceArchive:
    """Collector-only validation before publication; import never accepts a pending manifest."""
    return _read_collection(directory, instrument_id, None, "manifest.pending.json")


def _read_collection(
    directory: Path, instrument_id: UUID, now: datetime | None, manifest_name: str
) -> ValidatedPriceArchive:
    try:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if os.fstat(descriptor).st_mode & 0o077:
                raise ValueError
            return _load(
                lambda name, limit: _read(descriptor, name, limit),
                set(os.listdir(descriptor)),
                instrument_id,
                require_utc(now or datetime.now(UTC)),
                manifest_name,
            )
        finally:
            os.close(descriptor)
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        RecursionError,
        MarketProviderPayloadError,
    ):
        raise ValueError("ALPACA_PRICES_ARCHIVE_INVALID") from None


def validate_price_originals(
    originals: Mapping[str, bytes], *, instrument_id: UUID, now: datetime
) -> ValidatedPriceArchive:
    """Revalidate persisted original bytes using the same archive contract, without disk IO."""

    def read(name: str, limit: int) -> bytes:
        content = originals[name]
        if not isinstance(content, bytes) or not 0 < len(content) <= limit:
            raise ValueError
        return content

    try:
        return _load(read, set(originals), instrument_id, require_utc(now), "manifest.json")
    except (
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        RecursionError,
        MarketProviderPayloadError,
    ):
        raise ValueError("ALPACA_PRICES_ARCHIVE_INVALID") from None


def _load(
    read: Callable[[str, int], bytes],
    names: set[str],
    instrument_id: UUID,
    now: datetime,
    manifest_name: str,
) -> ValidatedPriceArchive:
    manifest_raw = read(manifest_name, 128 * 1024)
    manifest = _metadata(manifest_raw)
    if (
        set(manifest)
        != _COMMON_KEYS
        | set(UNKNOWN_QUALIFICATION)
        | {
            "schema",
            "status",
            "pagination_exhausted",
            "error_code",
            "started_at",
            "completed_at",
            "query",
            "pages",
        }
        or manifest.get("schema") != "nexus_alpaca_prices_collection_v1"
        or manifest.get("status") != "collected"
        or manifest.get("pagination_exhausted") is not True
        or "error_code" not in manifest
        or manifest["error_code"] is not None
        or any(
            type(manifest.get(k)) is not type(v) or manifest[k] != v
            for k, v in UNKNOWN_QUALIFICATION.items()
        )
        or manifest["instrument_id"] != str(instrument_id)
    ):
        raise ValueError
    started = require_utc(datetime.fromisoformat(manifest["started_at"]))
    completed = require_utc(datetime.fromisoformat(manifest["completed_at"]))
    resolved = require_utc(datetime.fromisoformat(manifest["identity_resolved_at"]))
    if not resolved <= started <= completed <= now:
        raise ValueError
    calendar = manifest["calendar"]
    query = manifest["query"]
    request, expected_calendar = collection_window(
        instrument_id,
        date.fromisoformat(calendar["start_session"]),
        date.fromisoformat(calendar["end_session"]),
        dict(query)["adjustment"],
        now=started,
    )
    symbol = _validated_us_symbol(manifest["resolved_symbol"])
    identity_date = date.fromisoformat(manifest["identity_asof"])
    if identity_date > started.date():
        raise ValueError
    expected_query = [list(pair) for pair in _bars_query(request, None, "sip", identity_date)]
    if (
        calendar != expected_calendar
        or query != expected_query
        or manifest["source"] != "alpaca"
        or manifest["origin"] != ORIGIN
        or manifest["path"] != f"/v2/stocks/{symbol}/bars"
    ):
        raise ValueError
    entries = manifest["pages"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_PAGES:
        raise ValueError
    originals: list[PriceOriginal] = []
    expected_files = {manifest_name}
    cursor: str | None = None
    seen: set[str] = set()
    previous_received = started
    for number, entry in enumerate(entries, 1):
        raw_name = f"page-{number:03}.json"
        receipt_name = f"page-{number:03}.receipt.json"
        if (
            set(entry) != {"raw_file", "receipt_file", "raw_sha256", "receipt_sha256"}
            or entry["raw_file"] != raw_name
            or entry["receipt_file"] != receipt_name
        ):
            raise ValueError
        expected_files.update((raw_name, receipt_name))
        raw = read(raw_name, MAX_PAGE_BYTES)
        receipt_raw = read(receipt_name, 64 * 1024)
        receipt = _metadata(receipt_raw)
        if (
            set(receipt)
            != _COMMON_KEYS
            | {"schema", "raw_file", "sha256", "byte_length", "received_at", "query"}
            or receipt.get("schema") != "nexus_alpaca_prices_receipt_v1"
            or receipt["raw_file"] != raw_name
            or receipt["sha256"] != sha256(raw).hexdigest()
            or type(receipt["byte_length"]) is not int
            or receipt["byte_length"] != len(raw)
            or entry["raw_sha256"] != sha256(raw).hexdigest()
            or entry["receipt_sha256"] != sha256(receipt_raw).hexdigest()
            or any(receipt[k] != manifest[k] for k in _COMMON_KEYS)
            or receipt["query"]
            != [list(pair) for pair in _bars_query(request, cursor, "sip", identity_date)]
        ):
            raise ValueError
        received = require_utc(datetime.fromisoformat(receipt["received_at"]))
        if not previous_received <= received <= completed:
            raise ValueError
        previous_received = received
        payload = load_exact_json(raw)
        if not isinstance(payload, dict) or payload.get("symbol") != symbol:
            raise ValueError
        cursor = price_page_cursor(payload)
        if (number == len(entries)) != (cursor is None) or cursor in seen:
            raise ValueError
        if cursor is not None:
            seen.add(cursor)
        originals.extend(
            (
                PriceOriginal(raw_name, raw, received),
                PriceOriginal(receipt_name, receipt_raw, received),
            )
        )
    if names != expected_files:
        raise ValueError
    originals.append(PriceOriginal(manifest_name, manifest_raw, completed))
    return ValidatedPriceArchive(
        request,
        symbol,
        identity_date,
        previous_received,
        sha256(manifest_raw).hexdigest(),
        tuple(originals),
    )

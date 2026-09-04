"""Validate a completed private collector archive without external I/O."""

import json
import os
import stat
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

from nexus.domain.time import require_utc
from nexus.providers.corporate_actions.alpaca_http import (
    CorporateActionsRequest,
    parse_corporate_actions_page,
)
from nexus.research.alpaca_actions import (
    MAX_EVENTS,
    MAX_RAW_BYTES,
    ActionEventObservation,
    ActionPageReference,
    AlpacaActionObservationDocument,
)


@dataclass(frozen=True)
class ActionOriginal:
    name: str
    content: bytes
    schema_version: str
    received_at: datetime


@dataclass(frozen=True)
class ValidatedActionArchive:
    document: AlpacaActionObservationDocument
    originals: tuple[ActionOriginal, ...]


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


def _read(directory_fd: int, name: str, limit: int) -> bytes:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 0 < info.st_size <= limit:
            raise ValueError
        raw = source.read(limit + 1)
        if len(raw) != info.st_size:
            raise ValueError
        return raw


def load_action_collection(
    directory: Path, *, instrument_id: UUID, symbol: str, now: datetime | None = None
) -> ValidatedActionArchive:
    """Only verified explicit page exhaustion can enter current read-only research."""
    try:
        cutoff = require_utc(now or datetime.now(UTC))
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if os.fstat(descriptor).st_mode & 0o077:
                raise ValueError
            return _load(descriptor, instrument_id, symbol, cutoff)
        finally:
            os.close(descriptor)
    except (OSError, ValueError, TypeError, KeyError, IndexError, RecursionError) as error:
        raise ValueError("ALPACA_ACTIONS_ARCHIVE_INVALID") from error


def _load(
    directory_fd: int, instrument_id: UUID, symbol: str, now: datetime
) -> ValidatedActionArchive:
    manifest_raw = _read(directory_fd, "manifest.json", 128 * 1024)
    manifest = _metadata(manifest_raw)
    if (
        manifest.get("schema") != "nexus_alpaca_actions_collection_v1"
        or manifest.get("status") != "collected"
        or manifest.get("pagination_exhausted") is not True
        or manifest.get("execution_locked") is not True
        or manifest.get("historical_backtest_eligible") is not False
        or manifest.get("source_completeness") != "unknown"
        or "error_code" not in manifest
        or manifest["error_code"] is not None
    ):
        raise ValueError
    query = manifest["query"]
    if not isinstance(query, list) or len(query) != 7:
        raise ValueError
    request = CorporateActionsRequest(
        symbol, date.fromisoformat(query[1][1]), date.fromisoformat(query[2][1])
    )
    if query != [list(pair) for pair in request.query()]:
        raise ValueError
    listed = manifest["pages"]
    if not isinstance(listed, list) or not 1 <= len(listed) <= 100:
        raise ValueError
    expected_files = {"manifest.json"}
    for number, entry in enumerate(listed, 1):
        expected = {
            "raw_file": f"page-{number:03}.json",
            "receipt_file": f"page-{number:03}.receipt.json",
        }
        if entry != expected:
            raise ValueError
        expected_files.update(expected.values())
    if set(os.listdir(directory_fd)) != expected_files:
        raise ValueError
    originals: list[ActionOriginal] = []
    pages: list[ActionPageReference] = []
    events: list[ActionEventObservation] = []
    total = 0
    raw_bytes = 0
    cursor: str | None = None
    seen: set[str] = set()
    for number, entry in enumerate(listed, 1):
        raw = _read(
            directory_fd, entry["raw_file"], min(8 * 1024 * 1024, MAX_RAW_BYTES - raw_bytes)
        )
        raw_bytes += len(raw)
        receipt_raw = _read(directory_fd, entry["receipt_file"], 64 * 1024)
        receipt = _metadata(receipt_raw)
        for metadata in (manifest, receipt):
            if (metadata.get("source"), metadata.get("origin"), metadata.get("path")) != (
                "alpaca",
                "https://data.alpaca.markets",
                "/v1/corporate-actions",
            ):
                raise ValueError
        if (
            receipt.get("schema") != "nexus_alpaca_actions_receipt_v1"
            or receipt.get("raw_file") != entry["raw_file"]
            or receipt.get("sha256") != sha256(raw).hexdigest()
            or type(receipt.get("byte_length")) is not int
            or receipt["byte_length"] != len(raw)
            or receipt.get("query") != [list(pair) for pair in request.query(cursor)]
        ):
            raise ValueError
        received = require_utc(datetime.fromisoformat(receipt["received_at"]))
        if received > now or (pages and received < pages[-1].received_at):
            raise ValueError
        parsed = parse_corporate_actions_page(raw)
        total += len(parsed.events)
        if total > MAX_EVENTS:
            raise ValueError
        indices: dict[str, int] = {}
        for event in parsed.events:
            index = indices.get(event.category, 0)
            indices[event.category] = index + 1
            values = {
                key: event.fields[key]
                for key in ActionEventObservation.model_fields
                if key in event.fields
                and key not in {"category", "provider_event_id", "page_number", "index"}
            }
            observation = ActionEventObservation.model_validate(
                dict(
                    category=event.category,
                    provider_event_id=event.fields["id"],
                    page_number=number,
                    index=index,
                    **values,
                )
            )
            if symbol in (observation.symbol, observation.old_symbol, observation.new_symbol):
                events.append(observation)
        next_cursor = parsed.next_cursor
        if ((number == len(listed)) != (next_cursor is None)) or next_cursor in seen:
            raise ValueError
        if next_cursor is not None:
            seen.add(next_cursor)
        cursor = next_cursor
        pages.append(
            ActionPageReference(
                page_number=number,
                raw_content_hash=sha256(raw).hexdigest(),
                raw_byte_length=len(raw),
                receipt_content_hash=sha256(receipt_raw).hexdigest(),
                receipt_byte_length=len(receipt_raw),
                received_at=received,
            )
        )
        originals.extend(
            (
                ActionOriginal(entry["raw_file"], raw, "alpaca_actions_raw_v1", received),
                ActionOriginal(
                    entry["receipt_file"], receipt_raw, "alpaca_actions_receipt_v1", received
                ),
            )
        )
    available = pages[-1].received_at
    originals.append(
        ActionOriginal("manifest.json", manifest_raw, "alpaca_actions_manifest_v1", available)
    )
    document = AlpacaActionObservationDocument(
        instrument_id=instrument_id,
        symbol=symbol,
        query_start=request.start,
        query_end=request.end,
        available_at=available,
        manifest_content_hash=sha256(manifest_raw).hexdigest(),
        manifest_byte_length=len(manifest_raw),
        pages=tuple(pages),
        events=tuple(events),
        total_event_count=total,
        unbound_event_count=total - len(events),
    )
    return ValidatedActionArchive(document, tuple(originals))

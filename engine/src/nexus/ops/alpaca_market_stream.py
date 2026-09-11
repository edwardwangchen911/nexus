"""One bounded Alpaca stock stream; archives originals and never reconnects."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import logging
import os
import stat
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any, NoReturn
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession
from websockets.asyncio.client import connect

from nexus.market.ingestion import MarketObjectStore, object_uri_matches_key
from nexus.market.intraday_ingestion import (
    _get_or_create_artifact,
    _insert_observation,
)
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.providers.market.snapshot_contracts import SnapshotQuote, SnapshotTrade
from nexus.providers.news import _unique, validate_symbol
from nexus.settings import EngineSettings

FEEDS = {
    "iex": ("wss://stream.data.alpaca.markets/v2/iex", "real_time"),
    "overnight": (
        "wss://stream.data.alpaca.markets/v1beta1/overnight",
        "indicative_real_time_quote_delayed_trade",
    ),
}
SCHEMA = "nexus_alpaca_market_stream_receipt_v1"
ARCHIVE_SCHEMA = "alpaca_stock_stream_v1"
BATCH_SCHEMA = "nexus_alpaca_market_stream_batch_receipt_v1"
# The frozen object remains the existing generic stock-stream envelope. The receipt
# schema distinguishes one-symbol and batch transcripts without widening source policy.
BATCH_ARCHIVE_SCHEMA = ARCHIVE_SCHEMA
MAX_BYTES = 8 * 1024 * 1024
MAX_FRAME = 2 * 1024 * 1024
MAX_ENVELOPE_BYTES = 2 * MAX_BYTES
MAX_FRAMES = 100
MAX_DURATION_SECONDS = 300
MAX_SYMBOLS = 32
CLOSE_TIMEOUT_SECONDS = 1


class MarketStreamError(ValueError):
    """Stable, value-free stock stream error."""


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise MarketStreamError("ALPACA_MARKET_STREAM_REQUEST_INVALID")


class _FixedConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return exc


@dataclass(frozen=True)
class MarketStreamArchive:
    symbol: str
    feed: str
    started_at: datetime
    completed_at: datetime
    trade: SnapshotTrade | None
    quote: SnapshotQuote | None
    raw_payload: bytes


@dataclass(frozen=True)
class MarketStreamImportResult:
    raw_sha256: str
    created_observations: int


@dataclass(frozen=True)
class MarketStreamBatchItem:
    symbol: str
    trade: SnapshotTrade | None
    quote: SnapshotQuote | None


@dataclass(frozen=True)
class MarketStreamBatchArchive:
    symbols: tuple[str, ...]
    feed: str
    started_at: datetime
    completed_at: datetime
    items: tuple[MarketStreamBatchItem, ...]
    raw_payload: bytes


@dataclass(frozen=True)
class MarketStreamBatchImportResult:
    raw_sha256: str
    created_observations: int
    symbols_observed: int


def _messages(raw: bytes) -> list[dict[str, Any]]:
    try:
        value = json.loads(raw, parse_float=Decimal, object_pairs_hook=_unique)
    except (TypeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_FRAME_INVALID") from None
    if not isinstance(value, list) or not value or any(not isinstance(x, dict) for x in value):
        raise MarketStreamError("ALPACA_MARKET_STREAM_FRAME_INVALID")
    return value


def _utc(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise MarketStreamError("ALPACA_MARKET_STREAM_TIME_INVALID")
    try:
        # datetime accepts at most microseconds and deliberately truncates provider nanoseconds.
        result = datetime.fromisoformat(value)
    except ValueError:
        raise MarketStreamError("ALPACA_MARKET_STREAM_TIME_INVALID") from None
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise MarketStreamError("ALPACA_MARKET_STREAM_TIME_INVALID")
    return result.astimezone(UTC)


def _trade(item: dict[str, Any], symbol: str) -> SnapshotTrade:
    allowed = {"T", "S", "i", "x", "p", "s", "c", "t", "z"}
    if (
        set(item) != allowed
        or item.get("T") != "t"
        or item.get("S") != symbol
        or type(item.get("i")) is not int
        or type(item.get("s")) is not int
        or item["s"] <= 0
        or not isinstance(item.get("x"), str)
        or not isinstance(item.get("z"), str)
        or not isinstance(item.get("c"), list)
        or any(not isinstance(value, str) for value in item["c"])
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_TRADE_INVALID")
    try:
        return SnapshotTrade(
            price=item["p"],
            observed_at=_utc(item["t"]),
            provider_trade_id=str(item["i"]),
        )
    except (TypeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_TRADE_INVALID") from None


def _quote(item: dict[str, Any], symbol: str) -> SnapshotQuote:
    allowed = {"T", "S", "bx", "bp", "bs", "ax", "ap", "as", "c", "t", "z"}
    if (
        set(item) != allowed
        or item.get("T") != "q"
        or item.get("S") != symbol
        or type(item.get("bs")) is not int
        or type(item.get("as")) is not int
        or item["bs"] < 0
        or item["as"] < 0
        or not isinstance(item.get("bx"), str)
        or not isinstance(item.get("ax"), str)
        or not isinstance(item.get("z"), str)
        or not isinstance(item.get("c"), list)
        or any(not isinstance(value, str) for value in item["c"])
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_QUOTE_INVALID")
    try:
        quote = SnapshotQuote(
            bid=item["bp"],
            ask=item["ap"],
            observed_at=_utc(item["t"]),
        )
    except (TypeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_QUOTE_INVALID") from None
    if quote.bid > quote.ask:
        raise MarketStreamError("ALPACA_MARKET_STREAM_QUOTE_INVALID")
    return quote


def _subscription(item: dict[str, Any], symbol: str) -> bool:
    if item.get("T") != "subscription":
        return False
    expected_empty = ("bars", "updatedBars", "dailyBars", "statuses", "lulds")
    return (
        item.get("trades") == [symbol]
        and item.get("quotes") == [symbol]
        and all(item.get(field) in (None, []) for field in expected_empty)
        and item.get("corrections") in (None, [symbol])
        and item.get("cancelErrors") in (None, [symbol])
    )


def _symbols(values: object) -> tuple[str, ...]:
    if not isinstance(values, list | tuple) or not 1 <= len(values) <= MAX_SYMBOLS:
        raise MarketStreamError("ALPACA_MARKET_STREAM_SYMBOLS_INVALID")
    try:
        symbols = tuple(sorted(validate_symbol(value) for value in values))
    except (TypeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_SYMBOLS_INVALID") from None
    if len(set(symbols)) != len(symbols):
        raise MarketStreamError("ALPACA_MARKET_STREAM_SYMBOLS_INVALID")
    return symbols


def _batch_subscription(item: dict[str, Any], symbols: tuple[str, ...]) -> bool:
    if item.get("T") != "subscription":
        return False
    expected_empty = ("bars", "updatedBars", "dailyBars", "statuses", "lulds")
    expected = set(symbols)

    def exact(values: object) -> bool:
        return (
            isinstance(values, list)
            and len(values) == len(symbols)
            and all(isinstance(value, str) for value in values)
            and set(values) == expected
        )

    return (
        exact(item.get("trades"))
        and exact(item.get("quotes"))
        and all(item.get(field) in (None, []) for field in expected_empty)
        and (item.get("corrections") is None or exact(item.get("corrections")))
        and (item.get("cancelErrors") is None or exact(item.get("cancelErrors")))
    )


def _validate_stream_receipt(
    receipt: dict[str, Any],
    frames: list[bytes],
    *,
    schema: str,
    identity_key: str,
) -> tuple[datetime, datetime]:
    required = {
        "schema",
        "source",
        "transport",
        "origin",
        "feed",
        "delivery_mode",
        identity_key,
        "started_at",
        "ended_at",
        "requested_duration_seconds",
        "requested_max_frames",
        "elapsed_monotonic_seconds",
        "authenticated",
        "subscription_confirmed",
        "status",
        "stop_reason",
        "trade_count",
        "quote_count",
        "frame_count",
        "frames",
    }
    if set(receipt) != required or (
        receipt.get("schema") != schema
        or receipt.get("source") != "alpaca"
        or receipt.get("transport") != "websocket"
        or not isinstance(receipt.get("feed"), str)
        or receipt["feed"] not in FEEDS
        or (receipt.get("origin"), receipt.get("delivery_mode")) != FEEDS[receipt["feed"]]
        or receipt.get("status") != "bounded_complete"
        or receipt.get("stop_reason") not in {"DURATION_LIMIT", "FRAME_LIMIT"}
        or type(receipt.get("authenticated")) is not bool
        or type(receipt.get("subscription_confirmed")) is not bool
        or type(receipt.get("trade_count")) is not int
        or type(receipt.get("quote_count")) is not int
        or type(receipt.get("frame_count")) is not int
        or type(receipt.get("requested_duration_seconds")) is not float
        or type(receipt.get("requested_max_frames")) is not int
        or type(receipt.get("elapsed_monotonic_seconds")) is not float
        or not isinstance(receipt.get("frames"), list)
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_INCOMPLETE")
    started = _utc(receipt["started_at"])
    ended = _utc(receipt["ended_at"])
    duration = receipt["requested_duration_seconds"]
    frame_limit = receipt["requested_max_frames"]
    elapsed = receipt["elapsed_monotonic_seconds"]
    if (
        not receipt["authenticated"]
        or not receipt["subscription_confirmed"]
        or not 0 < duration <= MAX_DURATION_SECONDS
        or not 1 <= frame_limit <= MAX_FRAMES
        or not 0 <= elapsed <= duration + CLOSE_TIMEOUT_SECONDS
        or not started <= ended <= started + timedelta(seconds=duration + CLOSE_TIMEOUT_SECONDS)
        or len(frames) != len(receipt["frames"]) != 0
        or len(frames) != receipt["frame_count"]
        or len(frames) > frame_limit
        or sum(map(len, frames)) > MAX_BYTES
        or (receipt["stop_reason"] == "FRAME_LIMIT" and len(frames) != frame_limit)
        or (receipt["stop_reason"] == "DURATION_LIMIT" and elapsed < duration)
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    return started, ended


def _replay_stream_frames(
    receipt: dict[str, Any],
    frames: list[bytes],
    *,
    symbols: tuple[str, ...],
    started: datetime,
    ended: datetime,
    batch: bool,
) -> tuple[
    dict[str, list[tuple[int, SnapshotTrade]]],
    dict[str, list[tuple[int, SnapshotQuote]]],
]:
    connected = authenticated = subscribed = False
    trades: dict[str, list[tuple[int, SnapshotTrade]]] = {symbol: [] for symbol in symbols}
    quotes: dict[str, list[tuple[int, SnapshotQuote]]] = {symbol: [] for symbol in symbols}
    sequence = 0
    previous_received: datetime | None = None
    for index, (entry, raw) in enumerate(zip(receipt["frames"], frames, strict=True)):
        if not isinstance(entry, dict) or entry != {
            "file": f"frame-{index:05}.json",
            "received_at": entry.get("received_at"),
            "raw_sha256": sha256(raw).hexdigest(),
            "raw_byte_length": len(raw),
        }:
            raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
        received = _utc(entry["received_at"])
        if not started <= received <= ended or (
            previous_received is not None and received < previous_received
        ):
            raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
        previous_received = received
        for item in _messages(raw):
            kind = item.get("T")
            if kind == "success" and item.get("msg") == "connected":
                if connected or authenticated:
                    raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
                connected = True
            elif kind == "success" and item.get("msg") == "authenticated":
                if not connected or authenticated:
                    raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
                authenticated = True
            elif kind == "subscription":
                valid_subscription = (
                    _batch_subscription(item, symbols) if batch else _subscription(item, symbols[0])
                )
                if not authenticated or subscribed or not valid_subscription:
                    raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
                subscribed = True
            elif kind in {"t", "q"}:
                symbol = item.get("S")
                if not subscribed or symbol not in trades:
                    raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
                sequence += 1
                if kind == "t":
                    trades[symbol].append((sequence, _trade(item, symbol)))
                else:
                    quotes[symbol].append((sequence, _quote(item, symbol)))
            else:
                raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    if (
        not connected
        or not authenticated
        or not subscribed
        or sum(map(len, trades.values())) != receipt["trade_count"]
        or sum(map(len, quotes.values())) != receipt["quote_count"]
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    return trades, quotes


def _frozen_stream_payload(receipt_raw: bytes, frames: list[bytes]) -> bytes:
    return json.dumps(
        {
            "schema": ARCHIVE_SCHEMA,
            "receipt_base64": base64.b64encode(receipt_raw).decode(),
            "frames_base64": [base64.b64encode(frame).decode() for frame in frames],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _parse_archive(
    receipt: dict[str, Any],
    frames: list[bytes],
    *,
    receipt_raw: bytes,
) -> MarketStreamArchive:
    started, ended = _validate_stream_receipt(receipt, frames, schema=SCHEMA, identity_key="symbol")
    symbol = validate_symbol(receipt["symbol"])
    trades, quotes = _replay_stream_frames(
        receipt,
        frames,
        symbols=(symbol,),
        started=started,
        ended=ended,
        batch=False,
    )
    if not trades[symbol] and not quotes[symbol]:
        raise MarketStreamError("ALPACA_MARKET_STREAM_EMPTY")
    latest_trade = max(
        trades[symbol],
        key=lambda value: (value[1].observed_at, value[0]),
        default=None,
    )
    latest_quote = max(
        quotes[symbol],
        key=lambda value: (value[1].observed_at, value[0]),
        default=None,
    )
    return MarketStreamArchive(
        symbol=symbol,
        feed=receipt["feed"],
        started_at=started,
        completed_at=ended,
        trade=None if latest_trade is None else latest_trade[1],
        quote=None if latest_quote is None else latest_quote[1],
        raw_payload=_frozen_stream_payload(receipt_raw, frames),
    )


def _parse_batch_archive(
    receipt: dict[str, Any],
    frames: list[bytes],
    *,
    receipt_raw: bytes,
) -> MarketStreamBatchArchive:
    started, ended = _validate_stream_receipt(
        receipt, frames, schema=BATCH_SCHEMA, identity_key="symbols"
    )
    symbols = _symbols(receipt["symbols"])
    if list(symbols) != receipt["symbols"]:
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    trades, quotes = _replay_stream_frames(
        receipt,
        frames,
        symbols=symbols,
        started=started,
        ended=ended,
        batch=True,
    )
    items = tuple(
        MarketStreamBatchItem(
            symbol=symbol,
            trade=(
                None
                if not trades[symbol]
                else max(
                    trades[symbol],
                    key=lambda value: (value[1].observed_at, value[0]),
                )[1]
            ),
            quote=(
                None
                if not quotes[symbol]
                else max(
                    quotes[symbol],
                    key=lambda value: (value[1].observed_at, value[0]),
                )[1]
            ),
        )
        for symbol in symbols
        if trades[symbol] or quotes[symbol]
    )
    if not items:
        raise MarketStreamError("ALPACA_MARKET_STREAM_EMPTY")
    return MarketStreamBatchArchive(
        symbols=symbols,
        feed=receipt["feed"],
        started_at=started,
        completed_at=ended,
        items=items,
        raw_payload=_frozen_stream_payload(receipt_raw, frames),
    )


def _load_stream_directory(directory: Path) -> tuple[dict[str, Any], bytes, list[bytes]]:
    try:
        directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        metadata = os.fstat(directory_fd)
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
            raise OSError
        receipt_raw = _read_private(directory_fd, "receipt.json", 256 * 1024)
        receipt = json.loads(receipt_raw, object_pairs_hook=_unique)
        entries = receipt.get("frames")
        if not isinstance(entries, list) or len(entries) > MAX_FRAMES:
            raise OSError
        expected = {"receipt.json", *(f"frame-{index:05}.json" for index in range(len(entries)))}
        if set(os.listdir(directory_fd)) != expected:
            raise OSError
        frames = []
        total_bytes = 0
        for index in range(len(entries)):
            frame = _read_private(directory_fd, f"frame-{index:05}.json", MAX_FRAME)
            total_bytes += len(frame)
            if total_bytes > MAX_BYTES:
                raise OSError
            frames.append(frame)
    except MarketStreamError:
        raise
    except (OSError, TypeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID") from None
    finally:
        if "directory_fd" in locals():
            os.close(directory_fd)
    return receipt, receipt_raw, frames


def load_market_stream_batch_archive(directory: Path) -> MarketStreamBatchArchive:
    receipt, receipt_raw, frames = _load_stream_directory(directory)
    return _parse_batch_archive(receipt, frames, receipt_raw=receipt_raw)


def load_market_stream_archive(directory: Path) -> MarketStreamArchive:
    receipt, receipt_raw, frames = _load_stream_directory(directory)
    return _parse_archive(receipt, frames, receipt_raw=receipt_raw)


def parse_market_stream_raw_payload(raw: bytes) -> MarketStreamArchive:
    """Parse one exact frozen stream envelope without filesystem access."""
    try:
        receipt, receipt_raw, frames = _decode_stream_envelope(raw)
        return _parse_archive(receipt, frames, receipt_raw=receipt_raw)
    except MarketStreamError:
        raise
    except (binascii.Error, json.JSONDecodeError, TypeError, UnicodeDecodeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID") from None


def parse_market_stream_batch_raw_payload(raw: bytes) -> MarketStreamBatchArchive:
    """Parse one exact frozen batch stream envelope without filesystem access."""
    try:
        receipt, receipt_raw, frames = _decode_stream_envelope(raw)
        return _parse_batch_archive(receipt, frames, receipt_raw=receipt_raw)
    except MarketStreamError:
        raise
    except (binascii.Error, json.JSONDecodeError, TypeError, UnicodeDecodeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID") from None


def _decode_stream_envelope(raw: bytes) -> tuple[dict[str, Any], bytes, list[bytes]]:
    if not 0 < len(raw) <= MAX_ENVELOPE_BYTES:
        raise ValueError
    envelope = json.loads(raw, object_pairs_hook=_unique)
    if (
        not isinstance(envelope, dict)
        or set(envelope) != {"schema", "receipt_base64", "frames_base64"}
        or envelope["schema"] != ARCHIVE_SCHEMA
        or not isinstance(envelope["receipt_base64"], str)
        or not isinstance(envelope["frames_base64"], list)
        or len(envelope["frames_base64"]) > MAX_FRAMES
        or any(not isinstance(value, str) for value in envelope["frames_base64"])
    ):
        raise ValueError
    receipt_raw = base64.b64decode(envelope["receipt_base64"], validate=True)
    frames = [base64.b64decode(value, validate=True) for value in envelope["frames_base64"]]
    if (
        not 0 < len(receipt_raw) <= 256 * 1024
        or any(not 0 < len(frame) <= MAX_FRAME for frame in frames)
        or sum(map(len, frames)) > MAX_BYTES
    ):
        raise ValueError
    receipt = json.loads(receipt_raw, object_pairs_hook=_unique)
    if not isinstance(receipt, dict):
        raise ValueError
    return receipt, receipt_raw, frames


async def _archive_stream_artifact(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    raw_payload: bytes,
    completed_at: datetime,
    observations: tuple[SnapshotTrade | SnapshotQuote, ...],
) -> tuple[DataArtifact, str]:
    raw_digest = sha256(raw_payload).hexdigest()
    raw_key = f"raw/market/alpaca/stream/{completed_at:%Y/%m/%d}/{raw_digest}.json"
    raw_uri = await store.archive_raw(raw_key, raw_payload)
    if not object_uri_matches_key(raw_uri, raw_key):
        raise RuntimeError("ALPACA_MARKET_STREAM_OBJECT_URI_MISMATCH")
    if await store.read_raw(raw_key, uri=raw_uri) != raw_payload:
        raise RuntimeError("ALPACA_MARKET_STREAM_OBJECT_READBACK_MISMATCH")
    observed_at = max(value.observed_at for value in observations)
    artifact, _ = await _get_or_create_artifact(
        session,
        uri=raw_uri,
        digest=raw_digest,
        byte_size=len(raw_payload),
        mime_type="application/json",
        schema_version=ARCHIVE_SCHEMA,
        source="alpaca",
        observed_at=observed_at,
        available_at=completed_at,
        received_at=completed_at,
    )
    if (
        artifact.observed_at != observed_at
        or artifact.available_at != completed_at
        or artifact.received_at != completed_at
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARTIFACT_REPLAY_CONFLICT")
    return artifact, raw_digest


async def _insert_stream_observations(
    session: AsyncSession,
    *,
    artifact: DataArtifact,
    instrument_id: UUID,
    trade: SnapshotTrade | None,
    quote: SnapshotQuote | None,
    completed_at: datetime,
    revision: str,
) -> int:
    created = 0
    if trade is not None:
        created += int(
            await _insert_observation(
                session,
                raw_artifact=artifact,
                instrument_id=instrument_id,
                source="alpaca",
                feed="iex",
                delivery_mode="real_time",
                event_kind="trade",
                bid=None,
                ask=None,
                last=trade.price,
                observed_at=trade.observed_at,
                available_at=completed_at,
                received_at=completed_at,
                revision=revision,
                quality_flags=trade.quality_flags,
                provider_binding=None,
            )
        )
    if quote is not None:
        created += int(
            await _insert_observation(
                session,
                raw_artifact=artifact,
                instrument_id=instrument_id,
                source="alpaca",
                feed="iex",
                delivery_mode="real_time",
                event_kind="quote",
                bid=quote.bid,
                ask=quote.ask,
                last=None,
                observed_at=quote.observed_at,
                available_at=completed_at,
                received_at=completed_at,
                revision=revision,
                quality_flags=(),
                provider_binding=None,
            )
        )
    return created


async def import_market_stream(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    instrument_id: UUID,
    directory: Path,
    imported_at: datetime,
) -> MarketStreamImportResult:
    """Import the latest bounded IEX trade and quote as display-only observations."""
    if imported_at.tzinfo is None or imported_at.utcoffset() != timedelta(0):
        raise MarketStreamError("ALPACA_MARKET_STREAM_IMPORT_TIME_INVALID")
    archive = load_market_stream_archive(directory)
    if archive.feed != "iex":
        raise MarketStreamError("ALPACA_MARKET_STREAM_IMPORT_FEED_UNSUPPORTED")
    observations = tuple(value for value in (archive.trade, archive.quote) if value is not None)
    if archive.completed_at > imported_at or any(
        value.observed_at > archive.completed_at for value in observations
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_TIME_INVALID")
    identity_rows = (
        await session.execute(
            select(InstrumentAlias, Instrument)
            .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
            .where(
                InstrumentAlias.instrument_id == instrument_id,
                InstrumentAlias.symbol == archive.symbol,
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= archive.completed_at.date(),
                (
                    InstrumentAlias.valid_to.is_(None)
                    | (InstrumentAlias.valid_to >= archive.completed_at.date())
                ),
                InstrumentAlias.created_at <= archive.completed_at,
                Instrument.currency == "USD",
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
            )
            .limit(2)
        )
    ).all()
    if len(identity_rows) != 1:
        raise MarketStreamError("ALPACA_MARKET_STREAM_IDENTITY_REQUIRED")
    artifact, raw_digest = await _archive_stream_artifact(
        session,
        store,
        raw_payload=archive.raw_payload,
        completed_at=archive.completed_at,
        observations=observations,
    )
    created = await _insert_stream_observations(
        session,
        artifact=artifact,
        instrument_id=instrument_id,
        trade=archive.trade,
        quote=archive.quote,
        completed_at=archive.completed_at,
        revision=raw_digest,
    )
    return MarketStreamImportResult(raw_sha256=raw_digest, created_observations=created)


async def import_market_stream_batch(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    bindings: dict[str, UUID],
    directory: Path,
    imported_at: datetime,
) -> MarketStreamBatchImportResult:
    """Import available observations from one bounded multi-symbol IEX transcript."""
    if imported_at.tzinfo is None or imported_at.utcoffset() != timedelta(0):
        raise MarketStreamError("ALPACA_MARKET_STREAM_IMPORT_TIME_INVALID")
    archive = load_market_stream_batch_archive(directory)
    if archive.feed != "iex":
        raise MarketStreamError("ALPACA_MARKET_STREAM_IMPORT_FEED_UNSUPPORTED")
    if (
        set(bindings) != set(archive.symbols)
        or any(type(value) is not UUID for value in bindings.values())
        or len(set(bindings.values())) != len(bindings)
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_BINDINGS_INVALID")
    observations = tuple(
        observation
        for item in archive.items
        for observation in (item.trade, item.quote)
        if observation is not None
    )
    if archive.completed_at > imported_at or any(
        value.observed_at > archive.completed_at for value in observations
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_TIME_INVALID")

    identity_rows = (
        await session.execute(
            select(InstrumentAlias, Instrument)
            .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
            .where(
                InstrumentAlias.instrument_id.in_(tuple(bindings.values())),
                InstrumentAlias.symbol.in_(archive.symbols),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.created_at <= archive.completed_at,
                Instrument.currency == "USD",
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
            )
        )
    ).all()
    items = {item.symbol: item for item in archive.items}
    for symbol, instrument_id in bindings.items():
        item = items.get(symbol)
        event_dates = {
            value.observed_at.date()
            for value in (() if item is None else (item.trade, item.quote))
            if value is not None
        } or {archive.completed_at.date()}
        for event_date in event_dates:
            matches = [
                alias
                for alias, _ in identity_rows
                if alias.symbol == symbol
                and alias.instrument_id == instrument_id
                and alias.valid_from <= event_date
                and (alias.valid_to is None or alias.valid_to >= event_date)
            ]
            if len(matches) != 1:
                raise MarketStreamError("ALPACA_MARKET_STREAM_IDENTITY_REQUIRED")

    artifact, raw_digest = await _archive_stream_artifact(
        session,
        store,
        raw_payload=archive.raw_payload,
        completed_at=archive.completed_at,
        observations=observations,
    )
    created = 0
    for item in archive.items:
        created += await _insert_stream_observations(
            session,
            artifact=artifact,
            instrument_id=bindings[item.symbol],
            trade=item.trade,
            quote=item.quote,
            completed_at=archive.completed_at,
            revision=raw_digest,
        )
    return MarketStreamBatchImportResult(
        raw_sha256=raw_digest,
        created_observations=created,
        symbols_observed=len(archive.items),
    )


def _read_private(directory_fd: int, name: str, maximum: int) -> bytes:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o077
            or not 0 < metadata.st_size <= maximum
        ):
            raise OSError
        with os.fdopen(descriptor, "rb") as source:
            payload = source.read(maximum + 1)
            descriptor = -1
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(payload) != metadata.st_size:
        raise OSError
    return payload


async def _collect_market_stream_symbols(
    *,
    symbols: tuple[str, ...],
    directory: Path,
    key: str,
    secret: str,
    feed: str,
    batch: bool,
    duration: float = 60,
    max_frames: int = MAX_FRAMES,
    connector: Any = _FixedConnect,
) -> dict[str, object]:
    """Collect one exact bounded transcript using the shared socket safety loop."""
    symbols = _symbols(symbols)
    try:
        url, delivery_mode = FEEDS[feed]
    except (KeyError, TypeError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_FEED_INVALID") from None
    if (
        type(duration) not in {int, float}
        or type(max_frames) is not int
        or not 0 < duration <= MAX_DURATION_SECONDS
        or not 1 <= max_frames <= MAX_FRAMES
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_LIMIT_INVALID")
    if any(
        not value
        or not value.isascii()
        or any(not 33 <= ord(character) <= 126 for character in value)
        for value in (key, secret)
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_CREDENTIALS_INVALID")
    try:
        parent = directory.parent.stat()
    except OSError:
        raise MarketStreamError("ALPACA_MARKET_STREAM_PRIVATE_PARENT_REQUIRED") from None
    if not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077:
        raise MarketStreamError("ALPACA_MARKET_STREAM_PRIVATE_PARENT_REQUIRED")
    try:
        directory.mkdir(mode=0o700)
    except OSError:
        raise MarketStreamError("ALPACA_MARKET_STREAM_NEW_OUTPUT_REQUIRED") from None

    started = datetime.now(UTC)
    start_monotonic = monotonic()
    requested_duration = float(duration)
    deadline = start_monotonic + requested_duration
    receipt: dict[str, Any] = {
        "schema": BATCH_SCHEMA if batch else SCHEMA,
        "source": "alpaca",
        "transport": "websocket",
        "origin": url,
        "feed": feed,
        "delivery_mode": delivery_mode,
        **({"symbols": list(symbols)} if batch else {"symbol": symbols[0]}),
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "authenticated": False,
        "subscription_confirmed": False,
        "status": "failed",
        "stop_reason": "CONNECT_FAILED",
        "trade_count": 0,
        "quote_count": 0,
        "frames": [],
        "requested_duration_seconds": requested_duration,
        "requested_max_frames": max_frames,
    }
    observed_symbols: set[str] = set()
    latest_observed_at = started
    total_bytes = 0
    connected = False
    cancelled: asyncio.CancelledError | None = None
    logger = logging.Logger("nexus.alpaca_market_stream.private", level=logging.CRITICAL + 1)
    logger.propagate = False
    try:
        async with connector(
            url,
            additional_headers={
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
                "Content-Type": "application/json",
            },
            open_timeout=min(10, duration),
            close_timeout=CLOSE_TIMEOUT_SECONDS,
            max_size=MAX_FRAME,
            max_queue=1,
            compression=None,
            logger=logger,
        ) as socket:
            try:
                receipt["stop_reason"] = "DISCONNECTED"
                while len(receipt["frames"]) < max_frames:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError
                    frame = await asyncio.wait_for(socket.recv(), timeout=remaining)
                    raw = frame.encode() if isinstance(frame, str) else frame
                    total_bytes += len(raw)
                    if total_bytes > MAX_BYTES:
                        raise MarketStreamError("BYTE_LIMIT")
                    messages = _messages(raw)
                    decoded = json.dumps(messages, ensure_ascii=False, default=str)
                    if any(value.encode() in raw or value in decoded for value in (key, secret)):
                        raise MarketStreamError("SECRET_REFLECTION")
                    received = datetime.now(UTC)
                    name = f"frame-{len(receipt['frames']):05}.json"
                    write_private(directory / name, raw)
                    receipt["frames"].append(
                        {
                            "file": name,
                            "received_at": received.isoformat().replace("+00:00", "Z"),
                            "raw_sha256": sha256(raw).hexdigest(),
                            "raw_byte_length": len(raw),
                        }
                    )
                    for item in messages:
                        kind = item.get("T")
                        if kind == "success" and item.get("msg") == "connected":
                            if connected or receipt["authenticated"]:
                                raise MarketStreamError("PROTOCOL_INVALID")
                            connected = True
                        elif kind == "success" and item.get("msg") == "authenticated":
                            if not connected or receipt["authenticated"]:
                                raise MarketStreamError("PROTOCOL_INVALID")
                            receipt["authenticated"] = True
                            await socket.send(
                                json.dumps(
                                    {
                                        "action": "subscribe",
                                        "trades": list(symbols),
                                        "quotes": list(symbols),
                                    },
                                    separators=(",", ":"),
                                )
                            )
                        elif kind == "subscription":
                            if (
                                not receipt["authenticated"]
                                or receipt["subscription_confirmed"]
                                or not (
                                    _batch_subscription(item, symbols)
                                    if batch
                                    else _subscription(item, symbols[0])
                                )
                            ):
                                raise MarketStreamError("SUBSCRIPTION_INVALID")
                            receipt["subscription_confirmed"] = True
                        elif kind in {"t", "q"}:
                            symbol = item.get("S")
                            if not receipt["subscription_confirmed"] or symbol not in symbols:
                                raise MarketStreamError("PROTOCOL_INVALID")
                            observation = (
                                _trade(item, symbol) if kind == "t" else _quote(item, symbol)
                            )
                            latest_observed_at = max(latest_observed_at, observation.observed_at)
                            if kind == "t":
                                receipt["trade_count"] += 1
                            else:
                                receipt["quote_count"] += 1
                            observed_symbols.add(symbol)
                        elif kind == "error":
                            code = item.get("code")
                            allowed = {400, 401, 402, 403, 404, 405, 406, 407, 409, 410, 500}
                            raise MarketStreamError(
                                f"PROVIDER_{code}"
                                if type(code) is int and code in allowed
                                else "PROVIDER_ERROR"
                            )
                        else:
                            raise MarketStreamError("PROTOCOL_INVALID")
                receipt["stop_reason"] = "FRAME_LIMIT"
            except TimeoutError:
                receipt["stop_reason"] = "DURATION_LIMIT"
            except MarketStreamError as error:
                receipt["stop_reason"] = str(error)
            except asyncio.CancelledError as error:
                receipt["stop_reason"] = "CANCELLED"
                cancelled = error
            except Exception:
                receipt["stop_reason"] = "DISCONNECTED"
            finally:
                ended = datetime.now(UTC)
                if latest_observed_at > ended + timedelta(seconds=1):
                    receipt["stop_reason"] = "PROTOCOL_INVALID"
                ended = max(ended, latest_observed_at)
                receipt["ended_at"] = ended.isoformat().replace("+00:00", "Z")
                receipt["elapsed_monotonic_seconds"] = monotonic() - start_monotonic
    except asyncio.CancelledError as error:
        receipt["stop_reason"] = "CANCELLED"
        cancelled = error
    except Exception:
        receipt["stop_reason"] = "DISCONNECTED"
    if "ended_at" not in receipt:
        receipt["ended_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        receipt["elapsed_monotonic_seconds"] = monotonic() - start_monotonic
    receipt["frame_count"] = len(receipt["frames"])
    receipt["status"] = (
        "bounded_complete"
        if receipt["subscription_confirmed"]
        and receipt["stop_reason"] in {"DURATION_LIMIT", "FRAME_LIMIT"}
        else "partial"
        if receipt["subscription_confirmed"]
        else "failed"
    )
    write_private(directory / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
    if cancelled is not None:
        raise cancelled
    result = {
        "status": receipt["status"],
        "stop_reason": receipt["stop_reason"],
        "authenticated": receipt["authenticated"],
        "subscription_confirmed": receipt["subscription_confirmed"],
        "trade_count": receipt["trade_count"],
        "quote_count": receipt["quote_count"],
        "frame_count": receipt["frame_count"],
    }
    if batch:
        result.update(
            symbols_requested=len(symbols),
            symbols_observed=len(observed_symbols),
        )
    return result


async def collect_market_stream(
    *,
    symbol: str,
    directory: Path,
    key: str,
    secret: str,
    feed: str = "iex",
    duration: float = 60,
    max_frames: int = MAX_FRAMES,
    connector: Any = _FixedConnect,
) -> dict[str, object]:
    return await _collect_market_stream_symbols(
        symbols=(validate_symbol(symbol),),
        directory=directory,
        key=key,
        secret=secret,
        feed=feed,
        batch=False,
        duration=duration,
        max_frames=max_frames,
        connector=connector,
    )


async def collect_market_stream_batch(
    *,
    symbols: tuple[str, ...],
    directory: Path,
    key: str,
    secret: str,
    duration: float = 60,
    max_frames: int = MAX_FRAMES,
    connector: Any = _FixedConnect,
) -> dict[str, object]:
    return await _collect_market_stream_symbols(
        symbols=symbols,
        directory=directory,
        key=key,
        secret=secret,
        feed="iex",
        batch=True,
        duration=duration,
        max_frames=max_frames,
        connector=connector,
    )


def _require_local_services(settings: EngineSettings) -> None:
    database = make_url(settings.database_url) if settings.database_url else None
    endpoint = urlsplit(settings.object_store_endpoint or "")
    local = {"localhost", "127.0.0.1", "::1"}
    if (
        database is None
        or database.drivername != "postgresql+asyncpg"
        or database.host not in local
        or database.query
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in local
        or endpoint.username
        or endpoint.password
        or endpoint.path
        or endpoint.query
        or endpoint.fragment
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_LOCAL_SERVICES_REQUIRED")


async def import_market_stream_local(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    directory: Path,
) -> MarketStreamImportResult:
    _require_local_services(settings)
    store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    async with asyncio.timeout(120):
        async with factory.begin() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            head = await session.scalar(
                text(
                    "SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END "
                    "FROM public.alembic_version"
                )
            )
            if head != CURRENT_MIGRATION_HEAD:
                raise MarketStreamError("ALPACA_MARKET_STREAM_SCHEMA_NOT_READY")
            imported_at = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(imported_at, datetime):
                raise RuntimeError("ALPACA_MARKET_STREAM_DATABASE_CLOCK_INVALID")
            return await import_market_stream(
                session,
                store,
                instrument_id=instrument_id,
                directory=directory,
                imported_at=imported_at,
            )


async def import_market_stream_batch_local(
    settings: EngineSettings,
    *,
    bindings: dict[str, UUID],
    directory: Path,
) -> MarketStreamBatchImportResult:
    _require_local_services(settings)
    store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    async with asyncio.timeout(120):
        async with factory.begin() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            head = await session.scalar(
                text(
                    "SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END "
                    "FROM public.alembic_version"
                )
            )
            if head != CURRENT_MIGRATION_HEAD:
                raise MarketStreamError("ALPACA_MARKET_STREAM_SCHEMA_NOT_READY")
            imported_at = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(imported_at, datetime):
                raise RuntimeError("ALPACA_MARKET_STREAM_DATABASE_CLOCK_INVALID")
            return await import_market_stream_batch(
                session,
                store,
                bindings=bindings,
                directory=directory,
                imported_at=imported_at,
            )


def _parse_bindings(values: list[str]) -> dict[str, UUID]:
    bindings: dict[str, UUID] = {}
    try:
        for value in values:
            symbol_raw, separator, instrument_raw = value.partition("=")
            if not separator:
                raise ValueError
            symbol = validate_symbol(symbol_raw)
            if symbol in bindings:
                raise ValueError
            bindings[symbol] = UUID(instrument_raw)
    except (AttributeError, TypeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_BINDINGS_INVALID") from None
    if not bindings:
        raise MarketStreamError("ALPACA_MARKET_STREAM_BINDINGS_INVALID")
    return bindings


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    collect_parser = commands.add_parser("collect")
    collect_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    collect_parser.add_argument("--symbol", required=True)
    collect_parser.add_argument("--feed", choices=tuple(FEEDS), default="iex")
    collect_parser.add_argument("--output-dir", type=Path, required=True)
    collect_parser.add_argument("--duration", type=float, default=60)
    collect_parser.add_argument("--max-frames", type=int, default=MAX_FRAMES)
    collect_batch_parser = commands.add_parser("collect-batch")
    collect_batch_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    collect_batch_parser.add_argument("--symbol", action="append", required=True)
    collect_batch_parser.add_argument("--output-dir", type=Path, required=True)
    collect_batch_parser.add_argument("--duration", type=float, default=60)
    collect_batch_parser.add_argument("--max-frames", type=int, default=MAX_FRAMES)
    import_parser = commands.add_parser("import")
    import_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    import_parser.add_argument("--instrument-id", type=UUID, required=True)
    import_parser.add_argument("--stream-dir", type=Path, required=True)
    import_parser.add_argument("--output", type=Path, required=True)
    import_batch_parser = commands.add_parser("import-batch")
    import_batch_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    import_batch_parser.add_argument("--binding", action="append", required=True)
    import_batch_parser.add_argument("--stream-dir", type=Path, required=True)
    import_batch_parser.add_argument("--output", type=Path, required=True)
    import_started = False
    try:
        args = parser.parse_args(argv)
        environment = load_local_environment(args.repo_root, os.environ)
        if args.command == "collect":
            result = asyncio.run(
                collect_market_stream(
                    symbol=args.symbol,
                    directory=args.output_dir,
                    key=environment.get("ALPACA_API_KEY_ID", ""),
                    secret=environment.get("ALPACA_API_SECRET_KEY", ""),
                    feed=args.feed,
                    duration=args.duration,
                    max_frames=args.max_frames,
                )
            )
        elif args.command == "collect-batch":
            result = asyncio.run(
                collect_market_stream_batch(
                    symbols=tuple(args.symbol),
                    directory=args.output_dir,
                    key=environment.get("ALPACA_API_KEY_ID", ""),
                    secret=environment.get("ALPACA_API_SECRET_KEY", ""),
                    duration=args.duration,
                    max_frames=args.max_frames,
                )
            )
        else:
            settings = EngineSettings.model_validate(environment)
            batch_bindings = (
                _parse_bindings(args.binding) if args.command == "import-batch" else None
            )
            descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as output:
                json.dump({"status": "started"}, output)
                output.flush()
                os.fsync(output.fileno())
                import_started = True
                imported: MarketStreamImportResult | MarketStreamBatchImportResult
                if args.command == "import":
                    imported = asyncio.run(
                        import_market_stream_local(
                            settings,
                            instrument_id=args.instrument_id,
                            directory=args.stream_dir,
                        )
                    )
                else:
                    if batch_bindings is None:
                        raise MarketStreamError("ALPACA_MARKET_STREAM_BINDINGS_INVALID")
                    imported = asyncio.run(
                        import_market_stream_batch_local(
                            settings,
                            bindings=batch_bindings,
                            directory=args.stream_dir,
                        )
                    )
                result = {
                    "status": "stored_display_quote",
                    "raw_sha256": imported.raw_sha256,
                    "created_observations": imported.created_observations,
                    **(
                        {"symbols_observed": imported.symbols_observed}
                        if isinstance(imported, MarketStreamBatchImportResult)
                        else {}
                    ),
                    "execution_locked": True,
                    "model_called": False,
                }
                output.seek(0)
                json.dump(result, output)
                output.truncate()
                output.flush()
                os.fsync(output.fileno())
    except MarketStreamError as error:
        result = {
            "status": "unknown" if import_started else "failed",
            "stop_reason": (
                "ALPACA_MARKET_STREAM_IMPORT_CHECK_PERSISTED_STATE"
                if import_started
                else str(error)
            ),
        }
    except (Exception, KeyboardInterrupt):
        result = {
            "status": "unknown" if import_started else "failed",
            "stop_reason": (
                "ALPACA_MARKET_STREAM_IMPORT_CHECK_PERSISTED_STATE"
                if import_started
                else "ALPACA_MARKET_STREAM_FAILED"
            ),
        }
    print(json.dumps(result))
    return 0 if result["status"] in {"bounded_complete", "stored_display_quote"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

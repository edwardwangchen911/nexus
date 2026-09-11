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
MAX_BYTES = 8 * 1024 * 1024
MAX_FRAME = 2 * 1024 * 1024
MAX_FRAMES = 100
MAX_DURATION_SECONDS = 300
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


def _parse_archive(
    receipt: dict[str, Any],
    frames: list[bytes],
    *,
    receipt_raw: bytes,
) -> MarketStreamArchive:
    required = {
        "schema",
        "source",
        "transport",
        "origin",
        "feed",
        "delivery_mode",
        "symbol",
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
        receipt.get("schema") != SCHEMA
        or receipt.get("source") != "alpaca"
        or receipt.get("transport") != "websocket"
        or not isinstance(receipt.get("feed"), str)
        or receipt["feed"] not in FEEDS
        or (
            receipt.get("origin"),
            receipt.get("delivery_mode"),
        )
        != FEEDS[receipt["feed"]]
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
    symbol = validate_symbol(receipt["symbol"])
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
    connected = authenticated = subscribed = False
    trades: list[tuple[int, SnapshotTrade]] = []
    quotes: list[tuple[int, SnapshotQuote]] = []
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
                if not authenticated or subscribed or not _subscription(item, symbol):
                    raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
                subscribed = True
            elif kind == "t":
                if not subscribed:
                    raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
                sequence += 1
                trades.append((sequence, _trade(item, symbol)))
            elif kind == "q":
                if not subscribed:
                    raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
                sequence += 1
                quotes.append((sequence, _quote(item, symbol)))
            else:
                raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    if (
        not connected
        or not authenticated
        or not subscribed
        or len(trades) != receipt["trade_count"]
        or len(quotes) != receipt["quote_count"]
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    if not trades and not quotes:
        raise MarketStreamError("ALPACA_MARKET_STREAM_EMPTY")
    raw_payload = json.dumps(
        {
            "schema": ARCHIVE_SCHEMA,
            "receipt_base64": base64.b64encode(receipt_raw).decode(),
            "frames_base64": [base64.b64encode(frame).decode() for frame in frames],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    latest_trade = max(trades, key=lambda value: (value[1].observed_at, value[0]), default=None)
    latest_quote = max(quotes, key=lambda value: (value[1].observed_at, value[0]), default=None)
    return MarketStreamArchive(
        symbol=symbol,
        feed=receipt["feed"],
        started_at=started,
        completed_at=ended,
        trade=None if latest_trade is None else latest_trade[1],
        quote=None if latest_quote is None else latest_quote[1],
        raw_payload=raw_payload,
    )


def load_market_stream_archive(directory: Path) -> MarketStreamArchive:
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
    return _parse_archive(receipt, frames, receipt_raw=receipt_raw)


def parse_market_stream_raw_payload(raw: bytes) -> MarketStreamArchive:
    """Parse one exact frozen stream envelope without filesystem access."""
    try:
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
        receipt = json.loads(receipt_raw, object_pairs_hook=_unique)
        if not isinstance(receipt, dict):
            raise ValueError
        return _parse_archive(receipt, frames, receipt_raw=receipt_raw)
    except MarketStreamError:
        raise
    except (binascii.Error, json.JSONDecodeError, TypeError, UnicodeDecodeError, ValueError):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID") from None


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
    raw_digest = sha256(archive.raw_payload).hexdigest()
    date_path = archive.completed_at.strftime("%Y/%m/%d")
    raw_key = f"raw/market/alpaca/stream/{date_path}/{raw_digest}.json"
    raw_uri = await store.archive_raw(raw_key, archive.raw_payload)
    if not object_uri_matches_key(raw_uri, raw_key):
        raise RuntimeError("ALPACA_MARKET_STREAM_OBJECT_URI_MISMATCH")
    if await store.read_raw(raw_key, uri=raw_uri) != archive.raw_payload:
        raise RuntimeError("ALPACA_MARKET_STREAM_OBJECT_READBACK_MISMATCH")
    observed_at = max(value.observed_at for value in observations)
    artifact, _ = await _get_or_create_artifact(
        session,
        uri=raw_uri,
        digest=raw_digest,
        byte_size=len(archive.raw_payload),
        mime_type="application/json",
        schema_version=ARCHIVE_SCHEMA,
        source="alpaca",
        observed_at=observed_at,
        available_at=archive.completed_at,
        received_at=archive.completed_at,
    )
    if (
        artifact.observed_at != observed_at
        or artifact.available_at != archive.completed_at
        or artifact.received_at != archive.completed_at
    ):
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARTIFACT_REPLAY_CONFLICT")
    created = 0
    if archive.trade is not None:
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
                last=archive.trade.price,
                observed_at=archive.trade.observed_at,
                available_at=archive.completed_at,
                received_at=archive.completed_at,
                revision=raw_digest,
                quality_flags=archive.trade.quality_flags,
                provider_binding=None,
            )
        )
    if archive.quote is not None:
        created += int(
            await _insert_observation(
                session,
                raw_artifact=artifact,
                instrument_id=instrument_id,
                source="alpaca",
                feed="iex",
                delivery_mode="real_time",
                event_kind="quote",
                bid=archive.quote.bid,
                ask=archive.quote.ask,
                last=None,
                observed_at=archive.quote.observed_at,
                available_at=archive.completed_at,
                received_at=archive.completed_at,
                revision=raw_digest,
                quality_flags=(),
                provider_binding=None,
            )
        )
    return MarketStreamImportResult(raw_sha256=raw_digest, created_observations=created)


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
    symbol = validate_symbol(symbol)
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
        "schema": SCHEMA,
        "source": "alpaca",
        "transport": "websocket",
        "origin": url,
        "feed": feed,
        "delivery_mode": delivery_mode,
        "symbol": symbol,
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
                                        "trades": [symbol],
                                        "quotes": [symbol],
                                    },
                                    separators=(",", ":"),
                                )
                            )
                        elif kind == "subscription":
                            if (
                                not receipt["authenticated"]
                                or receipt["subscription_confirmed"]
                                or not _subscription(item, symbol)
                            ):
                                raise MarketStreamError("SUBSCRIPTION_INVALID")
                            receipt["subscription_confirmed"] = True
                        elif kind == "t":
                            if not receipt["subscription_confirmed"]:
                                raise MarketStreamError("SUBSCRIPTION_INVALID")
                            _trade(item, symbol)
                            receipt["trade_count"] += 1
                        elif kind == "q":
                            if not receipt["subscription_confirmed"]:
                                raise MarketStreamError("SUBSCRIPTION_INVALID")
                            _quote(item, symbol)
                            receipt["quote_count"] += 1
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
                # The bounded receive window excludes websocket close cleanup.
                receipt["ended_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
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
    return {
        field: receipt[field]
        for field in (
            "status",
            "stop_reason",
            "authenticated",
            "subscription_confirmed",
            "trade_count",
            "quote_count",
            "frame_count",
        )
    }


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
    import_parser = commands.add_parser("import")
    import_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    import_parser.add_argument("--instrument-id", type=UUID, required=True)
    import_parser.add_argument("--stream-dir", type=Path, required=True)
    import_parser.add_argument("--output", type=Path, required=True)
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
        else:
            settings = EngineSettings.model_validate(environment)
            descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as output:
                json.dump({"status": "started"}, output)
                output.flush()
                os.fsync(output.fileno())
                import_started = True
                imported = asyncio.run(
                    import_market_stream_local(
                        settings,
                        instrument_id=args.instrument_id,
                        directory=args.stream_dir,
                    )
                )
                result = {
                    "status": "stored_display_quote",
                    "raw_sha256": imported.raw_sha256,
                    "created_observations": imported.created_observations,
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

"""Strict, read-only AllTick stock stream protocol primitives."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal
from urllib.parse import quote, urlsplit
from uuid import uuid4

from nexus.providers.market.stream_contracts import MarketStreamObservation

_BASE_URL = "wss://quote.alltick.co/quote-stock-b-ws-api"
_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,30}\.US$")


class AllTickStreamError(ValueError):
    """Stable, value-free AllTick stream error."""


class AllTickStreamState:
    def __init__(self, symbols: tuple[str, ...]) -> None:
        self._symbols = _symbols(symbols)
        self._state: dict[str, dict[str, object]] = {}

    def apply(
        self, frame: dict[str, Any], *, received_at: datetime
    ) -> tuple[MarketStreamObservation, ...]:
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise AllTickStreamError("ALLTICK_STREAM_FRAME_INVALID")
        if not isinstance(frame, dict) or type(frame.get("cmd_id")) is not int:
            raise AllTickStreamError("ALLTICK_STREAM_FRAME_INVALID")
        if frame["cmd_id"] in {22001, 22003, 22005}:
            return ()
        if frame["cmd_id"] not in {22998, 22999} or not isinstance(frame.get("data"), dict):
            raise AllTickStreamError("ALLTICK_STREAM_FRAME_INVALID")
        data = frame["data"]
        symbol = data.get("code")
        if not isinstance(symbol, str) or symbol not in self._symbols:
            raise AllTickStreamError("ALLTICK_STREAM_SYMBOL_INVALID")
        sequence = data.get("seq")
        if type(sequence) is not int or sequence < 0:
            raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
        provider_time, unit = _provider_time(data.get("tick_time"))
        current = self._state.setdefault(symbol, {})
        if frame["cmd_id"] == 22998:
            _trade(data, current)
        else:
            _quote(data, current)
        return (
            MarketStreamObservation(
                source="alltick",
                symbol=symbol,
                received_at=received_at,
                sequence=sequence,
                quote_time=provider_time if frame["cmd_id"] == 22999 else None,
                trade_time=provider_time if frame["cmd_id"] == 22998 else None,
                provider_time_unit=unit,
                delayed=None,
                bid=_state_number(current, "bid"),
                ask=_state_number(current, "ask"),
                last=_state_number(current, "last"),
                bid_size=_state_number(current, "bid_size"),
                ask_size=_state_number(current, "ask_size"),
                last_size=_state_number(current, "last_size"),
            ),
        )


def build_stock_stream_url(token: str) -> str:
    if (
        not isinstance(token, str)
        or not 1 <= len(token.encode()) <= 4096
        or not token.isascii()
        or any(ord(character) < 32 or ord(character) == 127 for character in token)
    ):
        raise AllTickStreamError("ALLTICK_STREAM_TOKEN_INVALID")
    return f"{_BASE_URL}?token={quote(token, safe='')}"


def redact_stock_stream_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        raise AllTickStreamError("ALLTICK_STREAM_URL_INVALID") from None
    if (
        parsed.scheme != "wss"
        or parsed.hostname != "quote.alltick.co"
        or port not in {None, 443}
        or parsed.path != "/quote-stock-b-ws-api"
        or not parsed.query.startswith("token=")
        or "&" in parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise AllTickStreamError("ALLTICK_STREAM_URL_INVALID")
    return f"{_BASE_URL}?token=REDACTED"


def build_subscriptions(
    symbols: tuple[str, ...], *, sequence: int
) -> tuple[dict[str, Any], ...]:
    checked = _symbols(symbols)
    _sequence(sequence)
    products = [{"code": symbol} for symbol in checked]
    return tuple(
        {
            "cmd_id": command,
            "seq_id": sequence + offset,
            "trace": str(uuid4()),
            "data": {"symbol_list": products},
        }
        for offset, command in enumerate((22004, 22002))
    )


def build_heartbeat(*, sequence: int) -> dict[str, Any]:
    _sequence(sequence)
    return {
        "cmd_id": 22000,
        "seq_id": sequence,
        "trace": str(uuid4()),
        "data": {},
    }


def _symbols(values: tuple[str, ...]) -> tuple[str, ...]:
    if (
        not isinstance(values, tuple)
        or not 1 <= len(values) <= 5
        or len(set(values)) != len(values)
        or any(_SYMBOL.fullmatch(value) is None for value in values)
    ):
        raise AllTickStreamError("ALLTICK_STREAM_SYMBOLS_INVALID")
    return values


def _sequence(value: int) -> None:
    if type(value) is not int or not 0 <= value <= 4_294_967_293:
        raise AllTickStreamError("ALLTICK_STREAM_REQUEST_INVALID")


def _provider_time(
    value: object,
) -> tuple[datetime, Literal["seconds", "milliseconds"]]:
    if type(value) is not int or value <= 0:
        raise AllTickStreamError("ALLTICK_STREAM_TIME_INVALID")
    if value <= 4_102_444_800:
        return datetime.fromtimestamp(value, UTC), "seconds"
    if value <= 4_102_444_800_000:
        return datetime.fromtimestamp(value / 1000, UTC), "milliseconds"
    raise AllTickStreamError("ALLTICK_STREAM_TIME_INVALID")


def _state_number(current: dict[str, object], key: str) -> Decimal | None:
    value = current.get(key)
    if value is None:
        return None
    if not isinstance(value, Decimal):
        raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
    return value


def _number(value: object, *, positive: bool) -> Decimal:
    if not isinstance(value, str) or not value:
        raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID") from None
    if not result.is_finite() or (result <= 0 if positive else result < 0):
        raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
    return result


def _trade(data: dict[str, object], current: dict[str, object]) -> None:
    if set(data) != {
        "code",
        "seq",
        "tick_time",
        "price",
        "volume",
        "turnover",
        "trade_direction",
    } or data.get("trade_direction") not in {0, 1, 2}:
        raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
    current["last"] = _number(data["price"], positive=True)
    current["last_size"] = _number(data["volume"], positive=False)
    _number(data["turnover"], positive=False)


def _quote(data: dict[str, object], current: dict[str, object]) -> None:
    if set(data) != {"code", "seq", "tick_time", "bids", "asks"}:
        raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
    for side in ("bids", "asks"):
        levels = data[side]
        if not isinstance(levels, list) or len(levels) > 10:
            raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
        if levels:
            level = levels[0]
            if not isinstance(level, dict) or set(level) != {"price", "volume"}:
                raise AllTickStreamError("ALLTICK_STREAM_QUOTE_INVALID")
            current["bid" if side == "bids" else "ask"] = _number(
                level["price"], positive=True
            )
            current["bid_size" if side == "bids" else "ask_size"] = _number(
                level["volume"], positive=False
            )

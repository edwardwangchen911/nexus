"""Strict, read-only Schwab Level One stream protocol primitives."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit

from nexus.providers.market.stream_contracts import MarketStreamObservation

_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
_STREAM_URL = "wss://streamer-api.schwab.com/ws"
_FIELDS = "0,1,2,3,4,5,6,7,8,9,13,16,25,34,35,37,38,39,40,41"
_METADATA_FIELDS = {"key", "delayed", "assetMainType", "assetSubType", "cusip"}


class SchwabStreamError(ValueError):
    """Stable, value-free Schwab stream error."""


@dataclass(frozen=True)
class SchwabStreamCredentials:
    access_token: str = field(repr=False)
    customer_id: str
    correlation_id: str
    channel: str
    function_id: str

    def __post_init__(self) -> None:
        if any(not _printable(value, 4096) for value in vars(self).values()):
            raise SchwabStreamError("SCHWAB_STREAM_CREDENTIALS_INVALID")


class SchwabLevelOneState:
    """Merge Schwab's documented change frames into explicit current observations."""

    def __init__(self, symbols: tuple[str, ...]) -> None:
        self._symbols = _symbols(symbols)
        self._state: dict[str, dict[str, object]] = {}

    def apply(
        self, frame: dict[str, Any], *, received_at: datetime
    ) -> tuple[MarketStreamObservation, ...]:
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise SchwabStreamError("SCHWAB_STREAM_FRAME_INVALID")
        if not isinstance(frame, dict) or set(frame) != {"data"} or not isinstance(
            frame["data"], list
        ):
            raise SchwabStreamError("SCHWAB_STREAM_FRAME_INVALID")
        observations: list[MarketStreamObservation] = []
        for envelope in frame["data"]:
            if (
                not isinstance(envelope, dict)
                or envelope.get("service") != "LEVELONE_EQUITIES"
                or envelope.get("command") not in {"SUBS", "ADD"}
                or type(envelope.get("timestamp")) is not int
                or envelope["timestamp"] < 0
                or not isinstance(envelope.get("content"), list)
            ):
                raise SchwabStreamError("SCHWAB_STREAM_FRAME_INVALID")
            for update in envelope["content"]:
                observations.append(
                    self._apply_update(update, envelope["timestamp"], received_at)
                )
        return tuple(observations)

    def _apply_update(
        self, update: object, sequence: int, received_at: datetime
    ) -> MarketStreamObservation:
        if not isinstance(update, dict) or not isinstance(update.get("key"), str):
            raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
        symbol = update["key"]
        if symbol not in self._symbols or any(
            not isinstance(key, str)
            or (key not in _METADATA_FIELDS and (not key.isascii() or not key.isdecimal()))
            for key in update
        ):
            raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
        if "delayed" in update and type(update["delayed"]) is not bool:
            raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
        if "assetMainType" in update and update["assetMainType"] != "EQUITY":
            raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
        current = self._state.setdefault(symbol, {})
        current.update(update)
        return MarketStreamObservation(
            source="schwab",
            symbol=symbol,
            received_at=received_at,
            sequence=sequence,
            quote_time=_epoch_millis(current.get("34")),
            trade_time=_epoch_millis(current.get("35")),
            provider_time_unit="milliseconds",
            delayed=_optional_bool(current.get("delayed")),
            bid=_decimal(current.get("1")),
            ask=_decimal(current.get("2")),
            last=_decimal(current.get("3")),
            bid_size=_decimal(current.get("4")),
            ask_size=_decimal(current.get("5")),
            last_size=_decimal(current.get("9")),
            total_volume=_decimal(current.get("8")),
            ask_exchange=_optional_text(current.get("6")),
            bid_exchange=_optional_text(current.get("7")),
            last_exchange=_optional_text(current.get("16")),
        )


def validate_streamer_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        raise SchwabStreamError("SCHWAB_STREAM_URL_INVALID") from None
    if (
        value != _STREAM_URL
        or parsed.scheme != "wss"
        or parsed.hostname != "streamer-api.schwab.com"
        or port not in {None, 443}
        or parsed.path != "/ws"
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise SchwabStreamError("SCHWAB_STREAM_URL_INVALID")
    return value


def build_login(credentials: SchwabStreamCredentials, *, request_id: int) -> dict[str, Any]:
    return {
        "requests": [
            {
                **_request_header(credentials, "ADMIN", "LOGIN", request_id),
                "parameters": {
                    "Authorization": credentials.access_token,
                    "SchwabClientChannel": credentials.channel,
                    "SchwabClientFunctionId": credentials.function_id,
                },
            }
        ]
    }


def build_level_one_subscription(
    credentials: SchwabStreamCredentials,
    symbols: tuple[str, ...],
    *,
    request_id: int,
) -> dict[str, Any]:
    checked = _symbols(symbols)
    return {
        "requests": [
            {
                **_request_header(
                    credentials, "LEVELONE_EQUITIES", "SUBS", request_id
                ),
                "parameters": {"keys": ",".join(checked), "fields": _FIELDS},
            }
        ]
    }


def _request_header(
    credentials: SchwabStreamCredentials, service: str, command: str, request_id: int
) -> dict[str, str]:
    if type(request_id) is not int or not 0 <= request_id <= 2_147_483_647:
        raise SchwabStreamError("SCHWAB_STREAM_REQUEST_INVALID")
    return {
        "service": service,
        "command": command,
        "requestid": str(request_id),
        "SchwabClientCustomerId": credentials.customer_id,
        "SchwabClientCorrelId": credentials.correlation_id,
    }


def _symbols(values: tuple[str, ...]) -> tuple[str, ...]:
    if (
        not isinstance(values, tuple)
        or not 1 <= len(values) <= 500
        or len(set(values)) != len(values)
        or any(_SYMBOL.fullmatch(value) is None for value in values)
    ):
        raise SchwabStreamError("SCHWAB_STREAM_SYMBOLS_INVALID")
    return values


def _printable(value: object, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value.encode()) <= maximum
        and value.isascii()
        and all(33 <= ord(character) <= 126 for character in value)
    )


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, str | int | float | Decimal):
        raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID") from None
    if not result.is_finite() or result < 0:
        raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
    return result


def _epoch_millis(value: object) -> datetime | None:
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= 4_102_444_800_000:
        raise SchwabStreamError("SCHWAB_STREAM_TIME_INVALID")
    return datetime.fromtimestamp(value / 1000, UTC)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.isascii() or len(value) > 32:
        raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
    return value or None


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if type(value) is not bool:
        raise SchwabStreamError("SCHWAB_STREAM_QUOTE_INVALID")
    return value

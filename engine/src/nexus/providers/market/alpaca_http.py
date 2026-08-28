"""Exact Alpaca historical-bar HTTP transport and symbol authority."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Literal, Protocol

from pydantic import SecretStr
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.market.alpaca import AlpacaRawResponse, MarketProviderPayloadError

_US_EXCHANGES = frozenset({"AMEX", "ARCA", "NASDAQ", "NYSE", "NYSEARCA"})
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
_CURSOR = re.compile(r"^[A-Za-z0-9._~=-]{1,512}$")


class FixedOriginBytes(Protocol):
    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes: ...


class InstrumentSymbolResolver(Protocol):
    async def resolve(self, instrument_id: InstrumentId, as_of: date) -> str: ...


class SqlInstrumentSymbolResolver:
    """Resolve one point-in-time ticker through the instrument master."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def resolve(self, instrument_id: InstrumentId, as_of: date) -> str:
        rows = (
            await self._session.execute(
                select(
                    InstrumentAlias.symbol,
                    Instrument.exchange,
                    Instrument.currency,
                )
                .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
                .where(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= as_of,
                    or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= as_of),
                )
            )
        ).all()
        if len(rows) != 1:
            raise MarketProviderPayloadError("INSTRUMENT_SYMBOL_AUTHORITY_INVALID")
        symbol, exchange, currency = rows[0]
        if exchange.upper() not in _US_EXCHANGES or currency.upper() != "USD":
            raise MarketProviderPayloadError("US_INSTRUMENT_REQUIRED")
        return _validated_us_symbol(symbol)


class AlpacaHttpTransport:
    """Build only Alpaca's frozen single-symbol bars operation."""

    def __init__(
        self,
        client: FixedOriginBytes,
        *,
        key_id: SecretStr,
        secret: SecretStr,
        feed: Literal["iex", "sip"],
        symbol_resolver: InstrumentSymbolResolver,
    ) -> None:
        if not key_id.get_secret_value().strip() or not secret.get_secret_value().strip():
            raise ValueError("ALPACA_CREDENTIALS_REQUIRED")
        self._client = client
        self._key_id = key_id
        self._secret = secret
        self._feed = feed
        self._symbol_resolver = symbol_resolver

    async def fetch_bars(
        self,
        request: BarsRequest,
        cursor: str | None,
    ) -> AlpacaRawResponse:
        symbol = _validated_us_symbol(
            await self._symbol_resolver.resolve(request.instrument_id, request.end.date())
        )
        query = _bars_query(request, cursor, self._feed)
        raw = await self._client.get_bytes(
            ("v2", "stocks", symbol, "bars"),
            query,
            (
                ("APCA-API-KEY-ID", self._key_id.get_secret_value()),
                ("APCA-API-SECRET-KEY", self._secret.get_secret_value()),
            ),
        )
        try:
            value = json.loads(raw, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise MarketProviderPayloadError("ALPACA_OBJECT_REQUIRED") from error
        if not isinstance(value, Mapping):
            raise MarketProviderPayloadError("ALPACA_OBJECT_REQUIRED")
        request_id = _optional_request_id(value.get("request_id"))
        return AlpacaRawResponse(
            payload=value,
            raw_payload=raw,
            request_id=request_id,
        )


def _bars_query(
    request: BarsRequest,
    cursor: str | None,
    feed: Literal["iex", "sip"],
) -> tuple[tuple[str, str], ...]:
    if request.end <= request.start:
        raise MarketProviderPayloadError("BAR_RANGE_INVALID")
    query = [
        ("adjustment", request.adjustment),
        ("end", _format_utc(request.end)),
        ("feed", feed),
        ("limit", "10000"),
        ("sort", "asc"),
        ("start", _format_utc(request.start)),
        ("timeframe", request.interval),
    ]
    if cursor is not None:
        if _CURSOR.fullmatch(cursor) is None:
            raise MarketProviderPayloadError("ALPACA_CURSOR_INVALID")
        query.append(("page_token", cursor))
    return tuple(query)


def _format_utc(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _validated_us_symbol(symbol: object) -> str:
    if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
        raise MarketProviderPayloadError("ALPACA_SYMBOL_INVALID")
    return symbol


def _optional_request_id(value: object) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not 1 <= len(value.encode("utf-8")) <= 128
        or any(ord(character) < 32 for character in value)
    ):
        raise MarketProviderPayloadError("ALPACA_REQUEST_ID_INVALID")
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise MarketProviderPayloadError("ALPACA_DUPLICATE_KEY")
        result[key] = value
    return result

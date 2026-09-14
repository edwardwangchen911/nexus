"""One-call Schwab price-history adapter for bounded paper execution windows."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from nexus.domain.time import require_utc
from nexus.providers.base import BarsRequest, MarketDataProvenance, PriceBar, ProviderPage
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_http import InstrumentSymbolResolver
from nexus.providers.market.schwab_oauth import (
    load_schwab_token,
    require_live_schwab_access_token,
)

_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
_NEW_YORK = ZoneInfo("America/New_York")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SchwabPriceHistoryProvider:
    """Fetch and normalize one single-symbol, five-minute Schwab response."""

    def __init__(
        self,
        repo_root: Path,
        client: httpx.AsyncClient,
        *,
        symbol_resolver: InstrumentSymbolResolver,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._repo_root = repo_root
        self._symbol_resolver = symbol_resolver
        self._now = now or (lambda: datetime.now(UTC))
        self._http = FixedOriginHttpClient(
            provider_name="schwab_price_history",
            origin=AllowedOrigin(scheme="https", host="api.schwabapi.com"),
            policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=8 * 1024 * 1024),
            client=client,
        )

    async def fetch_bars(
        self,
        request: BarsRequest,
        cursor: str | None = None,
    ) -> ProviderPage[PriceBar]:
        if cursor is not None:
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_CURSOR_UNSUPPORTED")
        if request.interval != "1Min" or request.adjustment != "raw":
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_AUTHORITY_MISMATCH")
        if (
            request.end - request.start != timedelta(minutes=5)
            or request.start.second != 0
            or request.start.microsecond != 0
        ):
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_RANGE_INVALID")

        checked_at = require_utc(self._now())
        access_token = require_live_schwab_access_token(
            load_schwab_token(self._repo_root), now=checked_at
        )
        symbol = await self._symbol_resolver.resolve(
            request.instrument_id, request.end.astimezone(_NEW_YORK).date()
        )
        if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_SYMBOL_INVALID")
        raw = await self._http.get_bytes(
            ("marketdata", "v1", "pricehistory"),
            (
                ("symbol", symbol),
                ("frequencyType", "minute"),
                ("frequency", "1"),
                ("startDate", _epoch_milliseconds(request.start)),
                ("endDate", _epoch_milliseconds(request.end)),
                ("needExtendedHoursData", "true"),
                ("needPreviousClose", "true"),
            ),
            (
                ("Accept", "application/json"),
                ("Authorization", f"Bearer {access_token}"),
            ),
        )
        received_at = require_utc(self._now())
        payload = _load_exact_json(raw)
        if not isinstance(payload, Mapping):
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_OBJECT_REQUIRED")
        if payload.get("symbol") != symbol or payload.get("empty") is not False:
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_RESPONSE_INVALID")
        raw_candles = payload.get("candles")
        if not isinstance(raw_candles, list):
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_CANDLES_INVALID")

        items: list[PriceBar] = []
        for raw_candle in raw_candles:
            if not isinstance(raw_candle, Mapping):
                raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_CANDLE_INVALID")
            observed_at = _epoch_datetime(raw_candle.get("datetime"))
            if not request.start <= observed_at < request.end:
                continue
            open_price = _decimal(raw_candle.get("open"), "open")
            high_price = _decimal(raw_candle.get("high"), "high")
            low_price = _decimal(raw_candle.get("low"), "low")
            close_price = _decimal(raw_candle.get("close"), "close")
            volume = _decimal(raw_candle.get("volume"), "volume")
            if (
                min(open_price, high_price, low_price, close_price) <= 0
                or high_price < max(open_price, close_price)
                or low_price > min(open_price, close_price)
                or volume < 0
            ):
                raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_OHLCV_INVALID")
            items.append(
                PriceBar(
                    instrument_id=request.instrument_id,
                    session=observed_at.astimezone(_NEW_YORK).date(),
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                    observed_at=observed_at,
                    available_at=received_at,
                    received_at=received_at,
                )
            )

        expected_observations = tuple(
            request.start + timedelta(minutes=index) for index in range(5)
        )
        if tuple(item.observed_at for item in items) != expected_observations:
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_WINDOW_INVALID")
        if len({item.session for item in items}) != 1 or any(
            item.observed_at is None or item.observed_at > received_at for item in items
        ):
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_WINDOW_INVALID")

        return ProviderPage(
            items=tuple(items),
            next_cursor=None,
            raw_payload=raw,
            source_request_id=None,
            as_of=received_at,
            provenance=MarketDataProvenance(
                source="schwab",
                feed="price_history",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="unknown",
            ),
        )


def _epoch_milliseconds(value: datetime) -> str:
    delta = require_utc(value) - _EPOCH
    return str(delta // timedelta(milliseconds=1))


def _epoch_datetime(value: object) -> datetime:
    if type(value) is not Decimal:
        raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_DATETIME_INVALID")
    milliseconds = _decimal(value, "datetime")
    if milliseconds != milliseconds.to_integral_value():
        raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_DATETIME_INVALID")
    try:
        return _EPOCH + timedelta(milliseconds=int(milliseconds))
    except (OverflowError, ValueError):
        raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_DATETIME_INVALID") from None


def _decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool | float) or not isinstance(value, Decimal | int | str):
        raise MarketProviderPayloadError(f"SCHWAB_PRICE_HISTORY_{field.upper()}_INVALID")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except InvalidOperation:
        raise MarketProviderPayloadError(
            f"SCHWAB_PRICE_HISTORY_{field.upper()}_INVALID"
        ) from None
    if not result.is_finite():
        raise MarketProviderPayloadError(f"SCHWAB_PRICE_HISTORY_{field.upper()}_INVALID")
    return result


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_DUPLICATE_KEY")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    del value
    raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_JSON_INVALID")


def _load_exact_json(raw: bytes) -> object:
    try:
        return json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_float=Decimal,
            parse_int=Decimal,
            parse_constant=_reject_json_constant,
        )
    except MarketProviderPayloadError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_JSON_INVALID") from None

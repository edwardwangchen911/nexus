"""Dependency-injected Alpaca raw-response normalization."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from nexus.providers.base import BarsRequest, MarketDataProvenance, PriceBar, ProviderPage

NEW_YORK = ZoneInfo("America/New_York")


class AlpacaTransport(Protocol):
    """Injected HTTP boundary; production composition owns credentials."""

    async def fetch_bars(self, request: BarsRequest, cursor: str | None) -> AlpacaRawResponse: ...


@dataclass(frozen=True)
class AlpacaRawResponse:
    payload: Mapping[str, object]
    raw_payload: bytes
    request_id: str | None


class MarketProviderError(RuntimeError):
    """Base safe market-provider error."""


class MarketProviderPayloadError(MarketProviderError):
    """The external payload could not be normalized safely."""


class AlpacaMarketDataProvider:
    """Normalize delayed Alpaca bars without importing an SDK or reading secrets."""

    def __init__(
        self,
        transport: AlpacaTransport,
        *,
        feed: Literal["iex", "sip"],
        delivery_mode: Literal["real_time", "delayed"],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._transport = transport
        self.feed = feed
        self.delivery_mode = delivery_mode
        self._now = now or (lambda: datetime.now(UTC))

    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None = None
    ) -> ProviderPage[PriceBar]:
        response = await self._transport.fetch_bars(request, cursor)
        return self._normalize(request, response)

    def _normalize(
        self, request: BarsRequest, response: AlpacaRawResponse
    ) -> ProviderPage[PriceBar]:
        payload = response.payload
        if (
            not isinstance(response.raw_payload, bytes)
            or not response.raw_payload
            or len(response.raw_payload) > 8 * 1024 * 1024
        ):
            raise MarketProviderPayloadError("raw payload is invalid")
        raw_bars = payload.get("bars")
        if not isinstance(raw_bars, list) or len(raw_bars) > 10_000:
            raise MarketProviderPayloadError("bars must be a list")
        received_at = self._now()
        items: list[PriceBar] = []
        for raw_bar in raw_bars:
            if not isinstance(raw_bar, Mapping):
                raise MarketProviderPayloadError("bar must be an object")
            observed_at = _parse_datetime(raw_bar.get("t"), "t")
            raw_available_at = raw_bar.get("available_at")
            available_at = (
                received_at
                if raw_available_at is None
                else _parse_datetime(raw_available_at, "available_at")
            )
            flags = raw_bar.get("quality_flags", [])
            if not isinstance(flags, list) or not all(isinstance(flag, str) for flag in flags):
                raise MarketProviderPayloadError("quality_flags must be strings")
            items.append(
                PriceBar(
                    instrument_id=request.instrument_id,
                    session=observed_at.astimezone(NEW_YORK).date(),
                    open=_decimal_string(raw_bar.get("o"), "o"),
                    high=_decimal_string(raw_bar.get("h"), "h"),
                    low=_decimal_string(raw_bar.get("l"), "l"),
                    close=_decimal_string(raw_bar.get("c"), "c"),
                    volume=_decimal_string(raw_bar.get("v"), "v"),
                    observed_at=observed_at,
                    available_at=available_at,
                    received_at=received_at,
                    revision=str(raw_bar.get("revision", "1")),
                    quality_flags=tuple(flags),
                )
            )
        next_cursor = payload.get("next_page_token")
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise MarketProviderPayloadError("next_page_token must be a string")
        return ProviderPage(
            items=tuple(items),
            next_cursor=next_cursor,
            raw_payload=response.raw_payload,
            source_request_id=response.request_id,
            as_of=received_at,
            provenance=MarketDataProvenance(
                source="alpaca",
                feed=self.feed,
                adjustment=request.adjustment,
                interval=request.interval,
                mode="read_only" if self.delivery_mode == "real_time" else "delayed",
                delivery_mode=self.delivery_mode,
            ),
        )


def _decimal_string(value: object, field: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise MarketProviderPayloadError(f"{field} must be a decimal string")
    if not isinstance(value, Decimal | int | str):
        raise MarketProviderPayloadError(f"{field} must be a decimal string")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except Exception as error:
        raise MarketProviderPayloadError(f"{field} must be a decimal string") from error
    if not result.is_finite():
        raise MarketProviderPayloadError(f"{field} must be a finite decimal string")
    return result


def _parse_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise MarketProviderPayloadError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise MarketProviderPayloadError(f"{field} must be an ISO timestamp") from error
    if parsed.tzinfo is None:
        raise MarketProviderPayloadError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)

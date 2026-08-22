"""Dependency-injected Alpaca normalization with bounded adapter-owned retry."""

import asyncio
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic
from typing import Protocol
from zoneinfo import ZoneInfo

import httpx

from nexus.providers.base import BarsRequest, PriceBar, ProviderPage

NEW_YORK = ZoneInfo("America/New_York")


class AlpacaTransport(Protocol):
    """Injected HTTP boundary; production composition owns credentials."""

    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None
    ) -> Mapping[str, object]: ...


class MarketProviderError(RuntimeError):
    """Base safe market-provider error."""


class MarketProviderDeadlineExceeded(MarketProviderError):
    """The adapter's absolute deadline was exhausted."""


class MarketProviderPayloadError(MarketProviderError):
    """The external payload could not be normalized safely."""


class AlpacaMarketDataProvider:
    """Normalize delayed Alpaca bars without importing an SDK or reading secrets."""

    def __init__(
        self,
        transport: AlpacaTransport,
        *,
        max_attempts: int = 3,
        deadline_seconds: float = 30,
        backoff_seconds: float = 0.25,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if max_attempts < 1 or deadline_seconds <= 0 or backoff_seconds < 0:
            raise ValueError("retry and deadline values must be bounded and positive")
        self._transport = transport
        self._max_attempts = max_attempts
        self._deadline_seconds = deadline_seconds
        self._backoff_seconds = backoff_seconds
        self._now = now or (lambda: datetime.now(UTC))

    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None = None
    ) -> ProviderPage[PriceBar]:
        deadline = monotonic() + self._deadline_seconds
        last_error: httpx.TransportError | None = None
        for attempt in range(self._max_attempts):
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise MarketProviderDeadlineExceeded("market provider deadline exceeded")
            try:
                async with asyncio.timeout(remaining):
                    payload = await self._transport.fetch_bars(request, cursor)
                return self._normalize(request, payload)
            except TimeoutError as error:
                raise MarketProviderDeadlineExceeded(
                    "market provider deadline exceeded"
                ) from error
            except httpx.TransportError as error:
                last_error = error
                if attempt + 1 == self._max_attempts:
                    break
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise MarketProviderDeadlineExceeded(
                        "market provider deadline exceeded"
                    ) from error
                await asyncio.sleep(min(self._backoff_seconds * (2**attempt), remaining))
        raise MarketProviderError("market provider retry budget exhausted") from last_error

    def _normalize(
        self, request: BarsRequest, payload: Mapping[str, object]
    ) -> ProviderPage[PriceBar]:
        raw_bars = payload.get("bars")
        if not isinstance(raw_bars, list):
            raise MarketProviderPayloadError("bars must be a list")
        received_at = self._now()
        items: list[PriceBar] = []
        for raw_bar in raw_bars:
            if not isinstance(raw_bar, Mapping):
                raise MarketProviderPayloadError("bar must be an object")
            observed_at = _parse_datetime(raw_bar.get("t"), "t")
            available_at = _parse_datetime(raw_bar.get("available_at"), "available_at")
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
        request_id = payload.get("request_id")
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise MarketProviderPayloadError("next_page_token must be a string")
        if request_id is not None and not isinstance(request_id, str):
            raise MarketProviderPayloadError("request_id must be a string")
        raw_payload = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
        return ProviderPage(
            items=tuple(items),
            next_cursor=next_cursor,
            raw_payload=raw_payload,
            source_request_id=request_id,
            as_of=received_at,
        )


def _decimal_string(value: object, field: str) -> Decimal:
    if not isinstance(value, str):
        raise MarketProviderPayloadError(f"{field} must be a decimal string")
    try:
        result = Decimal(value)
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

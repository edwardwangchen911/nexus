"""Fosun L1 adapters with PIT symbol resolution and provider-owned normalization."""

from __future__ import annotations

import hashlib
import re
from datetime import timedelta
from typing import Literal, Protocol
from uuid import UUID
from zoneinfo import ZoneInfo

from nexus.providers.base import (
    BarsRequest,
    DeliveryMode,
    MarketDataProvenance,
    PriceBar,
    ProviderPage,
)
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_http import InstrumentSymbolResolver
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    FosunMarketGatewayError,
    RawGatewayResponse,
    parse_bar_page,
    parse_quote_page,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    MarketSnapshot,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotTrade,
)

_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
_NEW_YORK = ZoneInfo("America/New_York")
_RECEIPT_FLAGS = (
    "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
    "INTRADAY_BID_ASK_UNAVAILABLE",
)


class FosunMarketGateway(Protocol):
    async def read_quotes_raw(self, codes: tuple[str, ...]) -> RawGatewayResponse: ...

    async def read_bars_raw(self, **kwargs: object) -> RawGatewayResponse: ...


class FosunMarketDataProvider:
    """Normalize daily or bounded minute bars without exposing a generic gateway call."""

    def __init__(
        self,
        gateway: FosunMarketGateway | FosunMarketGatewayClient,
        *,
        symbol_resolver: InstrumentSymbolResolver,
        delivery_mode: DeliveryMode,
    ) -> None:
        if delivery_mode not in {"unknown", "real_time", "delayed"}:
            raise ValueError("FOSUN_MARKET_DELIVERY_MODE_INVALID")
        self._gateway = gateway
        self._symbol_resolver = symbol_resolver
        self._delivery_mode = delivery_mode

    async def fetch_raw_bars(
        self,
        request: BarsRequest,
        cursor: str | None = None,
    ) -> RawGatewayResponse:
        limit = _validate_bar_request(request, cursor)
        symbol = await self._symbol_resolver.resolve(
            request.instrument_id,
            request.end.date(),
        )
        return await self._gateway.read_bars_raw(
            code=_provider_code(symbol),
            ktype="day" if request.interval == "1Day" else "minute",
            start_time=request.start,
            end_time=request.end,
            limit=limit,
        )

    async def normalize_bars(
        self,
        request: BarsRequest,
        response: RawGatewayResponse,
        cursor: str | None = None,
    ) -> ProviderPage[PriceBar]:
        limit = _validate_bar_request(request, cursor)
        symbol = await self._symbol_resolver.resolve(
            request.instrument_id,
            request.end.date(),
        )
        code = _provider_code(symbol)
        ktype: Literal["day", "minute"] = (
            "day" if request.interval == "1Day" else "minute"
        )
        try:
            page = parse_bar_page(
                response,
                expected_code=code,
                expected_ktype=ktype,
                start=request.start,
                end=request.end,
                limit=limit,
            )
        except FosunMarketGatewayError:
            raise MarketProviderPayloadError("FOSUN_BAR_CONTRACT_INVALID") from None
        revision = hashlib.sha256(response.raw_payload).hexdigest()
        bars = tuple(
            PriceBar(
                instrument_id=request.instrument_id,
                session=item.timestamp.astimezone(_NEW_YORK).date(),
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
                observed_at=item.timestamp,
                available_at=response.received_at,
                received_at=response.received_at,
                revision=revision,
                quality_flags=(),
            )
            for item in page.items
            if request.interval != "1Min"
            or item.timestamp + timedelta(minutes=1) <= response.received_at
        )
        return ProviderPage(
            items=bars,
            next_cursor=None,
            raw_payload=response.raw_payload,
            source_request_id=page.source_request_id,
            as_of=response.received_at,
            provenance=MarketDataProvenance(
                source="fosun",
                feed="l1",
                adjustment="raw",
                interval=request.interval,
                mode="read_only",
                delivery_mode=self._delivery_mode,
            ),
        )

    async def fetch_bars(
        self,
        request: BarsRequest,
        cursor: str | None = None,
    ) -> ProviderPage[PriceBar]:
        response = await self.fetch_raw_bars(request, cursor)
        return await self.normalize_bars(request, response, cursor)


class FosunSnapshotProvider:
    """Fetch quote bytes and normalize receipt-only last prices without fake spreads."""

    def __init__(
        self,
        gateway: FosunMarketGateway | FosunMarketGatewayClient,
        *,
        symbol_resolver: InstrumentSymbolResolver,
        delivery_mode: DeliveryMode,
    ) -> None:
        if delivery_mode not in {"unknown", "real_time", "delayed"}:
            raise ValueError("FOSUN_MARKET_DELIVERY_MODE_INVALID")
        self._gateway = gateway
        self._symbol_resolver = symbol_resolver
        self._delivery_mode = delivery_mode

    async def fetch_raw_snapshots(
        self,
        request: IntradaySnapshotRequest,
    ) -> RawSnapshotEnvelope:
        self._validate_request(request)
        codes: list[str] = []
        for item in request.instruments:
            symbol = await self._symbol_resolver.resolve(
                item.instrument_id,
                request.symbol_as_of,
            )
            if symbol != item.symbol:
                raise MarketProviderPayloadError("INTRADAY_SYMBOL_AUTHORITY_INVALID")
            codes.append(_provider_code(symbol))
        response = await self._gateway.read_quotes_raw(tuple(sorted(codes)))
        return RawSnapshotEnvelope(
            raw_payload=response.raw_payload,
            received_at=response.received_at,
            provenance=MarketDataProvenance(
                source="fosun",
                feed="l1",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode=self._delivery_mode,
            ),
        )

    def normalize_raw_snapshots(
        self,
        envelope: RawSnapshotEnvelope,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch:
        self._validate_request(request)
        if (
            envelope.provenance.source != "fosun"
            or envelope.provenance.feed != "l1"
            or envelope.provenance.adjustment != "raw"
            or envelope.provenance.interval != "1Min"
            or envelope.provenance.mode != "read_only"
            or envelope.provenance.delivery_mode != self._delivery_mode
        ):
            raise MarketProviderPayloadError("FOSUN_INTRADAY_AUTHORITY_MISMATCH")
        expected_codes = tuple(
            sorted(_provider_code(item.symbol) for item in request.instruments)
        )
        synthetic_response = RawGatewayResponse(
            operation="fosun_get_quotes",
            gateway_request_id=UUID_NIL,
            request_body_sha256="0" * 64,
            raw_payload=envelope.raw_payload,
            received_at=envelope.received_at,
        )
        try:
            page = parse_quote_page(
                synthetic_response,
                expected_codes=expected_codes,
            )
        except FosunMarketGatewayError:
            raise MarketProviderPayloadError("FOSUN_QUOTE_CONTRACT_INVALID") from None
        quotes = {item.instrument_code: item for item in page.items}
        snapshots: list[MarketSnapshot] = []
        for instrument in request.instruments:
            quote = quotes[_provider_code(instrument.symbol)]
            trade = (
                None
                if quote.price is None
                else SnapshotTrade(
                    price=quote.price,
                    observed_at=envelope.received_at,
                    provider_trade_id=None,
                    event_kind="snapshot",
                    quality_flags=_RECEIPT_FLAGS,
                )
            )
            snapshots.append(
                MarketSnapshot(
                    instrument_id=instrument.instrument_id,
                    symbol=instrument.symbol,
                    trade=trade,
                    quote=None,
                    minute_bar=None,
                )
            )
        return SnapshotBatch(
            snapshots=tuple(snapshots),
            raw_payload=envelope.raw_payload,
            received_at=envelope.received_at,
            provenance=envelope.provenance,
        )

    async def fetch_snapshots(
        self,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch:
        envelope = await self.fetch_raw_snapshots(request)
        return self.normalize_raw_snapshots(envelope, request)

    def _validate_request(self, request: IntradaySnapshotRequest) -> None:
        if request.feed != "l1" or request.delivery_mode != self._delivery_mode:
            raise MarketProviderPayloadError("FOSUN_INTRADAY_AUTHORITY_MISMATCH")


def _validate_bar_request(request: BarsRequest, cursor: str | None) -> int:
    if cursor is not None or request.adjustment != "raw" or request.end <= request.start:
        raise MarketProviderPayloadError("FOSUN_BAR_REQUEST_INVALID")
    if request.interval == "1Day":
        if request.end - request.start > timedelta(days=400):
            raise MarketProviderPayloadError("FOSUN_BAR_WINDOW_INVALID")
        return 500
    if request.end - request.start > timedelta(minutes=5):
        raise MarketProviderPayloadError("FOSUN_BAR_WINDOW_INVALID")
    return 2


def _provider_code(symbol: object) -> str:
    if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
        raise MarketProviderPayloadError("FOSUN_SYMBOL_INVALID")
    return f"us{symbol}"


UUID_NIL = UUID(int=0)

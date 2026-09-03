"""Fosun L1 adapters with PIT symbol resolution and provider-owned normalization."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars as xcals  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from nexus.audit.canonical import canonical_json
from nexus.domain.time import require_utc
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
    FosunBarType,
    FosunMarketGatewayClient,
    FosunMarketGatewayError,
    RawGatewayResponse,
    fosun_bars_request_body_sha256,
    parse_bar_page,
    parse_quote_page,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    MarketSnapshot,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotMinuteBar,
    SnapshotTrade,
)

_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
_NEW_YORK = ZoneInfo("America/New_York")
_RECEIPT_FLAGS = (
    "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
    "INTRADAY_BID_ASK_UNAVAILABLE",
)
_MAX_AGGREGATE_BYTES = 16 * 1024 * 1024
_HASH = re.compile(r"^[0-9a-f]{64}$")


class _FosunEvidenceItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["fosun_get_quotes", "fosun_get_bars"]
    instrument_codes: tuple[str, ...]
    gateway_request_id: UUID
    request_body_sha256: str
    response_body_sha256: str
    sent_at: datetime
    received_at: datetime
    body_base64: str

    @field_validator("request_body_sha256", "response_body_sha256")
    @classmethod
    def _canonical_hash(cls, value: str) -> str:
        if _HASH.fullmatch(value) is None:
            raise ValueError("snapshot evidence digest is invalid")
        return value

    @field_validator("sent_at", "received_at")
    @classmethod
    def _received_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class _FosunEvidenceEnvelope(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    schema_version: Literal["fosun_gateway_market_snapshot_v1"] = Field(alias="schema")
    responses: tuple[_FosunEvidenceItem, ...]


def encode_fosun_snapshot_evidence(
    responses: tuple[tuple[tuple[str, ...], RawGatewayResponse], ...],
) -> bytes:
    """Canonicalize bounded, exact gateway responses without parsing their bodies."""
    document = {
        "schema": "fosun_gateway_market_snapshot_v1",
        "responses": [
            {
                "operation": response.operation,
                "instrument_codes": list(codes),
                "gateway_request_id": str(response.gateway_request_id),
                "request_body_sha256": response.request_body_sha256,
                "response_body_sha256": hashlib.sha256(response.raw_payload).hexdigest(),
                "sent_at": _utc_text(response.sent_at),
                "received_at": _utc_text(response.received_at),
                "body_base64": base64.b64encode(response.raw_payload).decode("ascii"),
            }
            for codes, response in responses
        ],
    }
    payload = canonical_json(document).encode("utf-8")
    if not payload or len(payload) > _MAX_AGGREGATE_BYTES:
        raise MarketProviderPayloadError("FOSUN_SNAPSHOT_EVIDENCE_INVALID")
    decode_fosun_snapshot_evidence(payload)
    return payload


def decode_fosun_snapshot_evidence(
    payload: bytes,
    *,
    expected_codes: tuple[str, ...] | None = None,
    include_minute_bars: bool | None = None,
    bucket_start: datetime | None = None,
) -> tuple[tuple[tuple[str, ...], RawGatewayResponse], ...]:
    """Strictly decode the deterministic envelope and revalidate every digest."""
    try:
        if not payload or len(payload) > _MAX_AGGREGATE_BYTES:
            raise ValueError
        model = _FosunEvidenceEnvelope.model_validate(
            json.loads(payload, object_pairs_hook=_unique_object)
        )
        if not 1 <= len(model.responses) <= 11:
            raise ValueError
        decoded: list[tuple[tuple[str, ...], RawGatewayResponse]] = []
        for item in model.responses:
            if item.gateway_request_id.version != 7:
                raise ValueError
            body = base64.b64decode(item.body_base64, validate=True)
            if hashlib.sha256(body).hexdigest() != item.response_body_sha256:
                raise ValueError
            response = RawGatewayResponse(
                operation=item.operation,
                gateway_request_id=item.gateway_request_id,
                request_body_sha256=item.request_body_sha256,
                raw_payload=body,
                sent_at=item.sent_at,
                received_at=item.received_at,
            )
            decoded.append((item.instrument_codes, response))
        result = tuple(decoded)
        request_ids = tuple(response.gateway_request_id for _, response in result)
        if len(set(request_ids)) != len(request_ids):
            raise ValueError
        if expected_codes is not None:
            _validate_evidence_shape(
                result,
                expected_codes=expected_codes,
                include_minute_bars=bool(include_minute_bars),
                bucket_start=bucket_start,
            )
        return result
    except (
        ValueError,
        TypeError,
        ValidationError,
        json.JSONDecodeError,
        binascii.Error,
    ):
        raise MarketProviderPayloadError("FOSUN_SNAPSHOT_EVIDENCE_INVALID") from None


class FosunMarketGateway(Protocol):
    async def read_quotes_raw(self, codes: tuple[str, ...]) -> RawGatewayResponse: ...

    async def read_bars_raw(
        self,
        *,
        code: str,
        ktype: FosunBarType,
        start_time: datetime,
        end_time: datetime,
        limit: int,
    ) -> RawGatewayResponse: ...

    async def aclose(self) -> None: ...


class FosunSnapshotReadGuard(Protocol):
    """Operation-scoped checks around each actual gateway read."""

    async def before_read(
        self,
        operation: Literal["fosun_get_quotes", "fosun_get_bars"],
        instrument_codes: tuple[str, ...],
    ) -> bool: ...

    async def after_read(
        self,
        operation: Literal["fosun_get_quotes", "fosun_get_bars"],
        instrument_codes: tuple[str, ...],
        response: RawGatewayResponse,
    ) -> bool: ...


class FosunSnapshotReadAborted(RuntimeError):
    """Stable fail-closed outcome carrying only already-received evidence."""

    def __init__(
        self,
        result_code: str,
        envelope: RawSnapshotEnvelope | None,
    ) -> None:
        super().__init__(result_code)
        self.result_code = result_code
        self.envelope = envelope


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
        *,
        symbol: str | None = None,
        daily_session_count: int | None = None,
    ) -> RawGatewayResponse:
        limit = _requested_bar_limit(request, cursor, daily_session_count)
        if symbol is None:
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

    async def expected_bars_request_body_sha256(
        self,
        request: BarsRequest,
        cursor: str | None = None,
        *,
        symbol: str | None = None,
        daily_session_count: int | None = None,
    ) -> str:
        """Return the digest of the exact gateway body for a frozen bar request."""
        limit = _requested_bar_limit(request, cursor, daily_session_count)
        if symbol is None:
            symbol = await self._symbol_resolver.resolve(
                request.instrument_id,
                request.end.date(),
            )
        return fosun_bars_request_body_sha256(
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
        *,
        symbol: str | None = None,
    ) -> ProviderPage[PriceBar]:
        limit = _validate_bar_request(request, cursor)
        if symbol is None:
            symbol = await self._symbol_resolver.resolve(
                request.instrument_id,
                request.end.date(),
            )
        code = _provider_code(symbol)
        ktype: Literal["day", "minute"] = "day" if request.interval == "1Day" else "minute"
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
                session=(
                    item.timestamp.date()
                    if request.interval == "1Day"
                    else item.timestamp.astimezone(_NEW_YORK).date()
                ),
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
                observed_at=item.timestamp,
                available_at=response.received_at,
                received_at=response.received_at,
                revision=revision,
                quality_flags=()
                if page.sdk_requested_adjustment == "noward"
                else ("FOSUN_ADJUSTMENT_REQUEST_UNVERIFIED",),
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
        *,
        read_guard: FosunSnapshotReadGuard | None = None,
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
        ordered_codes = tuple(sorted(codes))
        bar_responses: list[tuple[tuple[str, ...], RawGatewayResponse]] = []
        quote_responses: list[tuple[tuple[str, ...], RawGatewayResponse]] = []
        try:
            if request.include_minute_bars:
                assert request.bucket_start is not None
                for code in ordered_codes:
                    codes_for_read = (code,)
                    if read_guard is not None and not await self._guard_before_read(
                        read_guard,
                        "fosun_get_bars",
                        codes_for_read,
                        bar_responses=bar_responses,
                    ):
                        raise self._aborted(
                            "INTRADAY_AUTHORITY_CHANGED_BEFORE_FETCH",
                            bar_responses=bar_responses,
                        )
                    try:
                        bar_response = await self._gateway.read_bars_raw(
                            code=code,
                            ktype="minute",
                            start_time=request.bucket_start - timedelta(minutes=2),
                            end_time=request.bucket_start,
                            limit=2,
                        )
                    except Exception:
                        raise self._aborted(
                            "INTRADAY_SNAPSHOT_TRANSPORT_FAILED",
                            bar_responses=bar_responses,
                        ) from None
                    bar_responses.append(((code,), bar_response))
                    if read_guard is not None and not await self._guard_after_read(
                        read_guard,
                        "fosun_get_bars",
                        codes_for_read,
                        bar_response,
                        bar_responses=bar_responses,
                    ):
                        raise self._aborted(
                            "INTRADAY_AUTHORITY_CHANGED_AFTER_FETCH",
                            bar_responses=bar_responses,
                        )
            if read_guard is not None and not await self._guard_before_read(
                read_guard,
                "fosun_get_quotes",
                ordered_codes,
                bar_responses=bar_responses,
            ):
                raise self._aborted(
                    "INTRADAY_AUTHORITY_CHANGED_BEFORE_FETCH",
                    bar_responses=bar_responses,
                )
            try:
                quote_response = await self._gateway.read_quotes_raw(ordered_codes)
            except Exception:
                raise self._aborted(
                    "INTRADAY_SNAPSHOT_TRANSPORT_FAILED",
                    bar_responses=bar_responses,
                ) from None
            frozen_quote_response = (ordered_codes, quote_response)
            quote_responses.append(frozen_quote_response)
            if read_guard is not None and not await self._guard_after_read(
                read_guard,
                "fosun_get_quotes",
                ordered_codes,
                quote_response,
                bar_responses=bar_responses,
                quote_response=frozen_quote_response,
            ):
                raise self._aborted(
                    "INTRADAY_AUTHORITY_CHANGED_AFTER_FETCH",
                    bar_responses=bar_responses,
                    quote_response=frozen_quote_response,
                )
            if any(
                response.received_at > quote_response.received_at
                for _, response in bar_responses
            ):
                raise self._aborted(
                    "INTRADAY_SNAPSHOT_EVIDENCE_INVALID",
                    bar_responses=bar_responses,
                    quote_response=frozen_quote_response,
                )
            responses = [frozen_quote_response, *bar_responses]
            raw_payload = encode_fosun_snapshot_evidence(tuple(responses))
            received_at = max(response.received_at for _, response in responses)
            return RawSnapshotEnvelope(
                raw_payload=raw_payload,
                received_at=received_at,
                provenance=MarketDataProvenance(
                    source="fosun",
                    feed="l1",
                    adjustment="raw",
                    interval="1Min",
                    mode="read_only",
                    delivery_mode=self._delivery_mode,
                ),
            )
        except asyncio.CancelledError:
            raise self._aborted(
                "INTRADAY_SNAPSHOT_READ_CANCELLED",
                bar_responses=bar_responses,
                quote_response=(quote_responses[0] if quote_responses else None),
            ) from None

    async def _guard_before_read(
        self,
        guard: FosunSnapshotReadGuard,
        operation: Literal["fosun_get_quotes", "fosun_get_bars"],
        instrument_codes: tuple[str, ...],
        *,
        bar_responses: list[tuple[tuple[str, ...], RawGatewayResponse]],
        quote_response: tuple[tuple[str, ...], RawGatewayResponse] | None = None,
    ) -> bool:
        try:
            return await guard.before_read(operation, instrument_codes)
        except FosunSnapshotReadAborted:
            raise
        except Exception:
            raise self._aborted(
                "INTRADAY_AUTHORITY_REVALIDATION_FAILED",
                bar_responses=bar_responses,
                quote_response=quote_response,
            ) from None

    async def _guard_after_read(
        self,
        guard: FosunSnapshotReadGuard,
        operation: Literal["fosun_get_quotes", "fosun_get_bars"],
        instrument_codes: tuple[str, ...],
        response: RawGatewayResponse,
        *,
        bar_responses: list[tuple[tuple[str, ...], RawGatewayResponse]],
        quote_response: tuple[tuple[str, ...], RawGatewayResponse] | None = None,
    ) -> bool:
        try:
            return await guard.after_read(operation, instrument_codes, response)
        except FosunSnapshotReadAborted:
            raise
        except Exception:
            raise self._aborted(
                "INTRADAY_AUTHORITY_REVALIDATION_FAILED",
                bar_responses=bar_responses,
                quote_response=quote_response,
            ) from None

    def _aborted(
        self,
        result_code: str,
        *,
        bar_responses: list[tuple[tuple[str, ...], RawGatewayResponse]],
        quote_response: tuple[tuple[str, ...], RawGatewayResponse] | None = None,
    ) -> FosunSnapshotReadAborted:
        responses = ([] if quote_response is None else [quote_response]) + bar_responses
        envelope = None
        if responses:
            raw_payload = encode_fosun_snapshot_evidence(tuple(responses))
            received_at = max(response.received_at for _, response in responses)
            envelope = RawSnapshotEnvelope(
                raw_payload=raw_payload,
                received_at=received_at,
                provenance=MarketDataProvenance(
                    source="fosun",
                    feed="l1",
                    adjustment="raw",
                    interval="1Min",
                    mode="read_only",
                    delivery_mode=self._delivery_mode,
                ),
            )
        return FosunSnapshotReadAborted(result_code, envelope)

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
        expected_codes = tuple(sorted(_provider_code(item.symbol) for item in request.instruments))
        evidence = decode_fosun_snapshot_evidence(
            envelope.raw_payload,
            expected_codes=expected_codes,
            include_minute_bars=request.include_minute_bars,
            bucket_start=request.bucket_start,
        )
        try:
            quote_response = evidence[0][1]
            page = parse_quote_page(
                quote_response,
                expected_codes=expected_codes,
            )
        except FosunMarketGatewayError:
            raise MarketProviderPayloadError("FOSUN_QUOTE_CONTRACT_INVALID") from None
        quotes = {item.instrument_code: item for item in page.items}
        bars = self._normalize_evidence_bars(evidence, request)
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
                    minute_bar=bars.get(_provider_code(instrument.symbol)),
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
        if request.bucket_start is None:
            raise MarketProviderPayloadError("FOSUN_INTRADAY_BUCKET_REQUIRED")

    def _normalize_evidence_bars(
        self,
        evidence: tuple[tuple[tuple[str, ...], RawGatewayResponse], ...],
        request: IntradaySnapshotRequest,
    ) -> dict[str, SnapshotMinuteBar]:
        if not request.include_minute_bars:
            return {}
        assert request.bucket_start is not None
        result: dict[str, SnapshotMinuteBar] = {}
        instruments = {_provider_code(item.symbol): item for item in request.instruments}
        for codes, response in evidence[1:]:
            code = codes[0]
            try:
                page = parse_bar_page(
                    response,
                    expected_code=code,
                    expected_ktype="minute",
                    start=request.bucket_start - timedelta(minutes=2),
                    end=request.bucket_start,
                    limit=2,
                )
            except FosunMarketGatewayError:
                raise MarketProviderPayloadError("FOSUN_BAR_CONTRACT_INVALID") from None
            complete = tuple(
                item
                for item in page.items
                if item.timestamp + timedelta(minutes=1) <= response.received_at
            )
            if not complete:
                continue
            item = complete[-1]
            instrument = instruments[code]
            result[code] = SnapshotMinuteBar(
                bar=PriceBar(
                    instrument_id=instrument.instrument_id,
                    session=item.timestamp.astimezone(_NEW_YORK).date(),
                    open=item.open,
                    high=item.high,
                    low=item.low,
                    close=item.close,
                    volume=item.volume,
                    observed_at=item.timestamp,
                    available_at=response.received_at,
                    received_at=response.received_at,
                    revision=hashlib.sha256(response.raw_payload).hexdigest(),
                    quality_flags=(),
                )
            )
        return result


def _requested_bar_limit(
    request: BarsRequest, cursor: str | None, daily_session_count: int | None
) -> int:
    maximum = _validate_bar_request(request, cursor)
    if request.interval != "1Day":
        if daily_session_count is not None:
            raise MarketProviderPayloadError("FOSUN_BAR_REQUEST_INVALID")
        return maximum
    count = daily_session_count
    if count is None:
        try:
            count = len(
                xcals.get_calendar("XNYS").sessions_in_range(
                    request.start.date(), request.end.date()
                )
            )
        except (ValueError, OverflowError) as error:
            raise MarketProviderPayloadError("FOSUN_BAR_WINDOW_INVALID") from error
    if type(count) is not int or not 1 <= count <= min(
        maximum, (request.end.date() - request.start.date()).days + 1
    ):
        raise MarketProviderPayloadError("FOSUN_BAR_WINDOW_INVALID")
    return count


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


def _validate_evidence_shape(
    evidence: tuple[tuple[tuple[str, ...], RawGatewayResponse], ...],
    *,
    expected_codes: tuple[str, ...],
    include_minute_bars: bool,
    bucket_start: datetime | None,
) -> None:
    if expected_codes != tuple(sorted(expected_codes)) or len(set(expected_codes)) != len(
        expected_codes
    ):
        raise ValueError
    quote_codes, quote = evidence[0]
    expected_count = 1 + (len(expected_codes) if include_minute_bars else 0)
    if (
        len(evidence) != expected_count
        or quote.operation != "fosun_get_quotes"
        or quote_codes != expected_codes
        or quote.request_body_sha256 != _request_digest({"codes": list(expected_codes)})
    ):
        raise ValueError
    if not include_minute_bars:
        return
    if bucket_start is None:
        raise ValueError
    for expected_code, (codes, response) in zip(expected_codes, evidence[1:], strict=True):
        if (
            codes != (expected_code,)
            or response.operation != "fosun_get_bars"
            or response.request_body_sha256
            != _request_digest(
                {
                    "code": expected_code,
                    "ktype": "minute",
                    "start_time": _utc_text(bucket_start - timedelta(minutes=2)),
                    "end_time": _utc_text(bucket_start),
                    "limit": 2,
                }
            )
        ):
            raise ValueError


def _request_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _utc_text(value: datetime) -> str:
    return require_utc(value).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

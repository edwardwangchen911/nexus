"""Fixed-origin Alpaca multi-symbol Snapshot transport and normalization."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal, cast
from zoneinfo import ZoneInfo

from pydantic import SecretStr, ValidationError

from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_http import (
    FixedOriginBytes,
    InstrumentSymbolResolver,
    load_exact_json,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    MarketSnapshot,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotMinuteBar,
    SnapshotQuote,
    SnapshotTrade,
)

_MAX_RAW_BYTES = 8 * 1024 * 1024
_SYMBOL_FIELDS = frozenset({"latestTrade", "latestQuote", "minuteBar", "dailyBar", "prevDailyBar"})
_TRADE_FIELDS = frozenset({"p", "t", "i", "x", "s", "c", "z"})
_QUOTE_FIELDS = frozenset({"bp", "ap", "t", "bx", "ax", "bs", "as", "c", "z"})
_BAR_FIELDS = frozenset({"o", "h", "l", "c", "v", "t", "n", "vw"})
_NEW_YORK = ZoneInfo("America/New_York")


class AlpacaSnapshotProvider:
    """Fetch one bounded batch without exposing a generic Alpaca request seam."""

    def __init__(
        self,
        client: FixedOriginBytes,
        *,
        key_id: SecretStr,
        secret: SecretStr,
        symbol_resolver: InstrumentSymbolResolver,
        feed: Literal["iex", "sip", "delayed_sip"],
        delivery_mode: Literal["real_time", "delayed"],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not key_id.get_secret_value().strip() or not secret.get_secret_value().strip():
            raise ValueError("ALPACA_CREDENTIALS_REQUIRED")
        self._client = client
        self._key_id = key_id
        self._secret = secret
        self._symbol_resolver = symbol_resolver
        expected_delivery = "delayed" if feed == "delayed_sip" else "real_time"
        if delivery_mode != expected_delivery:
            raise ValueError("ALPACA_INTRADAY_AUTHORITY_MISMATCH")
        self._feed = feed
        self._delivery_mode = delivery_mode
        self._now = now or (lambda: datetime.now(UTC))

    async def fetch_snapshots(self, request: IntradaySnapshotRequest) -> SnapshotBatch:
        envelope = await self.fetch_raw_snapshots(request)
        return self.normalize_raw_snapshots(envelope, request)

    def normalize_raw_snapshots(
        self,
        envelope: RawSnapshotEnvelope,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch:
        """Use the provider-owned normalizer selected by composition."""
        return normalize_snapshot_envelope(envelope, request=request)

    async def fetch_raw_snapshots(
        self, request: IntradaySnapshotRequest
    ) -> RawSnapshotEnvelope:
        """Return bounded transport bytes without parsing provider-controlled JSON."""
        if request.feed != self._feed or request.delivery_mode != self._delivery_mode:
            raise MarketProviderPayloadError("ALPACA_INTRADAY_AUTHORITY_MISMATCH")
        resolved: list[str] = []
        for item in request.instruments:
            symbol = await self._symbol_resolver.resolve(
                item.instrument_id,
                request.symbol_as_of,
            )
            if symbol != item.symbol:
                raise MarketProviderPayloadError("INTRADAY_SYMBOL_AUTHORITY_INVALID")
            resolved.append(symbol)
        raw = await self._client.get_bytes(
            ("v2", "stocks", "snapshots"),
            (
                ("currency", "USD"),
                ("feed", request.feed),
                ("symbols", ",".join(resolved)),
            ),
            (
                ("APCA-API-KEY-ID", self._key_id.get_secret_value()),
                ("APCA-API-SECRET-KEY", self._secret.get_secret_value()),
            ),
        )
        received_at = self._now()
        mode: Literal["delayed", "read_only"] = (
            "delayed" if request.delivery_mode == "delayed" else "read_only"
        )
        return RawSnapshotEnvelope(
            raw_payload=raw,
            received_at=received_at,
            provenance=MarketDataProvenance(
                source="alpaca",
                feed=request.feed,
                adjustment="raw",
                interval="1Min",
                mode=mode,
                delivery_mode=request.delivery_mode,
            ),
        )


def normalize_snapshot_envelope(
    envelope: RawSnapshotEnvelope,
    *,
    request: IntradaySnapshotRequest,
) -> SnapshotBatch:
    """Validate transport authority, then parse and normalize captured bytes."""
    expected_mode = "delayed" if request.delivery_mode == "delayed" else "read_only"
    provenance = envelope.provenance
    if (
        provenance.source != "alpaca"
        or provenance.feed != request.feed
        or provenance.delivery_mode != request.delivery_mode
        or provenance.adjustment != "raw"
        or provenance.interval != "1Min"
        or provenance.mode != expected_mode
    ):
        raise MarketProviderPayloadError("ALPACA_INTRADAY_AUTHORITY_MISMATCH")
    if not envelope.raw_payload or len(envelope.raw_payload) > _MAX_RAW_BYTES:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_SIZE_INVALID")
    payload = load_exact_json(envelope.raw_payload)
    return normalize_snapshot_payload(
        payload,
        raw_payload=envelope.raw_payload,
        request=request,
        received_at=envelope.received_at,
    )


def normalize_snapshot_payload(
    payload: object,
    *,
    raw_payload: bytes,
    request: IntradaySnapshotRequest,
    received_at: datetime,
) -> SnapshotBatch:
    """Normalize an already exact-decimal parse; reject injected floats and drift."""
    if not raw_payload or len(raw_payload) > _MAX_RAW_BYTES:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_SIZE_INVALID")
    if not isinstance(payload, Mapping):
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_OBJECT_REQUIRED")
    expected = {item.symbol: item for item in request.instruments}
    if not set(payload).issubset(expected):
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_TICKER_MISMATCH")
    snapshots: list[MarketSnapshot] = []
    for symbol, instrument in expected.items():
        raw_snapshot = payload.get(symbol)
        if raw_snapshot is None:
            snapshots.append(
                MarketSnapshot(
                    instrument_id=instrument.instrument_id,
                    symbol=symbol,
                    trade=None,
                    quote=None,
                    minute_bar=None,
                )
            )
            continue
        if not isinstance(raw_snapshot, Mapping):
            raise MarketProviderPayloadError("ALPACA_SNAPSHOT_OBJECT_REQUIRED")
        _require_known_fields(raw_snapshot, _SYMBOL_FIELDS, "ALPACA_SNAPSHOT_FIELD_UNKNOWN")
        for ignored in ("dailyBar", "prevDailyBar"):
            value = raw_snapshot.get(ignored)
            if value is not None and not isinstance(value, Mapping):
                raise MarketProviderPayloadError("ALPACA_SNAPSHOT_DAILY_BAR_INVALID")
        snapshots.append(
            MarketSnapshot(
                instrument_id=instrument.instrument_id,
                symbol=symbol,
                trade=_trade(raw_snapshot.get("latestTrade")),
                quote=_quote(raw_snapshot.get("latestQuote")),
                minute_bar=_minute_bar(
                    raw_snapshot.get("minuteBar"),
                    instrument_id=instrument.instrument_id,
                    received_at=received_at,
                ),
            )
        )
    if any(
        observed_at > received_at
        for snapshot in snapshots
        for observed_at in (
            None if snapshot.trade is None else snapshot.trade.observed_at,
            None if snapshot.quote is None else snapshot.quote.observed_at,
            None if snapshot.minute_bar is None else snapshot.minute_bar.bar.observed_at,
        )
        if observed_at is not None
    ):
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_FUTURE_FACT")
    mode: Literal["delayed", "read_only"] = (
        "delayed" if request.delivery_mode == "delayed" else "read_only"
    )
    return SnapshotBatch(
        snapshots=tuple(snapshots),
        raw_payload=raw_payload,
        received_at=received_at,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed=request.feed,
            adjustment="raw",
            interval="1Min",
            mode=mode,
            delivery_mode=request.delivery_mode,
        ),
    )


def _trade(value: object) -> SnapshotTrade | None:
    if value is None:
        return None
    mapping = _mapping(value, "ALPACA_SNAPSHOT_TRADE_INVALID")
    _require_known_fields(mapping, _TRADE_FIELDS, "ALPACA_SNAPSHOT_TRADE_FIELD_UNKNOWN")
    try:
        return SnapshotTrade(
            price=_positive_decimal(mapping.get("p"), "ALPACA_SNAPSHOT_TRADE_INVALID"),
            observed_at=_time(mapping.get("t")),
            provider_trade_id=_optional_provider_id(mapping.get("i")),
        )
    except ValidationError as error:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_TRADE_INVALID") from error


def _quote(value: object) -> SnapshotQuote | None:
    if value is None:
        return None
    mapping = _mapping(value, "ALPACA_SNAPSHOT_QUOTE_INVALID")
    _require_known_fields(mapping, _QUOTE_FIELDS, "ALPACA_SNAPSHOT_QUOTE_FIELD_UNKNOWN")
    bid = _positive_decimal(mapping.get("bp"), "ALPACA_SNAPSHOT_QUOTE_INVALID")
    ask = _positive_decimal(mapping.get("ap"), "ALPACA_SNAPSHOT_QUOTE_INVALID")
    if bid > ask:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_SPREAD_INVALID")
    try:
        return SnapshotQuote(bid=bid, ask=ask, observed_at=_time(mapping.get("t")))
    except ValidationError as error:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_QUOTE_INVALID") from error


def _minute_bar(
    value: object,
    *,
    instrument_id: InstrumentId,
    received_at: datetime,
) -> SnapshotMinuteBar | None:
    if value is None:
        return None
    mapping = _mapping(value, "ALPACA_SNAPSHOT_BAR_INVALID")
    _require_known_fields(mapping, _BAR_FIELDS, "ALPACA_SNAPSHOT_BAR_FIELD_UNKNOWN")
    open_price = _positive_decimal(mapping.get("o"), "ALPACA_SNAPSHOT_BAR_INVALID")
    high = _positive_decimal(mapping.get("h"), "ALPACA_SNAPSHOT_BAR_INVALID")
    low = _positive_decimal(mapping.get("l"), "ALPACA_SNAPSHOT_BAR_INVALID")
    close = _positive_decimal(mapping.get("c"), "ALPACA_SNAPSHOT_BAR_INVALID")
    volume = _decimal(mapping.get("v"), "ALPACA_SNAPSHOT_VOLUME_INVALID")
    if volume < 0:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_VOLUME_INVALID")
    if high < max(open_price, close, low) or low > min(open_price, close, high):
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_OHLC_INVALID")
    observed_at = _time(mapping.get("t"))
    if observed_at > received_at:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_FUTURE_FACT")
    if observed_at + timedelta(minutes=1) > received_at:
        return None
    try:
        bar = PriceBar(
            instrument_id=instrument_id,
            session=observed_at.astimezone(_NEW_YORK).date(),
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
            observed_at=observed_at,
            available_at=received_at,
            received_at=received_at,
            revision="snapshot-v1",
            quality_flags=(),
        )
    except ValidationError as error:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_BAR_INVALID") from error
    return SnapshotMinuteBar(bar=bar)


def _mapping(value: object, code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise MarketProviderPayloadError(code)
    return cast(Mapping[str, object], value)


def _require_known_fields(value: Mapping[str, object], allowed: frozenset[str], code: str) -> None:
    if not set(value).issubset(allowed):
        raise MarketProviderPayloadError(code)


def _decimal(value: object, code: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise MarketProviderPayloadError(code)
    if not isinstance(value, Decimal | int | str):
        raise MarketProviderPayloadError(code)
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except Exception as error:
        raise MarketProviderPayloadError(code) from error
    if not result.is_finite():
        raise MarketProviderPayloadError(code)
    return result


def _positive_decimal(value: object, code: str) -> Decimal:
    result = _decimal(value, code)
    if result <= 0:
        raise MarketProviderPayloadError(code)
    return result


def _time(value: object) -> datetime:
    if not isinstance(value, str):
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_TIME_INVALID")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_TIME_INVALID") from error
    if parsed.tzinfo is None:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_TIME_INVALID")
    return parsed.astimezone(UTC)


def _optional_provider_id(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, str | int | Decimal):
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_TRADE_ID_INVALID")
    result = str(value)
    if not result.isascii() or not 1 <= len(result) <= 128:
        raise MarketProviderPayloadError("ALPACA_SNAPSHOT_TRADE_ID_INVALID")
    return result

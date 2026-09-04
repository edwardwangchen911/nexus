"""Current split-price observations, distinct from executable or historical prices."""

import json
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from importlib.metadata import version
from typing import Any, Literal, Self
from uuid import UUID

import exchange_calendars  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus.agents.price_features import compute_close_features
from nexus.diagnostics.calendar_policy import XnysCalendarAdoption
from nexus.domain.time import require_utc
from nexus.persistence.models.diagnostics import DiagnosticPolicy
from nexus.providers.base import BarsRequest, PriceBar
from nexus.providers.market.alpaca import AlpacaMarketDataProvider, AlpacaRawResponse
from nexus.providers.market.alpaca_http import load_exact_json
from nexus.research.alpaca_prices_archive import ValidatedPriceArchive, validate_price_originals

SOURCE = "alpaca_current_trends"
SCHEMA = "alpaca_current_trends_v1"
FEATURES = ("close", "sma_20", "sma_200", "rsi_14", "volatility_20")
MAX_CANONICAL_BYTES = 2 * 1024 * 1024
PURPOSE_POLICY_ID = UUID("c15c693f-d286-54bd-a1bf-f74d6651cb9e")
PURPOSE_POLICY_KEY = "alpaca_current_trends"
PURPOSE_POLICY_VERSION = "alpaca_current_trends_v1"
PURPOSE_POLICY_PAYLOAD: dict[str, Any] = {
    "source": SOURCE,
    "schema": SCHEMA,
    "feed": "sip",
    "adjustment": "split",
    "currency": "USD",
    "timeframe": "1Day",
    "calendar": "XNYS",
    "features": list(FEATURES),
    "missing_session_rule": "last_contiguous_observed_segment",
    "algorithm": "nexus_decimal_price_features_v1",
    "historical_backtest_eligible": False,
    "market_price_eligible": False,
    "total_return_eligible": False,
    "execution_locked": True,
}


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PurposePolicyInput(_FrozenModel):
    row_id: UUID
    key: str
    version: str
    schema_version: Literal["1"]
    payload: dict[str, Any]
    created_at: datetime
    content_hash: str


def purpose_policy_input(record: DiagnosticPolicy | None, as_of: datetime) -> PurposePolicyInput:
    if record is None or (
        record.id != PURPOSE_POLICY_ID
        or record.key != PURPOSE_POLICY_KEY
        or record.version != PURPOSE_POLICY_VERSION
        or record.schema_version != "1"
        or record.payload != PURPOSE_POLICY_PAYLOAD
        or record.created_at > require_utc(as_of)
    ):
        raise ValueError("CURRENT_TRENDS_PURPOSE_INVALID")
    value = {
        "row_id": str(record.id),
        "key": record.key,
        "version": record.version,
        "schema_version": record.schema_version,
        "payload": record.payload,
        "created_at": require_utc(record.created_at).isoformat().replace("+00:00", "Z"),
    }
    digest = sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return PurposePolicyInput.model_validate({**value, "content_hash": digest})


class TrendOriginal(_FrozenModel):
    name: str
    raw_text: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_length: int = Field(gt=0, le=MAX_CANONICAL_BYTES)
    received_at: datetime


class CurrentPriceTrend(_FrozenModel):
    schema_version: Literal["alpaca_current_trends_v1"] = "alpaca_current_trends_v1"
    source: Literal["alpaca_current_trends"] = "alpaca_current_trends"
    instrument_id: UUID
    symbol: str
    manifest_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feed: Literal["sip"] = "sip"
    adjustment: Literal["split"] = "split"
    currency: Literal["USD"] = "USD"
    timeframe: Literal["1Day"] = "1Day"
    identity_asof: date
    query_start: date
    query_end: date
    first_observed_on: date | None
    last_observed_on: date | None
    observed_sessions: int = Field(ge=0, le=367)
    expected_sessions: int = Field(ge=1, le=367)
    contiguous_tail_sessions: int = Field(ge=0, le=367)
    missing_sessions: tuple[date, ...] = Field(max_length=367)
    features: dict[str, str | None]
    received_at: datetime
    materialized_at: datetime
    available_at: datetime
    calendar: dict[str, Any]
    purpose_policy: PurposePolicyInput
    originals: tuple[TrendOriginal, ...] = Field(min_length=3, max_length=21)
    historical_backtest_eligible: Literal[False] = False
    market_price_eligible: Literal[False] = False
    total_return_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @model_validator(mode="after")
    def _bounds(self) -> Self:
        if (
            not require_utc(self.received_at) <= require_utc(self.materialized_at)
            or self.available_at != self.materialized_at
            or self.identity_asof > self.received_at.date()
            or not 0 <= (self.query_end - self.query_start).days <= 366
            or set(self.features) != set(FEATURES)
            or any(v is not None and len(v) > 40 for v in self.features.values())
        ):
            raise ValueError("CURRENT_TRENDS_DOCUMENT_INVALID")
        return self

    def canonical_bytes(self) -> bytes:
        value = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        content = value.encode()
        if len(content) > MAX_CANONICAL_BYTES:
            raise ValueError("CURRENT_TRENDS_DOCUMENT_TOO_LARGE")
        return content

    @property
    def content_hash(self) -> str:
        return sha256(self.canonical_bytes()).hexdigest()

    @property
    def revision(self) -> str:
        return self.content_hash


class _OriginalTransport:
    def __init__(self, raw: bytes) -> None:
        self.raw = raw

    async def fetch_bars(self, request: BarsRequest, cursor: str | None) -> AlpacaRawResponse:
        payload = load_exact_json(self.raw)
        if not isinstance(payload, dict):
            raise ValueError("CURRENT_TRENDS_ORIGINAL_INVALID")
        return AlpacaRawResponse(payload, self.raw, None)


async def build_current_price_trend(
    archive: ValidatedPriceArchive,
    *,
    calendar: XnysCalendarAdoption,
    purpose_policy: PurposePolicyInput,
    materialized_at: datetime,
) -> CurrentPriceTrend:
    clock = require_utc(materialized_at)
    archive = validate_price_originals(
        {item.name: item.content for item in archive.originals},
        instrument_id=archive.request.instrument_id,
        now=clock,
    )
    if (
        archive.request.adjustment != "split"
        or archive.request.interval != "1Day"
        or calendar.created_at > clock
        or calendar.payload.effective_from > clock
        or purpose_policy.created_at > clock
        or version("exchange-calendars") != calendar.payload.package_version
    ):
        raise ValueError("CURRENT_TRENDS_PURPOSE_INVALID")
    bars: list[PriceBar] = []
    for original in archive.originals:
        if not original.name.startswith("page-") or ".receipt." in original.name:
            continue
        provider = AlpacaMarketDataProvider(
            _OriginalTransport(original.content),
            feed="sip",
            delivery_mode="real_time",
            now=lambda: original.received_at,
        )
        page = await provider.fetch_bars(archive.request)
        if any(
            bar.observed_at is None
            or bar.observed_at > original.received_at
            or bar.available_at is None
            or bar.available_at > original.received_at
            for bar in page.items
        ):
            raise ValueError("CURRENT_TRENDS_BARS_INVALID")
        bars.extend(page.items)
    declared = json.loads(archive.originals[-1].content)["calendar"]
    start, end = (date.fromisoformat(declared[key]) for key in ("start_session", "end_session"))
    sessions = tuple(
        day.date() for day in exchange_calendars.get_calendar("XNYS").sessions_in_range(start, end)
    )
    document = CurrentPriceTrend(
        instrument_id=archive.request.instrument_id,
        symbol=archive.symbol,
        manifest_content_hash=archive.manifest_content_hash,
        identity_asof=archive.identity_asof,
        query_start=start,
        query_end=end,
        received_at=archive.received_at,
        materialized_at=clock,
        available_at=clock,
        calendar=calendar.canonical_input(),
        purpose_policy=purpose_policy,
        originals=tuple(
            TrendOriginal(
                name=item.name,
                raw_text=item.content.decode("utf-8"),
                content_hash=sha256(item.content).hexdigest(),
                byte_length=len(item.content),
                received_at=item.received_at,
            )
            for item in archive.originals
        ),
        **trend_metrics(bars, sessions),
    )
    document.canonical_bytes()
    return document


def trend_metrics(bars: Sequence[PriceBar], sessions: Sequence[date]) -> dict[str, Any]:
    """Compute on the last contiguous covered segment, never across missing sessions."""
    expected = tuple(sessions)
    if not expected or len(expected) > 367 or expected != tuple(sorted(set(expected))):
        raise ValueError("CURRENT_TRENDS_CALENDAR_INVALID")
    by_session = {bar.session: bar for bar in bars}
    if (
        len(by_session) != len(bars)
        or len({bar.instrument_id for bar in bars}) > 1
        or not set(by_session) <= set(expected)
        or any(
            bar.quality_flags
            or any(
                not value.is_finite() or value <= 0
                for value in (bar.open, bar.high, bar.low, bar.close)
            )
            or not bar.volume.is_finite()
            or bar.volume < 0
            or bar.high < max(bar.open, bar.close)
            or bar.low > min(bar.open, bar.close)
            for bar in bars
        )
    ):
        raise ValueError("CURRENT_TRENDS_BARS_INVALID")
    observed = tuple(day for day in expected if day in by_session)
    tail: list[Decimal] = []
    if observed:
        for day in reversed(expected[: expected.index(observed[-1]) + 1]):
            if day not in by_session:
                break
            tail.append(by_session[day].close)
    try:
        calculated = compute_close_features(tuple(reversed(tail)), FEATURES)
    except (InvalidOperation, OverflowError, ZeroDivisionError):
        raise ValueError("CURRENT_TRENDS_BARS_INVALID") from None
    features = {key: calculated.values.get(key) for key in FEATURES}
    if any(
        value is not None
        and (len(value) > 40 or (name in {"close", "sma_20", "sma_200"} and Decimal(value) <= 0))
        for name, value in features.items()
    ):
        raise ValueError("CURRENT_TRENDS_BARS_INVALID")
    missing = tuple(day for day in expected if day not in by_session)
    return {
        "first_observed_on": observed[0] if observed else None,
        "last_observed_on": observed[-1] if observed else None,
        "observed_sessions": len(observed),
        "expected_sessions": len(expected),
        "contiguous_tail_sessions": len(tail),
        "missing_sessions": missing,
        "features": features,
    }

"""Published whole-series market source selection policy."""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.integrations.service import IntegrationStatusService
from nexus.persistence.models.data import SourcePolicy as SourcePolicyRecord
from nexus.providers.base import (
    PriceBar,
    PriceSeries,
    delivery_mode_matches_provider_mode,
)
from nexus.quality.price_revision import canonical_price_bars, price_bar_sort_key

SOURCE_POLICY_ID = UUID("3c60888d-e92d-5e04-913c-7d427004cfef")
SOURCE_POLICY_KEY = "market_price"
SOURCE_POLICY_VERSION = "source_policy_v1"
SOURCE_POLICY_PAYLOAD: dict[str, object] = {
    "priority": ["fosun", "alpaca", "fixture"],
    "fosun": {
        "mode": "read_only",
        "operation": "fosun_get_bars",
        "market_scope": "US",
    },
    "alpaca": {"feed": "sip", "modes": ["delayed", "fixture"]},
    "fixture": {"mode": "fixture"},
}
QUOTE_SOURCE_POLICY_ID = UUID("6d243c22-a020-5d1d-b884-e6240cc2d8bb")
QUOTE_SOURCE_POLICY_KEY = "market_quote"
QUOTE_SOURCE_POLICY_VERSION = "quote_source_policy_v1"
QUOTE_SOURCE_POLICY_PAYLOAD: dict[str, object] = {
    "priority": ["fosun", "alpaca", "fixture"],
    "fosun": {
        "mode": "read_only",
        "operation": "fosun_get_quotes",
        "market_scope": "US",
    },
    "alpaca": {
        "feed": "iex",
        "delivery_modes": ["real_time", "delayed", "unknown"],
    },
    "fixture": {"delivery_mode": "fixture"},
}
INTRADAY_SOURCE_POLICY_ID = UUID("8fab80c9-5b6a-5b2b-8f36-3b22319b54ef")
INTRADAY_SOURCE_POLICY_KEY = "intraday_monitor"
INTRADAY_SOURCE_POLICY_VERSION = "intraday_monitor_policy_v1"
INTRADAY_SOURCE_POLICY_PAYLOAD: dict[str, object] = {
    "priority": ["fosun", "alpaca", "fixture"],
    "fosun": {
        "mode": "read_only",
        "operation": "fosun_get_bars",
        "market_scope": "US",
    },
    "alpaca": {
        "feeds": ["iex", "sip", "delayed_sip"],
        "delivery_modes": ["real_time", "delayed"],
    },
    "fixture": {"delivery_mode": "fixture"},
    "purpose": "display_only_intraday_monitor",
}
_PRIORITY = ("fosun", "alpaca", "fixture")


def price_manifest_admissible_under_source_policy_v1(
    *,
    source: str,
    feed: str,
    delivery_mode: str,
    fosun_admission_frozen: bool = False,
) -> bool:
    """Validate persisted bar provenance against the published v1 policy.

    A historical Fosun manifest is intentionally rejected until its exact Task 5
    capability/status admission is frozen into the backtest authority bundle.
    """
    if source == "alpaca":
        return feed == "sip" and delivery_mode in {"delayed", "fixture"}
    if source == "fixture":
        return delivery_mode == "fixture"
    if source == "fosun":
        return fosun_admission_frozen
    return False


class SourcePolicy(BaseModel):
    """Runtime projection of the sole published source-policy record."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    record_id: UUID
    key: Literal["market_price"]
    version: Literal["source_policy_v1"] = "source_policy_v1"
    schema_version: Literal["1"]
    available_at: datetime
    instrument_tick_size: Decimal | None

    @field_validator("record_id")
    @classmethod
    def _canonical_record(cls, value: UUID) -> UUID:
        if value != SOURCE_POLICY_ID:
            raise ValueError("source policy must be bound to the published v1 record")
        return value

    @field_validator("available_at")
    @classmethod
    def _available_at_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class QuoteSourcePolicy(BaseModel):
    """Runtime projection of the independently published intraday quote policy."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    record_id: UUID
    key: Literal["market_quote"]
    version: Literal["quote_source_policy_v1"] = "quote_source_policy_v1"
    schema_version: Literal["1"]

    @field_validator("record_id")
    @classmethod
    def _canonical_record(cls, value: UUID) -> UUID:
        if value != QUOTE_SOURCE_POLICY_ID:
            raise ValueError("quote source policy must be bound to the published v1 record")
        return value


class FosunMarketAdmission(BaseModel):
    """Validated Task 5 authority scoped to one read-only market operation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    capability_id: UUID
    capability_schema_version: Literal["1"]
    capability_created_at: datetime
    status_id: UUID
    status_version: int
    capability_invalidated_at: datetime | None
    admitted_contract_hash: str
    provider: Literal["fosun"]
    mode: Literal["read_only"]
    operation: Literal["fosun_get_quotes", "fosun_get_bars"]
    market_scope: Literal["US"]
    contract_hash: str
    expires_at: datetime

    @field_validator("expires_at", "capability_created_at", "capability_invalidated_at")
    @classmethod
    def _expires_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class SourceSelection(BaseModel):
    model_config = ConfigDict(frozen=True)

    selected: PriceSeries | None
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    reasons: tuple[str, ...]


async def load_source_policy_v1(
    session: AsyncSession, *, instrument_tick_size: Decimal | None
) -> SourcePolicy:
    record = await session.scalar(
        select(SourcePolicyRecord).where(
            SourcePolicyRecord.key == SOURCE_POLICY_KEY,
            SourcePolicyRecord.version == SOURCE_POLICY_VERSION,
        )
    )
    if (
        record is None
        or record.id != SOURCE_POLICY_ID
        or record.schema_version != "1"
        or record.payload != SOURCE_POLICY_PAYLOAD
    ):
        raise ValueError("published source_policy_v1 is missing or invalid")
    policy = SourcePolicy(
        record_id=record.id,
        key=cast(Literal["market_price"], record.key),
        version=cast(Literal["source_policy_v1"], record.version),
        schema_version=cast(Literal["1"], record.schema_version),
        available_at=record.created_at,
        instrument_tick_size=instrument_tick_size,
    )
    return policy


async def load_quote_source_policy_v1(session: AsyncSession) -> QuoteSourcePolicy:
    """Load only the immutable quote-domain policy; bar policy cannot authorize quotes."""
    record = await session.scalar(
        select(SourcePolicyRecord).where(
            SourcePolicyRecord.key == QUOTE_SOURCE_POLICY_KEY,
            SourcePolicyRecord.version == QUOTE_SOURCE_POLICY_VERSION,
        )
    )
    if (
        record is None
        or record.id != QUOTE_SOURCE_POLICY_ID
        or record.schema_version != "1"
        or record.payload != QUOTE_SOURCE_POLICY_PAYLOAD
    ):
        raise ValueError("published quote_source_policy_v1 is missing or invalid")
    return QuoteSourcePolicy(
        record_id=record.id,
        key=cast(Literal["market_quote"], record.key),
        version=cast(Literal["quote_source_policy_v1"], record.version),
        schema_version=cast(Literal["1"], record.schema_version),
    )


async def load_fosun_market_admission(
    session: AsyncSession,
    *,
    decision_time: datetime,
    operation: str,
    market_scope: str,
) -> FosunMarketAdmission | None:
    decision_time = require_utc(decision_time)
    if operation not in {"fosun_get_quotes", "fosun_get_bars"} or market_scope != "US":
        return None
    service = IntegrationStatusService(session, decision_time)
    status = await service.find_status("fosun")
    capability = await service.capability("fosun", operation=operation, market_scope=market_scope)
    authority = capability.valid
    if (
        status is None
        or status.status != "read_only"
        or authority is None
        or authority.schema_version != "1"
        or status.admitted_contract_hash != authority.contract_hash
        or (
            status.capability_invalidated_at is not None
            and authority.created_at <= status.capability_invalidated_at
        )
    ):
        return None
    payload_operation = authority.payload.get("operation")
    payload_market_scope = authority.payload.get("market_scope")
    if payload_operation != operation or payload_market_scope != market_scope:
        return None
    admission = FosunMarketAdmission(
        capability_id=authority.id,
        capability_schema_version=cast(Literal["1"], authority.schema_version),
        capability_created_at=authority.created_at,
        status_id=status.id,
        status_version=status.version,
        capability_invalidated_at=status.capability_invalidated_at,
        admitted_contract_hash=status.admitted_contract_hash,
        provider="fosun",
        mode="read_only",
        operation=cast(Literal["fosun_get_quotes", "fosun_get_bars"], payload_operation),
        market_scope=cast(Literal["US"], payload_market_scope),
        contract_hash=authority.contract_hash,
        expires_at=authority.expires_at,
    )
    return admission


async def select_price_series(
    session: AsyncSession,
    candidates: Sequence[PriceSeries],
    instrument_tick_size: Decimal | None,
    decision_time: datetime,
) -> SourceSelection:
    """Load repository authority and choose one complete point-in-time series."""
    decision_time = require_utc(decision_time)
    try:
        policy = await load_source_policy_v1(session, instrument_tick_size=instrument_tick_size)
    except ValueError:
        return SourceSelection(
            selected=None,
            quality="FAIL",
            reasons=("SOURCE_POLICY_UNAVAILABLE",),
        )
    fosun_admission = None
    if any(candidate.source == "fosun" for candidate in candidates):
        fosun_admission = await load_fosun_market_admission(
            session,
            decision_time=decision_time,
            operation="fosun_get_bars",
            market_scope="US",
        )
    return apply_source_policy_v1(
        candidates,
        policy,
        decision_time,
        fosun_admission,
    )


def apply_source_policy_v1(
    candidates: Sequence[PriceSeries],
    policy: SourcePolicy,
    decision_time: datetime,
    fosun_admission: FosunMarketAdmission | None,
) -> SourceSelection:
    reasons: set[str] = set()
    eligible: list[PriceSeries] = []
    point_in_time_candidate_seen = False

    for candidate in candidates:
        if candidate.source not in _PRIORITY or candidate.quality == "FAIL":
            continue
        if not delivery_mode_matches_provider_mode(candidate.mode, candidate.delivery_mode):
            continue
        fosun_admitted = candidate.source == "fosun" and _valid_fosun_candidate(
            candidate, fosun_admission, decision_time
        )
        if candidate.source == "fosun" and not fosun_admitted:
            reasons.add("FOSUN_CAPABILITY_EXPIRED")
            continue
        if not price_manifest_admissible_under_source_policy_v1(
            source=candidate.source,
            feed=candidate.feed,
            delivery_mode=candidate.delivery_mode,
            fosun_admission_frozen=fosun_admitted,
        ):
            continue
        point_in_time_candidate_seen = True
        visible = tuple(bar for bar in candidate.bars if bar.available_at <= decision_time)
        if not visible:
            continue
        if len(visible) != len(candidate.bars):
            reasons.add("FUTURE_DATA_EXCLUDED")
        eligible.append(
            candidate.model_copy(update={"bars": canonical_price_bars(candidate, visible)})
        )

    if not eligible:
        return SourceSelection(
            selected=None,
            quality="FAIL",
            reasons=(
                "NO_POINT_IN_TIME_DATA" if point_in_time_candidate_seen else "NO_ELIGIBLE_SOURCE",
            ),
        )

    ordered = sorted(eligible, key=_series_sort_key)
    selected = ordered[0]
    cross_check = _cross_check(ordered, policy)
    if cross_check == "SOURCE_CONFLICT":
        return SourceSelection(selected=None, quality="FAIL", reasons=(cross_check,))
    if cross_check is not None:
        reasons.add(cross_check)
    quality: Literal["PASS", "PARTIAL", "FAIL"] = selected.quality
    if reasons.intersection({"TICK_SIZE_UNKNOWN", "FUTURE_DATA_EXCLUDED"}):
        quality = "PARTIAL"
    return SourceSelection(selected=selected, quality=quality, reasons=_ordered_reasons(reasons))


def _valid_fosun_candidate(
    candidate: PriceSeries,
    admission: FosunMarketAdmission | None,
    decision_time: datetime,
) -> bool:
    return (
        candidate.mode == "read_only"
        and admission is not None
        and admission.expires_at > decision_time
        and admission.admitted_contract_hash == admission.contract_hash
        and candidate.contract_hash == admission.contract_hash
    )


def _series_sort_key(series: PriceSeries) -> tuple[object, ...]:
    return (
        _PRIORITY.index(series.source),
        0 if series.quality == "PASS" else 1,
        series.source,
        series.feed,
        series.adjustment,
        series.interval,
        series.mode,
        series.contract_hash or "",
        tuple(price_bar_sort_key(bar) for bar in series.bars),
    )


def _cross_check(ordered: Sequence[PriceSeries], policy: SourcePolicy) -> str | None:
    sources = {series.source: series for series in reversed(ordered)}
    if "fosun" not in sources or "alpaca" not in sources:
        return None
    primary, secondary = sources["fosun"], sources["alpaca"]
    if primary.interval != secondary.interval or primary.adjustment != secondary.adjustment:
        return "SOURCE_CONFLICT"
    if primary.interval == "1Min":
        intraday_primary = {
            (bar.instrument_id, bar.session, bar.observed_at): bar for bar in primary.bars
        }
        intraday_secondary = {
            (bar.instrument_id, bar.session, bar.observed_at): bar for bar in secondary.bars
        }
        matches = sorted(intraday_primary.keys() & intraday_secondary.keys(), key=str)
        if not matches:
            return None
        return (
            "INTRADAY_DIVERGENCE_ALERT"
            if any(
                _relative_close_difference(intraday_primary[key], intraday_secondary[key])
                > Decimal("0.005")
                for key in matches
            )
            else None
        )
    daily_primary = {(bar.instrument_id, bar.session): bar for bar in primary.bars}
    daily_secondary = {(bar.instrument_id, bar.session): bar for bar in secondary.bars}
    if daily_primary.keys() != daily_secondary.keys():
        return "SOURCE_CONFLICT"
    if policy.instrument_tick_size is None:
        return "TICK_SIZE_UNKNOWN"
    for key, first_bar in daily_primary.items():
        first, second = first_bar.close, daily_secondary[key].close
        if abs(first - second) > max(
            policy.instrument_tick_size, min(abs(first), abs(second)) * Decimal("0.002")
        ):
            return "SOURCE_CONFLICT"
    return None


def _relative_close_difference(first: PriceBar, second: PriceBar) -> Decimal:
    denominator = min(abs(first.close), abs(second.close))
    if denominator == 0:
        return Decimal("Infinity") if first.close != second.close else Decimal(0)
    return abs(first.close - second.close) / denominator


def _ordered_reasons(reasons: set[str]) -> tuple[str, ...]:
    order = (
        "FOSUN_CAPABILITY_EXPIRED",
        "FUTURE_DATA_EXCLUDED",
        "TICK_SIZE_UNKNOWN",
        "INTRADAY_DIVERGENCE_ALERT",
    )
    return tuple(reason for reason in order if reason in reasons) + tuple(
        sorted(reasons.difference(order))
    )

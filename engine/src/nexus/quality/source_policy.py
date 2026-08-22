"""Published whole-series market source selection policy."""

import json
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, PrivateAttr, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.integrations.service import IntegrationStatusService
from nexus.persistence.models.data import SourcePolicy as SourcePolicyRecord
from nexus.providers.base import PriceBar, PriceSeries

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
_PRIORITY = ("fosun", "alpaca", "fixture")
_POLICY_LOADER_TOKEN = object()
_ADMISSION_LOADER_TOKEN = object()


class SourcePolicy(BaseModel):
    """Runtime projection of the sole published source-policy record."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    record_id: UUID
    key: Literal["market_price"]
    version: Literal["source_policy_v1"] = "source_policy_v1"
    schema_version: Literal["1"]
    payload_sha256: str
    instrument_tick_size: Decimal | None
    _loader_token: object | None = PrivateAttr(default=None)
    _identity_fingerprint: str | None = PrivateAttr(default=None)

    @field_validator("record_id")
    @classmethod
    def _canonical_record(cls, value: UUID) -> UUID:
        if value != SOURCE_POLICY_ID:
            raise ValueError("source policy must be bound to the published v1 record")
        return value


class FosunMarketAdmission(BaseModel):
    """Validated Task 5 authority scoped to the read-only bars operation."""

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
    operation: Literal["fosun_get_bars"]
    market_scope: Literal["US"]
    contract_hash: str
    expires_at: datetime
    _loader_token: object | None = PrivateAttr(default=None)
    _identity_fingerprint: str | None = PrivateAttr(default=None)

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
        payload_sha256=_canonical_hash(record.payload),
        instrument_tick_size=instrument_tick_size,
    )
    policy._loader_token = _POLICY_LOADER_TOKEN
    policy._identity_fingerprint = _model_fingerprint(policy)
    return policy


async def load_fosun_market_admission(
    session: AsyncSession,
    *,
    decision_time: datetime,
    operation: str,
    market_scope: str,
) -> FosunMarketAdmission | None:
    decision_time = require_utc(decision_time)
    if operation != "fosun_get_bars" or market_scope != "US":
        return None
    service = IntegrationStatusService(session, decision_time)
    status = await service.find_status("fosun")
    capability = await service.capability("fosun")
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
        operation=cast(Literal["fosun_get_bars"], payload_operation),
        market_scope=cast(Literal["US"], payload_market_scope),
        contract_hash=authority.contract_hash,
        expires_at=authority.expires_at,
    )
    admission._loader_token = _ADMISSION_LOADER_TOKEN
    admission._identity_fingerprint = _model_fingerprint(admission)
    return admission


def select_price_series(
    candidates: Sequence[PriceSeries],
    policy: SourcePolicy,
    decision_time: datetime,
    *,
    fosun_admission: FosunMarketAdmission | None = None,
) -> SourceSelection:
    decision_time = require_utc(decision_time)
    if not _has_loader_attestation(policy, _POLICY_LOADER_TOKEN):
        raise ValueError("source policy loader attestation is required")
    reasons: set[str] = set()
    eligible: list[PriceSeries] = []
    point_in_time_candidate_seen = False

    for candidate in candidates:
        if candidate.source not in _PRIORITY or candidate.quality == "FAIL":
            continue
        if candidate.source == "alpaca" and (
            candidate.feed != "sip" or candidate.mode not in {"fixture", "delayed"}
        ):
            continue
        if candidate.source == "fixture" and candidate.mode != "fixture":
            continue
        if candidate.source == "fosun" and not _valid_fosun_candidate(
            candidate, fosun_admission, decision_time
        ):
            reasons.add("FOSUN_CAPABILITY_EXPIRED")
            continue
        point_in_time_candidate_seen = True
        visible = tuple(bar for bar in candidate.bars if bar.available_at <= decision_time)
        if not visible:
            continue
        if len(visible) != len(candidate.bars):
            reasons.add("FUTURE_DATA_EXCLUDED")
        eligible.append(candidate.model_copy(update={"bars": _canonical_bars(candidate, visible)}))

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
        and _has_loader_attestation(admission, _ADMISSION_LOADER_TOKEN)
        and admission.expires_at > decision_time
        and admission.admitted_contract_hash == admission.contract_hash
        and candidate.contract_hash == admission.contract_hash
    )


def _has_loader_attestation(model: BaseModel, token: object) -> bool:
    return getattr(model, "_loader_token", None) is token and getattr(
        model, "_identity_fingerprint", None
    ) == _model_fingerprint(model)


def _model_fingerprint(model: BaseModel) -> str:
    return _canonical_hash(model.model_dump(mode="json"))


def _canonical_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha256(payload.encode("utf-8")).hexdigest()


def _canonical_bars(series: PriceSeries, bars: Sequence[PriceBar]) -> tuple[PriceBar, ...]:
    latest: dict[tuple[object, ...], PriceBar] = {}
    for bar in bars:
        identity: tuple[object, ...] = (bar.instrument_id, bar.session)
        if series.interval == "1Min":
            identity += (bar.observed_at,)
        current = latest.get(identity)
        if current is None or _revision_key(bar) > _revision_key(current):
            latest[identity] = bar
    return tuple(sorted(latest.values(), key=_bar_sort_key))


def _revision_key(bar: PriceBar) -> tuple[object, ...]:
    return (
        bar.available_at,
        bar.received_at or bar.available_at,
        bar.observed_at or bar.available_at,
        bar.revision,
        str(bar.open),
        str(bar.high),
        str(bar.low),
        str(bar.close),
        str(bar.volume),
        tuple(sorted(bar.quality_flags)),
    )


def _bar_sort_key(bar: PriceBar) -> tuple[object, ...]:
    return (
        str(bar.instrument_id),
        bar.session.isoformat(),
        bar.observed_at.isoformat() if bar.observed_at else "",
        bar.available_at.isoformat(),
        bar.received_at.isoformat() if bar.received_at else "",
        bar.revision,
        str(bar.open),
        str(bar.high),
        str(bar.low),
        str(bar.close),
        str(bar.volume),
        tuple(sorted(bar.quality_flags)),
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
        tuple(_bar_sort_key(bar) for bar in series.bars),
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

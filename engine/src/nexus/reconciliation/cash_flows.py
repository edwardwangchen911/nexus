"""Bounded cash-flow overlap reads with versioned identities and watermark semantics."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from nexus.providers.broker.base import (
    BrokerCashFlow,
    BrokerProvider,
    BrokerProviderError,
    CashFlowBusinessType,
    CashFlowProductCode,
    DateRange,
    SourceObservation,
)

CASH_FLOW_FALLBACK_VERSION = "broker_cash_flow_fallback_v1"
_STABLE_ID_VERSION = "provider_cash_flow_id_v1"
_PROVIDER_TIMEZONE = ZoneInfo("Asia/Hong_Kong")
_PAGE_LIMIT = 100
_MAX_PAGES = 50
_MAX_FACTS = 5_000


@dataclass(frozen=True)
class NormalizedCashFlow:
    provider_cash_flow_id: str
    identity_version: str
    identity_quality: Literal["STABLE", "FALLBACK_PARTIAL"]
    currency: str
    amount: Decimal
    occurred_at: datetime | None
    provider_business_date: date
    kind: str
    source_request_id: str
    response_as_of: datetime
    business_type: CashFlowBusinessType | None = None
    product_code: CashFlowProductCode | None = None


@dataclass(frozen=True)
class CashFlowSyncResult:
    facts: tuple[NormalizedCashFlow, ...]
    quality: Literal["PASS", "PARTIAL"]
    pagination_complete: bool
    watermark_date: date | None
    source_observations: tuple[SourceObservation, ...]


class _CashFlowWindowInvalidated(BrokerProviderError):
    pass


async def fetch_cash_flows(
    account_id: UUID,
    provider: BrokerProvider,
    *,
    sync_run_id: UUID,
    initial_start: date,
    end: date,
    previous_watermark: date | None,
) -> CashFlowSyncResult:
    """Read one date window; persisted cursors never cross fetch boundaries."""
    start = initial_start if previous_watermark is None else previous_watermark - timedelta(days=3)
    if end < start:
        raise ValueError("cash-flow end date precedes overlap start")
    window = DateRange(start=start, end=end)
    cursor: str | None = None
    seen_cursors: set[str] = set()
    seen_identities: set[str] = set()
    facts: list[NormalizedCashFlow] = []
    observations: list[SourceObservation] = []
    quality: Literal["PASS", "PARTIAL"] = "PASS"

    try:
        for _ in range(_MAX_PAGES):
            page = await provider.read_cash_flows(
                account_id,
                window,
                cursor,
                sync_run_id=sync_run_id,
                limit=_PAGE_LIMIT,
            )
            observations.append(
                SourceObservation(
                    source_request_id=page.source_request_id,
                    response_as_of=page.as_of,
                    quality=page.quality,
                )
            )
            if page.quality == "PARTIAL":
                quality = "PARTIAL"
            if len(page.items) > _PAGE_LIMIT:
                raise _CashFlowWindowInvalidated("cash-flow page exceeds fixed limit")
            if page.has_more and not page.items:
                raise _CashFlowWindowInvalidated("empty cash-flow continuation page")
            for item in page.items:
                normalized = _normalize(
                    item,
                    window,
                    source_request_id=page.source_request_id,
                    response_as_of=page.as_of,
                )
                if normalized.provider_cash_flow_id in seen_identities:
                    raise _CashFlowWindowInvalidated("duplicate cash-flow identity")
                seen_identities.add(normalized.provider_cash_flow_id)
                facts.append(normalized)
                if normalized.identity_quality == "FALLBACK_PARTIAL":
                    quality = "PARTIAL"
            if len(facts) > _MAX_FACTS:
                raise _CashFlowWindowInvalidated("cash-flow facts exceed fixed limit")
            if not page.has_more:
                return CashFlowSyncResult(
                    facts=tuple(facts),
                    quality=quality,
                    pagination_complete=True,
                    watermark_date=end,
                    source_observations=tuple(observations),
                )
            next_cursor = page.next_cursor
            if next_cursor is None or next_cursor == cursor or next_cursor in seen_cursors:
                raise _CashFlowWindowInvalidated("cash-flow cursor contradiction")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise _CashFlowWindowInvalidated("cash-flow page count exceeds fixed limit")
    except BrokerProviderError:
        return CashFlowSyncResult(
            facts=tuple(facts),
            quality="PARTIAL",
            pagination_complete=False,
            watermark_date=None,
            source_observations=tuple(observations),
        )


def _normalize(
    item: BrokerCashFlow,
    window: DateRange,
    *,
    source_request_id: str,
    response_as_of: datetime,
) -> NormalizedCashFlow:
    business_date = item.provider_business_date
    if business_date is None:
        if item.occurred_at is None:
            raise _CashFlowWindowInvalidated("cash-flow business date is missing")
        business_date = item.occurred_at.astimezone(_PROVIDER_TIMEZONE).date()
    if business_date < window.start or business_date > window.end:
        raise _CashFlowWindowInvalidated("cash flow is outside requested business dates")
    if not item.currency or len(item.currency) > 3 or not item.kind or len(item.kind) > 100:
        raise _CashFlowWindowInvalidated("cash flow contains invalid dimensions")
    if not item.amount.is_finite():
        raise _CashFlowWindowInvalidated("cash flow amount must be finite")
    provider_id = item.provider_cash_flow_id
    if provider_id is not None:
        if (
            provider_id != provider_id.strip()
            or not provider_id
            or len(provider_id.encode("utf-8")) > 128
        ):
            raise _CashFlowWindowInvalidated("provider cash-flow ID is invalid")
        identity = provider_id
        version = _STABLE_ID_VERSION
        identity_quality: Literal["STABLE", "FALLBACK_PARTIAL"] = "STABLE"
    else:
        if item.occurred_at is None:
            raise _CashFlowWindowInvalidated("date-only cash flow requires a stable provider ID")
        identity = f"fallback:v1:{_fallback_hash(item)}"
        version = CASH_FLOW_FALLBACK_VERSION
        identity_quality = "FALLBACK_PARTIAL"
    return NormalizedCashFlow(
        provider_cash_flow_id=identity,
        identity_version=version,
        identity_quality=identity_quality,
        currency=item.currency,
        amount=item.amount,
        occurred_at=item.occurred_at,
        provider_business_date=business_date,
        kind=item.kind,
        source_request_id=source_request_id,
        response_as_of=response_as_of,
        business_type=item.business_type,
        product_code=item.product_code,
    )


def _fallback_hash(item: BrokerCashFlow) -> str:
    if item.occurred_at is None:
        raise _CashFlowWindowInvalidated("fallback cash-flow identity requires an event time")
    amount = "0" if item.amount == 0 else format(item.amount.normalize(), "f")
    occurred_at = item.occurred_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload = json.dumps(
        {
            "version": CASH_FLOW_FALLBACK_VERSION,
            "currency": item.currency,
            "amount": amount,
            "occurred_at": occurred_at,
            "kind": item.kind,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

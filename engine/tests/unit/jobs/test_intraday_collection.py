"""Closed intraday collection handler result mapping."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest
from nexus.jobs.intraday_collection import build_intraday_collection_registry
from nexus.jobs.intraday_schedule import bucket_for, build_intraday_payload
from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from nexus.jobs.models import JobType
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import ClaimedJob
from nexus.jobs.worker import UnsafeJobError
from nexus.providers.base import InstrumentId, MarketDataProvenance
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.fosun import (
    FosunSnapshotReadAborted,
    FosunSnapshotReadGuard,
)
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    MarketSnapshot,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotQuote,
    SnapshotTrade,
)
from nexus.providers.settings import ProviderSettings

NOW = datetime(2026, 8, 28, 14, 30, 31, tzinfo=UTC)
INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
CALENDAR_ID = UUID("019d0000-0000-7000-8000-0000000000c1")


@dataclass
class _Provider:
    calls: int = 0
    snapshot: MarketSnapshot | None = None
    raw_payload: bytes | None = None

    async def fetch_raw_snapshots(
        self,
        request: IntradaySnapshotRequest,
        *,
        read_guard: FosunSnapshotReadGuard | None = None,
    ) -> RawSnapshotEnvelope:
        self.calls += 1
        payload = self.raw_payload
        if payload is None:
            payload = (
                b'{"AAPL":{"latestTrade":{"p":100,"t":"2026-08-28T14:30:31Z"},'
                b'"latestQuote":{"bp":99.99,"ap":100.01,'
                b'"t":"2026-08-28T14:30:31Z"}}}'
                if self.snapshot is not None
                else b'{"AAPL":{}}'
            )
        envelope = RawSnapshotEnvelope(
            raw_payload=payload,
            received_at=NOW,
            provenance=MarketDataProvenance(
                source="fosun",
                feed="l1",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="unknown",
            ),
        )
        if read_guard is not None:
            codes = tuple(f"us{item.symbol}" for item in request.instruments)
            if request.include_minute_bars:
                for code in codes:
                    bar_codes = (code,)
                    assert await read_guard.before_read("fosun_get_bars", bar_codes)
                    bar_response = RawGatewayResponse(
                        operation="fosun_get_bars",
                        gateway_request_id=UUID(
                            "019d0000-0000-7000-8000-0000000000f2"
                        ),
                        request_body_sha256="b" * 64,
                        raw_payload=payload,
                        sent_at=NOW - timedelta(milliseconds=250),
                        received_at=NOW,
                    )
                    assert await read_guard.after_read(
                        "fosun_get_bars", bar_codes, bar_response
                    )
            if not await read_guard.before_read("fosun_get_quotes", codes):
                raise FosunSnapshotReadAborted(
                    "INTRADAY_AUTHORITY_CHANGED_BEFORE_FETCH",
                    None,
                )
            response = RawGatewayResponse(
                operation="fosun_get_quotes",
                gateway_request_id=UUID("019d0000-0000-7000-8000-0000000000f1"),
                request_body_sha256="a" * 64,
                raw_payload=payload,
                sent_at=NOW - timedelta(milliseconds=250),
                received_at=NOW,
            )
            if not await read_guard.after_read("fosun_get_quotes", codes, response):
                raise FosunSnapshotReadAborted(
                    "INTRADAY_AUTHORITY_CHANGED_AFTER_FETCH",
                    envelope,
                )
        return envelope

    def normalize_raw_snapshots(
        self,
        envelope: RawSnapshotEnvelope,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch:
        if self.raw_payload == b'{"AAPL":{},"AAPL":{}}':
            raise MarketProviderPayloadError("FOSUN_QUOTE_CONTRACT_INVALID")
        snapshot = self.snapshot or MarketSnapshot(
            instrument_id=request.instruments[0].instrument_id,
            symbol=request.instruments[0].symbol,
            trade=None,
            quote=None,
            minute_bar=None,
        )
        return SnapshotBatch(
            snapshots=(snapshot,),
            raw_payload=envelope.raw_payload,
            received_at=envelope.received_at,
            provenance=envelope.provenance,
        )


async def _archive_raw(*_: object, **__: object) -> object:
    return SimpleNamespace(raw_artifact=SimpleNamespace())


def _context(payload: object, *, now: datetime = NOW) -> JobHandlerContext:
    claim = cast(
        ClaimedJob,
        SimpleNamespace(
            effective_at=bucket_for(now).start,
        ),
    )
    return JobHandlerContext(
        session=cast(object, SimpleNamespace()),
        claim=claim,
        payload=cast(object, payload),
        _clock=lambda: now,
    )


def _payload(
    *,
    include_minute_bars: bool = False,
    at: datetime = NOW,
) -> object:
    bucket_time = at.replace(second=1) if include_minute_bars else at
    return build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(bucket_time),
        instruments=((INSTRUMENT_ID, "AAPL"),),
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=(
            _authority("fosun_get_bars") if include_minute_bars else None
        ),
    )


def _settings() -> ProviderSettings:
    return ProviderSettings(
        runtime_mode="production",
        intraday_monitor_mode="production",
        fosun_market_gateway_origin="https://fosun-gateway.internal:8443",
        fosun_market_gateway_key_id="synthetic-market-current",
        fosun_market_gateway_secret="synthetic-market-secret",
        fosun_market_gateway_client_cert="/synthetic/market.crt",
        fosun_market_gateway_client_key="/synthetic/market.key",
        fosun_market_gateway_client_ca="/synthetic/ca.crt",
        sec_contact="owner@example.com",
        fred_api_key="fred-canary",
    )


def _authority(operation: str) -> FrozenFosunMarketAuthority:
    return FrozenFosunMarketAuthority(
        operation=operation,
        delivery_mode="unknown",
        capability_id=UUID(
            "019d0000-0000-7000-8000-0000000000e1"
            if operation == "fosun_get_quotes"
            else "019d0000-0000-7000-8000-0000000000e2"
        ),
        capability_schema_version="1",
        capability_created_at=datetime(2026, 8, 28, 13, tzinfo=UTC),
        capability_expires_at=datetime(2026, 8, 29, 13, tzinfo=UTC),
        status_id=UUID("019d0000-0000-7000-8000-0000000000e0"),
        status_version=1,
        status_created_at=datetime(2026, 8, 28, 12, tzinfo=UTC),
        status_mode_changed_at=datetime(2026, 8, 28, 12, tzinfo=UTC),
        capability_invalidated_at=None,
        contract_hash="a" * 64,
    )


async def _authority_valid(*_: object, **__: object) -> bool:
    return True


@pytest.mark.asyncio
async def test_collection_archives_raw_when_all_normalized_facts_are_missing() -> None:
    provider = _Provider()
    ingested: list[SnapshotBatch] = []

    async def record_ingest(
        _session: object,
        _store: object,
        batch: SnapshotBatch,
        **__: object,
    ) -> object:
        ingested.append(batch)
        return SimpleNamespace(
            created_trade_observations=0,
            created_quote_observations=0,
            created_bar_authorities=0,
            replayed=0,
        )

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=record_ingest,
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_payload()))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_SNAPSHOT_PARTIAL"
    assert provider.calls == 1
    assert len(ingested) == 1
    assert ingested[0].raw_payload == b'{"AAPL":{}}'


@pytest.mark.asyncio
async def test_expired_bucket_stops_before_provider_construction() -> None:
    constructions = 0

    def factory(_session: object) -> _Provider:
        nonlocal constructions
        constructions += 1
        return _Provider()

    registry = build_intraday_collection_registry(
        factory,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None
    expired = datetime(2026, 8, 28, 14, 31, 31, tzinfo=UTC)

    result = await spec.handler(_context(_payload(), now=expired))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_BUCKET_EXPIRED"
    assert constructions == 0


@pytest.mark.asyncio
async def test_authority_mismatch_stops_before_provider_construction() -> None:
    constructions = 0

    def factory(_session: object) -> _Provider:
        nonlocal constructions
        constructions += 1
        return _Provider()

    async def authority_invalid(*_: object, **__: object) -> bool:
        return False

    registry = build_intraday_collection_registry(
        factory,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=_archive_raw,
        revalidate=authority_invalid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    with pytest.raises(UnsafeJobError, match="INTRADAY_AUTHORITY_MISMATCH"):
        await spec.handler(_context(_payload()))

    assert constructions == 0


@pytest.mark.asyncio
async def test_any_missing_snapshot_category_is_partial_after_ingesting_available_facts() -> None:
    snapshot = MarketSnapshot(
        instrument_id=InstrumentId(INSTRUMENT_ID),
        symbol="AAPL",
        trade=SnapshotTrade(price=Decimal("100"), observed_at=NOW),
        quote=SnapshotQuote(bid=Decimal("99.99"), ask=Decimal("100.01"), observed_at=NOW),
        minute_bar=None,
    )
    provider = _Provider(snapshot=snapshot)
    ingestions = 0

    async def record_ingest(*_: object, **__: object) -> object:
        nonlocal ingestions
        ingestions += 1
        return SimpleNamespace(
            created_trade_observations=1,
            created_quote_observations=1,
            created_bar_authorities=0,
            replayed=0,
        )

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=record_ingest,
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_payload()))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_SNAPSHOT_PARTIAL"
    assert ingestions == 1


@pytest.mark.asyncio
async def test_missing_completed_minute_bar_does_not_bind_unpublished_bar() -> None:
    provider = _Provider(
        snapshot=MarketSnapshot(
            instrument_id=InstrumentId(INSTRUMENT_ID),
            symbol="AAPL",
            trade=SnapshotTrade(price=Decimal("100"), observed_at=NOW),
            quote=None,
            minute_bar=None,
        )
    )
    observed_bar_bindings: list[object] = []

    async def record_ingest(*_: object, **kwargs: object) -> object:
        observed_bar_bindings.append(kwargs["bar_bindings"])
        return SimpleNamespace(
            created_trade_observations=1,
            created_quote_observations=0,
            created_bar_authorities=0,
            replayed=0,
        )

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=record_ingest,
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    boundary_now = NOW.replace(second=1)
    result = await spec.handler(
        _context(
            _payload(include_minute_bars=True, at=boundary_now),
            now=boundary_now,
        )
    )

    assert result.result_code == "INTRADAY_SNAPSHOT_PARTIAL"
    assert observed_bar_bindings == [{}]


@pytest.mark.asyncio
async def test_authority_drift_after_fetch_archives_raw_without_publishing_facts() -> None:
    provider = _Provider(
        snapshot=MarketSnapshot(
            instrument_id=InstrumentId(INSTRUMENT_ID),
            symbol="AAPL",
            trade=SnapshotTrade(price=Decimal("100"), observed_at=NOW),
            quote=SnapshotQuote(
                bid=Decimal("99.99"),
                ask=Decimal("100.01"),
                observed_at=NOW,
            ),
            minute_bar=None,
        )
    )
    validations = iter((True, True, False))
    ingested: list[SnapshotBatch] = []

    async def revalidate(*_: object, **__: object) -> bool:
        return next(validations)

    async def record_ingest(
        _session: object,
        _store: object,
        batch: SnapshotBatch,
        **__: object,
    ) -> object:
        ingested.append(batch)
        return SimpleNamespace(
            created_trade_observations=0,
            created_quote_observations=0,
            created_bar_authorities=0,
            replayed=0,
        )

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=record_ingest,
        archive_raw=_archive_raw,
        revalidate=revalidate,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_payload()))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_AUTHORITY_CHANGED_AFTER_FETCH"
    assert ingested == []


@pytest.mark.asyncio
async def test_malformed_snapshot_is_archived_before_stable_skip() -> None:
    provider = _Provider(raw_payload=b'{"AAPL":{},"AAPL":{}}')
    archived: list[bytes] = []
    ingestions = 0

    async def archive(
        _session: object,
        _store: object,
        envelope: RawSnapshotEnvelope,
        **__: object,
    ) -> object:
        archived.append(envelope.raw_payload)
        return SimpleNamespace(raw_artifact=SimpleNamespace())

    async def ingest(*_: object, **__: object) -> object:
        nonlocal ingestions
        ingestions += 1
        raise AssertionError("malformed facts must not reach normalized ingestion")

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=ingest,
        archive_raw=archive,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_payload()))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_SNAPSHOT_PAYLOAD_INVALID"
    assert archived == [provider.raw_payload]
    assert ingestions == 0

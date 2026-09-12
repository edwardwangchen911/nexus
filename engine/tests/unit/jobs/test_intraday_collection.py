"""Closed intraday collection handler result mapping."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import nexus.jobs.intraday_collection as intraday_collection_module
import pytest
from nexus.jobs import intraday_schedule
from nexus.jobs.intraday_collection import build_intraday_collection_registry
from nexus.jobs.intraday_schedule import (
    bucket_for,
    build_alpaca_intraday_payload,
    build_intraday_payload,
    build_schwab_intraday_payload,
)
from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from nexus.jobs.models import JobType, RetryPolicy
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import ClaimedJob
from nexus.jobs.worker import UnsafeJobError
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar
from nexus.providers.http import HttpPolicyError
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
    SnapshotMinuteBar,
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
                        gateway_request_id=UUID("019d0000-0000-7000-8000-0000000000f2"),
                        request_body_sha256="b" * 64,
                        raw_payload=payload,
                        sent_at=NOW - timedelta(milliseconds=250),
                        received_at=NOW,
                    )
                    assert await read_guard.after_read("fosun_get_bars", bar_codes, bar_response)
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


@dataclass
class _AlpacaProvider:
    calls: int = 0
    snapshot: MarketSnapshot | None = None

    async def fetch_raw_snapshots(
        self,
        request: IntradaySnapshotRequest,
    ) -> RawSnapshotEnvelope:
        self.calls += 1
        return RawSnapshotEnvelope(
            raw_payload=b'{"AAPL":{}}',
            received_at=NOW,
            provenance=MarketDataProvenance(
                source="alpaca",
                feed=request.feed,
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode=request.delivery_mode,
            ),
        )

    def normalize_raw_snapshots(
        self,
        envelope: RawSnapshotEnvelope,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch:
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


@dataclass
class _UnavailableAlpacaProvider(_AlpacaProvider):
    error_code: str = "RETRY_BUDGET_EXHAUSTED"

    async def fetch_raw_snapshots(
        self,
        request: IntradaySnapshotRequest,
    ) -> RawSnapshotEnvelope:
        del request
        self.calls += 1
        raise HttpPolicyError(self.error_code)


@dataclass
class _SchwabProvider(_AlpacaProvider):
    async def fetch_raw_snapshots(
        self,
        request: IntradaySnapshotRequest,
    ) -> RawSnapshotEnvelope:
        self.calls += 1
        return RawSnapshotEnvelope(
            raw_payload=b'{"schema":"schwab_level_one_stream_v1"}',
            received_at=NOW,
            provenance=MarketDataProvenance(
                source="schwab",
                feed="l1",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="real_time",
            ),
        )

    def normalize_raw_snapshots(
        self,
        envelope: RawSnapshotEnvelope,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch:
        assert self.snapshot is not None
        return SnapshotBatch(
            snapshots=(self.snapshot,),
            raw_payload=envelope.raw_payload,
            received_at=envelope.received_at,
            provenance=envelope.provenance,
        )


async def _archive_raw(*_: object, **__: object) -> object:
    return SimpleNamespace(raw_artifact=SimpleNamespace())


def _context(
    payload: object,
    *,
    now: datetime = NOW,
    attempt_no: int = 1,
    effective_at: datetime | None = None,
) -> JobHandlerContext:
    class _Session:
        async def execute(self, _statement: object, _params: object) -> None:
            return None

    claim = cast(
        ClaimedJob,
        SimpleNamespace(
            effective_at=effective_at or bucket_for(now).start,
            attempt_no=attempt_no,
        ),
    )
    return JobHandlerContext(
        session=cast(object, _Session()),
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
        bars_authority=(_authority("fosun_get_bars") if include_minute_bars else None),
    )


def _alpaca_payload() -> object:
    return build_alpaca_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(NOW),
        instruments=((INSTRUMENT_ID, "AAPL"),),
        feed="iex",
        delivery_mode="real_time",
    )


def _schwab_payload() -> object:
    return build_schwab_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(NOW),
        instruments=(
            (INSTRUMENT_ID, "AAPL"),
            (UUID("019d0000-0000-7000-8000-0000000000a2"), "MSFT"),
        ),
    )


@pytest.mark.asyncio
async def test_schwab_requested_symbol_subset_is_ingested_then_reported_partial() -> None:
    provider = _SchwabProvider(
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
    ingested: list[SnapshotBatch] = []

    async def ingest(*args: object, **kwargs: object) -> object:
        del kwargs
        ingested.append(cast(SnapshotBatch, args[2]))
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
        ingest=ingest,
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_schwab_payload()))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_SNAPSHOT_PARTIAL"
    assert [item.symbol for item in ingested[0].snapshots] == ["AAPL"]


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


def _alpaca_settings() -> ProviderSettings:
    return ProviderSettings(
        runtime_mode="production",
        market_provider="alpaca",
        alpaca_market_mode="enabled",
        alpaca_key_id="key-id-canary",
        alpaca_secret_key="secret-canary",
        alpaca_feed="iex",
        alpaca_delivery_mode="real_time",
        intraday_monitor_mode="production",
        intraday_max_symbols=30,
        alpaca_intraday_feed="iex",
        alpaca_intraday_delivery_mode="real_time",
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


def test_collection_registry_is_one_shot() -> None:
    registry = build_intraday_collection_registry(
        lambda _session: _Provider(),
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )

    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)

    assert spec is not None
    assert spec.schema_version == "intraday_snapshot_job_v3"
    assert spec.payload_model is intraday_schedule.IntradaySnapshotJobPayloadV3
    assert spec.retry_policy is RetryPolicy.NEVER
    assert spec.max_attempts == 1


@pytest.mark.asyncio
async def test_fosun_fetch_timeout_archives_partial_evidence_before_terminal_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial = RawSnapshotEnvelope(
        raw_payload=b'{"partial":"first-completed-read"}',
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

    class _CancelledProvider(_Provider):
        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
            *,
            read_guard: FosunSnapshotReadGuard | None = None,
        ) -> RawSnapshotEnvelope:
            del request, read_guard
            self.calls += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise FosunSnapshotReadAborted(
                    "INTRADAY_SNAPSHOT_READ_CANCELLED",
                    partial,
                ) from None
            raise AssertionError("unreachable")

    provider = _CancelledProvider()
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
        raise AssertionError("cancelled evidence must never be normalized")

    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_FETCH_TIMEOUT_SECONDS",
        0.01,
    )
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
    assert result.result_code == "INTRADAY_SNAPSHOT_READ_CANCELLED"
    assert provider.calls == 1
    assert archived == [partial.raw_payload]
    assert ingestions == 0


@pytest.mark.asyncio
async def test_fosun_fetch_budget_includes_preflight_and_preserves_archive_reserve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial = RawSnapshotEnvelope(
        raw_payload=b'{"partial":"budgeted-read"}',
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

    class _CancelledProvider(_Provider):
        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
            *,
            read_guard: FosunSnapshotReadGuard | None = None,
        ) -> RawSnapshotEnvelope:
            del request, read_guard
            self.calls += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise FosunSnapshotReadAborted(
                    "INTRADAY_SNAPSHOT_READ_CANCELLED",
                    partial,
                ) from None
            raise AssertionError("unreachable")

    async def slow_preflight(*_: object, **__: object) -> bool:
        await asyncio.sleep(0.02)
        return True

    archived: list[bytes] = []

    async def slow_archive(
        _session: object,
        _store: object,
        envelope: RawSnapshotEnvelope,
        **__: object,
    ) -> object:
        await asyncio.sleep(0.045)
        archived.append(envelope.raw_payload)
        return SimpleNamespace(raw_artifact=SimpleNamespace())

    monkeypatch.setattr(intraday_collection_module, "_FOSUN_FETCH_TIMEOUT_SECONDS", 0.24)
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_INTERNAL_TIMEOUT_SECONDS",
        0.29,
        raising=False,
    )
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_SAFETY_MARGIN_SECONDS",
        0.01,
        raising=False,
    )
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_ABORT_ARCHIVE_TIMEOUT_SECONDS",
        0.05,
    )
    provider = _CancelledProvider()
    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=slow_archive,
        revalidate=slow_preflight,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    async with asyncio.timeout(0.30):
        result = await spec.handler(_context(_payload()))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_SNAPSHOT_READ_CANCELLED"
    assert provider.calls == 1
    assert archived == [partial.raw_payload]


@pytest.mark.asyncio
async def test_operator_cancellation_is_not_converted_to_terminal_skip() -> None:
    entered = asyncio.Event()

    class _CancelledProvider(_Provider):
        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
            *,
            read_guard: FosunSnapshotReadGuard | None = None,
        ) -> RawSnapshotEnvelope:
            del request, read_guard
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise FosunSnapshotReadAborted(
                    "INTRADAY_SNAPSHOT_READ_CANCELLED",
                    None,
                ) from None
            raise AssertionError("unreachable")

    registry = build_intraday_collection_registry(
        lambda _session: _CancelledProvider(),
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    task = asyncio.create_task(spec.handler(_context(_payload())))
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_same_tick_fetch_timeout_and_operator_cancel_propagates_operator_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial = RawSnapshotEnvelope(
        raw_payload=b'{"partial":"same-tick-cancel"}',
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

    class _CancelledProvider(_Provider):
        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
            *,
            read_guard: FosunSnapshotReadGuard | None = None,
        ) -> RawSnapshotEnvelope:
            del request, read_guard
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise FosunSnapshotReadAborted(
                    "INTRADAY_SNAPSHOT_READ_CANCELLED",
                    partial,
                ) from None
            raise AssertionError("unreachable")

    archived: list[bytes] = []

    async def archive(
        _session: object,
        _store: object,
        envelope: RawSnapshotEnvelope,
        **__: object,
    ) -> object:
        archived.append(envelope.raw_payload)
        return SimpleNamespace(raw_artifact=SimpleNamespace())

    real_timeout_at = asyncio.timeout_at
    operator_cancel_scheduled = False

    def timeout_at_with_same_tick_operator_cancel(when: float | None) -> asyncio.Timeout:
        nonlocal operator_cancel_scheduled
        if not operator_cancel_scheduled:
            task = asyncio.current_task()
            assert task is not None
            assert when is not None
            asyncio.get_running_loop().call_at(when, task.cancel)
            operator_cancel_scheduled = True
        return real_timeout_at(when)

    monkeypatch.setattr(intraday_collection_module, "_FOSUN_FETCH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        intraday_collection_module.asyncio,
        "timeout_at",
        timeout_at_with_same_tick_operator_cancel,
    )
    registry = build_intraday_collection_registry(
        lambda _session: _CancelledProvider(),
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=archive,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    task = asyncio.create_task(spec.handler(_context(_payload())))
    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled()
    assert archived == []


@pytest.mark.asyncio
@pytest.mark.parametrize("aborted", [False, True])
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_object_io_failure_preserves_stop_code_for_normal_and_aborted_archive(
    aborted: bool,
    error_code: str,
) -> None:
    envelope = RawSnapshotEnvelope(
        raw_payload=b'{"partial":"object-io-stop"}',
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

    class _ObjectIOProvider(_Provider):
        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
            *,
            read_guard: FosunSnapshotReadGuard | None = None,
        ) -> RawSnapshotEnvelope:
            del request, read_guard
            self.calls += 1
            if aborted:
                raise FosunSnapshotReadAborted(
                    "INTRADAY_AUTHORITY_REVALIDATION_FAILED",
                    envelope,
                )
            return envelope

        def normalize_raw_snapshots(
            self,
            raw: RawSnapshotEnvelope,
            request: IntradaySnapshotRequest,
        ) -> SnapshotBatch:
            del raw, request
            raise AssertionError("failed raw archive must not be normalized")

    async def unavailable_archive(*_: object, **__: object) -> object:
        raise ProviderObjectIOUnavailable(error_code)

    provider = _ObjectIOProvider()
    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=unavailable_archive,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    with pytest.raises(UnsafeJobError, match=error_code):
        await spec.handler(_context(_payload()))

    assert provider.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_object_io_failure_during_normalized_ingest_preserves_stop_code(
    error_code: str,
) -> None:
    provider = _Provider()
    archived: list[bytes] = []

    async def archive(
        _session: object,
        _store: object,
        envelope: RawSnapshotEnvelope,
        **__: object,
    ) -> object:
        archived.append(envelope.raw_payload)
        return SimpleNamespace(raw_artifact=SimpleNamespace())

    async def unavailable_ingest(*_: object, **__: object) -> object:
        raise ProviderObjectIOUnavailable(error_code)

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=unavailable_ingest,
        archive_raw=archive,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    with pytest.raises(UnsafeJobError, match=error_code):
        await spec.handler(_context(_payload()))

    assert provider.calls == 1
    assert archived == [b'{"AAPL":{}}']


@pytest.mark.asyncio
async def test_raw_archive_phase_deadline_is_terminal_before_worker_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_INTERNAL_TIMEOUT_SECONDS",
        0.06,
    )
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_SAFETY_MARGIN_SECONDS",
        0.005,
    )
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_ABORT_ARCHIVE_TIMEOUT_SECONDS",
        0.01,
    )
    provider = _Provider()

    async def blocked_archive(*_: object, **__: object) -> object:
        await asyncio.Event().wait()

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=blocked_archive,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    with pytest.raises(UnsafeJobError, match="INTRADAY_RAW_ARCHIVE_DEADLINE_EXCEEDED"):
        await spec.handler(_context(_payload()))

    assert provider.calls == 1


@pytest.mark.asyncio
async def test_ingest_phase_deadline_preserves_archived_raw_and_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_INTERNAL_TIMEOUT_SECONDS",
        0.06,
    )
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_SAFETY_MARGIN_SECONDS",
        0.005,
    )
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_ABORT_ARCHIVE_TIMEOUT_SECONDS",
        0.01,
    )
    provider = _Provider()
    archived: list[bytes] = []

    async def archive(
        _session: object,
        _store: object,
        envelope: RawSnapshotEnvelope,
        **__: object,
    ) -> object:
        archived.append(envelope.raw_payload)
        return SimpleNamespace(raw_artifact=SimpleNamespace())

    async def blocked_ingest(*_: object, **__: object) -> object:
        await asyncio.Event().wait()

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=blocked_ingest,
        archive_raw=archive,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    with pytest.raises(UnsafeJobError, match="INTRADAY_INGEST_DEADLINE_EXCEEDED"):
        await spec.handler(_context(_payload()))

    assert provider.calls == 1
    assert archived == [b'{"AAPL":{}}']


@pytest.mark.asyncio
async def test_operator_cancellation_during_ingest_is_not_phase_timeout() -> None:
    provider = _Provider()
    ingest_started = asyncio.Event()

    async def blocked_ingest(*_: object, **__: object) -> object:
        ingest_started.set()
        await asyncio.Event().wait()

    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        ingest=blocked_ingest,
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None
    task = asyncio.create_task(spec.handler(_context(_payload())))
    await ingest_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled()
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_reclaimed_attempt_is_terminal_without_provider_construction() -> None:
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

    result = await spec.handler(_context(_payload(), attempt_no=2))

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_ONE_SHOT_ALREADY_ATTEMPTED"
    assert constructions == 0


@pytest.mark.asyncio
async def test_claim_bucket_mismatch_is_terminal_without_provider_construction() -> None:
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

    result = await spec.handler(
        _context(
            _payload(),
            effective_at=bucket_for(NOW).start + timedelta(microseconds=1),
        )
    )

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_BUCKET_AUTHORITY_MISMATCH"
    assert constructions == 0


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
async def test_alpaca_collection_uses_plain_fetch_and_never_injects_fosun_bindings() -> None:
    provider = _AlpacaProvider()
    bindings: list[tuple[object, object, object]] = []

    async def record_ingest(*_: object, **kwargs: object) -> object:
        bindings.append(
            (
                kwargs["quote_binding"],
                kwargs.get("bar_binding"),
                kwargs["bar_bindings"],
            )
        )
        return SimpleNamespace(
            created_trade_observations=0,
            created_quote_observations=0,
            created_bar_authorities=0,
            replayed=0,
        )

    registry = build_intraday_collection_registry(
        lambda _session: cast(object, provider),
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_alpaca_settings(),
        ingest=record_ingest,
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_alpaca_payload()))

    assert result.result_code == "INTRADAY_SNAPSHOT_PARTIAL"
    assert provider.calls == 1
    assert bindings == [(None, None, None)]


@pytest.mark.asyncio
async def test_alpaca_non_bar_bucket_drops_provider_injected_minute_bar() -> None:
    observed = NOW.replace(second=0, microsecond=0) - timedelta(minutes=1)
    provider = _AlpacaProvider(
        snapshot=MarketSnapshot(
            instrument_id=InstrumentId(INSTRUMENT_ID),
            symbol="AAPL",
            trade=SnapshotTrade(price=Decimal("100"), observed_at=NOW),
            quote=SnapshotQuote(
                bid=Decimal("99.99"),
                ask=Decimal("100.01"),
                observed_at=NOW,
            ),
            minute_bar=SnapshotMinuteBar(
                bar=PriceBar(
                    instrument_id=InstrumentId(INSTRUMENT_ID),
                    session=NOW.date(),
                    open=Decimal("99.90"),
                    high=Decimal("100.10"),
                    low=Decimal("99.80"),
                    close=Decimal("100"),
                    volume=Decimal("1200"),
                    observed_at=observed,
                    available_at=NOW,
                    received_at=NOW,
                    revision="snapshot-v1",
                )
            ),
        )
    )
    ingested: list[SnapshotBatch] = []
    bindings: list[tuple[object, object, object]] = []

    async def record_ingest(
        _session: object,
        _store: object,
        batch: SnapshotBatch,
        **kwargs: object,
    ) -> object:
        ingested.append(batch)
        bindings.append(
            (
                kwargs["quote_binding"],
                kwargs.get("bar_binding"),
                kwargs["bar_bindings"],
            )
        )
        return SimpleNamespace(
            created_trade_observations=1,
            created_quote_observations=1,
            created_bar_authorities=0,
            replayed=0,
        )

    registry = build_intraday_collection_registry(
        lambda _session: cast(object, provider),
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_alpaca_settings(),
        ingest=record_ingest,
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    result = await spec.handler(_context(_alpaca_payload()))

    assert result.outcome == "succeeded"
    assert result.result_code == "INTRADAY_SNAPSHOT_COLLECTED"
    assert len(ingested) == 1
    assert ingested[0].snapshots[0].minute_bar is None
    assert bindings == [(None, None, None)]


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

    result = await spec.handler(
        _context(
            _payload(),
            now=expired,
            effective_at=bucket_for(NOW).start,
        )
    )

    assert result.outcome == "skipped"
    assert result.result_code == "INTRADAY_BUCKET_EXPIRED"
    assert constructions == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_code", "job_code"),
    (
        ("DEADLINE_EXCEEDED", "INTRADAY_PROVIDER_UNAVAILABLE"),
        ("RETRY_BUDGET_EXHAUSTED", "INTRADAY_PROVIDER_UNAVAILABLE"),
        ("STATUS_REJECTED", "INTRADAY_PROVIDER_STATUS_REJECTED"),
        ("RESPONSE_TOO_LARGE", "INTRADAY_PROVIDER_POLICY_REJECTED"),
    ),
)
async def test_alpaca_transport_failure_maps_to_a_safe_actionable_code(
    provider_code: str,
    job_code: str,
) -> None:
    """Sanitized provider failures must not collapse into generic HANDLER_FAILED."""

    provider = _UnavailableAlpacaProvider(error_code=provider_code)
    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_alpaca_settings(),
        archive_raw=_archive_raw,
        revalidate=_authority_valid,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    with pytest.raises(UnsafeJobError, match=job_code):
        await spec.handler(_context(_alpaca_payload()))

    assert provider.calls == 1


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
async def test_non_bar_bucket_with_trade_and_quote_is_complete() -> None:
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

    assert result.outcome == "succeeded"
    assert result.result_code == "INTRADAY_SNAPSHOT_COLLECTED"
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

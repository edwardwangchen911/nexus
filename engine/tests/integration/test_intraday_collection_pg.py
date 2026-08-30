"""PostgreSQL idempotency for intraday Snapshot scheduling."""

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import nexus.jobs.intraday_collection as intraday_collection_module
import pytest
from alembic import command
from alembic.config import Config
from nexus.jobs.intraday_collection import (
    _IntradayAuthorityGuard,
    build_intraday_collection_registry,
)
from nexus.jobs.intraday_schedule import (
    IntradaySnapshotJobPayloadV2,
    bucket_for,
    build_intraday_payload,
    revalidate_intraday_payload_authority,
    schedule_due_intraday_snapshot,
)
from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import claim_next_job, enqueue_job
from nexus.jobs.worker import run_job_worker_once
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    IntradayBarSetAuthority,
    PriceBarRecord,
    PriceSeriesManifest,
    ProviderCapability,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    Watchlist,
    WatchlistMember,
)
from nexus.persistence.models.runtime import IntegrationStatus, Job
from nexus.persistence.session import session_factory
from nexus.provider_worker import _assert_intraday_job_queue_drained
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar
from nexus.providers.market.fosun import (
    FosunSnapshotReadAborted,
    FosunSnapshotReadGuard,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    MarketSnapshot,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotInstrument,
    SnapshotMinuteBar,
    SnapshotQuote,
    SnapshotTrade,
)
from nexus.providers.settings import ProviderSettings
from nexus.settings import EngineSettings
from sqlalchemy import delete, func, select, update

NOW = datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC)
CALENDAR_ID = UUID("019d0000-0000-7000-8000-0000000000c1")
LATEST_CALENDAR_ID = UUID("019d0000-0000-7000-8000-0000000000c2")
WATCHLIST_ID = UUID("8c7cc45f-9420-5ea5-a75e-7df8a8a63b58")
AAPL_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
MSFT_ID = UUID("019d0000-0000-7000-8000-0000000000a2")


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
        operation=operation,  # type: ignore[arg-type]
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


@pytest.fixture(autouse=True)
def _frozen_provider_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the scheduler tests focused on their database/watchlist authority."""

    async def load(*_args: object, operation: str, **_kwargs: object):
        return _authority(operation)

    monkeypatch.setattr(
        "nexus.jobs.intraday_schedule.load_frozen_fosun_market_authority",
        load,
    )


async def _seed_schedule(database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=CALENDAR_ID,
                exchange="XNYS",
                version="intraday-v1",
                source="exchange_calendars",
                range_start=date(2026, 8, 28),
                range_end=date(2026, 8, 28),
                query_parameters={},
                completeness_status="complete",
                content_hash="c" * 64,
                published_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=date(2026, 8, 28),
                decision_at=datetime(2026, 8, 28, 21, tzinfo=UTC),
                market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                market_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
            )
        )
        session.add(Watchlist(id=WATCHLIST_ID, name="default_private"))
        for instrument_id, symbol in ((MSFT_ID, "MSFT"), (AAPL_ID, "AAPL")):
            session.add(
                Instrument(
                    id=instrument_id,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="large_cap",
                )
            )
            session.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                )
            )
            session.add(WatchlistMember(watchlist_id=WATCHLIST_ID, instrument_id=instrument_id))


@pytest.mark.asyncio
async def test_cancelled_partial_raw_commits_independently_of_handler_rollback(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    bucket = bucket_for(NOW)
    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket,
        instruments=((AAPL_ID, "AAPL"),),
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=None,
    )
    envelope = RawSnapshotEnvelope(
        raw_payload=b'{"partial":"committed-before-main-rollback"}',
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

    class _Store:
        async def archive_raw(self, key: str, body: bytes) -> str:
            assert body == envelope.raw_payload
            return f"memory://{key}"

    class _Provider:
        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
            *,
            read_guard: FosunSnapshotReadGuard | None = None,
        ) -> RawSnapshotEnvelope:
            del request, read_guard
            raise FosunSnapshotReadAborted(
                "INTRADAY_AUTHORITY_REVALIDATION_FAILED",
                envelope,
            )

        def normalize_raw_snapshots(
            self,
            raw: RawSnapshotEnvelope,
            request: IntradaySnapshotRequest,
        ) -> SnapshotBatch:
            del raw, request
            raise AssertionError("partial evidence must not be normalized")

    async def valid(*_args: object, **_kwargs: object) -> bool:
        return True

    registry = build_intraday_collection_registry(
        lambda _session: _Provider(),
        cast(object, _Store()),
        lambda: NOW,
        settings=_settings(),
        revalidate=valid,  # type: ignore[arg-type]
        raw_archive_session_factory=factory,
    )
    spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
    assert spec is not None

    with pytest.raises(RuntimeError, match="synthetic main rollback"):
        async with factory() as session, session.begin():
            result = await spec.handler(
                JobHandlerContext(
                    session=session,
                    claim=cast(
                        object,
                        SimpleNamespace(attempt_no=1, effective_at=bucket.start),
                    ),
                    payload=payload,
                    _clock=lambda: NOW,
                )
            )
            assert result.result_code == "INTRADAY_AUTHORITY_REVALIDATION_FAILED"
            raise RuntimeError("synthetic main rollback")

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("aborted", "error_code"),
    [
        (False, "OBJECT_IO_SATURATED"),
        (True, "OBJECT_IO_DEADLINE_EXCEEDED"),
    ],
)
async def test_intraday_worker_preserves_object_io_stop_code_without_retry(
    migrated_database_url: str,
    aborted: bool,
    error_code: str,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(session, now=NOW, settings=_settings())
    assert job is not None
    envelope = RawSnapshotEnvelope(
        raw_payload=b'{"partial":"object-io-worker-stop"}',
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

    class _Provider:
        calls = 0

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

    async def valid(*_args: object, **_kwargs: object) -> bool:
        return True

    provider = _Provider()
    registry = build_intraday_collection_registry(
        lambda _session: provider,
        cast(object, SimpleNamespace()),
        lambda: NOW,
        settings=_settings(),
        archive_raw=unavailable_archive,
        revalidate=valid,  # type: ignore[arg-type]
        raw_archive_session_factory=factory,
    )

    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.PROVIDER,
        worker_id=f"intraday-object-io-{aborted}",
        worker_started_at=NOW,
        process_version="test-v1",
        migration_head="0032_ticker_alias_authority",
        clock=lambda: NOW,
        claim_registered_only=True,
    )

    async with factory() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == error_code
        assert persisted.attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
    assert provider.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_intraday_worker_preserves_normalized_object_io_stop_code(
    migrated_database_url: str,
    error_code: str,
) -> None:
    boundary_now = NOW.replace(second=1, microsecond=0)
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(
            session,
            now=boundary_now,
            settings=_alpaca_settings(),
        )
    assert job is not None
    assert job.payload["include_minute_bars"] is True

    class _Provider:
        calls = 0

        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
        ) -> RawSnapshotEnvelope:
            self.calls += 1
            return RawSnapshotEnvelope(
                raw_payload=b'{"synthetic":"normalized-object-io-stop"}',
                received_at=boundary_now,
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
            observed_bar_at = boundary_now.replace(second=0) - timedelta(minutes=1)
            snapshots = tuple(
                MarketSnapshot(
                    instrument_id=item.instrument_id,
                    symbol=item.symbol,
                    trade=SnapshotTrade(
                        price=Decimal("100"),
                        observed_at=boundary_now,
                    ),
                    quote=SnapshotQuote(
                        bid=Decimal("99.99"),
                        ask=Decimal("100.01"),
                        observed_at=boundary_now,
                    ),
                    minute_bar=SnapshotMinuteBar(
                        bar=PriceBar(
                            instrument_id=item.instrument_id,
                            session=boundary_now.date(),
                            open=Decimal("99.90"),
                            high=Decimal("100.10"),
                            low=Decimal("99.80"),
                            close=Decimal("100"),
                            volume=Decimal("1200"),
                            observed_at=observed_bar_at,
                            available_at=boundary_now,
                            received_at=boundary_now,
                            revision="synthetic-v1",
                        )
                    ),
                )
                for item in request.instruments
            )
            return SnapshotBatch(
                snapshots=snapshots,
                raw_payload=envelope.raw_payload,
                received_at=envelope.received_at,
                provenance=envelope.provenance,
            )

    class _Store:
        async def archive_raw(self, key: str, payload: bytes) -> str:
            assert payload == b'{"synthetic":"normalized-object-io-stop"}'
            return f"memory://{key}"

        async def read_raw(self, key: str, *, uri: str) -> bytes:
            del key, uri
            raise AssertionError("normalized failure must not read raw bytes")

        async def put_normalized(self, key: str, payload: bytes) -> str:
            del key, payload
            raise ProviderObjectIOUnavailable(error_code)

        async def put_temporary(self, key: str, payload: bytes) -> None:
            del key, payload
            raise AssertionError("intraday ingestion must not create temporary objects")

        async def promote(self, temporary_key: str, final_key: str) -> str:
            del temporary_key, final_key
            raise AssertionError("intraday ingestion must not promote temporary objects")

        async def delete_temporary(self, key: str) -> None:
            del key
            raise AssertionError("intraday ingestion must not delete temporary objects")

    async def valid(*_args: object, **_kwargs: object) -> bool:
        return True

    provider = _Provider()
    registry = build_intraday_collection_registry(
        lambda _session: cast(object, provider),
        cast(object, _Store()),
        lambda: boundary_now,
        settings=_alpaca_settings(),
        revalidate=valid,  # type: ignore[arg-type]
        raw_archive_session_factory=factory,
    )

    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.PROVIDER,
        worker_id=f"intraday-normalized-object-io-{error_code.lower()}",
        worker_started_at=boundary_now,
        process_version="test-v1",
        migration_head="0032_ticker_alias_authority",
        clock=lambda: boundary_now,
        claim_registered_only=True,
    )

    async with factory() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == error_code
        assert persisted.attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_intraday_worker_terminalizes_internal_ingest_deadline_with_raw_only(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_INTERNAL_TIMEOUT_SECONDS",
        0.25,
    )
    monkeypatch.setattr(
        intraday_collection_module,
        "_FOSUN_HANDLER_SAFETY_MARGIN_SECONDS",
        0.02,
    )
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(
            session,
            now=NOW,
            settings=_alpaca_settings(),
        )
    assert job is not None

    class _Provider:
        calls = 0

        async def fetch_raw_snapshots(
            self,
            request: IntradaySnapshotRequest,
        ) -> RawSnapshotEnvelope:
            self.calls += 1
            return RawSnapshotEnvelope(
                raw_payload=b'{"synthetic":"internal-ingest-deadline"}',
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
            return SnapshotBatch(
                snapshots=tuple(
                    MarketSnapshot(
                        instrument_id=item.instrument_id,
                        symbol=item.symbol,
                        trade=None,
                        quote=None,
                        minute_bar=None,
                    )
                    for item in request.instruments
                ),
                raw_payload=envelope.raw_payload,
                received_at=envelope.received_at,
                provenance=envelope.provenance,
            )

    class _Store:
        async def archive_raw(self, key: str, payload: bytes) -> str:
            assert payload == b'{"synthetic":"internal-ingest-deadline"}'
            return f"memory://{key}"

        async def read_raw(self, key: str, *, uri: str) -> bytes:
            del key, uri
            raise AssertionError("ingest timeout must not read raw bytes")

        async def put_normalized(self, key: str, payload: bytes) -> str:
            del key, payload
            raise AssertionError("blocked ingest must not write normalized objects")

        async def put_temporary(self, key: str, payload: bytes) -> None:
            del key, payload
            raise AssertionError("temporary objects are disabled")

        async def promote(self, temporary_key: str, final_key: str) -> str:
            del temporary_key, final_key
            raise AssertionError("temporary promotion is disabled")

        async def delete_temporary(self, key: str) -> None:
            del key
            raise AssertionError("temporary deletion is disabled")

    async def valid(*_args: object, **_kwargs: object) -> bool:
        return True

    async def blocked_ingest(*_args: object, **_kwargs: object) -> object:
        await asyncio.Event().wait()

    provider = _Provider()
    registry = build_intraday_collection_registry(
        lambda _session: cast(object, provider),
        cast(object, _Store()),
        lambda: NOW,
        settings=_alpaca_settings(),
        ingest=blocked_ingest,
        revalidate=valid,  # type: ignore[arg-type]
        raw_archive_session_factory=factory,
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.PROVIDER,
        worker_id="intraday-internal-ingest-deadline",
        worker_started_at=NOW,
        process_version="test-v1",
        migration_head="0032_ticker_alias_authority",
        clock=lambda: NOW,
        claim_registered_only=True,
    )

    async with factory() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "INTRADAY_INGEST_DEADLINE_EXCEEDED"
        assert persisted.attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_schedule_enqueues_one_canonical_job_on_replay(
    migrated_database_url: str,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def schedule() -> Job | None:
        async with factory() as session, session.begin():
            return await schedule_due_intraday_snapshot(session, now=NOW, settings=_settings())

    first, replay = await asyncio.gather(schedule(), schedule())

    assert first is not None and replay is not None
    assert first.id == replay.id
    assert first.priority == 290
    assert first.retry_policy == RetryPolicy.NEVER.value
    assert first.max_attempts == 1
    assert first.payload["instruments"] == [
        {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
        {"instrument_id": str(MSFT_ID), "symbol": "MSFT"},
    ]
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1


@pytest.mark.asyncio
async def test_schedule_reuses_first_frozen_job_when_watchlist_changes_within_bucket(
    migrated_database_url: str,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        first = await schedule_due_intraday_snapshot(
            session,
            now=NOW,
            settings=_settings(),
        )
    assert first is not None

    async with factory() as session, session.begin():
        await session.execute(
            delete(WatchlistMember).where(WatchlistMember.instrument_id == MSFT_ID)
        )

    async with factory() as session, session.begin():
        replay = await schedule_due_intraday_snapshot(
            session,
            now=NOW + timedelta(milliseconds=500),
            settings=_settings(),
        )

    assert replay is not None
    assert replay.id == first.id
    assert replay.payload == first.payload
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1


@pytest.mark.asyncio
async def test_ready_intraday_claim_preempts_slow_daily_shard_queue(
    migrated_database_url: str,
) -> None:
    """A ready 15-second snapshot must not expire behind the 49-child daily train."""
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        intraday = await schedule_due_intraday_snapshot(
            session,
            now=NOW,
            settings=_settings(),
        )
        daily = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.COLLECT_FOSUN_DAILY_INSTRUMENT,
                scope_kind="instrument",
                scope_id=str(AAPL_ID),
                effective_at=NOW - timedelta(seconds=7),
                input_hash="f" * 64,
            ),
            schema_version="fosun_daily_instrument_v2",
            payload={"synthetic": "ready-daily-shard"},
            queue=QueueName.PROVIDER,
            retry_policy=RetryPolicy.NEVER,
            max_attempts=1,
            available_at=NOW - timedelta(seconds=7),
        )
    assert intraday is not None
    assert daily.priority == 280

    async with factory() as session, session.begin():
        claimed = await claim_next_job(
            session,
            queue=QueueName.PROVIDER,
            worker_id="intraday-fairness-probe",
            now=NOW,
        )

    assert claimed is not None
    assert claimed.job_id == intraday.id


@pytest.mark.asyncio
async def test_schedule_enqueues_explicit_alpaca_v2_without_fosun_authority(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_schedule(migrated_database_url)

    async def forbidden_fosun_authority(*_: object, **__: object) -> object:
        raise AssertionError("explicit Alpaca scheduling must not load Fosun authority")

    monkeypatch.setattr(
        "nexus.jobs.intraday_schedule.load_frozen_fosun_market_authority",
        forbidden_fosun_authority,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(
            session,
            now=NOW,
            settings=_alpaca_settings(),
        )

    assert job is not None
    assert job.schema_version == "intraday_snapshot_job_v2"
    assert job.scope_id == "alpaca-intraday"
    assert job.payload["source"] == "alpaca"
    assert job.payload["feed"] == "iex"
    assert job.payload["quote_authority"] is None
    assert job.payload["bars_authority"] is None


@pytest.mark.asyncio
async def test_schedule_selects_latest_complete_overlapping_calendar_authority(
    migrated_database_url: str,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=LATEST_CALENDAR_ID,
                exchange="XNYS",
                version="intraday-v2",
                source="exchange_calendars",
                range_start=date(2026, 8, 28),
                range_end=date(2026, 8, 28),
                query_parameters={"revision": 2},
                completeness_status="complete",
                content_hash="d" * 64,
                published_at=datetime(2026, 8, 2, tzinfo=UTC),
            )
        )
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=LATEST_CALENDAR_ID,
                session=date(2026, 8, 28),
                decision_at=datetime(2026, 8, 28, 21, tzinfo=UTC),
                market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                market_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
            )
        )

    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(session, now=NOW, settings=_settings())

    assert job is not None
    assert job.payload["calendar_authority_id"] == str(LATEST_CALENDAR_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "now",
    (
        datetime(2026, 8, 28, 13, 29, 59, tzinfo=UTC),
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
    ),
)
async def test_schedule_noops_before_open_and_at_half_open_close(
    migrated_database_url: str,
    now: datetime,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(
            session,
            now=now,
            settings=_settings(),
        )
    assert job is None
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0


@pytest.mark.asyncio
async def test_execution_authority_revalidation_detects_watchlist_drift(
    migrated_database_url: str,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(session, now=NOW, settings=_settings())
    assert job is not None
    payload = IntradaySnapshotJobPayloadV2.model_validate(job.payload)

    async with factory() as session, session.begin():
        assert await revalidate_intraday_payload_authority(
            session,
            payload,
            effective_at=job.effective_at,
            settings=_settings(),
        )

    async with factory() as session, session.begin():
        await session.execute(
            delete(WatchlistMember).where(WatchlistMember.instrument_id == MSFT_ID)
        )

    async with factory() as session, session.begin():
        assert not await revalidate_intraday_payload_authority(
            session,
            payload,
            effective_at=job.effective_at,
            settings=_settings(),
        )


@pytest.mark.asyncio
async def test_intraday_read_guard_holds_status_revocation_lock_across_the_call_boundary(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    status_id = _authority("fosun_get_quotes").status_id
    async with factory() as session, session.begin():
        session.add(
            IntegrationStatus(
                id=status_id,
                provider="fosun",
                status="read_only",
                last_success_at=NOW,
                version=1,
                mode_changed_at=NOW,
                capability_invalidated_at=None,
                admitted_contract_hash="a" * 64,
            )
        )

    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(NOW),
        instruments=((AAPL_ID, "AAPL"),),
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=None,
    )
    request = IntradaySnapshotRequest(
        instruments=(
            SnapshotInstrument(
                instrument_id=InstrumentId(AAPL_ID),
                symbol="AAPL",
            ),
        ),
        symbol_as_of="2026-08-28",
        bucket_start=None,
        include_minute_bars=False,
        feed="l1",
        delivery_mode="unknown",
    )
    revocation_applied = asyncio.Event()

    async def valid(*_args: object, **_kwargs: object) -> bool:
        return True

    async def revoke() -> None:
        async with factory() as session, session.begin():
            await session.execute(
                update(IntegrationStatus)
                .where(IntegrationStatus.id == status_id)
                .values(
                    status="disabled",
                    version=2,
                    mode_changed_at=NOW,
                    capability_invalidated_at=NOW,
                    admitted_contract_hash=None,
                )
            )
            revocation_applied.set()

    async with factory() as session, session.begin():
        guard = _IntradayAuthorityGuard(
            context=JobHandlerContext(
                session=session,
                claim=cast(object, SimpleNamespace()),
                payload=payload,
                _clock=lambda: NOW,
            ),
            payload=payload,
            settings=_settings(),
            revalidate=valid,  # type: ignore[arg-type]
            request=request,
        )
        assert await guard.before_read("fosun_get_quotes", ("usAAPL",))
        revocation = asyncio.create_task(revoke())
        await asyncio.sleep(0.05)
        assert not revocation_applied.is_set()

    await asyncio.wait_for(revocation, timeout=2)
    assert revocation_applied.is_set()


@pytest.mark.asyncio
async def test_intraday_read_guard_holds_capability_scope_lock_across_the_call_boundary(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(NOW),
        instruments=((AAPL_ID, "AAPL"),),
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=None,
    )
    request = IntradaySnapshotRequest(
        instruments=(
            SnapshotInstrument(
                instrument_id=InstrumentId(AAPL_ID),
                symbol="AAPL",
            ),
        ),
        symbol_as_of="2026-08-28",
        bucket_start=None,
        include_minute_bars=False,
        feed="l1",
        delivery_mode="unknown",
    )
    capability_applied = asyncio.Event()

    async def valid(*_args: object, **_kwargs: object) -> bool:
        return True

    async def insert_capability() -> None:
        async with factory() as session, session.begin():
            session.add(
                ProviderCapability(
                    id=UUID("019d0000-0000-7000-8000-0000000000f2"),
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash="b" * 64,
                    expires_at=NOW + timedelta(hours=2),
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_quotes",
                        "market_scope": "US",
                        "delivery_mode": "unknown",
                    },
                    created_at=NOW - timedelta(seconds=1),
                )
            )
        capability_applied.set()

    async with factory() as session, session.begin():
        guard = _IntradayAuthorityGuard(
            context=JobHandlerContext(
                session=session,
                claim=cast(object, SimpleNamespace()),
                payload=payload,
                _clock=lambda: NOW,
            ),
            payload=payload,
            settings=_settings(),
            revalidate=valid,  # type: ignore[arg-type]
            request=request,
        )
        assert await guard.before_read("fosun_get_quotes", ("usAAPL",))
        insertion = asyncio.create_task(insert_capability())
        await asyncio.sleep(0.05)
        assert not capability_applied.is_set()

    await asyncio.wait_for(insertion, timeout=2)
    assert capability_applied.is_set()


def test_v1_queue_drain_gate_does_not_claim_or_terminalize_legacy_job(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0030_fosun_daily_staging")

    async def seed_legacy() -> UUID:
        factory = session_factory(EngineSettings(database_url=temporary_database_url))
        legacy_payload = {"schema": "intraday_snapshot_job_v1"}
        async with factory() as session, session.begin():
            legacy = await enqueue_job(
                session,
                key=JobKeyV1(
                    job_type=JobType.COLLECT_INTRADAY_SNAPSHOT,
                    scope_kind="provider",
                    scope_id="alpaca-intraday",
                    effective_at=NOW,
                    input_hash=job_input_hash(legacy_payload),
                ),
                schema_version="intraday_snapshot_job_v1",
                payload=legacy_payload,
                queue=QueueName.PROVIDER,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
            )
            return legacy.id

    legacy_id = asyncio.run(seed_legacy())

    command.upgrade(config, "head")

    async def assert_legacy_unchanged() -> None:
        factory = session_factory(EngineSettings(database_url=temporary_database_url))
        async with factory() as session:
            with pytest.raises(
                RuntimeError,
                match="INTRADAY_V1_QUEUE_DRAIN_REQUIRED",
            ):
                await _assert_intraday_job_queue_drained(session)

        async with factory() as session:
            legacy = await session.get(Job, legacy_id)
            assert legacy is not None
            assert (legacy.status, legacy.attempt_count, legacy.last_error_code) == (
                "queued",
                0,
                None,
            )

    asyncio.run(assert_legacy_unchanged())

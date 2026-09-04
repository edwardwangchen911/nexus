"""PostgreSQL contracts for durable provider collection Jobs."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from datetime import time as time_of_day
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Literal, cast
from uuid import UUID

import nexus.jobs.provider_collection as provider_collection_module
import pytest
from nexus.jobs.daily_payloads import (
    DailyStagePayload,
    FosunDailyInstrumentPayload,
    FosunDailyPublishPayload,
)
from nexus.jobs.daily_pipeline import (
    build_daily_pipeline_registry,
    schedule_due_daily_pipeline,
)
from nexus.jobs.errors import RetryableJobError, UnsafeJobError
from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.provider_collection import (
    _require_frozen_instrument_alias,
    build_provider_collection_registry,
)
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult, JobRegistry
from nexus.jobs.repository import ClaimedJob
from nexus.jobs.worker import run_job_worker_once
from nexus.market.artifact_authority import data_artifact_identity_sha256
from nexus.market.database_series import load_daily_series
from nexus.market.ingestion import MarketObjectStore
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    FosunDailyBarStaging,
    MacroObservation,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
    ProviderCapability,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.research import FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.models.runtime import IntegrationStatus, Job
from nexus.persistence.session import session_factory
from nexus.providers.base import (
    BarsRequest,
    MarketDataProvenance,
    PriceBar,
    ProviderPage,
)
from nexus.providers.composition import ProviderBundle, build_provider_bundle
from nexus.providers.fred import FredRawDocument, FredSeriesClient
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.fosun_gateway import (
    RawGatewayResponse,
    fosun_bars_request_body_sha256,
)
from nexus.providers.sec import SecCompanyFactsClient, SecRawDocument
from nexus.providers.settings import ProviderRuntimeMode, ProviderSettings
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

SEC_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "sec"

NOW = datetime(2026, 8, 28, 21, tzinfo=UTC)
MARKET_CLOSE = datetime(2026, 8, 28, 20, tzinfo=UTC)
UNIVERSE_ID = UUID("0198d500-1000-7000-8000-000000002911")
INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000002912")
ROOT_JOB_ID = UUID("0198d500-1000-7000-8000-000000002913")
CLAIM_JOB_ID = UUID("0198d500-1000-7000-8000-000000002914")
LEASE_TOKEN = UUID("0198d500-1000-7000-8000-000000002915")
CALENDAR_ID = UUID("0198d500-1000-7000-8000-000000002916")


@dataclass
class InMemoryMarketObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)
    temporary: dict[str, bytes] = field(default_factory=dict)

    async def archive_raw(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        if uri != f"memory://{key}":
            raise ValueError("raw object URI does not match key")
        return self.objects[key]

    async def put_normalized(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"

    async def put_temporary(self, key: str, payload: bytes) -> None:
        self.temporary[key] = payload

    async def promote(self, temporary_key: str, final_key: str) -> str:
        self.objects.setdefault(final_key, self.temporary[temporary_key])
        return f"memory://{final_key}"

    async def delete_temporary(self, key: str) -> None:
        self.temporary.pop(key, None)


@dataclass
class UnavailableReadMarketObjectStore(InMemoryMarketObjectStore):
    error_code: str = "OBJECT_IO_SATURATED"

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        del key, uri
        raise ProviderObjectIOUnavailable(self.error_code)


@dataclass
class UnavailableNormalizedMarketObjectStore(InMemoryMarketObjectStore):
    error_code: str = "OBJECT_IO_SATURATED"

    async def put_normalized(self, key: str, payload: bytes) -> str:
        del key, payload
        raise ProviderObjectIOUnavailable(self.error_code)


@dataclass
class UnavailableArchiveMarketObjectStore(InMemoryMarketObjectStore):
    error_code: str = "OBJECT_IO_SATURATED"

    async def archive_raw(self, key: str, payload: bytes) -> str:
        del key, payload
        raise ProviderObjectIOUnavailable(self.error_code)


@dataclass
class BlockingReadMarketObjectStore(InMemoryMarketObjectStore):
    read_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_read: asyncio.Event = field(default_factory=asyncio.Event)

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        self.read_started.set()
        await self.release_read.wait()
        return await super().read_raw(key, uri=uri)


@dataclass
class VirtualDelayedMarketObjectStore(InMemoryMarketObjectStore):
    """Model object latency without making the 49-instrument test sleep."""

    operation_seconds: float = 0.2
    virtual_seconds: float = 0.0
    active_operations: int = 0
    max_active_operations: int = 0
    normalized_writes: int = 0
    fail_normalized_write_at: int | None = None

    async def _delay(self, *, operations: int = 1) -> None:
        started_at = self.virtual_seconds
        self.active_operations += 1
        self.max_active_operations = max(
            self.max_active_operations,
            self.active_operations,
        )
        try:
            await asyncio.sleep(0)
            self.virtual_seconds = max(
                self.virtual_seconds,
                started_at + self.operation_seconds * operations,
            )
        finally:
            self.active_operations -= 1

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        await self._delay()
        return await super().read_raw(key, uri=uri)

    async def put_temporary(self, key: str, payload: bytes) -> None:
        await self._delay()
        await super().put_temporary(key, payload)

    async def promote(self, temporary_key: str, final_key: str) -> str:
        await self._delay(operations=2)
        return await super().promote(temporary_key, final_key)

    async def delete_temporary(self, key: str) -> None:
        await self._delay()
        await super().delete_temporary(key)

    async def put_normalized(self, key: str, payload: bytes) -> str:
        # Model the slowest successful create-only replay: conditional PUT
        # conflicts, followed by one bounded GET that verifies exact bytes.
        await self._delay(operations=2)
        self.normalized_writes += 1
        if self.normalized_writes == self.fail_normalized_write_at:
            raise OSError("synthetic normalized object failure")
        self.objects.setdefault(key, payload)
        return f"memory://{key}"


@dataclass
class InMemoryRawEvidenceStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        self.objects.setdefault(key, content)
        return f"memory://sha256/{key.rsplit('/', 1)[-1]}"


@dataclass
class UnavailableRawEvidenceStore(InMemoryRawEvidenceStore):
    error_code: str = "OBJECT_IO_SATURATED"

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        del key, content
        raise ProviderObjectIOUnavailable(self.error_code)


@dataclass
class RecordingMarketProvider:
    calls: list[BarsRequest] = field(default_factory=list)

    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None = None
    ) -> ProviderPage[PriceBar]:
        assert cursor is None
        self.calls.append(request)
        bar = PriceBar(
            instrument_id=request.instrument_id,
            session=date(2026, 8, 28),
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=Decimal("1000"),
            observed_at=MARKET_CLOSE,
            available_at=NOW,
            received_at=NOW,
            revision="alpaca-r1",
        )
        return ProviderPage(
            items=(bar,),
            next_cursor=None,
            raw_payload=b'{"bars":[{"fixture":"bounded"}]}',
            source_request_id="alpaca-bounded-1",
            as_of=NOW,
            provenance=MarketDataProvenance(
                source="alpaca",
                feed="sip",
                adjustment="all",
                interval="1Day",
                mode="delayed",
                delivery_mode="delayed",
            ),
        )


@dataclass
class RecordingRawFosunProvider:
    calls: list[BarsRequest] = field(default_factory=list)
    daily_session_counts: list[int | None] = field(default_factory=list)
    legacy_request_limit: bool = False
    call_started_monotonic: list[float] = field(default_factory=list)
    normalized: int = 0
    received_at: datetime = NOW
    raw_archived: Callable[[], bool] | None = None
    allow_normalize: bool = False
    normalize_error: bool = False
    unexpected_normalize_error: bool = False
    unexpected_normalize_at: int | None = None
    delivery_mode: Literal["unknown", "real_time", "delayed"] = "unknown"
    bar_session: date = date(2026, 8, 28)
    bar_sessions: tuple[date, ...] | None = None
    distinct_raw_by_instrument: bool = False
    fetch_started: asyncio.Event | None = None
    fetch_release: asyncio.Event | None = None

    async def fetch_raw_bars(
        self,
        request: BarsRequest,
        cursor: str | None = None,
        *,
        symbol: str | None = None,
        daily_session_count: int | None = None,
    ) -> RawGatewayResponse:
        assert cursor is None
        assert request.interval == "1Day"
        assert request.adjustment == "raw"
        assert request.end - request.start == timedelta(days=400)
        self.call_started_monotonic.append(perf_counter())
        self.calls.append(request)
        self.daily_session_counts.append(daily_session_count)
        if self.fetch_started is not None:
            self.fetch_started.set()
        if self.fetch_release is not None:
            await self.fetch_release.wait()
        raw_payload = (
            json.dumps(
                {
                    "instrument_id": str(request.instrument_id),
                    "items": [],
                    "source_request_id": "safe",
                    "has_more": False,
                    "next_cursor": None,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            if self.distinct_raw_by_instrument
            else b'{"items":[],"source_request_id":"safe","has_more":false,"next_cursor":null}'
        )
        return RawGatewayResponse(
            operation="fosun_get_bars",
            gateway_request_id=UUID("019d0000-0000-7000-8000-0000000000f1"),
            request_body_sha256=await self.expected_bars_request_body_sha256(
                request,
                symbol=symbol,
                daily_session_count=500 if self.legacy_request_limit else daily_session_count,
            ),
            raw_payload=raw_payload,
            sent_at=self.received_at - timedelta(milliseconds=250),
            received_at=self.received_at,
        )

    async def expected_bars_request_body_sha256(
        self,
        request: BarsRequest,
        cursor: str | None = None,
        *,
        symbol: str | None = None,
        daily_session_count: int | None = None,
    ) -> str:
        assert cursor is None
        offset = int(request.instrument_id) - INSTRUMENT_ID.int
        assert symbol == ("AAPL" if offset == 0 else f"TST{offset}")
        return fosun_bars_request_body_sha256(
            code=f"us{symbol}",
            ktype="day",
            start_time=request.start,
            end_time=request.end,
            limit=daily_session_count or 1,
        )

    async def normalize_bars(
        self,
        request: BarsRequest,
        response: RawGatewayResponse,
        cursor: str | None = None,
        *,
        symbol: str | None = None,
    ) -> ProviderPage[PriceBar]:
        del cursor
        offset = int(request.instrument_id) - INSTRUMENT_ID.int
        assert symbol == ("AAPL" if offset == 0 else f"TST{offset}")
        self.normalized += 1
        if self.unexpected_normalize_error or self.unexpected_normalize_at == self.normalized:
            raise RuntimeError("synthetic normalize failure")
        if self.normalize_error:
            raise MarketProviderPayloadError("FOSUN_BAR_CONTRACT_INVALID")
        if not self.allow_normalize:
            raise AssertionError("authority drift must stop before normalization")
        assert self.raw_archived is not None and self.raw_archived()
        bars = tuple(
            PriceBar(
                instrument_id=request.instrument_id,
                session=session,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
                volume=Decimal("1000"),
                observed_at=datetime.combine(
                    session,
                    time_of_day(20),
                    tzinfo=UTC,
                ),
                available_at=self.received_at,
                received_at=self.received_at,
                revision="fosun-r1",
            )
            for session in self.bar_sessions or (self.bar_session,)
        )
        return ProviderPage(
            items=bars,
            next_cursor=None,
            raw_payload=response.raw_payload,
            source_request_id="safe",
            as_of=self.received_at,
            provenance=MarketDataProvenance(
                source="fosun",
                feed="l1",
                adjustment="raw",
                interval="1Day",
                mode="read_only",
                delivery_mode=self.delivery_mode,
            ),
        )


@dataclass
class FixtureSecClient:
    calls: int = 0

    async def fetch_company_facts_and_submission_history(
        self, cik: str
    ) -> tuple[SecRawDocument, tuple[SecRawDocument, ...]]:
        assert cik == "320193"
        self.calls += 1
        return (
            SecRawDocument(
                raw_payload=(SEC_FIXTURES / "companyfacts.json").read_bytes(),
                received_at=NOW,
            ),
            (
                SecRawDocument(
                    raw_payload=(SEC_FIXTURES / "submissions.json").read_bytes(),
                    received_at=NOW,
                ),
                SecRawDocument(
                    raw_payload=(SEC_FIXTURES / "submissions-001.json").read_bytes(),
                    received_at=NOW,
                ),
            ),
        )


@dataclass
class MalformedAccessionSecClient(FixtureSecClient):
    async def fetch_company_facts_and_submission_history(
        self, cik: str
    ) -> tuple[SecRawDocument, tuple[SecRawDocument, ...]]:
        company_facts, submissions = await super().fetch_company_facts_and_submission_history(cik)
        first = replace(
            submissions[0],
            raw_payload=submissions[0].raw_payload.replace(
                b"0000320193-26-000001",
                b"0000320193-26-000001?query",
            ),
        )
        return company_facts, (first, *submissions[1:])


@dataclass
class FixtureFredClient:
    calls: list[str] = field(default_factory=list)

    async def fetch_alfred_series(
        self, series: str, real_time_start: str, real_time_end: str
    ) -> FredRawDocument:
        assert real_time_start == real_time_end == "2026-08-28"
        self.calls.append(series)
        return FredRawDocument(
            raw_payload=json.dumps(
                {
                    "units": "percent" if series != "DEXHKUS" else "hkd_per_usd",
                    "observations": [
                        {
                            "date": "2026-08-27",
                            "realtime_start": "2026-08-28",
                            "realtime_end": "2026-08-28",
                            "revision": "2026-08-28",
                            "value": "4.25",
                        }
                    ],
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode(),
            received_at=NOW,
        )


def _bundle(
    provider: RecordingMarketProvider,
    *,
    sec_client: FixtureSecClient | None = None,
    fred_client: FixtureFredClient | None = None,
) -> ProviderBundle:
    fixture = build_provider_bundle(
        ProviderSettings(runtime_mode="fixture"),
        http_client_factory=lambda *_: pytest.fail("HTTP factory called"),
    )
    return ProviderBundle(
        mode=ProviderRuntimeMode.PRODUCTION,
        market_source="alpaca",
        market_provider_factory=lambda _session: provider,
        snapshot_provider_factory=None,
        sec_client=(
            fixture.sec_client if sec_client is None else cast(SecCompanyFactsClient, sec_client)
        ),
        fred_client=(
            fixture.fred_client if fred_client is None else cast(FredSeriesClient, fred_client)
        ),
    )


def _fosun_bundle(
    provider: RecordingRawFosunProvider,
    *,
    sec_client: FixtureSecClient | None = None,
    fred_client: FixtureFredClient | None = None,
) -> ProviderBundle:
    fixture = build_provider_bundle(
        ProviderSettings(runtime_mode="fixture"),
        http_client_factory=lambda *_: pytest.fail("HTTP factory called"),
    )
    return ProviderBundle(
        mode=ProviderRuntimeMode.PRODUCTION,
        market_source="fosun",
        market_provider_factory=lambda _session: cast(object, provider),  # type: ignore[arg-type]
        snapshot_provider_factory=None,
        sec_client=(
            fixture.sec_client if sec_client is None else cast(SecCompanyFactsClient, sec_client)
        ),
        fred_client=(
            fixture.fred_client if fred_client is None else cast(FredSeriesClient, fred_client)
        ),
    )


def _fosun_authority(
    *,
    checked_at: datetime = NOW,
    delivery_mode: Literal["unknown", "real_time", "delayed"] = "unknown",
) -> FrozenFosunMarketAuthority:
    return FrozenFosunMarketAuthority(
        operation="fosun_get_bars",
        delivery_mode=delivery_mode,
        capability_id=UUID("019d0000-0000-7000-8000-0000000000e1"),
        capability_schema_version="1",
        capability_created_at=checked_at - timedelta(hours=2),
        capability_expires_at=checked_at + timedelta(hours=2),
        status_id=UUID("019d0000-0000-7000-8000-0000000000e2"),
        status_version=1,
        status_created_at=checked_at - timedelta(hours=3),
        status_mode_changed_at=checked_at - timedelta(hours=3),
        capability_invalidated_at=None,
        contract_hash="a" * 64,
    )


def _add_fosun_authority_rows(
    session: AsyncSession,
    frozen: FrozenFosunMarketAuthority,
    *,
    last_success_at: datetime,
) -> None:
    session.add(
        ProviderCapability(
            id=frozen.capability_id,
            provider="fosun",
            capability="READ_ONLY",
            contract_hash=frozen.contract_hash,
            expires_at=frozen.capability_expires_at,
            schema_version="1",
            payload={
                "operation": "fosun_get_bars",
                "market_scope": "US",
                "delivery_mode": frozen.delivery_mode,
            },
            created_at=frozen.capability_created_at,
        )
    )
    session.add(
        IntegrationStatus(
            id=frozen.status_id,
            provider="fosun",
            status="read_only",
            last_success_at=last_success_at,
            version=frozen.status_version,
            mode_changed_at=frozen.status_mode_changed_at,
            capability_invalidated_at=None,
            admitted_contract_hash=frozen.contract_hash,
            created_at=frozen.status_created_at,
        )
    )


def _payload(
    *,
    content_hash: str = "a" * 64,
    market_source: str | None = "alpaca",
    market_authority: FrozenFosunMarketAuthority | None = None,
) -> DailyStagePayload:
    return DailyStagePayload(
        root_job_id=str(ROOT_JOB_ID),
        calendar_authority_id="0198d500-1000-7000-8000-000000002916",
        session="2026-08-28",
        market_close_at="2026-08-28T20:00:00.000000Z",
        scheduled_for="2026-08-28T20:30:00.000000Z",
        quality_cutoff_at="2026-08-28T22:00:00.000000Z",
        profile_id="local_safe_v1",
        target_id=None,
        universe_snapshot_id=str(UNIVERSE_ID),
        universe_content_hash=content_hash,
        market_source=market_source,
        market_authority=market_authority,
    )


def _context(
    session: AsyncSession,
    payload: DailyStagePayload,
    *,
    job_type: JobType = JobType.COLLECT_MARKET,
    clock: Callable[[], datetime] = lambda: NOW,
) -> JobHandlerContext:
    claim = ClaimedJob(
        job_id=CLAIM_JOB_ID,
        job_type=job_type.value,
        schema_version="daily_pipeline_stage_v1",
        payload=payload.model_dump(mode="json"),
        queue=QueueName.PROVIDER.value,
        priority=280,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE.value,
        max_attempts=3,
        attempt_no=1,
        effective_at=NOW,
        worker_id="provider-integration-worker",
        lease_token=LEASE_TOKEN,
        fence_token=1,
        expires_at=NOW,
        version=1,
        dependencies_satisfied=True,
        dependency_result_code=None,
    )
    return JobHandlerContext(
        session=session,
        claim=claim,
        payload=payload,
        _clock=clock,
    )


async def _seed_universe(
    session: AsyncSession,
    *,
    extra_instruments: int = 0,
) -> None:
    session.add(
        Instrument(
            id=INSTRUMENT_ID,
            asset_type="equity",
            exchange="NASDAQ",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="large_cap",
        )
    )
    session.add(
        InstrumentAlias(
            instrument_id=INSTRUMENT_ID,
            symbol="AAPL",
            alias_type="ticker",
            valid_from=date(2020, 1, 1),
            valid_to=None,
        )
    )
    session.add(
        InstrumentAlias(
            instrument_id=INSTRUMENT_ID,
            symbol="320193",
            alias_type="cik",
            valid_from=date(2020, 1, 1),
            valid_to=None,
        )
    )
    session.add(
        UniverseSnapshot(
            id=UNIVERSE_ID,
            key="us_liquid",
            version="provider-collection-v1",
            effective_from=date(2026, 8, 28),
            source="fixture",
            content_hash="a" * 64,
            published_at=None,
        )
    )
    await session.flush()
    session.add(
        UniverseMember(
            universe_snapshot_id=UNIVERSE_ID,
            instrument_id=INSTRUMENT_ID,
            inclusion_reason="fixture",
        )
    )
    for index in range(1, extra_instruments + 1):
        instrument_id = UUID(int=INSTRUMENT_ID.int + index)
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
            UniverseMember(
                universe_snapshot_id=UNIVERSE_ID,
                instrument_id=instrument_id,
                inclusion_reason="fixture",
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=instrument_id,
                symbol=f"TST{index}",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )
    await session.flush()
    await session.execute(
        update(UniverseSnapshot)
        .where(UniverseSnapshot.id == UNIVERSE_ID)
        .values(published_at=datetime(2026, 8, 1, tzinfo=UTC))
    )
    # Keep this synthetic successor future so wall-clock scheduling selects the fixture day.
    next_session = max(date(2026, 8, 31), datetime.now(UTC).date() + timedelta(days=7))
    while next_session.weekday() >= 5:
        next_session += timedelta(days=1)
    session.add(
        ExchangeCalendarAuthorityRecord(
            id=CALENDAR_ID,
            exchange="XNYS",
            version="provider-collection-v1",
            source="exchange_calendars",
            range_start=(MARKET_CLOSE - timedelta(days=400)).date(),
            range_end=next_session,
            query_parameters={},
            completeness_status="complete",
            content_hash="c" * 64,
            published_at=datetime(2026, 8, 1, tzinfo=UTC),
        )
    )
    session.add_all(
        (
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=date(2026, 8, 28),
                decision_at=NOW,
                market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                market_close_at=MARKET_CLOSE,
            ),
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=next_session,
                decision_at=datetime.combine(next_session, time_of_day(21), UTC),
                market_open_at=datetime.combine(next_session, time_of_day(13, 30), UTC),
                market_close_at=datetime.combine(next_session, time_of_day(20), UTC),
            ),
        )
    )


def _benchmark_daily_sessions(*, count: int = 275) -> tuple[date, ...]:
    """Return a stable full-size daily window ending at the fixture session."""
    sessions: list[date] = []
    cursor = date(2026, 8, 28)
    while len(sessions) < count:
        if cursor.weekday() < 5:
            sessions.append(cursor)
        cursor -= timedelta(days=1)
    return tuple(reversed(sessions))


def _add_benchmark_calendar_sessions(
    session: AsyncSession,
    sessions: tuple[date, ...],
) -> None:
    session.add_all(
        ExchangeCalendarSessionRecord(
            calendar_authority_id=CALENDAR_ID,
            session=session_date,
            decision_at=datetime.combine(
                session_date,
                time_of_day(21),
                tzinfo=UTC,
            ),
            market_open_at=datetime.combine(
                session_date,
                time_of_day(13, 30),
                tzinfo=UTC,
            ),
            market_close_at=datetime.combine(
                session_date,
                time_of_day(20),
                tzinfo=UTC,
            ),
        )
        for session_date in sessions
        if session_date != date(2026, 8, 28)
    )


async def _materialize_fosun_daily_jobs(
    factory: async_sessionmaker[AsyncSession],
    *,
    now: datetime,
) -> tuple[tuple[Job, ...], Job]:
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id="local_safe_v1",
        )
    assert root is not None

    async def forbidden_stage(*_: object) -> JobHandlerResult:
        pytest.fail("materialization-only registry executed a stage")

    registry = build_daily_pipeline_registry(
        profile_id="local_safe_v1",
        stage_executor=forbidden_stage,  # type: ignore[arg-type]
        enabled_stage_types=frozenset({JobType.COLLECT_MARKET}),
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="fosun-daily-materializer",
        worker_started_at=now,
        process_version="test-v1",
        migration_head=CURRENT_MIGRATION_HEAD,
        clock=lambda: now,
        claim_registered_only=True,
    )
    async with factory() as session:
        children = tuple(
            (
                await session.scalars(
                    select(Job)
                    .where(Job.job_type == JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
                    .order_by(Job.available_at, Job.id)
                )
            ).all()
        )
        publisher = await session.scalar(
            select(Job).where(Job.job_type == JobType.PUBLISH_FOSUN_DAILY.value)
        )
    assert children and publisher is not None
    return children, publisher


async def _stage_materialized_fosun_daily_children(
    factory: async_sessionmaker[AsyncSession],
    store: InMemoryMarketObjectStore,
    *,
    children: tuple[Job, ...],
    publisher: Job,
    checked_at: datetime,
) -> None:
    """Seed child success and exact raw staging without exercising call pacing."""
    publish_payload = FosunDailyPublishPayload.model_validate(publisher.payload)
    child_refs = {UUID(child.job_id): child for child in publish_payload.children}
    staged: list[tuple[Job, FosunDailyInstrumentPayload, DataArtifact, UUID]] = []
    async with factory() as session, session.begin():
        persisted_children = tuple(
            (
                await session.scalars(
                    select(Job)
                    .where(Job.id.in_(child.id for child in children))
                    .order_by(Job.effective_at, Job.id)
                )
            ).all()
        )
        for ordinal, child in enumerate(persisted_children):
            child_payload = FosunDailyInstrumentPayload.model_validate(child.payload)
            child_ref = child_refs[child.id]
            assert child.input_hash == child_ref.input_hash
            raw_payload = json.dumps(
                {
                    "instrument_id": child_payload.instrument_id,
                    "items": [],
                    "source_request_id": f"staged-{ordinal}",
                    "has_more": False,
                    "next_cursor": None,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            raw_digest = sha256(raw_payload).hexdigest()
            gateway_request_id = UUID(f"019d0000-0000-7000-8000-{ordinal + 1:012x}")
            raw_key = (
                f"raw/market/fosun/{checked_at:%Y/%m/%d}/{gateway_request_id}/{raw_digest}.json"
            )
            store.objects[raw_key] = raw_payload
            artifact = DataArtifact(
                uri=f"memory://{raw_key}",
                sha256=raw_digest,
                byte_size=len(raw_payload),
                mime_type="application/json",
                schema_version="fosun_gateway_market_bars_v1",
                source="fosun",
                observed_at=checked_at,
                available_at=checked_at,
                received_at=checked_at,
            )
            session.add(artifact)
            staged.append((child, child_payload, artifact, gateway_request_id))
        await session.flush()

        for child, child_payload, artifact, gateway_request_id in staged:
            ordinal = child_payload.ordinal
            request_end_at = datetime.combine(
                date.fromisoformat(child_payload.session),
                datetime.min.time(),
                tzinfo=UTC,
            )
            common = {
                "root_job_id": UUID(child_payload.root_job_id),
                "fetch_job_id": child.id,
                "instrument_id": UUID(child_payload.instrument_id),
                "schema_version": "fosun_daily_staging_v1",
                "child_input_hash": child.input_hash,
                "calendar_authority_id": UUID(child_payload.calendar_authority_id),
                "universe_snapshot_id": UUID(child_payload.universe_snapshot_id),
                "universe_content_hash": child_payload.universe_content_hash,
                "ordinal": ordinal,
                "total_instruments": len(children),
                "request_start_at": request_end_at - timedelta(days=400),
                "request_end_at": request_end_at,
            }
            session.add_all(
                (
                    FosunDailyBarStaging(
                        **common,
                        record_kind="reservation",
                        recorded_at=checked_at + timedelta(milliseconds=ordinal * 2),
                    ),
                    FosunDailyBarStaging(
                        **common,
                        record_kind="fetched",
                        recorded_at=checked_at + timedelta(milliseconds=ordinal * 2 + 1),
                        raw_artifact_id=artifact.id,
                        raw_artifact_identity_sha256=data_artifact_identity_sha256(artifact),
                        gateway_request_id=gateway_request_id,
                        request_body_sha256=(
                            await RecordingRawFosunProvider().expected_bars_request_body_sha256(
                                BarsRequest(
                                    instrument_id=UUID(child_payload.instrument_id),
                                    start=request_end_at - timedelta(days=400),
                                    end=request_end_at,
                                    interval="1Day",
                                    adjustment="raw",
                                ),
                                symbol=child_payload.instrument_symbol,
                            )
                        ),
                        sent_at=checked_at - timedelta(milliseconds=250),
                        received_at=checked_at,
                        provider_capability_id=publish_payload.market_authority.capability_id,
                        provider_status_id=publish_payload.market_authority.status_id,
                        provider_status_version=publish_payload.market_authority.status_version,
                        provider_contract_hash=publish_payload.market_authority.contract_hash,
                        provider_authorized_at=checked_at,
                        delivery_mode=publish_payload.market_authority.delivery_mode,
                    ),
                )
            )
            child.status = "running"
            child.started_at = checked_at
            child.attempt_count = 1
            child.version += 1
        await session.flush()
        for child, _child_payload, _artifact, _gateway_request_id in staged:
            child.status = "succeeded"
            child.finished_at = checked_at
            child.result_code = "FOSUN_DAILY_INSTRUMENT_STAGED"
            child.version += 1
        await session.flush()


async def _run_provider_once(
    factory: async_sessionmaker[AsyncSession],
    *,
    registry: JobRegistry,
    now: datetime,
    worker_id: str,
) -> bool:
    return await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.PROVIDER,
        worker_id=worker_id,
        worker_started_at=now,
        process_version="test-v1",
        migration_head=CURRENT_MIGRATION_HEAD,
        clock=lambda: now,
        claim_registered_only=True,
    )


def _claimed_context_for_job(
    session: AsyncSession,
    job: Job,
    *,
    now: datetime,
) -> JobHandlerContext:
    payload_model = (
        FosunDailyInstrumentPayload
        if job.job_type == JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value
        else FosunDailyPublishPayload
    )
    payload = payload_model.model_validate(job.payload)
    return JobHandlerContext(
        session=session,
        claim=ClaimedJob(
            job_id=job.id,
            job_type=job.job_type,
            schema_version=job.schema_version,
            payload=dict(job.payload),
            queue=job.queue,
            priority=job.priority,
            retry_policy=job.retry_policy,
            max_attempts=job.max_attempts,
            attempt_no=job.attempt_count + 1,
            effective_at=job.effective_at,
            worker_id="provider-direct-replay",
            lease_token=LEASE_TOKEN,
            fence_token=1,
            expires_at=now + timedelta(minutes=1),
            version=job.version,
            dependencies_satisfied=True,
            dependency_result_code=None,
        ),
        payload=payload,
        _clock=lambda: now,
    )


@pytest.mark.asyncio
async def test_market_collection_replays_without_duplicate_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
    provider = RecordingMarketProvider()
    registry = build_provider_collection_registry(
        _bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None

    async with factory() as session, session.begin():
        first = await spec.handler(_context(session, _payload()))
    async with factory() as session, session.begin():
        replay = await spec.handler(_context(session, _payload()))

    assert first.result_code == "MARKET_COLLECTED"
    assert replay.result_code == "MARKET_REPLAYED"
    assert len(provider.calls) == 2
    assert provider.calls[0].instrument_id == INSTRUMENT_ID
    assert provider.calls[0].interval == "1Day"
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 1


@pytest.mark.asyncio
async def test_fosun_daily_child_reservation_allows_exactly_one_gateway_call(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    assert len(children) == 1
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=checked_at,
        worker_id="fosun-daily-child-once",
    )
    assert len(provider.calls) == 1
    async with factory() as session:
        persisted_child = await session.get(Job, children[0].id)
        staging = tuple(
            (
                await session.scalars(
                    select(FosunDailyBarStaging).order_by(FosunDailyBarStaging.record_kind)
                )
            ).all()
        )
    assert persisted_child is not None
    assert {row.record_kind for row in staging} == {"reservation", "fetched"}

    child_spec = registry.resolve(JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
    assert child_spec is not None
    with pytest.raises(UnsafeJobError, match="FOSUN_DAILY_FETCH_ALREADY_RESERVED"):
        async with factory() as session, session.begin():
            await child_spec.handler(
                _claimed_context_for_job(session, persisted_child, now=checked_at)
            )
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_fosun_daily_child_exhausted_phase_budget_stops_before_reservation(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    provider = RecordingRawFosunProvider(
        delivery_mode="real_time",
        received_at=checked_at,
    )
    loader_calls = 0

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        nonlocal loader_calls
        loader_calls += 1
        if loader_calls == 2:
            await asyncio.sleep(0.3)
        return frozen

    original = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    child_spec = original.resolve(JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
    assert child_spec is not None
    registry = JobRegistry((replace(child_spec, timeout_seconds=0.2),))

    assert await _run_provider_once(
        factory,
        registry=registry,
        now=checked_at,
        worker_id="fosun-daily-phase-budget-exhausted",
    )

    async with factory() as session:
        persisted = await session.get(Job, children[0].id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_DAILY_PHASE_BUDGET_EXHAUSTED"
        assert await session.scalar(select(func.count()).select_from(FosunDailyBarStaging)) == 0
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
    assert provider.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exit_failure", "expected_error_code"),
    [
        ("timeout", "FOSUN_DAILY_ACQUISITION_DEADLINE_EXCEEDED"),
        ("error", "FOSUN_DAILY_ACQUISITION_TRANSITION_FAILED"),
    ],
)
async def test_fosun_daily_child_archives_response_when_permit_exit_fails(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    exit_failure: str,
    expected_error_code: str,
) -> None:
    monkeypatch.setattr(
        provider_collection_module,
        "_FOSUN_DAILY_HANDLER_INTERNAL_TIMEOUT_SECONDS",
        0.55,
    )
    monkeypatch.setattr(
        provider_collection_module,
        "_FOSUN_DAILY_OUTER_MARGIN_SECONDS",
        0.02,
    )
    monkeypatch.setattr(
        provider_collection_module,
        "_FOSUN_DAILY_FETCH_TIMEOUT_SECONDS",
        0.20,
    )
    monkeypatch.setattr(
        provider_collection_module,
        "_FOSUN_DAILY_RAW_ARCHIVE_RESERVE_SECONDS",
        0.10,
    )
    monkeypatch.setattr(
        provider_collection_module,
        "_FOSUN_DAILY_FINALIZE_RESERVE_SECONDS",
        0.05,
    )
    original_permit = provider_collection_module._fosun_daily_call_permit

    @asynccontextmanager
    async def delayed_permit_exit(*args: object, **kwargs: object):
        async with original_permit(*args, **kwargs):  # type: ignore[arg-type]
            yield
            if exit_failure == "timeout":
                await asyncio.sleep(0.5)
            raise RuntimeError("synthetic gate finalization failure")

    monkeypatch.setattr(
        provider_collection_module,
        "_fosun_daily_call_permit",
        delayed_permit_exit,
    )
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        delivery_mode="real_time",
        received_at=checked_at,
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    original = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    child_spec = original.resolve(JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
    assert child_spec is not None
    registry = JobRegistry((replace(child_spec, timeout_seconds=0.6),))

    assert await _run_provider_once(
        factory,
        registry=registry,
        now=checked_at,
        worker_id="fosun-daily-permit-exit-deadline",
    )

    async with factory() as session:
        persisted = await session.get(Job, children[0].id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == expected_error_code
        staged_kinds = list(
            await session.scalars(select(FosunDailyBarStaging.record_kind))
        )
        assert staged_kinds == ["reservation"]
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataArtifact)
                .where(DataArtifact.schema_version == "fosun_gateway_market_bars_v1")
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0
    assert len(provider.calls) == 1
    assert any(key.startswith("raw/market/fosun/") for key in store.objects)


@pytest.mark.asyncio
async def test_fosun_daily_child_rejects_calendar_close_mismatch_before_gateway_call(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
    )

    async def forbidden_loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        pytest.fail("calendar authority must fail before provider authority lookup")

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=forbidden_loader,
    )
    child_spec = registry.resolve(JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
    assert child_spec is not None
    async with factory() as session, session.begin():
        context = _claimed_context_for_job(session, children[0], now=checked_at)
        payload = cast(FosunDailyInstrumentPayload, context.payload).model_copy(
            update={"market_close_at": "2026-08-28T20:01:00.000000Z"}
        )
        with pytest.raises(
            UnsafeJobError,
            match="FOSUN_DAILY_CALENDAR_AUTHORITY_MISMATCH",
        ):
            await child_spec.handler(
                JobHandlerContext(
                    session=session,
                    claim=context.claim,
                    payload=payload,
                    _clock=lambda: checked_at,
                )
            )

    assert provider.calls == []
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(FosunDailyBarStaging)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_child_rejects_alias_drift_before_gateway_call(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    child_payload = FosunDailyInstrumentPayload.model_validate(children[0].payload)
    assert child_payload.instrument_symbol == "AAPL"

    async with factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE instrument_aliases DROP CONSTRAINT IF EXISTS "
                "ex_instrument_aliases_ticker_instrument_period"
            )
        )
        await session.execute(
            text(
                "ALTER TABLE instrument_aliases DROP CONSTRAINT IF EXISTS "
                "ex_instrument_aliases_ticker_symbol_period"
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT_ID,
                symbol="MSFT",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )

    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
    )

    async def forbidden_loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        pytest.fail("alias authority must fail before provider authority lookup")

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=forbidden_loader,
    )
    child_spec = registry.resolve(JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
    assert child_spec is not None
    async with factory() as session, session.begin():
        with pytest.raises(UnsafeJobError, match="FOSUN_DAILY_ALIAS_AUTHORITY_MISMATCH"):
            await child_spec.handler(_claimed_context_for_job(session, children[0], now=checked_at))

    assert provider.calls == []
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(FosunDailyBarStaging)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_alias_revalidation_rejects_symbol_owned_by_another_instrument(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    child_payload = FosunDailyInstrumentPayload.model_validate(children[0].payload)
    other_instrument_id = UUID("0198d500-1000-7000-8000-000000002999")

    async with factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE instrument_aliases DROP CONSTRAINT IF EXISTS "
                "ex_instrument_aliases_ticker_instrument_period"
            )
        )
        await session.execute(
            text(
                "ALTER TABLE instrument_aliases DROP CONSTRAINT IF EXISTS "
                "ex_instrument_aliases_ticker_symbol_period"
            )
        )
        session.add(
            Instrument(
                id=other_instrument_id,
                asset_type="equity",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="large_cap",
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=other_instrument_id,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2021, 1, 1),
                valid_to=None,
            )
        )

    async with factory() as session, session.begin():
        with pytest.raises(UnsafeJobError, match="FOSUN_DAILY_ALIAS_AUTHORITY_MISMATCH"):
            await _require_frozen_instrument_alias(session, child_payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_request_limit", [False, True])
async def test_fosun_daily_publisher_atomically_publishes_all_staged_children(
    migrated_database_url: str,
    legacy_request_limit: bool,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    await _materialize_fosun_daily_jobs(factory, now=checked_at)
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        legacy_request_limit=legacy_request_limit,
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=checked_at,
        worker_id="fosun-daily-fetch",
    )
    async with factory() as session:
        before_staging = dict((await session.execute(text(
            "SELECT id::text,md5(row_to_json(t)::text) FROM fosun_daily_bar_staging t"
        ))).all())
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=checked_at,
        worker_id="fosun-daily-publish",
    )

    assert len(provider.calls) == provider.normalized == 1
    assert provider.daily_session_counts == [1]
    async with factory() as session:
        after_staging = dict((await session.execute(text(
            "SELECT id::text,md5(row_to_json(t)::text) FROM fosun_daily_bar_staging t"
        ))).all())
        assert before_staging and all(after_staging.get(k) == v for k, v in before_staging.items())
        publisher = await session.scalar(
            select(Job).where(Job.job_type == JobType.PUBLISH_FOSUN_DAILY.value)
        )
        assert publisher is not None
        assert publisher.status == "succeeded"
        assert publisher.result_code == "MARKET_COLLECTED"
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 1


@pytest.mark.asyncio
async def test_fosun_daily_publisher_prepares_49_objects_within_handler_budget(
    migrated_database_url: str,
) -> None:
    """Publish 49 real-size daily pages within the fixed 45-second budget."""
    # Keep the virtual 49-child pacing behind PostgreSQL's real clock. The
    # injected worker clock advances 48 * 7 seconds without making this test sleep.
    checked_at = datetime.now(UTC) - timedelta(minutes=10)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    benchmark_sessions = _benchmark_daily_sessions()
    assert len(benchmark_sessions) == 275
    async with factory() as session, session.begin():
        await _seed_universe(session, extra_instruments=48)
        _add_benchmark_calendar_sessions(session, benchmark_sessions)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    assert len(children) == 49
    store = VirtualDelayedMarketObjectStore(operation_seconds=4.0)
    await _stage_materialized_fosun_daily_children(
        factory,
        store,
        children=children,
        publisher=publisher,
        checked_at=checked_at,
    )
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        bar_sessions=benchmark_sessions,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )

    started_at = perf_counter()
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-49-publish-budget",
    )
    publisher_elapsed_seconds = perf_counter() - started_at

    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "succeeded", persisted.last_error_code
        manifests = tuple(
            await session.scalars(
                select(PriceSeriesManifest).order_by(PriceSeriesManifest.instrument)
            )
        )
        assert len(manifests) == 49
        authorized_times = {manifest.provider_authorized_at for manifest in manifests}
        assert len(authorized_times) == 1
        publish_authorized_at = next(iter(authorized_times))
        assert publish_authorized_at is not None
        assert publish_authorized_at > publisher.available_at
        latest_staging_at = await session.scalar(select(func.max(FosunDailyBarStaging.recorded_at)))
        assert latest_staging_at is not None
        assert publish_authorized_at >= latest_staging_at
        assert (
            await load_daily_series(
                session,
                INSTRUMENT_ID,
                publish_authorized_at - timedelta(microseconds=1),
            )
            == ()
        )
        assert len(await load_daily_series(session, INSTRUMENT_ID, publish_authorized_at)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 49 * len(
            benchmark_sessions
        )
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 49
    # 49/32 = two waves: raw GET is 2*4s; normalized conflict verification
    # is 2 waves * (PUT+GET) * 4s, leaving 21s of the fixed 45s handler.
    assert store.virtual_seconds <= 24.0
    assert store.max_active_operations == 32
    assert publisher_elapsed_seconds + store.virtual_seconds < 45.0, (
        f"publisher={publisher_elapsed_seconds:.3f}s + "
        f"object_budget={store.virtual_seconds:.3f}s exceeds 45s handler budget"
    )


@pytest.mark.asyncio
async def test_fosun_daily_publisher_object_failure_publishes_zero_database_facts(
    migrated_database_url: str,
) -> None:
    """All normalized objects must be ready before the first DB fact is written."""
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session, extra_instruments=48)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    store = VirtualDelayedMarketObjectStore(fail_normalized_write_at=49)
    await _stage_materialized_fosun_daily_children(
        factory,
        store,
        children=children,
        publisher=publisher,
        checked_at=checked_at,
    )
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )

    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-49-object-failure",
    )

    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "queued"
        assert persisted.last_error_code == "HANDLER_FAILED"
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataArtifact)
                .where(DataArtifact.schema_version == "price_bars_v1")
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
@pytest.mark.parametrize("object_phase", ["read_raw", "put_normalized"])
async def test_fosun_daily_publisher_retries_transient_object_io_failure(
    migrated_database_url: str,
    error_code: str,
    object_phase: Literal["read_raw", "put_normalized"],
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    store = (
        UnavailableReadMarketObjectStore(error_code=error_code)
        if object_phase == "read_raw"
        else UnavailableNormalizedMarketObjectStore(error_code=error_code)
    )
    await _stage_materialized_fosun_daily_children(
        factory,
        store,
        children=children,
        publisher=publisher,
        checked_at=checked_at,
    )
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: True,
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id=f"fosun-daily-{object_phase}-{error_code.lower()}",
    )

    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "queued"
        assert persisted.last_error_code == error_code
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_fosun_daily_child_preserves_object_io_stop_code_without_retry(
    migrated_database_url: str,
    error_code: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    provider = RecordingRawFosunProvider(
        received_at=checked_at,
        delivery_mode="real_time",
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(
            MarketObjectStore,
            UnavailableArchiveMarketObjectStore(error_code=error_code),
        ),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )

    assert await _run_provider_once(
        factory,
        registry=registry,
        now=checked_at,
        worker_id=f"fosun-daily-child-object-io-{error_code.lower()}",
    )

    async with factory() as session:
        persisted = await session.get(Job, children[0].id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == error_code
        assert persisted.attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        staged_kinds = list(
            await session.scalars(select(FosunDailyBarStaging.record_kind))
        )
        assert staged_kinds == ["reservation"]
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_fosun_daily_publisher_rejects_tampered_request_body_digest(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-digest-fetch",
    )
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE fosun_daily_bar_staging DISABLE TRIGGER USER"))
        await session.execute(
            update(FosunDailyBarStaging)
            .where(FosunDailyBarStaging.record_kind == "fetched")
            .values(request_body_sha256="a" * 64)
        )
        await session.execute(text("ALTER TABLE fosun_daily_bar_staging ENABLE TRIGGER USER"))

    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-digest-publish",
    )

    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_DAILY_REQUEST_BODY_MISMATCH"
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_publisher_rejects_raw_artifact_uri_drift_before_lock(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: True,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-uri-drift-fetch",
    )
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE data_artifacts DISABLE TRIGGER USER"))
        artifact = await session.scalar(
            select(DataArtifact).where(
                DataArtifact.schema_version == "fosun_gateway_market_bars_v1"
            )
        )
        assert artifact is not None
        expected_key = artifact.uri.removeprefix("memory://")
        artifact.uri = f"memory://wrong-object?claimed={expected_key}"
        await session.flush()
        await session.execute(text("ALTER TABLE data_artifacts ENABLE TRIGGER USER"))

    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-uri-drift-publish",
    )

    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_DAILY_STAGING_AUTHORITY_MISMATCH"
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_partial_normalization_failure_publishes_zero_facts(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session, extra_instruments=1)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    assert len(children) == 2
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        distinct_raw_by_instrument=True,
        received_at=checked_at,
        unexpected_normalize_at=2,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-partial-fetch-0",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[1].available_at,
        worker_id="fosun-daily-partial-fetch-1",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-partial-publish",
    )

    assert len(provider.calls) == provider.normalized == 2
    assert not any(key.startswith("normalized/market/") for key in store.objects)
    assert store.temporary == {}
    async with factory() as session:
        persisted_publisher = await session.get(Job, publisher.id)
        assert persisted_publisher is not None
        assert persisted_publisher.status == "queued"
        assert persisted_publisher.last_error_code == "HANDLER_FAILED"
        assert await session.scalar(select(func.count()).select_from(FosunDailyBarStaging)) == 4
        reservations = tuple(
            (
                await session.scalars(
                    select(FosunDailyBarStaging)
                    .where(FosunDailyBarStaging.record_kind == "reservation")
                    .order_by(FosunDailyBarStaging.recorded_at)
                )
            ).all()
        )
        assert len(reservations) == 2
        assert reservations[1].recorded_at - reservations[0].recorded_at >= timedelta(seconds=7)
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_paces_actual_gateway_calls_after_slow_revalidation(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "nexus.jobs.provider_collection._FOSUN_DAILY_CALL_INTERVAL_SECONDS",
        0.2,
    )
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session, extra_instruments=1)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    assert len(children) == 2

    first_inner_check_started = asyncio.Event()
    release_first_inner_check = asyncio.Event()
    loader_calls = 0

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        nonlocal loader_calls
        loader_calls += 1
        if loader_calls == 2:
            first_inner_check_started.set()
            await release_first_inner_check.wait()
        return frozen

    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        distinct_raw_by_instrument=True,
        received_at=checked_at,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    first = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=children[1].available_at,
            worker_id="fosun-daily-slow-revalidation-first",
        )
    )
    await asyncio.wait_for(first_inner_check_started.wait(), timeout=2)
    second = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=children[1].available_at,
            worker_id="fosun-daily-slow-revalidation-second",
        )
    )
    await asyncio.sleep(0.15)
    release_first_inner_check.set()
    assert await asyncio.gather(first, second) == [True, True]

    assert len(provider.call_started_monotonic) == 2
    assert provider.call_started_monotonic[1] - provider.call_started_monotonic[0] >= 0.19


@pytest.mark.asyncio
async def test_fosun_daily_authority_drift_during_permit_wait_stops_next_gateway_call(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session, extra_instruments=1)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    assert len(children) == 2
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        distinct_raw_by_instrument=True,
        received_at=checked_at,
    )
    second_run = False
    drifted = False
    second_outer_check = asyncio.Event()

    async def loader(
        *_: object,
        **__: object,
    ) -> FrozenFosunMarketAuthority | None:
        if not second_run:
            return frozen
        if not second_outer_check.is_set():
            second_outer_check.set()
            return frozen
        return None if drifted else frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-before-wait",
    )
    assert len(provider.calls) == 1

    second_run = True
    second = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=children[1].available_at,
            worker_id="fosun-daily-drift-during-wait",
        )
    )
    await asyncio.wait_for(second_outer_check.wait(), timeout=2)
    drifted = True
    assert await second

    assert len(provider.calls) == 1
    async with factory() as session:
        persisted = await session.get(Job, children[1].id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_FETCH"
        staged = tuple(
            (
                await session.scalars(
                    select(FosunDailyBarStaging).order_by(FosunDailyBarStaging.recorded_at)
                )
            ).all()
        )
        assert [row.record_kind for row in staged].count("reservation") == 1
        assert [row.record_kind for row in staged].count("fetched") == 1
        assert {row.fetch_job_id for row in staged} == {children[0].id}
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_fetch_holds_status_revocation_lock_until_gateway_returns(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    fetch_started = asyncio.Event()
    fetch_release = asyncio.Event()
    revocation_applied = asyncio.Event()
    provider = RecordingRawFosunProvider(
        received_at=checked_at,
        fetch_started=fetch_started,
        fetch_release=fetch_release,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    worker = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=children[0].available_at,
            worker_id="fosun-daily-status-lock-fetch",
        )
    )
    await asyncio.wait_for(fetch_started.wait(), timeout=2)

    async def revoke() -> None:
        async with factory() as session, session.begin():
            await session.execute(
                update(IntegrationStatus)
                .where(IntegrationStatus.id == frozen.status_id)
                .values(
                    status="disabled",
                    version=frozen.status_version + 1,
                    mode_changed_at=checked_at + timedelta(seconds=1),
                    capability_invalidated_at=checked_at + timedelta(seconds=1),
                    admitted_contract_hash=None,
                )
            )
            revocation_applied.set()

    revocation = asyncio.create_task(revoke())
    try:
        await asyncio.sleep(0.05)
        assert not revocation_applied.is_set()
    finally:
        fetch_release.set()
        await asyncio.gather(worker, revocation)

    assert revocation_applied.is_set()


@pytest.mark.asyncio
async def test_fosun_daily_fetch_holds_capability_scope_lock_until_gateway_returns(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    fetch_started = asyncio.Event()
    fetch_release = asyncio.Event()
    capability_applied = asyncio.Event()
    provider = RecordingRawFosunProvider(
        received_at=checked_at,
        fetch_started=fetch_started,
        fetch_release=fetch_release,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    worker = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=children[0].available_at,
            worker_id="fosun-daily-capability-lock-fetch",
        )
    )
    await asyncio.wait_for(fetch_started.wait(), timeout=2)

    async def insert_new_capability() -> None:
        async with factory() as session, session.begin():
            session.add(
                ProviderCapability(
                    id=UUID("019d0000-0000-7000-8000-0000000000f1"),
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash="b" * 64,
                    expires_at=checked_at + timedelta(hours=2),
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_bars",
                        "market_scope": "US",
                        "delivery_mode": "real_time",
                    },
                    created_at=checked_at - timedelta(seconds=1),
                )
            )
        capability_applied.set()

    insertion = asyncio.create_task(insert_new_capability())
    try:
        await asyncio.sleep(0.05)
        assert not capability_applied.is_set()
    finally:
        fetch_release.set()
        await asyncio.gather(worker, insertion)

    assert capability_applied.is_set()


@pytest.mark.asyncio
async def test_fosun_daily_capability_insert_first_blocks_call_then_fails_closed(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    capability_holds_scope = asyncio.Event()
    release_capability = asyncio.Event()

    async def insert_new_capability() -> None:
        async with factory() as session, session.begin():
            session.add(
                ProviderCapability(
                    id=UUID("019d0000-0000-7000-8000-0000000000f3"),
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash="b" * 64,
                    expires_at=checked_at + timedelta(hours=2),
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_bars",
                        "market_scope": "US",
                        "delivery_mode": "real_time",
                    },
                    created_at=checked_at - timedelta(seconds=1),
                )
            )
            await session.flush()
            await session.execute(
                update(IntegrationStatus)
                .where(IntegrationStatus.id == frozen.status_id)
                .values(
                    version=frozen.status_version + 1,
                    mode_changed_at=checked_at - timedelta(microseconds=1),
                    admitted_contract_hash="b" * 64,
                )
            )
            capability_holds_scope.set()
            await release_capability.wait()

    insertion = asyncio.create_task(insert_new_capability())
    await asyncio.wait_for(capability_holds_scope.wait(), timeout=2)
    provider = RecordingRawFosunProvider(received_at=checked_at)
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    worker = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=children[0].available_at,
            worker_id="fosun-daily-capability-first-fetch",
        )
    )
    try:
        async with asyncio.timeout(2):
            while True:
                async with factory() as session:
                    persisted = await session.get(Job, children[0].id)
                if persisted is not None and persisted.status == "running":
                    break
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        async with factory() as session:
            reservations = await session.scalar(
                select(func.count())
                .select_from(FosunDailyBarStaging)
                .where(FosunDailyBarStaging.record_kind == "reservation")
            )
        assert reservations == 0
        assert provider.calls == []
        assert not worker.done()
    finally:
        release_capability.set()
        await asyncio.gather(worker, insertion)

    assert provider.calls == []
    async with factory() as session:
        persisted = await session.get(Job, children[0].id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_FETCH"
        assert (
            await session.scalar(
                select(func.count())
                .select_from(FosunDailyBarStaging)
                .where(FosunDailyBarStaging.record_kind == "reservation")
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_publisher_capability_lock_precedes_object_reads(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: True,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-publisher-lock-fetch",
    )
    capability_holds_scope = asyncio.Event()
    release_capability = asyncio.Event()

    async def insert_new_capability() -> None:
        async with factory() as session, session.begin():
            session.add(
                ProviderCapability(
                    id=UUID("019d0000-0000-7000-8000-0000000000f4"),
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash="b" * 64,
                    expires_at=checked_at + timedelta(hours=2),
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_bars",
                        "market_scope": "US",
                        "delivery_mode": "real_time",
                    },
                    created_at=checked_at - timedelta(seconds=1),
                )
            )
            await session.flush()
            await session.execute(
                update(IntegrationStatus)
                .where(IntegrationStatus.id == frozen.status_id)
                .values(
                    version=frozen.status_version + 1,
                    mode_changed_at=checked_at - timedelta(microseconds=1),
                    admitted_contract_hash="b" * 64,
                )
            )
            capability_holds_scope.set()
            await release_capability.wait()

    insertion = asyncio.create_task(insert_new_capability())
    await asyncio.wait_for(capability_holds_scope.wait(), timeout=2)
    publication = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=publisher.available_at,
            worker_id="fosun-daily-publisher-capability-first",
        )
    )
    try:
        await asyncio.sleep(0.05)
        assert provider.normalized == 0
        assert not publication.done()
    finally:
        release_capability.set()
        await asyncio.gather(publication, insertion)

    assert provider.normalized == 0
    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_PUBLISH"
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_fosun_daily_publisher_holds_status_and_raw_locks_through_commit(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    store = BlockingReadMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: True,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-artifact-lock-fetch",
    )
    async with factory() as session:
        raw_artifact_id = await session.scalar(
            select(FosunDailyBarStaging.raw_artifact_id).where(
                FosunDailyBarStaging.record_kind == "fetched"
            )
        )
    assert raw_artifact_id is not None
    publication = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=publisher.available_at,
            worker_id="fosun-daily-artifact-lock-publish",
        )
    )
    await asyncio.wait_for(store.read_started.wait(), timeout=2)
    try:
        async with factory() as session, session.begin():
            status_lock_available = await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"fosun-integration-status:{frozen.status_id}"},
            )
            raw_lock_available = await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"fosun-data-artifact:{raw_artifact_id}"},
            )
        assert status_lock_available is False
        assert raw_lock_available is False
        assert not publication.done()
    finally:
        store.release_read.set()
        await asyncio.wait_for(publication, timeout=5)

    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "succeeded"
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 1


@pytest.mark.asyncio
async def test_fosun_daily_revocation_lock_precedes_permit_and_blocks_gateway_call(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(factory, now=checked_at)
    revoke_holds_lock = asyncio.Event()
    release_revoke = asyncio.Event()

    async def revoke() -> None:
        async with factory() as session, session.begin():
            await session.execute(
                update(IntegrationStatus)
                .where(IntegrationStatus.id == frozen.status_id)
                .values(
                    status="disabled",
                    version=frozen.status_version + 1,
                    mode_changed_at=checked_at + timedelta(seconds=1),
                    capability_invalidated_at=checked_at + timedelta(seconds=1),
                    admitted_contract_hash=None,
                )
            )
            revoke_holds_lock.set()
            await release_revoke.wait()

    revocation = asyncio.create_task(revoke())
    await asyncio.wait_for(revoke_holds_lock.wait(), timeout=2)
    provider = RecordingRawFosunProvider(received_at=checked_at)
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    worker = asyncio.create_task(
        _run_provider_once(
            factory,
            registry=registry,
            now=children[0].available_at,
            worker_id="fosun-daily-revoke-before-permit",
        )
    )
    try:
        async with asyncio.timeout(2):
            while True:
                async with factory() as session:
                    persisted = await session.get(Job, children[0].id)
                if persisted is not None and persisted.status == "running":
                    break
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        async with factory() as session:
            reservations = await session.scalar(
                select(func.count())
                .select_from(FosunDailyBarStaging)
                .where(FosunDailyBarStaging.record_kind == "reservation")
            )
        assert reservations == 0
        assert provider.calls == []
        assert not worker.done()
    finally:
        release_revoke.set()
        await asyncio.gather(worker, revocation)

    assert provider.calls == []
    async with factory() as session:
        persisted = await session.get(Job, children[0].id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_FETCH"
        assert (
            await session.scalar(
                select(func.count())
                .select_from(FosunDailyBarStaging)
                .where(FosunDailyBarStaging.record_kind == "reservation")
            )
            == 0
        )


@pytest.mark.asyncio
async def test_fosun_daily_post_publish_authority_drift_rolls_back_all_facts(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
    )
    authority_results = iter((frozen, frozen, frozen, frozen, frozen, None))

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority | None:
        return next(authority_results)

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-drift-fetch",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-drift-publish",
    )

    async with factory() as session:
        persisted_publisher = await session.get(Job, publisher.id)
        assert persisted_publisher is not None
        assert persisted_publisher.status == "dead_letter"
        assert persisted_publisher.last_error_code == "FOSUN_MARKET_AUTHORITY_CHANGED_AFTER_PUBLISH"
        assert await session.scalar(select(func.count()).select_from(FosunDailyBarStaging)) == 2
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_fosun_post_fetch_drift_archives_raw_and_creates_zero_facts(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, _publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    provider = RecordingRawFosunProvider()
    store = InMemoryMarketObjectStore()
    loader_results = iter((frozen, frozen, None))

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority | None:
        return next(loader_results)

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-post-fetch-drift",
    )
    assert len(provider.calls) == 1
    assert provider.normalized == 0
    assert list(store.objects.values()) == [
        b'{"items":[],"source_request_id":"safe","has_more":false,"next_cursor":null}'
    ]
    async with factory() as session:
        raw = await session.scalar(
            select(DataArtifact).where(
                DataArtifact.schema_version == "fosun_gateway_market_bars_v1"
            )
        )
        assert raw is not None
        assert raw.sha256 in raw.uri
        staged_kinds = set(await session.scalars(select(FosunDailyBarStaging.record_kind)))
        assert staged_kinds == {"reservation"}
        child = await session.get(Job, children[0].id)
        assert child is not None
        assert child.status == "dead_letter"
        assert child.last_error_code == "FOSUN_MARKET_AUTHORITY_CHANGED_AFTER_FETCH"
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataArtifact)
                .where(DataArtifact.schema_version == "price_bars_v1")
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0


@pytest.mark.asyncio
async def test_fosun_malformed_response_is_archived_before_stable_skip(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    provider = RecordingRawFosunProvider(
        normalize_error=True,
        delivery_mode="real_time",
        received_at=checked_at,
    )
    store = InMemoryMarketObjectStore()

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-malformed-fetch",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-malformed-publish",
    )
    assert provider.normalized == 1
    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_MARKET_PAYLOAD_INVALID"
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataArtifact)
                .where(DataArtifact.schema_version == "fosun_gateway_market_bars_v1")
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0


@pytest.mark.asyncio
async def test_fosun_missing_calendar_session_is_archived_without_market_facts(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        allow_normalize=True,
        delivery_mode="real_time",
        received_at=checked_at,
        bar_session=date(2026, 8, 27),
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-incomplete-fetch",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-incomplete-publish",
    )
    assert provider.normalized == 1
    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "FOSUN_MARKET_PAGE_INCOMPLETE"
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataArtifact)
                .where(DataArtifact.schema_version == "fosun_gateway_market_bars_v1")
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0


@pytest.mark.asyncio
async def test_fosun_raw_metadata_survives_outer_job_rollback(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    provider = RecordingRawFosunProvider(
        unexpected_normalize_error=True,
        delivery_mode="real_time",
        received_at=checked_at,
    )

    async def loader(*_: object, **__: object) -> FrozenFosunMarketAuthority:
        return frozen

    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
        market_authority_loader=loader,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-raw-survives-fetch",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-raw-survives-publish",
    )

    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataArtifact)
                .where(DataArtifact.schema_version == "fosun_gateway_market_bars_v1")
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(FosunDailyBarStaging)) == 2
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0


@pytest.mark.asyncio
async def test_fosun_unknown_delivery_keeps_raw_staging_but_publishes_zero_facts(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(checked_at=checked_at)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(
            session,
            frozen,
            last_success_at=checked_at,
        )
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        received_at=checked_at,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
        allow_normalize=True,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id="fosun-daily-unknown-fetch",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id="fosun-daily-unknown-publish",
    )
    assert provider.normalized == 1
    async with factory() as session:
        persisted = await session.get(Job, publisher.id)
        assert persisted is not None
        assert persisted.status == "dead_letter"
        assert persisted.last_error_code == "SOURCE_CONFLICT"
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_mode", ("real_time", "delayed"))
async def test_fosun_proven_delivery_creates_bound_v2_price_authority(
    migrated_database_url: str,
    delivery_mode: Literal["real_time", "delayed"],
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode=delivery_mode,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(
            session,
            frozen,
            last_success_at=checked_at,
        )
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )
    store = InMemoryMarketObjectStore()
    provider = RecordingRawFosunProvider(
        received_at=checked_at,
        raw_archived=lambda: any(key.startswith("raw/market/fosun/") for key in store.objects),
        allow_normalize=True,
        delivery_mode=delivery_mode,
    )
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, store),
        InMemoryRawEvidenceStore(),
        lambda: checked_at,
        raw_archive_session_factory=factory,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=children[0].available_at,
        worker_id=f"fosun-daily-{delivery_mode}-fetch",
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=publisher.available_at,
        worker_id=f"fosun-daily-{delivery_mode}-publish",
    )
    assert len(provider.calls) == 1
    assert provider.calls[0].start == datetime(2025, 7, 24, tzinfo=UTC)
    assert provider.calls[0].end == datetime(2026, 8, 28, tzinfo=UTC)
    async with factory() as session:
        manifest = await session.scalar(select(PriceSeriesManifest))
        authority = await session.scalar(select(PriceBarSetAuthority))
        assert manifest is not None and authority is not None
        assert manifest.delivery_mode == delivery_mode
        assert manifest.provider_capability_id == frozen.capability_id
        assert manifest.provider_status_id == frozen.status_id
        assert manifest.provider_status_version == frozen.status_version
        assert manifest.provider_contract_hash == frozen.contract_hash
        assert manifest.provider_authorized_at is not None
        assert manifest.provider_authorized_at >= checked_at
        assert manifest.gateway_request_start_at == provider.calls[0].start
        assert manifest.gateway_request_end_at == provider.calls[0].end
        assert authority.hash_version == "price_bar_set_sha256_v2"
        assert authority.policy_version == "agent_price_authority_v2"


@pytest.mark.asyncio
async def test_market_collection_rejects_a_frozen_universe_hash_mismatch(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
    provider = RecordingMarketProvider()
    registry = build_provider_collection_registry(
        _bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None

    async with factory() as session, session.begin():
        result = await spec.handler(_context(session, _payload(content_hash="b" * 64)))

    assert result.outcome == "skipped"
    assert result.result_code == "FROZEN_UNIVERSE_MISMATCH"
    assert provider.calls == []


@pytest.mark.asyncio
async def test_fosun_daily_materializes_more_than_ten_single_call_jobs(
    migrated_database_url: str,
) -> None:
    checked_at = datetime.now(UTC)
    frozen = _fosun_authority(
        checked_at=checked_at,
        delivery_mode="real_time",
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session, extra_instruments=10)
        _add_fosun_authority_rows(session, frozen, last_success_at=checked_at)
    children, publisher = await _materialize_fosun_daily_jobs(
        factory,
        now=checked_at,
    )

    assert len(children) == 11
    assert all(child.max_attempts == 1 for child in children)
    assert publisher.max_attempts == 3


@pytest.mark.asyncio
async def test_legacy_fosun_daily_batch_rejects_before_provider(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session, extra_instruments=100)
    provider = RecordingRawFosunProvider()
    registry = build_provider_collection_registry(
        _fosun_bundle(provider),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None
    frozen = _fosun_authority(delivery_mode="real_time")

    async with factory() as session, session.begin():
        result = await spec.handler(
            _context(
                session,
                _payload(market_source="fosun", market_authority=frozen),
            )
        )

    assert result.outcome == "skipped"
    assert result.result_code == "FOSUN_DAILY_SHARDING_REQUIRED"
    assert provider.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_explicit_alpaca_market_preserves_retryable_object_io_stop_code(
    migrated_database_url: str,
    error_code: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
    provider = RecordingMarketProvider()
    registry = build_provider_collection_registry(
        _bundle(provider),
        cast(
            MarketObjectStore,
            UnavailableArchiveMarketObjectStore(error_code=error_code),
        ),
        InMemoryRawEvidenceStore(),
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None

    async with factory() as session, session.begin():
        with pytest.raises(RetryableJobError, match=error_code):
            await spec.handler(_context(session, _payload()))

    assert len(provider.calls) == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_present", [False, True])
async def test_alpaca_market_collection_requires_explicit_page_exhaustion(
    migrated_database_url: str,
    terminal_present: bool,
) -> None:
    from nexus.providers.market.alpaca import AlpacaMarketDataProvider
    from nexus.providers.market.alpaca_http import AlpacaHttpTransport
    from pydantic import SecretStr

    from tests.support.provider_fakes import (
        FixedInstrumentSymbolResolver,
        RecordingFixedOriginClient,
    )

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
    client = RecordingFixedOriginClient(
        payload=b'{"symbol":"AAPL","bars":[],"next_page_token":null}'
        if terminal_present else b'{"symbol":"AAPL","bars":[]}'
    )
    provider = AlpacaMarketDataProvider(
        AlpacaHttpTransport(
            client,
            key_id=SecretStr("id-canary"),
            secret=SecretStr("secret-canary"),
            feed="sip",
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
        ),
        feed="sip",
        delivery_mode="delayed",
        now=lambda: NOW,
    )
    registry = build_provider_collection_registry(
        _bundle(cast(RecordingMarketProvider, provider)),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None
    async with factory() as session, session.begin():
        if terminal_present:
            result = await spec.handler(_context(session, _payload()))
            assert (result.outcome, result.result_code) == ("skipped", "MARKET_NO_DATA")
        else:
            with pytest.raises(MarketProviderPayloadError, match="next_page_token"):
                await spec.handler(_context(session, _payload()))

    assert len(client.calls) == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0


@pytest.mark.asyncio
async def test_sec_macro_collection_persists_exact_evidence_and_replays(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
    sec_client = FixtureSecClient()
    fred_client = FixtureFredClient()
    raw_store = InMemoryRawEvidenceStore()
    registry = build_provider_collection_registry(
        _bundle(
            RecordingMarketProvider(),
            sec_client=sec_client,
            fred_client=fred_client,
        ),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        raw_store,
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_SEC_MACRO.value)
    assert spec is not None

    async with factory() as session, session.begin():
        first = await spec.handler(
            _context(session, _payload(), job_type=JobType.COLLECT_SEC_MACRO)
        )
    async with factory() as session, session.begin():
        replay = await spec.handler(
            _context(session, _payload(), job_type=JobType.COLLECT_SEC_MACRO)
        )

    assert first.result_code == "SEC_MACRO_COLLECTED"
    assert replay.result_code == "SEC_MACRO_REPLAYED"
    assert sec_client.calls == 2
    assert set(fred_client.calls) == {
        "DFF",
        "DGS2",
        "DGS10",
        "CPIAUCSL",
        "UNRATE",
        "INDPRO",
        "VIXCLS",
        "DEXHKUS",
    }
    assert len(fred_client.calls) == 16
    assert any(key.startswith("raw/sec/") for key in raw_store.objects)
    assert any(key.startswith("raw/fred/") for key in raw_store.objects)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MacroObservation)) == 8
        fact_count = await session.scalar(select(func.count()).select_from(FinancialFactRecord))
        assert fact_count == 12


@pytest.mark.asyncio
async def test_sec_macro_rejects_unsafe_accession_before_any_object_or_fact(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
    raw_store = InMemoryRawEvidenceStore()
    registry = build_provider_collection_registry(
        _bundle(
            RecordingMarketProvider(),
            sec_client=MalformedAccessionSecClient(),
            fred_client=FixtureFredClient(),
        ),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        raw_store,
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_SEC_MACRO.value)
    assert spec is not None

    async with factory() as session, session.begin():
        with pytest.raises(RuntimeError, match="SEC_SUBMISSIONS_INVALID"):
            await spec.handler(
                _context(session, _payload(), job_type=JobType.COLLECT_SEC_MACRO)
            )

    assert raw_store.objects == {}
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 0
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_sec_macro_preserves_retryable_object_io_stop_code(
    migrated_database_url: str,
    error_code: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
    registry = build_provider_collection_registry(
        _bundle(
            RecordingMarketProvider(),
            sec_client=FixtureSecClient(),
            fred_client=FixtureFredClient(),
        ),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        UnavailableRawEvidenceStore(error_code=error_code),
        lambda: NOW,
    )
    spec = registry.resolve(JobType.COLLECT_SEC_MACRO.value)
    assert spec is not None

    async with factory() as session, session.begin():
        with pytest.raises(RetryableJobError, match=error_code):
            await spec.handler(
                _context(session, _payload(), job_type=JobType.COLLECT_SEC_MACRO)
            )

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MacroObservation)) == 0
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_sec_macro_worker_preserves_object_io_stop_code_for_retry(
    migrated_database_url: str,
    error_code: str,
) -> None:
    frozen = _fosun_authority(checked_at=NOW, delivery_mode="real_time")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_universe(session)
        _add_fosun_authority_rows(session, frozen, last_success_at=NOW)
        root = await schedule_due_daily_pipeline(
            session,
            now=NOW,
            profile_id="local_safe_v1",
        )
    assert root is not None

    async def forbidden_stage(*_: object) -> JobHandlerResult:
        pytest.fail("materialization-only registry executed a stage")

    materializer = build_daily_pipeline_registry(
        profile_id="local_safe_v1",
        stage_executor=forbidden_stage,  # type: ignore[arg-type]
        enabled_stage_types=frozenset({JobType.COLLECT_SEC_MACRO}),
    )
    assert await run_job_worker_once(
        factory,
        registry=materializer,
        queue=QueueName.GENERAL,
        worker_id="sec-macro-materializer",
        worker_started_at=NOW,
        process_version="test-v1",
        migration_head=CURRENT_MIGRATION_HEAD,
        clock=lambda: NOW,
        claim_registered_only=True,
    )
    async with factory() as session:
        sec_job = await session.scalar(
            select(Job).where(Job.job_type == JobType.COLLECT_SEC_MACRO.value)
        )
    assert sec_job is not None
    assert sec_job.status == "queued"

    sec_client = FixtureSecClient()
    fred_client = FixtureFredClient()
    registry = build_provider_collection_registry(
        _fosun_bundle(
            RecordingRawFosunProvider(),
            sec_client=sec_client,
            fred_client=fred_client,
        ),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        UnavailableRawEvidenceStore(error_code=error_code),
        lambda: NOW,
    )
    assert await _run_provider_once(
        factory,
        registry=registry,
        now=sec_job.available_at,
        worker_id=f"sec-macro-object-io-{error_code.lower()}",
    )

    async with factory() as session:
        persisted = await session.get(Job, sec_job.id)
        assert persisted is not None
        assert persisted.status == "queued"
        assert persisted.last_error_code == error_code
        assert persisted.attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(MacroObservation)) == 0
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0
    assert sec_client.calls == 1
    assert fred_client.calls == []

"""PostgreSQL contracts for durable provider collection Jobs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from nexus.jobs.daily_payloads import DailyStagePayload
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.provider_collection import build_provider_collection_registry
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import ClaimedJob
from nexus.market.ingestion import MarketObjectStore
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    MacroObservation,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.research import FinancialFactRecord
from nexus.persistence.session import session_factory
from nexus.providers.base import (
    BarsRequest,
    MarketDataProvenance,
    PriceBar,
    ProviderPage,
)
from nexus.providers.composition import ProviderBundle, build_provider_bundle
from nexus.providers.fred import FredRawDocument, FredSeriesClient
from nexus.providers.sec import SecCompanyFactsClient, SecRawDocument
from nexus.providers.settings import ProviderRuntimeMode, ProviderSettings
from nexus.settings import EngineSettings
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

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

    async def put_temporary(self, key: str, payload: bytes) -> None:
        self.temporary[key] = payload

    async def promote(self, temporary_key: str, final_key: str) -> str:
        self.objects.setdefault(final_key, self.temporary[temporary_key])
        return f"memory://{final_key}"

    async def delete_temporary(self, key: str) -> None:
        self.temporary.pop(key, None)


@dataclass
class InMemoryRawEvidenceStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        self.objects.setdefault(key, content)
        return f"memory://sha256/{key.rsplit('/', 1)[-1]}"


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
        market_provider_factory=lambda _session: provider,
        sec_client=(
            fixture.sec_client
            if sec_client is None
            else cast(SecCompanyFactsClient, sec_client)
        ),
        fred_client=(
            fixture.fred_client
            if fred_client is None
            else cast(FredSeriesClient, fred_client)
        ),
    )


def _payload(*, content_hash: str = "a" * 64) -> DailyStagePayload:
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
    )


def _context(
    session: AsyncSession,
    payload: DailyStagePayload,
    *,
    job_type: JobType = JobType.COLLECT_MARKET,
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
        _clock=lambda: NOW,
    )


async def _seed_universe(session: AsyncSession) -> None:
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
    await session.flush()
    await session.execute(
        update(UniverseSnapshot)
        .where(UniverseSnapshot.id == UNIVERSE_ID)
        .values(published_at=datetime(2026, 8, 1, tzinfo=UTC))
    )
    session.add(
        ExchangeCalendarAuthorityRecord(
            id=CALENDAR_ID,
            exchange="XNYS",
            version="provider-collection-v1",
            source="exchange_calendars",
            range_start=date(2026, 8, 28),
            range_end=date(2026, 8, 31),
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
                session=date(2026, 8, 31),
                decision_at=datetime(2026, 8, 31, 21, tzinfo=UTC),
                market_open_at=datetime(2026, 8, 31, 13, 30, tzinfo=UTC),
                market_close_at=datetime(2026, 8, 31, 20, tzinfo=UTC),
            ),
        )
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
        assert (
            await session.scalar(
                select(func.count()).select_from(MacroObservation)
            )
            == 8
        )
        fact_count = await session.scalar(
            select(func.count()).select_from(FinancialFactRecord)
        )
        assert fact_count == 12

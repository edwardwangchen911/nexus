"""Provider collection Job admission and fixture isolation contracts."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest
from nexus.jobs.daily_payloads import (
    DailyStagePayload,
    FosunDailyChildRef,
    FosunDailyInstrumentPayload,
    FosunDailyPublishPayload,
)
from nexus.jobs.daily_pipeline import build_daily_pipeline_registry
from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.provider_collection import (
    _archive_fosun_price_raw,
    _bounded_async_map,
    _require_staging_common,
    build_provider_collection_registry,
)
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import ClaimedJob
from nexus.jobs.worker import UnsafeJobError
from nexus.market.ingestion import MarketObjectStore
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.persistence.models.data import FosunDailyBarStaging
from nexus.providers.composition import ProviderBundle, build_provider_bundle
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.providers.settings import ProviderRuntimeMode, ProviderSettings
from sqlalchemy import CheckConstraint
from sqlalchemy.ext.asyncio import AsyncSession

FROZEN_NOW = datetime(2026, 8, 28, 21, tzinfo=UTC)
ROOT_JOB_ID = UUID("0198d500-1000-7000-8000-000000002901")
CLAIM_JOB_ID = UUID("0198d500-1000-7000-8000-000000002902")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_daily_child_archive_preserves_object_io_stop_code(error_code: str) -> None:
    class _Session:
        rollback_calls = 0

        async def rollback(self) -> None:
            self.rollback_calls += 1
            raise AssertionError("object I/O failure must not be masked by rollback")

    class _UnavailableStore:
        async def archive_raw(self, key: str, payload: bytes) -> str:
            del key, payload
            raise ProviderObjectIOUnavailable(error_code)

    response = RawGatewayResponse(
        operation="fosun_get_bars",
        gateway_request_id=UUID("0198d500-1000-7000-8000-000000002909"),
        request_body_sha256="a" * 64,
        raw_payload=b'{"bars":[]}',
        sent_at=FROZEN_NOW - timedelta(milliseconds=100),
        received_at=FROZEN_NOW,
    )

    session = _Session()
    with pytest.raises(UnsafeJobError, match=error_code):
        await _archive_fosun_price_raw(
            cast(AsyncSession, session),
            cast(MarketObjectStore, _UnavailableStore()),
            response,
            session_factory=None,
        )
    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_bounded_async_map_raises_first_completed_error_and_stops_queued_work() -> None:
    started: set[int] = set()

    async def operation(item: int) -> int:
        started.add(item)
        if item == 0:
            await asyncio.sleep(0.02)
            raise RuntimeError("earlier input failed later")
        if item == 1:
            raise ProviderObjectIOUnavailable("OBJECT_IO_SATURATED")
        await asyncio.sleep(0.05)
        return item

    with pytest.raises(ProviderObjectIOUnavailable, match="OBJECT_IO_SATURATED"):
        await _bounded_async_map(tuple(range(49)), operation)

    assert 48 not in started


@pytest.mark.asyncio
async def test_bounded_async_map_parent_cancellation_cancels_running_siblings() -> None:
    all_started = asyncio.Event()
    release = asyncio.Event()
    started: set[int] = set()
    cancelled: set[int] = set()

    async def operation(item: int) -> int:
        started.add(item)
        if len(started) == 3:
            all_started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.add(item)
            raise
        return item

    task = asyncio.create_task(_bounded_async_map((0, 1, 2), operation))
    await asyncio.wait_for(all_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert cancelled == started == {0, 1, 2}


def test_fosun_daily_payload_rejects_unverified_fiftieth_instrument() -> None:
    authority = FrozenFosunMarketAuthority(
        operation="fosun_get_bars",
        delivery_mode="real_time",
        capability_id=UUID("0198d500-1000-7000-8000-000000002907"),
        capability_schema_version="1",
        capability_created_at=FROZEN_NOW - timedelta(hours=1),
        capability_expires_at=FROZEN_NOW + timedelta(hours=1),
        status_id=UUID("0198d500-1000-7000-8000-000000002908"),
        status_version=1,
        status_created_at=FROZEN_NOW - timedelta(hours=2),
        status_mode_changed_at=FROZEN_NOW - timedelta(hours=2),
        capability_invalidated_at=None,
        contract_hash="a" * 64,
    )
    payload = {
        "root_job_id": str(ROOT_JOB_ID),
        "calendar_authority_id": "0198d500-1000-7000-8000-000000002904",
        "session": "2026-08-28",
        "market_close_at": "2026-08-28T20:00:00.000000Z",
        "scheduled_for": "2026-08-28T21:00:00.000000Z",
        "profile_id": "local_safe_v1",
        "universe_snapshot_id": "0198d500-1000-7000-8000-000000002905",
        "universe_content_hash": "c" * 64,
        "instrument_id": "0198d500-1000-7000-8000-000000002906",
        "instrument_alias_id": "0198d500-1000-7000-8000-000000002903",
        "instrument_symbol": "AAPL",
        "instrument_alias_identity_sha256": "d" * 64,
        "ordinal": 49,
        "total_instruments": 50,
        "market_authority": authority,
    }

    with pytest.raises(ValueError):
        FosunDailyInstrumentPayload.model_validate(payload)


def test_fosun_daily_staging_metadata_enforces_same_hard_universe_cap() -> None:
    constraint = next(
        item
        for item in FosunDailyBarStaging.__table__.constraints
        if isinstance(item, CheckConstraint)
        and item.name == "ck_fosun_daily_bar_staging_shard_bounds"
    )

    assert "total_instruments BETWEEN 1 AND 49" in str(constraint.sqltext)
    assert "BETWEEN 1 AND 100" not in str(constraint.sqltext)


LEASE_TOKEN = UUID("0198d500-1000-7000-8000-000000002903")


@dataclass
class InMemoryRawEvidenceStore:
    objects: dict[str, bytes] = field(default_factory=dict)
    external_calls: int = 0

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        self.objects.setdefault(key, content)
        return f"memory://sha256/{key.rsplit('/', 1)[-1]}"


@dataclass
class InMemoryMarketObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)
    temporary: dict[str, bytes] = field(default_factory=dict)
    external_calls: int = 0

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


def _fixture_bundle():
    return build_provider_bundle(
        ProviderSettings(runtime_mode="fixture"),
        http_client_factory=lambda *_: pytest.fail("HTTP factory called"),
    )


def _context(job_type: JobType) -> JobHandlerContext:
    payload = DailyStagePayload(
        root_job_id=str(ROOT_JOB_ID),
        calendar_authority_id="0198d500-1000-7000-8000-000000002904",
        session="2026-08-28",
        market_close_at="2026-08-28T20:00:00.000000Z",
        scheduled_for="2026-08-28T20:30:00.000000Z",
        quality_cutoff_at="2026-08-28T22:00:00.000000Z",
        profile_id="local_safe_v1",
        target_id=None,
    )
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
        effective_at=FROZEN_NOW,
        worker_id="provider-fixture-worker",
        lease_token=LEASE_TOKEN,
        fence_token=1,
        expires_at=datetime(2026, 8, 28, 21, 1, tzinfo=UTC),
        version=1,
        dependencies_satisfied=True,
        dependency_result_code=None,
    )
    return JobHandlerContext(
        session=cast(AsyncSession, object()),
        claim=claim,
        payload=payload,
        _clock=lambda: FROZEN_NOW,
    )


def test_registry_uses_existing_daily_payload_and_bounded_general_handlers() -> None:
    """A queue/schema drift would make daily jobs unclaimable or overly permissive."""
    registry = build_provider_collection_registry(
        _fixture_bundle(),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: FROZEN_NOW,
    )

    assert registry.job_types == {
        JobType.COLLECT_MARKET.value,
        JobType.COLLECT_SEC_MACRO.value,
        JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value,
        JobType.PUBLISH_FOSUN_DAILY.value,
    }
    expected_payload_models = {
        JobType.COLLECT_MARKET: DailyStagePayload,
        JobType.COLLECT_SEC_MACRO: DailyStagePayload,
        JobType.COLLECT_FOSUN_DAILY_INSTRUMENT: FosunDailyInstrumentPayload,
        JobType.PUBLISH_FOSUN_DAILY: FosunDailyPublishPayload,
    }
    for spec in registry.specs:
        assert spec.payload_model is expected_payload_models[spec.job_type]
        assert spec.queue is QueueName.PROVIDER
        assert spec.timeout_seconds == 45
        assert spec.max_attempts == (
            1 if spec.job_type is JobType.COLLECT_FOSUN_DAILY_INSTRUMENT else 3
        )


def test_daily_registry_reuses_provider_specs_without_duplicate_placeholder() -> None:
    """The worker must claim collection jobs with the exact provider handler authority."""
    provider = build_provider_collection_registry(
        _fixture_bundle(),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: FROZEN_NOW,
    )

    async def forbidden_stage_executor(*_: object) -> object:
        pytest.fail("generic collection placeholder called")

    daily = build_daily_pipeline_registry(
        profile_id="local_safe_v1",
        stage_executor=forbidden_stage_executor,  # type: ignore[arg-type]
        enabled_stage_types=frozenset({JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO}),
        stage_specs=tuple(
            spec
            for spec in provider.specs
            if spec.job_type in {JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO}
        ),
    )

    assert daily.resolve(JobType.COLLECT_MARKET.value) is provider.resolve(
        JobType.COLLECT_MARKET.value
    )
    assert daily.resolve(JobType.COLLECT_SEC_MACRO.value) is provider.resolve(
        JobType.COLLECT_SEC_MACRO.value
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("job_type", (JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO))
async def test_fixture_collection_skips_without_database_or_external_calls(
    job_type: JobType,
) -> None:
    """Fixture mode must remain useful under a socket guard and empty database seam."""
    market_store = InMemoryMarketObjectStore()
    raw_store = InMemoryRawEvidenceStore()
    registry = build_provider_collection_registry(
        _fixture_bundle(),
        cast(MarketObjectStore, market_store),
        raw_store,
        lambda: FROZEN_NOW,
    )
    spec = registry.resolve(job_type.value)
    assert spec is not None

    result = await spec.handler(_context(job_type))

    assert result.outcome == "skipped"
    assert result.result_code == "FIXTURE_PROVIDER_DATA_UNAVAILABLE"
    assert market_store.objects == {}
    assert raw_store.objects == {}
    assert market_store.external_calls == raw_store.external_calls == 0


@pytest.mark.asyncio
async def test_provider_profile_mismatch_fails_before_fixture_or_external_work() -> None:
    registry = build_provider_collection_registry(
        _fixture_bundle(),
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: FROZEN_NOW,
        expected_profile_id="provider_production_v1",
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None

    with pytest.raises(UnsafeJobError, match="PROVIDER_PROFILE_MISMATCH"):
        await spec.handler(_context(JobType.COLLECT_MARKET))


@pytest.mark.asyncio
async def test_fosun_missing_frozen_authority_skips_before_provider_construction() -> None:
    fixture = _fixture_bundle()
    constructions = 0

    def forbidden_provider(_session: object) -> object:
        nonlocal constructions
        constructions += 1
        raise AssertionError("Fosun provider must not be constructed")

    bundle = ProviderBundle(
        mode=ProviderRuntimeMode.PRODUCTION,
        market_source="fosun",
        market_provider_factory=forbidden_provider,  # type: ignore[arg-type]
        snapshot_provider_factory=None,
        sec_client=fixture.sec_client,
        fred_client=fixture.fred_client,
    )
    registry = build_provider_collection_registry(
        bundle,
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: FROZEN_NOW,
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None

    result = await spec.handler(_context(JobType.COLLECT_MARKET))

    assert result.outcome == "skipped"
    assert result.result_code == "FOSUN_MARKET_AUTHORITY_UNAVAILABLE"
    assert constructions == 0


@pytest.mark.asyncio
async def test_alpaca_rejects_legacy_unstamped_job_before_provider_construction() -> None:
    fixture = _fixture_bundle()
    constructions = 0

    def forbidden_provider(_session: object) -> object:
        nonlocal constructions
        constructions += 1
        raise AssertionError("unstamped legacy job must not construct Alpaca provider")

    bundle = ProviderBundle(
        mode=ProviderRuntimeMode.PRODUCTION,
        market_source="alpaca",
        market_provider_factory=forbidden_provider,  # type: ignore[arg-type]
        snapshot_provider_factory=None,
        sec_client=fixture.sec_client,
        fred_client=fixture.fred_client,
    )
    registry = build_provider_collection_registry(
        bundle,
        cast(MarketObjectStore, InMemoryMarketObjectStore()),
        InMemoryRawEvidenceStore(),
        lambda: FROZEN_NOW,
    )
    spec = registry.resolve(JobType.COLLECT_MARKET.value)
    assert spec is not None

    result = await spec.handler(_context(JobType.COLLECT_MARKET))

    assert result.outcome == "skipped"
    assert result.result_code == "MARKET_SOURCE_MISMATCH"
    assert constructions == 0


def test_fosun_daily_publish_payload_rejects_duplicate_provider_symbols() -> None:
    authority = FrozenFosunMarketAuthority(
        operation="fosun_get_bars",
        delivery_mode="real_time",
        capability_id=UUID("0198d500-1000-7000-8000-000000002907"),
        capability_schema_version="1",
        capability_created_at=FROZEN_NOW - timedelta(hours=1),
        capability_expires_at=FROZEN_NOW + timedelta(hours=1),
        status_id=UUID("0198d500-1000-7000-8000-000000002908"),
        status_version=1,
        status_created_at=FROZEN_NOW - timedelta(hours=2),
        status_mode_changed_at=FROZEN_NOW - timedelta(hours=2),
        capability_invalidated_at=None,
        contract_hash="a" * 64,
    )
    children = (
        FosunDailyChildRef(
            job_id="0198d500-1000-7000-8000-000000002911",
            instrument_id="0198d500-1000-7000-8000-000000002921",
            instrument_alias_id="0198d500-1000-7000-8000-000000002931",
            instrument_symbol="AAPL",
            instrument_alias_identity_sha256="b" * 64,
            input_hash="c" * 64,
            ordinal=0,
        ),
        FosunDailyChildRef(
            job_id="0198d500-1000-7000-8000-000000002912",
            instrument_id="0198d500-1000-7000-8000-000000002922",
            instrument_alias_id="0198d500-1000-7000-8000-000000002932",
            instrument_symbol="AAPL",
            instrument_alias_identity_sha256="d" * 64,
            input_hash="e" * 64,
            ordinal=1,
        ),
    )

    with pytest.raises(ValueError, match="provider symbol is duplicated"):
        FosunDailyPublishPayload(
            root_job_id=str(ROOT_JOB_ID),
            calendar_authority_id="0198d500-1000-7000-8000-000000002904",
            session="2026-08-28",
            market_close_at="2026-08-28T20:00:00.000000Z",
            scheduled_for="2026-08-28T21:00:00.000000Z",
            profile_id="local_safe_v1",
            universe_snapshot_id="0198d500-1000-7000-8000-000000002905",
            universe_content_hash="f" * 64,
            market_authority=authority,
            children=children,
        )


@pytest.mark.parametrize(
    ("field_name", "wrong_value"),
    (
        ("root_job_id", UUID("0198d500-1000-7000-8000-000000002999")),
        ("fetch_job_id", UUID("0198d500-1000-7000-8000-000000002998")),
        ("instrument_id", UUID("0198d500-1000-7000-8000-000000002997")),
        ("record_kind", "fetched"),
        ("schema_version", "wrong_v1"),
        ("child_input_hash", "f" * 64),
        ("calendar_authority_id", UUID("0198d500-1000-7000-8000-000000002996")),
        ("universe_snapshot_id", UUID("0198d500-1000-7000-8000-000000002995")),
        ("universe_content_hash", "e" * 64),
        ("ordinal", 1),
        ("total_instruments", 2),
        ("request_start_at", datetime(2025, 7, 25, tzinfo=UTC)),
        ("request_end_at", datetime(2026, 8, 29, tzinfo=UTC)),
    ),
)
def test_fosun_daily_publisher_rejects_every_mismatched_staging_authority_field(
    field_name: str,
    wrong_value: object,
) -> None:
    authority = FrozenFosunMarketAuthority(
        operation="fosun_get_bars",
        delivery_mode="real_time",
        capability_id=UUID("0198d500-1000-7000-8000-000000002907"),
        capability_schema_version="1",
        capability_created_at=FROZEN_NOW - timedelta(hours=1),
        capability_expires_at=FROZEN_NOW + timedelta(hours=1),
        status_id=UUID("0198d500-1000-7000-8000-000000002908"),
        status_version=1,
        status_created_at=FROZEN_NOW - timedelta(hours=2),
        status_mode_changed_at=FROZEN_NOW - timedelta(hours=2),
        capability_invalidated_at=None,
        contract_hash="a" * 64,
    )
    instrument_id = "0198d500-1000-7000-8000-000000002906"
    universe_id = "0198d500-1000-7000-8000-000000002905"
    child = FosunDailyInstrumentPayload(
        root_job_id=str(ROOT_JOB_ID),
        calendar_authority_id="0198d500-1000-7000-8000-000000002904",
        session="2026-08-28",
        market_close_at="2026-08-28T20:00:00.000000Z",
        scheduled_for="2026-08-28T21:00:00.000000Z",
        profile_id="local_safe_v1",
        universe_snapshot_id=universe_id,
        universe_content_hash="c" * 64,
        instrument_id=instrument_id,
        instrument_alias_id="0198d500-1000-7000-8000-000000002903",
        instrument_symbol="AAPL",
        instrument_alias_identity_sha256="d" * 64,
        ordinal=0,
        total_instruments=1,
        market_authority=authority,
    )
    child_ref = FosunDailyChildRef(
        job_id=str(CLAIM_JOB_ID),
        instrument_id=instrument_id,
        instrument_alias_id=child.instrument_alias_id,
        instrument_symbol=child.instrument_symbol,
        instrument_alias_identity_sha256=child.instrument_alias_identity_sha256,
        input_hash="b" * 64,
        ordinal=0,
    )
    publish = FosunDailyPublishPayload(
        root_job_id=str(ROOT_JOB_ID),
        calendar_authority_id=child.calendar_authority_id,
        session=child.session,
        market_close_at=child.market_close_at,
        scheduled_for=child.scheduled_for,
        profile_id=child.profile_id,
        universe_snapshot_id=universe_id,
        universe_content_hash=child.universe_content_hash,
        market_authority=authority,
        children=(child_ref,),
    )
    row = FosunDailyBarStaging(
        root_job_id=ROOT_JOB_ID,
        fetch_job_id=CLAIM_JOB_ID,
        instrument_id=UUID(instrument_id),
        record_kind="reservation",
        schema_version="fosun_daily_staging_v1",
        child_input_hash=child_ref.input_hash,
        calendar_authority_id=UUID(child.calendar_authority_id),
        universe_snapshot_id=UUID(universe_id),
        universe_content_hash=child.universe_content_hash,
        ordinal=0,
        total_instruments=1,
        request_start_at=datetime(2025, 7, 24, tzinfo=UTC),
        request_end_at=datetime(2026, 8, 28, tzinfo=UTC),
    )
    setattr(row, field_name, wrong_value)

    with pytest.raises(UnsafeJobError, match="FOSUN_DAILY_STAGING_AUTHORITY_MISMATCH"):
        _require_staging_common(
            row,
            child_ref=child_ref,
            child_payload=child,
            publish_payload=publish,
            expected_kind="reservation",
        )

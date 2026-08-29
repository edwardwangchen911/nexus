"""Provider collection Job admission and fixture isolation contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

import pytest
from nexus.jobs.daily_payloads import DailyStagePayload
from nexus.jobs.daily_pipeline import build_daily_pipeline_registry
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.provider_collection import build_provider_collection_registry
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import ClaimedJob
from nexus.jobs.worker import UnsafeJobError
from nexus.market.ingestion import MarketObjectStore
from nexus.providers.composition import ProviderBundle, build_provider_bundle
from nexus.providers.settings import ProviderRuntimeMode, ProviderSettings
from sqlalchemy.ext.asyncio import AsyncSession

FROZEN_NOW = datetime(2026, 8, 28, 21, tzinfo=UTC)
ROOT_JOB_ID = UUID("0198d500-1000-7000-8000-000000002901")
CLAIM_JOB_ID = UUID("0198d500-1000-7000-8000-000000002902")
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
    }
    for spec in registry.specs:
        assert spec.payload_model is DailyStagePayload
        assert spec.queue is QueueName.PROVIDER
        assert spec.timeout_seconds == 45
        assert spec.max_attempts == 3


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
        enabled_stage_types=frozenset(
            {JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO}
        ),
        stage_specs=provider.specs,
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

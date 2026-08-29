"""Closed durable handler for bounded display-only intraday Snapshots."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.jobs.errors import UnsafeJobError
from nexus.jobs.intraday_schedule import (
    IntradaySnapshotJobPayloadV1,
    revalidate_intraday_payload_authority,
)
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import (
    Clock,
    JobHandlerContext,
    JobHandlerResult,
    JobHandlerSpec,
    JobRegistry,
)
from nexus.market.ingestion import MarketObjectStore
from nexus.market.intraday_ingestion import (
    IntradayIngestionResult,
    IntradayRawArchive,
    archive_intraday_snapshot_raw,
    ingest_intraday_snapshot,
)
from nexus.persistence.models.data import DataArtifact
from nexus.providers.base import InstrumentId
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotInstrument,
)
from nexus.providers.settings import ProviderSettings


class IntradaySnapshotProvider(Protocol):
    async def fetch_raw_snapshots(
        self, request: IntradaySnapshotRequest
    ) -> RawSnapshotEnvelope: ...

    def normalize_raw_snapshots(
        self,
        envelope: RawSnapshotEnvelope,
        request: IntradaySnapshotRequest,
    ) -> SnapshotBatch: ...


ProviderFactory = Callable[[object], IntradaySnapshotProvider]


class IngestSnapshot(Protocol):
    async def __call__(
        self,
        session: AsyncSession,
        object_store: MarketObjectStore,
        batch: SnapshotBatch,
        *,
        raw_artifact: DataArtifact | None = None,
        commit: bool = True,
    ) -> IntradayIngestionResult: ...


class RevalidateAuthority(Protocol):
    async def __call__(
        self,
        session: AsyncSession,
        payload: IntradaySnapshotJobPayloadV1,
        *,
        effective_at: datetime,
        settings: ProviderSettings,
    ) -> bool: ...


class ArchiveRawSnapshot(Protocol):
    async def __call__(
        self,
        session: AsyncSession,
        object_store: MarketObjectStore,
        envelope: RawSnapshotEnvelope,
        *,
        commit: bool = True,
    ) -> IntradayRawArchive: ...


def build_intraday_collection_registry(
    provider_factory: ProviderFactory,
    store: MarketObjectStore,
    now: Clock,
    *,
    settings: ProviderSettings,
    ingest: IngestSnapshot = ingest_intraday_snapshot,
    archive_raw: ArchiveRawSnapshot = archive_intraday_snapshot_raw,
    revalidate: RevalidateAuthority = revalidate_intraday_payload_authority,
) -> JobRegistry:
    """Build the one-handler registry without constructing an external provider."""
    _ = now

    async def collect(context: JobHandlerContext) -> JobHandlerResult:
        payload = cast(IntradaySnapshotJobPayloadV1, context.payload)
        finished_at = context.finished_at()
        bucket_end = _parse_utc(payload.bucket_end)
        if finished_at >= bucket_end + timedelta(seconds=45):
            return JobHandlerResult(
                outcome="skipped",
                result_code="INTRADAY_BUCKET_EXPIRED",
            )
        authority_valid = await revalidate(
            cast(AsyncSession, context.session),
            payload,
            effective_at=context.claim.effective_at,
            settings=settings,
        )
        if not authority_valid:
            raise UnsafeJobError("INTRADAY_AUTHORITY_MISMATCH")
        request = IntradaySnapshotRequest(
            instruments=tuple(
                SnapshotInstrument(
                    instrument_id=InstrumentId(item.instrument_id),
                    symbol=item.symbol,
                )
                for item in payload.instruments
            ),
            symbol_as_of=payload.session,
            feed=payload.feed,
            delivery_mode=payload.delivery_mode,
        )
        provider = provider_factory(context.session)
        envelope = await provider.fetch_raw_snapshots(request)
        archived = await archive_raw(
            cast(AsyncSession, context.session),
            store,
            envelope,
            commit=False,
        )
        try:
            batch = provider.normalize_raw_snapshots(envelope, request)
        except MarketProviderPayloadError:
            return JobHandlerResult(
                outcome="skipped",
                result_code="INTRADAY_SNAPSHOT_PAYLOAD_INVALID",
            )
        contract_matches = _batch_matches_payload(batch, request)
        authority_still_valid = await revalidate(
            cast(AsyncSession, context.session),
            payload,
            effective_at=context.claim.effective_at,
            settings=settings,
        )
        if not contract_matches or not authority_still_valid:
            await ingest(
                cast(AsyncSession, context.session),
                store,
                _raw_only(batch),
                raw_artifact=archived.raw_artifact,
                commit=False,
            )
            return JobHandlerResult(
                outcome="skipped",
                result_code=(
                    "INTRADAY_CONTRACT_MISMATCH"
                    if not contract_matches
                    else "INTRADAY_AUTHORITY_CHANGED_AFTER_FETCH"
                ),
            )
        incomplete = sum(
            snapshot.trade is None
            or snapshot.quote is None
            or snapshot.minute_bar is None
            for snapshot in batch.snapshots
        )
        ingestion = cast(
            IntradayIngestionResult,
            await ingest(
                cast(AsyncSession, context.session),
                store,
                batch,
                raw_artifact=archived.raw_artifact,
                commit=False,
            ),
        )
        if incomplete:
            return JobHandlerResult(
                outcome="skipped",
                result_code="INTRADAY_SNAPSHOT_PARTIAL",
            )
        created = (
            ingestion.created_trade_observations
            + ingestion.created_quote_observations
            + ingestion.created_bar_authorities
        )
        if created > 0:
            return JobHandlerResult(
                outcome="succeeded",
                result_code="INTRADAY_SNAPSHOT_COLLECTED",
            )
        if ingestion.replayed > 0:
            return JobHandlerResult(
                outcome="succeeded",
                result_code="INTRADAY_SNAPSHOT_REPLAYED",
            )
        return JobHandlerResult(
            outcome="skipped",
            result_code="INTRADAY_SNAPSHOT_PARTIAL",
        )

    return JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.COLLECT_INTRADAY_SNAPSHOT,
                schema_version="intraday_snapshot_job_v1",
                payload_model=IntradaySnapshotJobPayloadV1,
                queue=QueueName.PROVIDER,
                timeout_seconds=10,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace="intraday.snapshot",
                handler=collect,
            ),
        )
    )


def _batch_matches_payload(
    batch: SnapshotBatch,
    request: IntradaySnapshotRequest,
) -> bool:
    expected = tuple((item.instrument_id, item.symbol) for item in request.instruments)
    actual = tuple((item.instrument_id, item.symbol) for item in batch.snapshots)
    return (
        actual == expected
        and batch.provenance.source == "alpaca"
        and batch.provenance.feed == request.feed
        and batch.provenance.delivery_mode == request.delivery_mode
    )


def _raw_only(batch: SnapshotBatch) -> SnapshotBatch:
    return batch.model_copy(
        update={
            "snapshots": tuple(
                snapshot.model_copy(
                    update={"trade": None, "quote": None, "minute_bar": None}
                )
                for snapshot in batch.snapshots
            )
        }
    )


def _parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)

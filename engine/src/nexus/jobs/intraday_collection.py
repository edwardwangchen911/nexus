"""Closed durable handler for bounded display-only intraday Snapshots."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.jobs.intraday_schedule import IntradaySnapshotJobPayloadV1
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
    ingest_intraday_snapshot,
)
from nexus.providers.base import InstrumentId
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    SnapshotBatch,
    SnapshotInstrument,
)


class IntradaySnapshotProvider(Protocol):
    async def fetch_snapshots(self, request: IntradaySnapshotRequest) -> SnapshotBatch: ...


ProviderFactory = Callable[[object], IntradaySnapshotProvider]


class IngestSnapshot(Protocol):
    async def __call__(
        self,
        session: AsyncSession,
        object_store: MarketObjectStore,
        batch: SnapshotBatch,
        *,
        commit: bool = True,
    ) -> IntradayIngestionResult: ...


def build_intraday_collection_registry(
    provider_factory: ProviderFactory,
    store: MarketObjectStore,
    now: Clock,
    *,
    ingest: IngestSnapshot = ingest_intraday_snapshot,
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
        request = IntradaySnapshotRequest(
            instruments=tuple(
                SnapshotInstrument(
                    instrument_id=InstrumentId(item.instrument_id),
                    symbol=item.symbol,
                )
                for item in payload.instruments
            ),
            feed=payload.feed,
            delivery_mode=payload.delivery_mode,
        )
        provider = provider_factory(context.session)
        batch = await provider.fetch_snapshots(request)
        if not _batch_matches_payload(batch, request):
            return JobHandlerResult(
                outcome="skipped",
                result_code="INTRADAY_CONTRACT_MISMATCH",
            )
        missing = sum(
            snapshot.trade is None and snapshot.quote is None and snapshot.minute_bar is None
            for snapshot in batch.snapshots
        )
        ingestion: IntradayIngestionResult | None = None
        if missing < len(batch.snapshots):
            ingestion = cast(
                IntradayIngestionResult,
                await ingest(
                    cast(AsyncSession, context.session),
                    store,
                    batch,
                    commit=False,
                ),
            )
        if missing:
            return JobHandlerResult(
                outcome="skipped",
                result_code="INTRADAY_SNAPSHOT_PARTIAL",
            )
        assert ingestion is not None
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
                queue=QueueName.GENERAL,
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


def _parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)

"""Closed durable handler for bounded display-only intraday Snapshots."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.jobs.errors import UnsafeJobError
from nexus.jobs.intraday_schedule import (
    IntradaySnapshotJobPayloadV2,
    revalidate_intraday_payload_authority,
)
from nexus.jobs.market_authority import ProviderMarketBinding, binding_at
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
from nexus.providers.market.fosun import (
    FosunSnapshotReadAborted,
    FosunSnapshotReadGuard,
)
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotInstrument,
)
from nexus.providers.settings import ProviderSettings


class IntradaySnapshotProvider(Protocol):
    async def fetch_raw_snapshots(
        self,
        request: IntradaySnapshotRequest,
        *,
        read_guard: FosunSnapshotReadGuard | None = None,
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
        quote_binding: ProviderMarketBinding | None = None,
        bar_binding: ProviderMarketBinding | None = None,
        bar_bindings: Mapping[UUID, ProviderMarketBinding] | None = None,
        commit: bool = True,
    ) -> IntradayIngestionResult: ...


class RevalidateAuthority(Protocol):
    async def __call__(
        self,
        session: AsyncSession,
        payload: IntradaySnapshotJobPayloadV2,
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


class _IntradayAuthorityGuard:
    """Bind each response to the exact authority visible after that read."""

    def __init__(
        self,
        *,
        context: JobHandlerContext,
        payload: IntradaySnapshotJobPayloadV2,
        settings: ProviderSettings,
        revalidate: RevalidateAuthority,
        request: IntradaySnapshotRequest,
    ) -> None:
        self._context = context
        self._payload = payload
        self._settings = settings
        self._revalidate = revalidate
        self._instrument_by_code = {
            f"us{item.symbol}": item.instrument_id for item in request.instruments
        }
        self.quote_binding: ProviderMarketBinding | None = None
        self.bar_bindings: dict[UUID, ProviderMarketBinding] = {}

    async def before_read(
        self,
        operation: Literal["fosun_get_quotes", "fosun_get_bars"],
        instrument_codes: tuple[str, ...],
    ) -> bool:
        if not self._shape_matches(operation, instrument_codes):
            return False
        return await self._valid_now()

    async def after_read(
        self,
        operation: Literal["fosun_get_quotes", "fosun_get_bars"],
        instrument_codes: tuple[str, ...],
        response: RawGatewayResponse,
    ) -> bool:
        if response.operation != operation or not self._shape_matches(
            operation, instrument_codes
        ):
            return False
        checked_at = self._context.finished_at()
        valid = await self._revalidate(
            cast(AsyncSession, self._context.session),
            self._payload,
            effective_at=checked_at,
            settings=self._settings,
        )
        if not valid:
            return False
        if operation == "fosun_get_quotes":
            self.quote_binding = binding_at(
                self._payload.quote_authority,
                authorized_at=checked_at,
            )
        else:
            authority = self._payload.bars_authority
            if authority is None:
                return False
            for code in instrument_codes:
                self.bar_bindings[self._instrument_by_code[code]] = binding_at(
                    authority,
                    authorized_at=checked_at,
                )
        return True

    async def _valid_now(self) -> bool:
        return await self._revalidate(
            cast(AsyncSession, self._context.session),
            self._payload,
            effective_at=self._context.finished_at(),
            settings=self._settings,
        )

    def _shape_matches(
        self,
        operation: Literal["fosun_get_quotes", "fosun_get_bars"],
        instrument_codes: tuple[str, ...],
    ) -> bool:
        expected = tuple(sorted(self._instrument_by_code))
        if operation == "fosun_get_quotes":
            return instrument_codes == expected
        return (
            self._payload.include_minute_bars
            and len(instrument_codes) == 1
            and instrument_codes[0] in self._instrument_by_code
        )


def build_intraday_collection_registry(
    provider_factory: ProviderFactory,
    store: MarketObjectStore,
    now: Clock,
    *,
    settings: ProviderSettings,
    ingest: IngestSnapshot = ingest_intraday_snapshot,
    archive_raw: ArchiveRawSnapshot = archive_intraday_snapshot_raw,
    revalidate: RevalidateAuthority = revalidate_intraday_payload_authority,
    raw_archive_session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> JobRegistry:
    """Build the one-handler registry without constructing an external provider."""
    _ = now

    async def collect(context: JobHandlerContext) -> JobHandlerResult:
        payload = cast(IntradaySnapshotJobPayloadV2, context.payload)
        pre_check_at = context.finished_at()
        bucket_end = _parse_utc(payload.bucket_end)
        if pre_check_at >= bucket_end + timedelta(seconds=45):
            return JobHandlerResult(
                outcome="skipped",
                result_code="INTRADAY_BUCKET_EXPIRED",
            )
        authority_valid = await revalidate(
            cast(AsyncSession, context.session),
            payload,
            effective_at=pre_check_at,
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
            bucket_start=_parse_utc(payload.bucket_start),
            include_minute_bars=payload.include_minute_bars,
            feed=payload.feed,
            delivery_mode=payload.delivery_mode,
        )
        provider = provider_factory(context.session)
        read_guard = _IntradayAuthorityGuard(
            context=context,
            payload=payload,
            settings=settings,
            revalidate=revalidate,
            request=request,
        )
        try:
            envelope = await provider.fetch_raw_snapshots(
                request,
                read_guard=read_guard,
            )
        except FosunSnapshotReadAborted as exc:
            if exc.envelope is not None:
                await _archive_snapshot_raw(
                    cast(AsyncSession, context.session),
                    store,
                    exc.envelope,
                    archive=archive_raw,
                    session_factory=raw_archive_session_factory,
                )
            return JobHandlerResult(
                outcome="skipped",
                result_code=exc.result_code,
            )
        archived = await _archive_snapshot_raw(
            cast(AsyncSession, context.session),
            store,
            envelope,
            archive=archive_raw,
            session_factory=raw_archive_session_factory,
        )
        try:
            batch = provider.normalize_raw_snapshots(envelope, request)
        except MarketProviderPayloadError:
            return JobHandlerResult(
                outcome="skipped",
                result_code="INTRADAY_SNAPSHOT_PAYLOAD_INVALID",
            )
        published_bar_ids = {
            UUID(str(snapshot.instrument_id))
            for snapshot in batch.snapshots
            if snapshot.minute_bar is not None
        }
        contract_matches = _batch_matches_payload(batch, request)
        post_check_at = context.finished_at()
        authority_still_valid = await revalidate(
            cast(AsyncSession, context.session),
            payload,
            effective_at=post_check_at,
            settings=settings,
        )
        bars_authority = payload.bars_authority
        published_bar_bindings = (
            {}
            if bars_authority is None
            else {
                instrument_id: binding_at(
                    bars_authority,
                    authorized_at=post_check_at,
                )
                for instrument_id in read_guard.bar_bindings
                if instrument_id in published_bar_ids
            }
        )
        binding_complete = read_guard.quote_binding is not None and (
            not payload.include_minute_bars
            or set(published_bar_bindings) == published_bar_ids
        )
        if not contract_matches or not authority_still_valid or not binding_complete:
            return JobHandlerResult(
                outcome="skipped",
                result_code=(
                    "INTRADAY_CONTRACT_MISMATCH"
                    if not contract_matches or not binding_complete
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
                quote_binding=read_guard.quote_binding,
                bar_bindings=(
                    None
                    if not payload.include_minute_bars
                    else published_bar_bindings
                ),
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
                schema_version="intraday_snapshot_job_v2",
                payload_model=IntradaySnapshotJobPayloadV2,
                queue=QueueName.PROVIDER,
                timeout_seconds=30,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace="intraday.snapshot",
                handler=collect,
            ),
        )
    )


async def _archive_snapshot_raw(
    session: AsyncSession,
    store: MarketObjectStore,
    envelope: RawSnapshotEnvelope,
    *,
    archive: ArchiveRawSnapshot,
    session_factory: async_sessionmaker[AsyncSession] | None,
) -> IntradayRawArchive:
    if session_factory is None:
        return await archive(session, store, envelope, commit=False)
    async with session_factory() as archive_session:
        archived = await archive(
            archive_session,
            store,
            envelope,
            commit=True,
        )
        await archive_session.refresh(archived.raw_artifact)
        archive_session.expunge(archived.raw_artifact)
        return archived


def _batch_matches_payload(
    batch: SnapshotBatch,
    request: IntradaySnapshotRequest,
) -> bool:
    expected = tuple((item.instrument_id, item.symbol) for item in request.instruments)
    actual = tuple((item.instrument_id, item.symbol) for item in batch.snapshots)
    return (
        actual == expected
        and batch.provenance.source == "fosun"
        and batch.provenance.feed == request.feed
        and batch.provenance.delivery_mode == request.delivery_mode
    )


def _parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)

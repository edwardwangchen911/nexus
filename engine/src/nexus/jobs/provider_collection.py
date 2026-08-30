"""Durable daily collection handlers for bounded research-data providers."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import Protocol, TypeVar, cast
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.audit.canonical import canonical_json
from nexus.domain.evidence import EvidenceRef
from nexus.jobs.daily_payloads import (
    FOSUN_DAILY_INSTRUMENT_SCHEMA,
    FOSUN_DAILY_MAX_INSTRUMENTS,
    FOSUN_DAILY_PUBLISH_SCHEMA,
    STAGE_SCHEMA,
    DailyStagePayload,
    FosunDailyChildRef,
    FosunDailyInstrumentPayload,
    FosunDailyPublishPayload,
    instrument_alias_identity_sha256,
    parse_utc,
)
from nexus.jobs.errors import RetryableJobError, UnsafeJobError
from nexus.jobs.market_authority import (
    FrozenFosunMarketAuthority,
    ProviderMarketBinding,
    acquire_fosun_market_authority_locks,
    binding_at,
    load_frozen_fosun_market_authority,
)
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import (
    JobHandler,
    JobHandlerContext,
    JobHandlerResult,
    JobHandlerSpec,
    JobRegistry,
)
from nexus.market.artifact_authority import (
    data_artifact_identity_sha256,
    lock_and_load_data_artifacts,
)
from nexus.market.ingestion import (
    MarketObjectStore,
    MarketRawArchive,
    PreparedPricePage,
    archive_fosun_price_page_raw,
    ingest_price_page,
    object_uri_matches_key,
    persist_prepared_price_page,
    prepare_price_page,
    serialize_price_bars,
)
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    FosunDailyBarStaging,
    MacroObservation,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.research import (
    Filing,
    FinancialFactRecord,
    ResearchEvidenceRef,
)
from nexus.persistence.models.runtime import Job
from nexus.providers.base import BarsRequest, InstrumentId, PriceBar, ProviderPage
from nexus.providers.composition import ProviderBundle
from nexus.providers.fred import FRED_SERIES, normalize_alfred_observation
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.fosun_gateway import FosunMarketGatewayError, RawGatewayResponse
from nexus.providers.sec import (
    SecInstrumentIdentity,
    is_canonical_sec_accession_number,
    normalize_company_facts,
)
from nexus.providers.settings import ProviderRuntimeMode

_FOSUN_DAILY_HANDLER_INTERNAL_TIMEOUT_SECONDS = 44.0
_FOSUN_DAILY_OUTER_MARGIN_SECONDS = 1.0
_FOSUN_DAILY_FETCH_TIMEOUT_SECONDS = 10.0
_FOSUN_DAILY_RAW_ARCHIVE_RESERVE_SECONDS = 6.0
_FOSUN_DAILY_FINALIZE_RESERVE_SECONDS = 3.0


class RawEvidenceStore(Protocol):
    """Content-addressed evidence boundary used by provider collection Jobs."""

    async def put_if_absent(self, *, key: str, content: bytes) -> str: ...


class RawFirstFosunMarketProvider(Protocol):
    """Narrow Fosun seam that preserves exact bytes before interpretation."""

    async def fetch_raw_bars(
        self,
        request: BarsRequest,
        cursor: str | None = None,
        *,
        symbol: str | None = None,
    ) -> RawGatewayResponse: ...

    async def normalize_bars(
        self,
        request: BarsRequest,
        response: RawGatewayResponse,
        cursor: str | None = None,
        *,
        symbol: str | None = None,
    ) -> ProviderPage[PriceBar]: ...

    async def expected_bars_request_body_sha256(
        self,
        request: BarsRequest,
        cursor: str | None = None,
        *,
        symbol: str | None = None,
    ) -> str: ...


MarketAuthorityLoader = Callable[
    ...,
    Awaitable[FrozenFosunMarketAuthority | None],
]

_FOSUN_DAILY_OBJECT_CONCURRENCY = 32
_FOSUN_DAILY_CALL_INTERVAL_SECONDS = 7.0
_LEGACY_DAILY_MAX_INSTRUMENTS = 100

_T = TypeVar("_T")
_R = TypeVar("_R")


def build_provider_collection_registry(
    bundle: ProviderBundle,
    market_object_store: MarketObjectStore,
    raw_evidence_store: RawEvidenceStore,
    now: Callable[[], datetime],
    *,
    expected_profile_id: str | None = None,
    raw_archive_session_factory: async_sessionmaker[AsyncSession] | None = None,
    market_authority_loader: MarketAuthorityLoader = load_frozen_fosun_market_authority,
) -> JobRegistry:
    """Build the closed provider collection registry for one runtime mode."""

    async def collect_market(context: JobHandlerContext) -> JobHandlerResult:
        _require_provider_profile(context, expected_profile_id)
        if bundle.mode is ProviderRuntimeMode.FIXTURE:
            return _fixture_skip()
        payload = cast(DailyStagePayload, context.payload)
        if bundle.market_source == "fosun" and (
            payload.market_source != "fosun" or payload.market_authority is None
        ):
            return JobHandlerResult(
                outcome="skipped",
                result_code="FOSUN_MARKET_AUTHORITY_UNAVAILABLE",
            )
        if bundle.market_source == "fosun":
            return JobHandlerResult(
                outcome="skipped",
                result_code="FOSUN_DAILY_SHARDING_REQUIRED",
            )
        if bundle.market_source != "fosun" and payload.market_source != bundle.market_source:
            return JobHandlerResult(
                outcome="skipped",
                result_code="MARKET_SOURCE_MISMATCH",
            )
        universe = await _frozen_universe(context, payload)
        if universe is None:
            return JobHandlerResult(
                outcome="skipped",
                result_code="FROZEN_UNIVERSE_MISMATCH",
            )
        max_instruments = _LEGACY_DAILY_MAX_INSTRUMENTS
        instruments = tuple(
            (
                await context.session.scalars(
                    select(Instrument)
                    .join(
                        UniverseMember,
                        UniverseMember.instrument_id == Instrument.id,
                    )
                    .where(
                        UniverseMember.universe_snapshot_id == universe.id,
                        Instrument.currency == "USD",
                        Instrument.exchange.in_(US_EQUITY_EXCHANGES),
                    )
                    .order_by(Instrument.id)
                    .limit(max_instruments + 1)
                )
            ).all()
        )
        if not instruments:
            return JobHandlerResult(
                outcome="skipped",
                result_code="FROZEN_UNIVERSE_EMPTY",
            )
        if len(instruments) > max_instruments:
            return JobHandlerResult(
                outcome="skipped",
                result_code="FROZEN_UNIVERSE_TOO_LARGE",
            )
        market_close = parse_utc(payload.market_close_at)
        request_start_at, request_end_at = _daily_request_window(market_close)
        provider = bundle.market_provider(context.session)
        collected = 0
        replayed = 0
        source_conflict = False
        for instrument in instruments:
            cursor: str | None = None
            seen_cursors: set[str] = set()
            for _page_number in range(10):
                page = await provider.fetch_bars(
                    BarsRequest(
                        instrument_id=InstrumentId(instrument.id),
                        start=request_start_at,
                        end=request_end_at,
                        interval="1Day",
                        adjustment="all",
                    ),
                    cursor,
                )
                if page.items:
                    if any(item.instrument_id != instrument.id for item in page.items):
                        raise RuntimeError("MARKET_PROVIDER_IDENTITY_MISMATCH")
                    digest = sha256(serialize_price_bars(page.items)).hexdigest()
                    exists = await context.session.scalar(
                        select(DataArtifact.id).where(DataArtifact.sha256 == digest)
                    )
                    if exists is not None:
                        replayed += 1
                    else:
                        try:
                            result = await ingest_price_page(
                                context.session,
                                market_object_store,
                                page,
                                commit=False,
                            )
                        except ProviderObjectIOUnavailable as error:
                            raise RetryableJobError(error.error_code) from error
                        collected += 1
                        source_conflict = source_conflict or result.price_authority is None
                cursor = page.next_cursor
                if cursor is None:
                    break
                if cursor in seen_cursors:
                    raise RuntimeError("MARKET_PROVIDER_CURSOR_CYCLE")
                seen_cursors.add(cursor)
            else:
                raise RuntimeError("MARKET_PROVIDER_PAGE_LIMIT")
        _ = now
        if source_conflict:
            return JobHandlerResult(outcome="skipped", result_code="SOURCE_CONFLICT")
        if collected:
            return JobHandlerResult(outcome="succeeded", result_code="MARKET_COLLECTED")
        if replayed:
            return JobHandlerResult(outcome="succeeded", result_code="MARKET_REPLAYED")
        return JobHandlerResult(outcome="skipped", result_code="MARKET_NO_DATA")

    async def collect_sec_macro(context: JobHandlerContext) -> JobHandlerResult:
        _require_provider_profile(context, expected_profile_id)
        if bundle.mode is ProviderRuntimeMode.FIXTURE:
            return _fixture_skip()
        payload = cast(DailyStagePayload, context.payload)
        universe = await _frozen_universe(context, payload)
        if universe is None:
            return JobHandlerResult(
                outcome="skipped",
                result_code="FROZEN_UNIVERSE_MISMATCH",
            )
        instruments = await _sec_instruments(context, payload, universe)
        if instruments is None:
            return JobHandlerResult(
                outcome="skipped",
                result_code="FROZEN_UNIVERSE_TOO_LARGE",
            )
        try:
            sec_collected, sec_replayed = await _collect_sec(
                context,
                bundle=bundle,
                store=raw_evidence_store,
                instruments=instruments,
            )
            macro_collected, macro_replayed = await _collect_macro(
                context,
                payload=payload,
                bundle=bundle,
                store=raw_evidence_store,
            )
        except ProviderObjectIOUnavailable as error:
            raise RetryableJobError(error.error_code) from error
        _ = now()
        if sec_collected or macro_collected:
            return JobHandlerResult(
                outcome="succeeded",
                result_code="SEC_MACRO_COLLECTED",
            )
        if sec_replayed or macro_replayed:
            return JobHandlerResult(
                outcome="succeeded",
                result_code="SEC_MACRO_REPLAYED",
            )
        return JobHandlerResult(outcome="skipped", result_code="SEC_MACRO_NO_DATA")

    async def collect_fosun_daily_instrument(
        context: JobHandlerContext,
    ) -> JobHandlerResult:
        loop = asyncio.get_running_loop()
        handler_started_at = loop.time()
        worker_deadline = context.monotonic_deadline()
        overall_deadline = (
            handler_started_at + _FOSUN_DAILY_HANDLER_INTERNAL_TIMEOUT_SECONDS
        )
        if worker_deadline is not None:
            overall_deadline = min(
                overall_deadline,
                worker_deadline - _FOSUN_DAILY_OUTER_MARGIN_SECONDS,
            )
        archive_deadline = overall_deadline - _FOSUN_DAILY_FINALIZE_RESERVE_SECONDS
        acquisition_deadline = (
            archive_deadline - _FOSUN_DAILY_RAW_ARCHIVE_RESERVE_SECONDS
        )
        latest_call_start_deadline = (
            acquisition_deadline - _FOSUN_DAILY_FETCH_TIMEOUT_SECONDS
        )
        current_task = asyncio.current_task()
        if current_task is None:
            raise UnsafeJobError("FOSUN_DAILY_ASYNC_TASK_REQUIRED")
        cancellation_baseline = current_task.cancelling()
        _require_provider_profile(context, expected_profile_id)
        if bundle.mode is ProviderRuntimeMode.FIXTURE:
            return _fixture_skip()
        if bundle.market_source != "fosun":
            raise UnsafeJobError("FOSUN_DAILY_PROVIDER_MISMATCH")
        if raw_archive_session_factory is None:
            raise UnsafeJobError("FOSUN_DAILY_STAGING_SESSION_REQUIRED")
        payload = cast(FosunDailyInstrumentPayload, context.payload)
        frozen = payload.market_authority
        response: RawGatewayResponse | None = None
        acquisition_deadline_exhausted = False
        acquisition_transition_error: Exception | None = None
        try:
            async with asyncio.timeout_at(acquisition_deadline):
                await _require_fosun_daily_instrument(context, payload)
                pre_check = context.finished_at()
                if not await _matches_frozen_market_authority(
                    context,
                    loader=market_authority_loader,
                    frozen=frozen,
                    check_time=pre_check,
                ):
                    raise UnsafeJobError("FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_FETCH")
                request_start_at, request_end_at = _daily_request_window(
                    parse_utc(payload.market_close_at)
                )
                provider = cast(
                    RawFirstFosunMarketProvider,
                    bundle.market_provider(context.session),
                )
                if (
                    not callable(getattr(provider, "fetch_raw_bars", None))
                    or not callable(getattr(provider, "normalize_bars", None))
                    or not callable(
                        getattr(provider, "expected_bars_request_body_sha256", None)
                    )
                ):
                    raise UnsafeJobError("FOSUN_RAW_FIRST_PROVIDER_REQUIRED")
                request = BarsRequest(
                    instrument_id=InstrumentId(UUID(payload.instrument_id)),
                    start=request_start_at,
                    end=request_end_at,
                    interval="1Day",
                    adjustment="raw",
                )
                async with _fosun_daily_call_permit(
                    raw_archive_session_factory,
                    context=context,
                    payload=payload,
                    market_authority_loader=market_authority_loader,
                    request_start_at=request_start_at,
                    request_end_at=request_end_at,
                    latest_call_start_deadline=latest_call_start_deadline,
                ):
                    fetch_deadline = min(
                        acquisition_deadline,
                        loop.time() + _FOSUN_DAILY_FETCH_TIMEOUT_SECONDS,
                    )
                    async with asyncio.timeout_at(fetch_deadline):
                        response = await provider.fetch_raw_bars(
                            request,
                            symbol=payload.instrument_symbol,
                        )
        except TimeoutError:
            if current_task.cancelling() > cancellation_baseline:
                raise asyncio.CancelledError from None
            acquisition_deadline_exhausted = True
        except Exception as error:
            if response is None:
                raise
            acquisition_transition_error = error
        if response is None:
            raise UnsafeJobError("FOSUN_DAILY_PHASE_BUDGET_EXHAUSTED")
        try:
            async with asyncio.timeout_at(archive_deadline):
                archived = await _archive_fosun_price_raw(
                    cast(AsyncSession, context.session),
                    market_object_store,
                    response,
                    session_factory=raw_archive_session_factory,
                )
        except TimeoutError:
            if current_task.cancelling() > cancellation_baseline:
                raise asyncio.CancelledError from None
            raise UnsafeJobError("FOSUN_DAILY_RAW_ARCHIVE_DEADLINE_EXCEEDED") from None
        if acquisition_deadline_exhausted:
            raise UnsafeJobError("FOSUN_DAILY_ACQUISITION_DEADLINE_EXCEEDED")
        if acquisition_transition_error is not None:
            raise UnsafeJobError("FOSUN_DAILY_ACQUISITION_TRANSITION_FAILED") from (
                acquisition_transition_error
            )
        try:
            async with asyncio.timeout_at(overall_deadline):
                post_check = context.finished_at()
                if not await _matches_frozen_market_authority(
                    context,
                    loader=market_authority_loader,
                    frozen=frozen,
                    check_time=post_check,
                ):
                    raise UnsafeJobError("FOSUN_MARKET_AUTHORITY_CHANGED_AFTER_FETCH")
                await _stage_fosun_daily_fetch(
                    raw_archive_session_factory,
                    context=context,
                    payload=payload,
                    request_start_at=request_start_at,
                    request_end_at=request_end_at,
                    response=response,
                    raw_artifact=archived.raw_artifact,
                    authorized_at=post_check,
                )
        except TimeoutError:
            if current_task.cancelling() > cancellation_baseline:
                raise asyncio.CancelledError from None
            raise UnsafeJobError("FOSUN_DAILY_FINALIZE_DEADLINE_EXCEEDED") from None
        return JobHandlerResult(
            outcome="succeeded",
            result_code="FOSUN_DAILY_INSTRUMENT_STAGED",
        )

    async def publish_fosun_daily(context: JobHandlerContext) -> JobHandlerResult:
        _require_provider_profile(context, expected_profile_id)
        if bundle.mode is ProviderRuntimeMode.FIXTURE:
            return _fixture_skip()
        if bundle.market_source != "fosun":
            raise UnsafeJobError("FOSUN_DAILY_PROVIDER_MISMATCH")
        payload = cast(FosunDailyPublishPayload, context.payload)
        return await _publish_fosun_daily(
            context,
            payload=payload,
            bundle=bundle,
            market_object_store=market_object_store,
            market_authority_loader=market_authority_loader,
        )

    return JobRegistry(
        (
            _spec(JobType.COLLECT_MARKET, collect_market),
            JobHandlerSpec(
                job_type=JobType.COLLECT_FOSUN_DAILY_INSTRUMENT,
                schema_version=FOSUN_DAILY_INSTRUMENT_SCHEMA,
                payload_model=FosunDailyInstrumentPayload,
                queue=QueueName.PROVIDER,
                timeout_seconds=45,
                retry_policy=RetryPolicy.NEVER,
                max_attempts=1,
                idempotency_namespace=JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value,
                handler=collect_fosun_daily_instrument,
            ),
            JobHandlerSpec(
                job_type=JobType.PUBLISH_FOSUN_DAILY,
                schema_version=FOSUN_DAILY_PUBLISH_SCHEMA,
                payload_model=FosunDailyPublishPayload,
                queue=QueueName.PROVIDER,
                timeout_seconds=45,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace=JobType.PUBLISH_FOSUN_DAILY.value,
                handler=publish_fosun_daily,
            ),
            _spec(JobType.COLLECT_SEC_MACRO, collect_sec_macro),
        )
    )


async def _require_fosun_daily_instrument(
    context: JobHandlerContext,
    payload: FosunDailyInstrumentPayload,
) -> None:
    await _require_fosun_daily_calendar(context, payload)
    snapshot = await context.session.get(
        UniverseSnapshot,
        UUID(payload.universe_snapshot_id),
        populate_existing=True,
    )
    if (
        snapshot is None
        or snapshot.content_hash != payload.universe_content_hash
        or snapshot.effective_from > date.fromisoformat(payload.session)
        or snapshot.published_at is None
        or snapshot.published_at > parse_utc(payload.scheduled_for)
    ):
        raise UnsafeJobError("FROZEN_UNIVERSE_MISMATCH")
    instrument_ids = tuple(
        await context.session.scalars(
            select(Instrument.id)
            .join(UniverseMember, UniverseMember.instrument_id == Instrument.id)
            .where(
                UniverseMember.universe_snapshot_id == snapshot.id,
                Instrument.currency == "USD",
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
            )
            .order_by(Instrument.id)
            .limit(FOSUN_DAILY_MAX_INSTRUMENTS + 1)
        )
    )
    if (
        len(instrument_ids) != payload.total_instruments
        or len(instrument_ids) > FOSUN_DAILY_MAX_INSTRUMENTS
        or instrument_ids[payload.ordinal] != UUID(payload.instrument_id)
    ):
        raise UnsafeJobError("FOSUN_DAILY_FROZEN_SET_MISMATCH")
    await _require_frozen_instrument_alias(context.session, payload)


async def _require_frozen_instrument_alias(
    session: AsyncSession,
    payload: FosunDailyInstrumentPayload,
) -> None:
    session_date = date.fromisoformat(payload.session)
    aliases = tuple(
        (
            await session.scalars(
                select(InstrumentAlias)
                .where(
                    or_(
                        InstrumentAlias.instrument_id == UUID(payload.instrument_id),
                        func.upper(InstrumentAlias.symbol) == payload.instrument_symbol.upper(),
                    ),
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= session_date,
                    or_(
                        InstrumentAlias.valid_to.is_(None),
                        InstrumentAlias.valid_to >= session_date,
                    ),
                )
                .order_by(InstrumentAlias.id)
                .limit(2)
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    if len(aliases) != 1:
        raise UnsafeJobError("FOSUN_DAILY_ALIAS_AUTHORITY_MISMATCH")
    alias = aliases[0]
    identity = instrument_alias_identity_sha256(
        alias_id=str(alias.id),
        instrument_id=str(alias.instrument_id),
        symbol=alias.symbol,
        alias_type=alias.alias_type,
        valid_from=alias.valid_from,
        valid_to=alias.valid_to,
        created_at=alias.created_at,
    )
    if (
        str(alias.id) != payload.instrument_alias_id
        or alias.symbol != payload.instrument_symbol
        or identity != payload.instrument_alias_identity_sha256
    ):
        raise UnsafeJobError("FOSUN_DAILY_ALIAS_AUTHORITY_MISMATCH")


async def _require_fosun_daily_calendar(
    context: JobHandlerContext,
    payload: FosunDailyInstrumentPayload | FosunDailyPublishPayload,
) -> None:
    authority_id = UUID(payload.calendar_authority_id)
    session = date.fromisoformat(payload.session)
    calendar = await context.session.get(
        ExchangeCalendarAuthorityRecord,
        authority_id,
        populate_existing=True,
    )
    session_row = await context.session.scalar(
        select(ExchangeCalendarSessionRecord)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == authority_id,
            ExchangeCalendarSessionRecord.session == session,
        )
        .execution_options(populate_existing=True)
    )
    if (
        calendar is None
        or calendar.exchange != "XNYS"
        or calendar.completeness_status != "complete"
        or not (calendar.range_start <= session <= calendar.range_end)
        or calendar.published_at > parse_utc(payload.scheduled_for)
        or session_row is None
        or session_row.session != session
        or session_row.market_close_at != parse_utc(payload.market_close_at)
    ):
        raise UnsafeJobError("FOSUN_DAILY_CALENDAR_AUTHORITY_MISMATCH")


def _staging_common(
    *,
    context: JobHandlerContext,
    payload: FosunDailyInstrumentPayload,
    child_input_hash: str,
    record_kind: str,
    request_start_at: datetime,
    request_end_at: datetime,
) -> dict[str, object]:
    return {
        "root_job_id": UUID(payload.root_job_id),
        "fetch_job_id": context.claim.job_id,
        "instrument_id": UUID(payload.instrument_id),
        "record_kind": record_kind,
        "schema_version": "fosun_daily_staging_v1",
        "child_input_hash": child_input_hash,
        "calendar_authority_id": UUID(payload.calendar_authority_id),
        "universe_snapshot_id": UUID(payload.universe_snapshot_id),
        "universe_content_hash": payload.universe_content_hash,
        "ordinal": payload.ordinal,
        "total_instruments": payload.total_instruments,
        "request_start_at": request_start_at,
        "request_end_at": request_end_at,
    }


@asynccontextmanager
async def _fosun_daily_call_permit(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    context: JobHandlerContext,
    payload: FosunDailyInstrumentPayload,
    market_authority_loader: MarketAuthorityLoader,
    request_start_at: datetime,
    request_end_at: datetime,
    latest_call_start_deadline: float,
) -> AsyncIterator[None]:
    """Serialize daily calls and pace durable reservations by database time."""
    async with session_factory() as gate_session, gate_session.begin():
        # Everything before the external call, including the independently
        # committed reservation, must finish early enough to leave the full
        # gateway, raw-archive, finalization, and worker margins intact.
        async with asyncio.timeout_at(latest_call_start_deadline):
            await gate_session.execute(
                text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtextextended('fosun-daily-market-call-gate', 0))"
                )
            )
            while True:
                delay = await gate_session.scalar(
                    text(
                        "SELECT GREATEST(0.0, COALESCE(EXTRACT(EPOCH FROM ("
                        "MAX(recorded_at) + "
                        "pg_catalog.make_interval(secs => :interval_seconds) - "
                        "clock_timestamp()"
                        ")), 0.0)) FROM fosun_daily_bar_staging "
                        "WHERE record_kind = 'reservation'"
                    ),
                    {"interval_seconds": _FOSUN_DAILY_CALL_INTERVAL_SECONDS},
                )
                seconds = float(delay or 0.0)
                if seconds <= 0:
                    break
                await asyncio.sleep(min(seconds, _FOSUN_DAILY_CALL_INTERVAL_SECONDS))
            # Match both mutation-trigger lock domains. Holding this transaction
            # across the yielded fetch makes capability/status drift and call start
            # mutually exclusive instead of relying on a read-then-call gap.
            await acquire_fosun_market_authority_locks(
                gate_session,
                (payload.market_authority,),
            )
            # The durable pacing wait can outlive any earlier validation. Re-read
            # every frozen input under the call permit immediately before yielding
            # control to the single gateway invocation.
            await _require_fosun_daily_instrument(context, payload)
            if not await _matches_frozen_market_authority(
                context,
                loader=market_authority_loader,
                frozen=payload.market_authority,
                check_time=context.finished_at(),
            ):
                raise UnsafeJobError("FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_FETCH")
            # Persist the one-shot reservation only after every potentially slow
            # revalidation and immediately before the external call. The global
            # gate remains held, so the reservation clock is also the call clock.
            await _reserve_fosun_daily_fetch(
                session_factory,
                context=context,
                payload=payload,
                request_start_at=request_start_at,
                request_end_at=request_end_at,
            )
        yield


async def _reserve_fosun_daily_fetch(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    context: JobHandlerContext,
    payload: FosunDailyInstrumentPayload,
    request_start_at: datetime,
    request_end_at: datetime,
) -> None:
    """Commit a durable one-shot reservation before any gateway request."""
    try:
        async with session_factory() as staging_session, staging_session.begin():
            child = await staging_session.get(Job, context.claim.job_id)
            if (
                child is None
                or child.job_type != JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value
                or child.schema_version != FOSUN_DAILY_INSTRUMENT_SCHEMA
                or child.retry_policy != RetryPolicy.NEVER.value
                or child.max_attempts != 1
                or child.payload != payload.model_dump(mode="json")
            ):
                raise UnsafeJobError("FOSUN_DAILY_CHILD_AUTHORITY_MISMATCH")
            existing = await staging_session.scalar(
                select(FosunDailyBarStaging.id)
                .where(FosunDailyBarStaging.fetch_job_id == context.claim.job_id)
                .limit(1)
            )
            if existing is not None:
                raise UnsafeJobError("FOSUN_DAILY_FETCH_ALREADY_RESERVED")
            staging_session.add(
                FosunDailyBarStaging(
                    **_staging_common(
                        context=context,
                        payload=payload,
                        child_input_hash=child.input_hash,
                        record_kind="reservation",
                        request_start_at=request_start_at,
                        request_end_at=request_end_at,
                    )
                )
            )
    except IntegrityError as error:
        raise UnsafeJobError("FOSUN_DAILY_FETCH_ALREADY_RESERVED") from error


async def _stage_fosun_daily_fetch(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    context: JobHandlerContext,
    payload: FosunDailyInstrumentPayload,
    request_start_at: datetime,
    request_end_at: datetime,
    response: RawGatewayResponse,
    raw_artifact: DataArtifact,
    authorized_at: datetime,
) -> None:
    frozen = payload.market_authority
    binding = binding_at(frozen, authorized_at=authorized_at)
    try:
        async with session_factory() as staging_session, staging_session.begin():
            child = await staging_session.get(Job, context.claim.job_id)
            reservation = await staging_session.scalar(
                select(FosunDailyBarStaging).where(
                    FosunDailyBarStaging.fetch_job_id == context.claim.job_id,
                    FosunDailyBarStaging.record_kind == "reservation",
                )
            )
            if child is None or reservation is None:
                raise UnsafeJobError("FOSUN_DAILY_RESERVATION_MISSING")
            if reservation.child_input_hash != child.input_hash:
                raise UnsafeJobError("FOSUN_DAILY_RESERVATION_MISMATCH")
            staging_session.add(
                FosunDailyBarStaging(
                    **_staging_common(
                        context=context,
                        payload=payload,
                        child_input_hash=child.input_hash,
                        record_kind="fetched",
                        request_start_at=request_start_at,
                        request_end_at=request_end_at,
                    ),
                    raw_artifact_id=raw_artifact.id,
                    raw_artifact_identity_sha256=data_artifact_identity_sha256(raw_artifact),
                    gateway_request_id=response.gateway_request_id,
                    request_body_sha256=response.request_body_sha256,
                    sent_at=response.sent_at,
                    received_at=response.received_at,
                    provider_capability_id=binding.provider_capability_id,
                    provider_status_id=binding.provider_status_id,
                    provider_status_version=binding.provider_status_version,
                    provider_contract_hash=binding.provider_contract_hash,
                    provider_authorized_at=binding.provider_authorized_at,
                    delivery_mode=frozen.delivery_mode,
                )
            )
    except IntegrityError as error:
        raise UnsafeJobError("FOSUN_DAILY_FETCH_STAGE_CONFLICT") from error


async def _publish_fosun_daily(
    context: JobHandlerContext,
    *,
    payload: FosunDailyPublishPayload,
    bundle: ProviderBundle,
    market_object_store: MarketObjectStore,
    market_authority_loader: MarketAuthorityLoader,
) -> JobHandlerResult:
    """Normalize staged raw pages, then publish every instrument atomically."""
    frozen = payload.market_authority
    # Match the database trigger order before reading any mutable authority:
    # capability scope -> integration status -> artifact ids (below).
    await acquire_fosun_market_authority_locks(
        cast(AsyncSession, context.session),
        (frozen,),
    )
    if not await _matches_frozen_market_authority(
        context,
        loader=market_authority_loader,
        frozen=frozen,
        check_time=context.finished_at(),
    ):
        raise UnsafeJobError("FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_PUBLISH")
    await _require_fosun_daily_calendar(context, payload)
    child_payloads = await _require_fosun_daily_publish_set(context, payload)
    rows = tuple(
        (
            await context.session.scalars(
                select(FosunDailyBarStaging)
                .where(FosunDailyBarStaging.root_job_id == UUID(payload.root_job_id))
                .order_by(
                    FosunDailyBarStaging.ordinal,
                    FosunDailyBarStaging.record_kind,
                )
            )
        ).all()
    )
    expected_pairs = {
        (UUID(child.job_id), kind)
        for child in payload.children
        for kind in ("reservation", "fetched")
    }
    if (
        len(rows) != len(expected_pairs)
        or {(row.fetch_job_id, row.record_kind) for row in rows} != expected_pairs
    ):
        raise UnsafeJobError("FOSUN_DAILY_STAGING_INCOMPLETE")
    rows_by_job_kind = {(row.fetch_job_id, row.record_kind): row for row in rows}
    fetched_by_job = {row.fetch_job_id: row for row in rows if row.record_kind == "fetched"}
    raw_artifact_ids: list[UUID] = []
    for candidate_row in fetched_by_job.values():
        if candidate_row.raw_artifact_id is None:
            raise UnsafeJobError("FOSUN_DAILY_RAW_ARTIFACT_MISSING")
        raw_artifact_ids.append(candidate_row.raw_artifact_id)
    locked_artifacts = await lock_and_load_data_artifacts(
        cast(AsyncSession, context.session),
        raw_artifact_ids,
    )
    if len(locked_artifacts) != len(set(raw_artifact_ids)):
        raise UnsafeJobError("FOSUN_DAILY_RAW_ARTIFACT_MISSING")
    provider = cast(RawFirstFosunMarketProvider, bundle.market_provider(context.session))
    if not callable(getattr(provider, "normalize_bars", None)) or not callable(
        getattr(provider, "expected_bars_request_body_sha256", None)
    ):
        raise UnsafeJobError("FOSUN_RAW_FIRST_PROVIDER_REQUIRED")

    staged: list[
        tuple[
            FosunDailyChildRef,
            FosunDailyBarStaging,
            DataArtifact,
            BarsRequest,
            ProviderMarketBinding,
        ]
    ] = []
    for child_ref in payload.children:
        child_id = UUID(child_ref.job_id)
        child_payload = child_payloads[child_id]
        reservation = rows_by_job_kind.get((child_id, "reservation"))
        fetched_row = fetched_by_job.get(UUID(child_ref.job_id))
        if reservation is None or fetched_row is None:
            raise UnsafeJobError("FOSUN_DAILY_STAGING_INCOMPLETE")
        _require_staging_common(
            reservation,
            child_ref=child_ref,
            child_payload=child_payload,
            publish_payload=payload,
            expected_kind="reservation",
        )
        _require_staging_common(
            fetched_row,
            child_ref=child_ref,
            child_payload=child_payload,
            publish_payload=payload,
            expected_kind="fetched",
        )
        if fetched_row.recorded_at < reservation.recorded_at:
            raise UnsafeJobError("FOSUN_DAILY_STAGING_AUTHORITY_MISMATCH")
        raw_artifact_id = fetched_row.raw_artifact_id
        if raw_artifact_id is None:
            raise UnsafeJobError("FOSUN_DAILY_RAW_ARTIFACT_MISSING")
        artifact = locked_artifacts.get(raw_artifact_id)
        if artifact is None:
            raise UnsafeJobError("FOSUN_DAILY_RAW_ARTIFACT_MISSING")
        _require_staged_binding(fetched_row, child_ref.input_hash, frozen, artifact)
        assert fetched_row.gateway_request_id is not None
        assert fetched_row.request_body_sha256 is not None
        assert fetched_row.sent_at is not None
        assert fetched_row.received_at is not None
        request = BarsRequest(
            instrument_id=InstrumentId(UUID(child_ref.instrument_id)),
            start=fetched_row.request_start_at,
            end=fetched_row.request_end_at,
            interval="1Day",
            adjustment="raw",
        )
        assert fetched_row.provider_authorized_at is not None
        staged.append(
            (
                child_ref,
                fetched_row,
                artifact,
                request,
                binding_at(
                    frozen,
                    authorized_at=fetched_row.provider_authorized_at,
                ),
            )
        )

    async def read_staged_raw(
        item: tuple[
            FosunDailyChildRef,
            FosunDailyBarStaging,
            DataArtifact,
            BarsRequest,
            ProviderMarketBinding,
        ],
    ) -> bytes:
        _child_ref, row, artifact, _request, _provider_binding = item
        assert row.received_at is not None
        raw_key = _fosun_raw_key(
            row.gateway_request_id,
            artifact.sha256,
            row.received_at,
        )
        try:
            raw_payload = await market_object_store.read_raw(raw_key, uri=artifact.uri)
        except ProviderObjectIOUnavailable as error:
            raise RetryableJobError(error.error_code) from error
        except (KeyError, ValueError, RuntimeError) as error:
            raise UnsafeJobError("FOSUN_DAILY_RAW_OBJECT_UNAVAILABLE") from error
        if (
            len(raw_payload) != artifact.byte_size
            or sha256(raw_payload).hexdigest() != artifact.sha256
        ):
            raise UnsafeJobError("FOSUN_DAILY_RAW_OBJECT_MISMATCH")
        return raw_payload

    raw_payloads = await _bounded_async_map(staged, read_staged_raw)
    pending: list[
        tuple[ProviderPage[PriceBar], ProviderMarketBinding, DataArtifact, datetime, datetime]
    ] = []
    for (
        child_ref,
        row,
        artifact,
        request,
        provider_binding,
    ), raw_payload in zip(staged, raw_payloads, strict=True):
        child_payload = child_payloads[UUID(child_ref.job_id)]
        assert row.gateway_request_id is not None
        assert row.request_body_sha256 is not None
        assert row.sent_at is not None
        assert row.received_at is not None
        response = RawGatewayResponse(
            operation="fosun_get_bars",
            gateway_request_id=row.gateway_request_id,
            request_body_sha256=row.request_body_sha256,
            raw_payload=raw_payload,
            sent_at=row.sent_at,
            received_at=row.received_at,
        )
        try:
            expected_request_body_sha256 = await provider.expected_bars_request_body_sha256(
                request,
                symbol=child_payload.instrument_symbol,
            )
        except (FosunMarketGatewayError, MarketProviderPayloadError) as error:
            raise UnsafeJobError("FOSUN_DAILY_REQUEST_BODY_MISMATCH") from error
        if row.request_body_sha256 != expected_request_body_sha256:
            raise UnsafeJobError("FOSUN_DAILY_REQUEST_BODY_MISMATCH")
        try:
            page = await provider.normalize_bars(
                request,
                response,
                symbol=child_payload.instrument_symbol,
            )
        except MarketProviderPayloadError as error:
            raise UnsafeJobError("FOSUN_MARKET_PAYLOAD_INVALID") from error
        if not _fosun_page_matches_authority(page, response, frozen):
            raise UnsafeJobError("FOSUN_MARKET_CONTRACT_MISMATCH")
        page = page.model_copy(
            update={
                "items": tuple(
                    bar.model_copy(update={"revision": artifact.sha256}) for bar in page.items
                )
            }
        )
        if any(item.instrument_id != UUID(child_ref.instrument_id) for item in page.items):
            raise UnsafeJobError("MARKET_PROVIDER_IDENTITY_MISMATCH")
        if not await _fosun_daily_page_is_complete(
            context,
            payload=payload,
            request=request,
            page=page,
        ):
            raise UnsafeJobError("FOSUN_MARKET_PAGE_INCOMPLETE")
        pending.append(
            (
                page,
                provider_binding,
                artifact,
                row.request_start_at,
                row.request_end_at,
            )
        )

    async def prepare_pending(
        item: tuple[
            ProviderPage[PriceBar],
            ProviderMarketBinding,
            DataArtifact,
            datetime,
            datetime,
        ],
    ) -> PreparedPricePage:
        page, provider_binding, raw_artifact, request_start_at, request_end_at = item
        return await prepare_price_page(
            market_object_store,
            page,
            provider_binding=provider_binding,
            raw_artifact=raw_artifact,
            calendar_authority_id=UUID(payload.calendar_authority_id),
            gateway_request_start_at=request_start_at,
            gateway_request_end_at=request_end_at,
        )

    try:
        prepared_pages = await _bounded_async_map(pending, prepare_pending)
    except ProviderObjectIOUnavailable as error:
        raise RetryableJobError(error.error_code) from error

    # One atomic aggregate must have one visibility time. Child fetch authority
    # proves each raw page was admissible when fetched, but it cannot make the
    # complete 49-instrument batch visible before the publisher has prepared and
    # revalidated every page. PostgreSQL supplies the post-prepare publication
    # floor so an injected, lagging, or rolled-back application clock cannot
    # expose the batch to an earlier historical decision.
    database_publish_at = await context.session.scalar(text("SELECT clock_timestamp()"))
    if not isinstance(database_publish_at, datetime):
        raise UnsafeJobError("FOSUN_DAILY_PUBLICATION_CLOCK_UNAVAILABLE")
    publish_authorized_at = max(
        context.finished_at(),
        database_publish_at,
        *(
            max(
                row.recorded_at,
                cast(datetime, row.received_at),
                cast(datetime, row.provider_authorized_at),
            )
            for _child_ref, row, _artifact, _request, _binding in staged
        ),
    )
    if not await _matches_frozen_market_authority(
        context,
        loader=market_authority_loader,
        frozen=frozen,
        check_time=publish_authorized_at,
    ):
        raise UnsafeJobError("FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_PUBLISH")
    publish_binding = binding_at(frozen, authorized_at=publish_authorized_at)

    to_persist: list[
        tuple[
            DataArtifact,
            datetime,
            datetime,
            PreparedPricePage,
        ]
    ] = []
    for (
        page,
        _child_binding,
        raw_artifact,
        request_start_at,
        request_end_at,
    ), prepared in zip(pending, prepared_pages, strict=True):
        exact = await context.session.scalar(
            select(PriceSeriesManifest.id)
            .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
            .where(
                DataArtifact.sha256 == prepared.parquet_digest,
                PriceSeriesManifest.instrument == str(page.items[0].instrument_id),
                PriceSeriesManifest.source == "fosun",
                PriceSeriesManifest.feed == "l1",
                PriceSeriesManifest.delivery_mode == frozen.delivery_mode,
                PriceSeriesManifest.adjustment == "raw",
                PriceSeriesManifest.interval == "1Day",
                PriceSeriesManifest.gateway_transport_artifact_id == raw_artifact.id,
                PriceSeriesManifest.daily_calendar_authority_id
                == UUID(payload.calendar_authority_id),
                PriceSeriesManifest.gateway_request_start_at == request_start_at,
                PriceSeriesManifest.gateway_request_end_at == request_end_at,
                PriceSeriesManifest.provider_capability_id
                == publish_binding.provider_capability_id,
                PriceSeriesManifest.provider_status_id == publish_binding.provider_status_id,
                PriceSeriesManifest.provider_status_version
                == publish_binding.provider_status_version,
                PriceSeriesManifest.provider_contract_hash
                == publish_binding.provider_contract_hash,
                PriceSeriesManifest.provider_authorized_at
                == publish_binding.provider_authorized_at,
            )
        )
        if exact is not None:
            continue
        any_artifact = await context.session.scalar(
            select(DataArtifact.id).where(DataArtifact.sha256 == prepared.parquet_digest)
        )
        if any_artifact is not None:
            raise UnsafeJobError("FOSUN_MARKET_REPLAY_CONFLICT")
        to_persist.append(
            (
                raw_artifact,
                request_start_at,
                request_end_at,
                prepared,
            )
        )

    collected = 0
    for (
        raw_artifact,
        request_start_at,
        request_end_at,
        prepared,
    ) in to_persist:
        result = await persist_prepared_price_page(
            context.session,
            prepared,
            provider_binding=publish_binding,
            raw_artifact=raw_artifact,
            calendar_authority_id=UUID(payload.calendar_authority_id),
            gateway_request_start_at=request_start_at,
            gateway_request_end_at=request_end_at,
        )
        if result.price_authority is None:
            raise UnsafeJobError("SOURCE_CONFLICT")
        collected += 1

    if not await _matches_frozen_market_authority(
        context,
        loader=market_authority_loader,
        frozen=frozen,
        check_time=context.finished_at(),
    ):
        raise UnsafeJobError("FOSUN_MARKET_AUTHORITY_CHANGED_AFTER_PUBLISH")
    return JobHandlerResult(
        outcome="succeeded",
        result_code="MARKET_COLLECTED" if collected else "MARKET_REPLAYED",
    )


async def _require_fosun_daily_publish_set(
    context: JobHandlerContext,
    payload: FosunDailyPublishPayload,
) -> dict[UUID, FosunDailyInstrumentPayload]:
    snapshot = await context.session.get(
        UniverseSnapshot,
        UUID(payload.universe_snapshot_id),
    )
    if (
        snapshot is None
        or snapshot.content_hash != payload.universe_content_hash
        or snapshot.effective_from > date.fromisoformat(payload.session)
        or snapshot.published_at is None
        or snapshot.published_at > parse_utc(payload.scheduled_for)
    ):
        raise UnsafeJobError("FROZEN_UNIVERSE_MISMATCH")
    expected_ids = tuple(UUID(child.instrument_id) for child in payload.children)
    actual_ids = tuple(
        await context.session.scalars(
            select(Instrument.id)
            .join(UniverseMember, UniverseMember.instrument_id == Instrument.id)
            .where(
                UniverseMember.universe_snapshot_id == UUID(payload.universe_snapshot_id),
                Instrument.currency == "USD",
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
            )
            .order_by(Instrument.id)
            .limit(FOSUN_DAILY_MAX_INSTRUMENTS + 1)
        )
    )
    if actual_ids != expected_ids:
        raise UnsafeJobError("FOSUN_DAILY_FROZEN_SET_MISMATCH")
    jobs = tuple(
        (
            await context.session.scalars(
                select(Job).where(Job.id.in_(UUID(child.job_id) for child in payload.children))
            )
        ).all()
    )
    by_id = {job.id: job for job in jobs}
    parsed: dict[UUID, FosunDailyInstrumentPayload] = {}
    for child in payload.children:
        job = by_id.get(UUID(child.job_id))
        if (
            job is None
            or job.job_type != JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value
            or job.schema_version != FOSUN_DAILY_INSTRUMENT_SCHEMA
            or job.input_hash != child.input_hash
            or job.status != "succeeded"
            or job.payload.get("root_job_id") != payload.root_job_id
            or job.payload.get("instrument_id") != child.instrument_id
            or job.payload.get("ordinal") != child.ordinal
        ):
            raise UnsafeJobError("FOSUN_DAILY_CHILD_AUTHORITY_MISMATCH")
        try:
            child_payload = FosunDailyInstrumentPayload.model_validate(job.payload)
        except ValidationError as error:
            raise UnsafeJobError("FOSUN_DAILY_CHILD_AUTHORITY_MISMATCH") from error
        if (
            child_payload.root_job_id != payload.root_job_id
            or child_payload.calendar_authority_id != payload.calendar_authority_id
            or child_payload.session != payload.session
            or child_payload.market_close_at != payload.market_close_at
            or child_payload.profile_id != payload.profile_id
            or child_payload.universe_snapshot_id != payload.universe_snapshot_id
            or child_payload.universe_content_hash != payload.universe_content_hash
            or child_payload.market_authority != payload.market_authority
            or child_payload.instrument_id != child.instrument_id
            or child_payload.instrument_alias_id != child.instrument_alias_id
            or child_payload.instrument_symbol != child.instrument_symbol
            or child_payload.instrument_alias_identity_sha256
            != child.instrument_alias_identity_sha256
            or child_payload.ordinal != child.ordinal
            or child_payload.total_instruments != len(payload.children)
            or job.available_at != parse_utc(child_payload.scheduled_for)
            or job.effective_at != parse_utc(child_payload.scheduled_for)
        ):
            raise UnsafeJobError("FOSUN_DAILY_CHILD_AUTHORITY_MISMATCH")
        await _require_frozen_instrument_alias(context.session, child_payload)
        parsed[job.id] = child_payload
    return parsed


def _require_staging_common(
    row: FosunDailyBarStaging,
    *,
    child_ref: FosunDailyChildRef,
    child_payload: FosunDailyInstrumentPayload,
    publish_payload: FosunDailyPublishPayload,
    expected_kind: str,
) -> None:
    request_start_at, request_end_at = _daily_request_window(
        parse_utc(publish_payload.market_close_at)
    )
    if (
        row.root_job_id != UUID(publish_payload.root_job_id)
        or row.fetch_job_id != UUID(child_ref.job_id)
        or row.instrument_id != UUID(child_ref.instrument_id)
        or row.record_kind != expected_kind
        or row.schema_version != "fosun_daily_staging_v1"
        or row.child_input_hash != child_ref.input_hash
        or row.calendar_authority_id != UUID(publish_payload.calendar_authority_id)
        or row.universe_snapshot_id != UUID(publish_payload.universe_snapshot_id)
        or row.universe_content_hash != publish_payload.universe_content_hash
        or row.ordinal != child_ref.ordinal
        or row.total_instruments != len(publish_payload.children)
        or row.request_start_at != request_start_at
        or row.request_end_at != request_end_at
        or child_payload.instrument_id != child_ref.instrument_id
        or child_payload.instrument_alias_id != child_ref.instrument_alias_id
        or child_payload.instrument_symbol != child_ref.instrument_symbol
        or child_payload.instrument_alias_identity_sha256
        != child_ref.instrument_alias_identity_sha256
        or child_payload.ordinal != child_ref.ordinal
    ):
        raise UnsafeJobError("FOSUN_DAILY_STAGING_AUTHORITY_MISMATCH")


def _require_staged_binding(
    row: FosunDailyBarStaging,
    expected_input_hash: str,
    frozen: FrozenFosunMarketAuthority,
    artifact: DataArtifact,
) -> None:
    assert row.received_at is not None
    expected_key = _fosun_raw_key(row.gateway_request_id, artifact.sha256, row.received_at)
    if (
        row.child_input_hash != expected_input_hash
        or row.provider_capability_id != frozen.capability_id
        or row.provider_status_id != frozen.status_id
        or row.provider_status_version != frozen.status_version
        or row.provider_contract_hash != frozen.contract_hash
        or row.delivery_mode != frozen.delivery_mode
        or artifact.source != "fosun"
        or artifact.schema_version != "fosun_gateway_market_bars_v1"
        or artifact.mime_type != "application/json"
        or artifact.observed_at != row.received_at
        or artifact.available_at != row.received_at
        or artifact.received_at != row.received_at
        or row.raw_artifact_identity_sha256 != data_artifact_identity_sha256(artifact)
        or not object_uri_matches_key(artifact.uri, expected_key)
    ):
        raise UnsafeJobError("FOSUN_DAILY_STAGING_AUTHORITY_MISMATCH")


def _fosun_raw_key(
    gateway_request_id: UUID | None,
    digest: str,
    received_at: datetime,
) -> str:
    if gateway_request_id is None:
        raise UnsafeJobError("FOSUN_DAILY_GATEWAY_REQUEST_MISSING")
    date_path = received_at.astimezone(UTC).strftime("%Y/%m/%d")
    return f"raw/market/fosun/{date_path}/{gateway_request_id}/{digest}.json"


def _daily_request_window(market_close: datetime) -> tuple[datetime, datetime]:
    """Return the exact UTC date bounds sent for a daily-bar request."""
    request_end_at = datetime.combine(market_close.date(), time.min, tzinfo=UTC)
    return request_end_at - timedelta(days=400), request_end_at


async def _archive_fosun_price_raw(
    session: AsyncSession,
    object_store: MarketObjectStore,
    response: RawGatewayResponse,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None,
) -> MarketRawArchive:
    try:
        if session_factory is None:
            return await archive_fosun_price_page_raw(
                session,
                object_store,
                response,
                commit=False,
            )
        async with session_factory() as archive_session:
            archived = await archive_fosun_price_page_raw(
                archive_session,
                object_store,
                response,
                commit=True,
            )
            await archive_session.refresh(archived.raw_artifact)
            archive_session.expunge(archived.raw_artifact)
            return archived
    except ProviderObjectIOUnavailable as error:
        raise UnsafeJobError(error.error_code) from error


async def _matches_frozen_market_authority(
    context: JobHandlerContext,
    *,
    loader: MarketAuthorityLoader,
    frozen: FrozenFosunMarketAuthority,
    check_time: datetime,
) -> bool:
    current = await loader(
        context.session,
        check_time=check_time,
        operation="fosun_get_bars",
        delivery_mode=frozen.delivery_mode,
    )
    return current == frozen


async def _fosun_daily_page_is_complete(
    context: JobHandlerContext,
    *,
    payload: DailyStagePayload | FosunDailyPublishPayload,
    request: BarsRequest,
    page: ProviderPage[PriceBar],
) -> bool:
    expected = tuple(
        await context.session.scalars(
            select(ExchangeCalendarSessionRecord.session)
            .where(
                ExchangeCalendarSessionRecord.calendar_authority_id
                == UUID(payload.calendar_authority_id),
                ExchangeCalendarSessionRecord.session >= request.start.date(),
                ExchangeCalendarSessionRecord.session <= request.end.date(),
            )
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    actual = tuple(item.session for item in page.items)
    return bool(expected) and actual == expected


def _fosun_page_matches_authority(
    page: ProviderPage[PriceBar],
    response: RawGatewayResponse,
    frozen: FrozenFosunMarketAuthority,
) -> bool:
    return (
        response.operation == "fosun_get_bars"
        and page.raw_payload == response.raw_payload
        and page.next_cursor is None
        and page.as_of == response.received_at
        and page.provenance.source == "fosun"
        and page.provenance.feed == "l1"
        and page.provenance.delivery_mode == frozen.delivery_mode
        and page.provenance.adjustment == "raw"
        and page.provenance.interval == "1Day"
        and page.provenance.mode == "read_only"
    )


def _fixture_skip() -> JobHandlerResult:
    return JobHandlerResult(
        outcome="skipped",
        result_code="FIXTURE_PROVIDER_DATA_UNAVAILABLE",
    )


def _require_provider_profile(
    context: JobHandlerContext,
    expected_profile_id: str | None,
) -> None:
    if expected_profile_id is None:
        return
    payload = cast(DailyStagePayload, context.payload)
    if payload.profile_id != expected_profile_id:
        raise UnsafeJobError("PROVIDER_PROFILE_MISMATCH")


async def _frozen_universe(
    context: JobHandlerContext,
    payload: DailyStagePayload,
) -> UniverseSnapshot | None:
    if payload.universe_snapshot_id is None or payload.universe_content_hash is None:
        return None
    snapshot = await context.session.get(
        UniverseSnapshot,
        UUID(payload.universe_snapshot_id),
    )
    session_date = date.fromisoformat(payload.session)
    scheduled_for = parse_utc(payload.scheduled_for)
    if (
        snapshot is None
        or snapshot.content_hash != payload.universe_content_hash
        or snapshot.effective_from > session_date
        or snapshot.published_at is None
        or snapshot.published_at > scheduled_for
    ):
        return None
    return snapshot


async def _sec_instruments(
    context: JobHandlerContext,
    payload: DailyStagePayload,
    universe: UniverseSnapshot,
) -> tuple[SecInstrumentIdentity, ...] | None:
    records = tuple(
        (
            await context.session.scalars(
                select(Instrument)
                .join(UniverseMember, UniverseMember.instrument_id == Instrument.id)
                .where(
                    UniverseMember.universe_snapshot_id == universe.id,
                    Instrument.currency == "USD",
                    Instrument.exchange.in_(US_EQUITY_EXCHANGES),
                )
                .order_by(Instrument.id)
                .limit(101)
            )
        ).all()
    )
    if len(records) > 100:
        return None
    as_of = date.fromisoformat(payload.session)
    identities: list[SecInstrumentIdentity] = []
    for instrument in records:
        alias_rows = (
            (
                await context.session.execute(
                    select(InstrumentAlias.alias_type, InstrumentAlias.symbol)
                    .where(
                        InstrumentAlias.instrument_id == instrument.id,
                        InstrumentAlias.alias_type.in_(("ticker", "cik")),
                        InstrumentAlias.valid_from <= as_of,
                        or_(
                            InstrumentAlias.valid_to.is_(None),
                            InstrumentAlias.valid_to >= as_of,
                        ),
                    )
                    .order_by(
                        InstrumentAlias.alias_type,
                        InstrumentAlias.valid_from.desc(),
                    )
                )
            )
            .tuples()
            .all()
        )
        aliases: dict[str, str] = {}
        for alias_type, symbol in alias_rows:
            aliases.setdefault(alias_type, symbol)
        ticker = aliases.get("ticker")
        cik = aliases.get("cik")
        if ticker is None or cik is None:
            continue
        identity = SecInstrumentIdentity(
            instrument_id=instrument.id,
            cik=cik,
            symbol=ticker,
            exchange=instrument.exchange,
            currency=instrument.currency,
        )
        identity.assert_us_sec_eligible()
        identities.append(identity)
    return tuple(identities)


async def _collect_sec(
    context: JobHandlerContext,
    *,
    bundle: ProviderBundle,
    store: RawEvidenceStore,
    instruments: Sequence[SecInstrumentIdentity],
) -> tuple[int, int]:
    collected = 0
    replayed = 0
    for instrument in instruments:
        (
            company_facts,
            submissions,
        ) = await bundle.sec_client.fetch_company_facts_and_submission_history(instrument.cik)
        submission_payloads = tuple(document.raw_payload for document in submissions)
        accession_metadata = _submission_metadata(submission_payloads)
        evidence_payloads = {
            accession: _sec_accession_content(
                company_facts.raw_payload,
                submission_payloads,
                accession,
            )
            for accession in accession_metadata
        }
        await _archive_raw(
            store,
            source="sec",
            label=f"companyfacts-{instrument.cik}",
            received_at=company_facts.received_at,
            payload=company_facts.raw_payload,
        )
        for index, document in enumerate(submissions):
            await _archive_raw(
                store,
                source="sec",
                label=f"submissions-{instrument.cik}-{index}",
                received_at=document.received_at,
                payload=document.raw_payload,
            )
        evidence_by_accession: dict[str, EvidenceRef] = {}
        for accession, (accepted_at, _form) in accession_metadata.items():
            content = evidence_payloads[accession]
            digest = sha256(content).hexdigest()
            location = await store.put_if_absent(
                key=f"evidence/sec/{accession}/{digest}.json",
                content=content,
            )
            evidence_by_accession[accession] = EvidenceRef(
                source="sec",
                object_id=accession,
                available_at=accepted_at,
                location=location,
                content_hash=digest,
            )
        facts = normalize_company_facts(
            company_facts.raw_payload,
            tuple(document.raw_payload for document in submissions),
            instrument=instrument,
            evidence_by_accession=evidence_by_accession,
        )
        accessions = sorted({fact.accession_number for fact in facts})
        for accession in accessions:
            accession_facts = tuple(fact for fact in facts if fact.accession_number == accession)
            first = accession_facts[0]
            existing = await context.session.scalar(
                select(ResearchEvidenceRef).where(
                    ResearchEvidenceRef.source == "sec",
                    ResearchEvidenceRef.provider_object_id == accession,
                    ResearchEvidenceRef.revision == first.revision,
                )
            )
            if existing is not None:
                if existing.content_hash != first.evidence_ref.content_hash:
                    raise RuntimeError("SEC_EVIDENCE_CONFLICT")
                replayed += 1
                continue
            evidence = ResearchEvidenceRef(
                source="sec",
                provider_object_id=accession,
                revision=first.revision,
                available_at=first.accepted_at,
                location=first.evidence_ref.location,
                content_hash=first.evidence_ref.content_hash,
                byte_length=len(evidence_payloads[accession]),
            )
            context.session.add(evidence)
            await context.session.flush()
            filing = Filing(
                instrument_id=instrument.instrument_id,
                evidence_ref_id=evidence.id,
                source="sec",
                provider_object_id=accession,
                accession_number=accession,
                form=first.form,
                filed_at=first.filed_at,
                accepted_at=first.accepted_at,
                revision=first.revision,
            )
            context.session.add(filing)
            await context.session.flush()
            context.session.add_all(
                FinancialFactRecord(
                    filing_id=filing.id,
                    evidence_ref_id=evidence.id,
                    instrument_id=fact.instrument_id,
                    canonical_fact=fact.canonical_fact,
                    raw_taxonomy=fact.raw_taxonomy,
                    raw_tag=fact.raw_tag,
                    unit=fact.unit,
                    value=fact.value,
                    period_kind=fact.period_kind,
                    period_start=fact.period_start,
                    period_end=fact.period_end,
                    fiscal_year=fact.fiscal_year,
                    fiscal_period=fact.fiscal_period,
                    form=fact.form,
                    filed_at=fact.filed_at,
                    accepted_at=fact.accepted_at,
                    accession_number=fact.accession_number,
                    frame=fact.frame,
                    revision=fact.revision,
                )
                for fact in accession_facts
            )
            collected += 1
    await context.session.flush()
    return collected, replayed


async def _collect_macro(
    context: JobHandlerContext,
    *,
    payload: DailyStagePayload,
    bundle: ProviderBundle,
    store: RawEvidenceStore,
) -> tuple[int, int]:
    calendar = await context.session.get(
        ExchangeCalendarAuthorityRecord,
        UUID(payload.calendar_authority_id),
    )
    if (
        calendar is None
        or calendar.exchange != "XNYS"
        or calendar.completeness_status != "complete"
        or calendar.published_at > parse_utc(payload.scheduled_for)
    ):
        raise RuntimeError("CALENDAR_AUTHORITY_MISMATCH")
    calendar_sessions = tuple(
        (
            await context.session.scalars(
                select(ExchangeCalendarSessionRecord)
                .where(ExchangeCalendarSessionRecord.calendar_authority_id == calendar.id)
                .order_by(ExchangeCalendarSessionRecord.session)
            )
        ).all()
    )
    calendar_content = canonical_json(
        {
            "authority_id": str(calendar.id),
            "content_hash": calendar.content_hash,
            "sessions": [
                {
                    "session": item.session.isoformat(),
                    "market_open_at": item.market_open_at.isoformat(),
                    "market_close_at": item.market_close_at.isoformat(),
                }
                for item in calendar_sessions
            ],
        }
    ).encode()
    calendar_digest = sha256(calendar_content).hexdigest()
    calendar_location = await store.put_if_absent(
        key=f"evidence/calendar/XNYS/{calendar_digest}.json",
        content=calendar_content,
    )
    calendar_object_id = f"calendar:XNYS:{calendar.id}"
    calendar_evidence = await context.session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == "exchange_calendars",
            ResearchEvidenceRef.provider_object_id == calendar_object_id,
            ResearchEvidenceRef.revision == calendar.content_hash,
        )
    )
    if calendar_evidence is None:
        calendar_evidence = ResearchEvidenceRef(
            source="exchange_calendars",
            provider_object_id=calendar_object_id,
            revision=calendar.content_hash,
            available_at=calendar.published_at,
            location=calendar_location,
            content_hash=calendar_digest,
            byte_length=len(calendar_content),
        )
        context.session.add(calendar_evidence)
        await context.session.flush()
    elif calendar_evidence.content_hash != calendar_digest:
        raise RuntimeError("CALENDAR_EVIDENCE_CONFLICT")

    def next_session_open(base_date: date) -> datetime:
        for item in calendar_sessions:
            if item.session > base_date:
                return item.market_open_at
        raise RuntimeError("CALENDAR_NEXT_SESSION_UNAVAILABLE")

    collected = 0
    replayed = 0
    for series in sorted(FRED_SERIES):
        document = await bundle.fred_client.fetch_alfred_series(
            series,
            payload.session,
            payload.session,
        )
        location, digest = await _archive_raw(
            store,
            source="fred",
            label=f"{series}-{payload.session}",
            received_at=document.received_at,
            payload=document.raw_payload,
        )
        raw = _json_mapping(document.raw_payload, "FRED_PAYLOAD_INVALID")
        unit = raw.get("units")
        observations = raw.get("observations")
        if (
            not isinstance(unit, str)
            or not unit
            or not isinstance(observations, list)
            or len(observations) > 10_000
        ):
            raise RuntimeError("FRED_PAYLOAD_INVALID")
        for raw_observation in observations:
            if not isinstance(raw_observation, Mapping):
                raise RuntimeError("FRED_PAYLOAD_INVALID")
            revision = raw_observation.get("revision")
            if not isinstance(revision, str) or not revision:
                raise RuntimeError("FRED_PAYLOAD_INVALID")
            object_id = f"{series}:{revision}"
            evidence_ref = EvidenceRef(
                source="fred",
                object_id=object_id,
                available_at=document.received_at,
                location=location,
                content_hash=digest,
            )
            normalized = normalize_alfred_observation(
                raw_observation,
                series=series,
                unit=unit,
                evidence_ref=evidence_ref,
                calendar_evidence_ref=EvidenceRef(
                    source="exchange_calendars",
                    object_id=calendar_object_id,
                    available_at=calendar.published_at,
                    location=calendar_location,
                    content_hash=calendar_digest,
                ),
                next_session_open=next_session_open,
            )
            existing = await context.session.scalar(
                select(ResearchEvidenceRef).where(
                    ResearchEvidenceRef.source == "fred",
                    ResearchEvidenceRef.provider_object_id == object_id,
                    ResearchEvidenceRef.revision == normalized.revision,
                )
            )
            if existing is not None:
                if existing.content_hash != digest:
                    raise RuntimeError("FRED_EVIDENCE_CONFLICT")
                replayed += 1
                continue
            evidence = ResearchEvidenceRef(
                source="fred",
                provider_object_id=object_id,
                revision=normalized.revision,
                available_at=normalized.available_at,
                location=location,
                content_hash=digest,
                byte_length=len(document.raw_payload),
            )
            context.session.add(evidence)
            await context.session.flush()
            context.session.add(
                MacroObservation(
                    source="fred",
                    series=series,
                    observed_at=datetime.combine(
                        normalized.observed_on,
                        time.min,
                        UTC,
                    ),
                    available_at=normalized.available_at,
                    revision=normalized.revision,
                    value=normalized.value,
                    unit=normalized.unit,
                    provider_object_id=object_id,
                    authority_version=normalized.authority_version,
                    real_time_start=normalized.real_time_start,
                    real_time_end=normalized.real_time_end,
                    evidence_ref_id=evidence.id,
                    calendar_evidence_ref_id=calendar_evidence.id,
                    calendar_source="exchange_calendars",
                    calendar_provider_object_id=calendar_object_id,
                    calendar_revision=calendar.content_hash,
                    display_sanity_only=normalized.display_sanity_only,
                )
            )
            collected += 1
    await context.session.flush()
    return collected, replayed


async def _archive_raw(
    store: RawEvidenceStore,
    *,
    source: str,
    label: str,
    received_at: datetime,
    payload: bytes,
) -> tuple[str, str]:
    digest = sha256(payload).hexdigest()
    date_path = received_at.astimezone(UTC).strftime("%Y/%m/%d")
    location = await store.put_if_absent(
        key=f"raw/{source}/{date_path}/{label}/{digest}.json",
        content=payload,
    )
    return location, digest


def _submission_metadata(
    payloads: Sequence[bytes],
) -> dict[str, tuple[datetime, str]]:
    result: dict[str, tuple[datetime, str]] = {}
    for payload in payloads:
        document = _json_mapping(payload, "SEC_SUBMISSIONS_INVALID")
        filings = document.get("filings")
        records = filings.get("recent") if isinstance(filings, Mapping) else document
        if not isinstance(records, Mapping):
            raise RuntimeError("SEC_SUBMISSIONS_INVALID")
        accessions = records.get("accessionNumber")
        accepted = records.get("acceptanceDateTime")
        forms = records.get("form")
        if (
            not isinstance(accessions, list)
            or not isinstance(accepted, list)
            or not isinstance(forms, list)
            or not len(accessions) == len(accepted) == len(forms)
        ):
            raise RuntimeError("SEC_SUBMISSIONS_INVALID")
        for accession, accepted_at, form in zip(
            accessions,
            accepted,
            forms,
            strict=True,
        ):
            if (
                not all(isinstance(value, str) for value in (accession, accepted_at, form))
                or not is_canonical_sec_accession_number(accession)
            ):
                raise RuntimeError("SEC_SUBMISSIONS_INVALID")
            parsed = datetime.fromisoformat(
                cast(str, accepted_at).replace("Z", "+00:00")
            ).astimezone(UTC)
            value = parsed, cast(str, form)
            key = cast(str, accession)
            if key in result and result[key] != value:
                raise RuntimeError("SEC_SUBMISSIONS_CONFLICT")
            result[key] = value
    return result


def _sec_accession_content(
    company_facts_payload: bytes,
    submissions_payloads: Sequence[bytes],
    accession: str,
) -> bytes:
    company_facts = _json_mapping(company_facts_payload, "SEC_COMPANY_FACTS_INVALID")
    facts_root = company_facts.get("facts")
    if not isinstance(facts_root, Mapping):
        raise RuntimeError("SEC_COMPANY_FACTS_INVALID")
    entries: list[dict[str, object]] = []
    for taxonomy, tags in facts_root.items():
        if not isinstance(taxonomy, str) or not isinstance(tags, Mapping):
            raise RuntimeError("SEC_COMPANY_FACTS_INVALID")
        for tag, tagged in tags.items():
            units = tagged.get("units") if isinstance(tagged, Mapping) else None
            if not isinstance(tag, str) or not isinstance(units, Mapping):
                raise RuntimeError("SEC_COMPANY_FACTS_INVALID")
            for unit, facts in units.items():
                if not isinstance(unit, str) or not isinstance(facts, list):
                    raise RuntimeError("SEC_COMPANY_FACTS_INVALID")
                for fact in facts:
                    if isinstance(fact, Mapping) and fact.get("accn") == accession:
                        entries.append(
                            {
                                "taxonomy": taxonomy,
                                "tag": tag,
                                "unit": unit,
                                "fact": dict(fact),
                            }
                        )
    metadata = _submission_metadata(submissions_payloads).get(accession)
    if metadata is None or not entries:
        raise RuntimeError("SEC_ACCESSION_EVIDENCE_INCOMPLETE")
    accepted_at, form = metadata
    return canonical_json(
        {
            "accession": accession,
            "accepted_at": accepted_at.isoformat(),
            "form": form,
            "facts": sorted(entries, key=canonical_json),
        }
    ).encode()


def _json_mapping(payload: bytes, error_code: str) -> Mapping[str, object]:
    try:
        value = json.loads(payload)
    except Exception as error:
        raise RuntimeError(error_code) from error
    if not isinstance(value, Mapping):
        raise RuntimeError(error_code)
    return value


async def _bounded_async_map(
    items: Sequence[_T],
    operation: Callable[[_T], Awaitable[_R]],
) -> tuple[_R, ...]:
    """Run bounded external I/O, fail fast, and retain success result order."""
    if not items:
        return ()

    work: asyncio.Queue[tuple[int, _T]] = asyncio.Queue()
    completions: asyncio.Queue[tuple[int, _R | None, BaseException | None]] = asyncio.Queue()
    stop = asyncio.Event()
    for index, item in enumerate(items):
        work.put_nowait((index, item))

    async def worker() -> None:
        while not stop.is_set():
            try:
                index, item = work.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                result = await operation(item)
            except BaseException as error:
                # Set the stop flag before publishing the failure so no worker
                # can begin queued I/O after the first observed exception.
                stop.set()
                await completions.put((index, None, error))
                return
            await completions.put((index, result, None))

    workers = [
        asyncio.create_task(worker())
        for _ in range(min(_FOSUN_DAILY_OBJECT_CONCURRENCY, len(items)))
    ]
    results: dict[int, _R] = {}
    try:
        while len(results) < len(items):
            index, result, error = await completions.get()
            if error is not None:
                raise error
            results[index] = cast(_R, result)
        await asyncio.gather(*workers)
    except BaseException:
        stop.set()
        for task in workers:
            if not task.done():
                task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise
    return tuple(results[index] for index in range(len(items)))


def _spec(job_type: JobType, handler: JobHandler) -> JobHandlerSpec:
    return JobHandlerSpec(
        job_type=job_type,
        schema_version=STAGE_SCHEMA,
        payload_model=DailyStagePayload,
        queue=QueueName.PROVIDER,
        timeout_seconds=45,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=3,
        idempotency_namespace=job_type.value,
        handler=handler,
    )

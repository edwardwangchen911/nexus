"""Durable daily collection handlers for bounded research-data providers."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import Protocol, cast
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.domain.evidence import EvidenceRef
from nexus.jobs.daily_payloads import STAGE_SCHEMA, DailyStagePayload, parse_utc
from nexus.jobs.errors import UnsafeJobError
from nexus.jobs.market_authority import (
    FrozenFosunMarketAuthority,
    ProviderMarketBinding,
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
from nexus.market.ingestion import (
    MarketObjectStore,
    archive_fosun_price_page_raw,
    ingest_price_page,
    serialize_price_bars,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataArtifact, MacroObservation, PriceSeriesManifest
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
from nexus.providers.base import BarsRequest, InstrumentId, PriceBar, ProviderPage
from nexus.providers.composition import ProviderBundle
from nexus.providers.fred import FRED_SERIES, normalize_alfred_observation
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.providers.sec import SecInstrumentIdentity, normalize_company_facts
from nexus.providers.settings import ProviderRuntimeMode


class RawEvidenceStore(Protocol):
    """Content-addressed evidence boundary used by provider collection Jobs."""

    async def put_if_absent(self, *, key: str, content: bytes) -> str: ...


class RawFirstFosunMarketProvider(Protocol):
    """Narrow Fosun seam that preserves exact bytes before interpretation."""

    async def fetch_raw_bars(
        self,
        request: BarsRequest,
        cursor: str | None = None,
    ) -> RawGatewayResponse: ...

    async def normalize_bars(
        self,
        request: BarsRequest,
        response: RawGatewayResponse,
        cursor: str | None = None,
    ) -> ProviderPage[PriceBar]: ...


MarketAuthorityLoader = Callable[
    ...,
    Awaitable[FrozenFosunMarketAuthority | None],
]

_FOSUN_DAILY_MAX_INSTRUMENTS = 10
_LEGACY_DAILY_MAX_INSTRUMENTS = 100


def build_provider_collection_registry(
    bundle: ProviderBundle,
    market_object_store: MarketObjectStore,
    raw_evidence_store: RawEvidenceStore,
    now: Callable[[], datetime],
    *,
    expected_profile_id: str | None = None,
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
        if (
            bundle.market_source != "fosun"
            and payload.market_source != bundle.market_source
        ):
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
        max_instruments = (
            _FOSUN_DAILY_MAX_INSTRUMENTS
            if bundle.market_source == "fosun"
            else _LEGACY_DAILY_MAX_INSTRUMENTS
        )
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
                        Instrument.exchange.in_(
                            ("AMEX", "ARCA", "NASDAQ", "NYSE", "NYSEARCA", "XNYS")
                        ),
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
        if bundle.market_source == "fosun":
            return await _collect_fosun_market(
                context,
                payload=payload,
                bundle=bundle,
                market_object_store=market_object_store,
                market_authority_loader=market_authority_loader,
                instruments=instruments,
                market_close=market_close,
            )
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
                        start=market_close - timedelta(days=400),
                        end=market_close,
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
                        result = await ingest_price_page(
                            context.session,
                            market_object_store,
                            page,
                            commit=False,
                        )
                        collected += 1
                        source_conflict = (
                            source_conflict or result.price_authority is None
                        )
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

    return JobRegistry(
        (
            _spec(JobType.COLLECT_MARKET, collect_market),
            _spec(JobType.COLLECT_SEC_MACRO, collect_sec_macro),
        )
    )


async def _collect_fosun_market(
    context: JobHandlerContext,
    *,
    payload: DailyStagePayload,
    bundle: ProviderBundle,
    market_object_store: MarketObjectStore,
    market_authority_loader: MarketAuthorityLoader,
    instruments: Sequence[Instrument],
    market_close: datetime,
) -> JobHandlerResult:
    """Fetch every response raw-first and publish facts only after all post checks."""
    frozen = payload.market_authority
    if frozen is None:
        return JobHandlerResult(
            outcome="skipped",
            result_code="FOSUN_MARKET_AUTHORITY_UNAVAILABLE",
        )
    first_check = context.finished_at()
    if not await _matches_frozen_market_authority(
        context,
        loader=market_authority_loader,
        frozen=frozen,
        check_time=first_check,
    ):
        return JobHandlerResult(
            outcome="skipped",
            result_code="FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_FETCH",
        )
    provider = cast(RawFirstFosunMarketProvider, bundle.market_provider(context.session))
    if not callable(getattr(provider, "fetch_raw_bars", None)) or not callable(
        getattr(provider, "normalize_bars", None)
    ):
        raise RuntimeError("FOSUN_RAW_FIRST_PROVIDER_REQUIRED")

    pending: list[
        tuple[ProviderPage[PriceBar], ProviderMarketBinding, DataArtifact]
    ] = []
    replayed = 0
    for instrument in instruments:
        request = BarsRequest(
            instrument_id=InstrumentId(instrument.id),
            start=market_close - timedelta(days=400),
            end=market_close,
            interval="1Day",
            adjustment="raw",
        )
        pre_check = context.finished_at()
        if not await _matches_frozen_market_authority(
            context,
            loader=market_authority_loader,
            frozen=frozen,
            check_time=pre_check,
        ):
            return JobHandlerResult(
                outcome="skipped",
                result_code="FOSUN_MARKET_AUTHORITY_CHANGED_BEFORE_FETCH",
            )
        response = await provider.fetch_raw_bars(request)
        archived = await archive_fosun_price_page_raw(
            cast(AsyncSession, context.session),
            market_object_store,
            response,
            commit=False,
        )
        post_check = context.finished_at()
        if not await _matches_frozen_market_authority(
            context,
            loader=market_authority_loader,
            frozen=frozen,
            check_time=post_check,
        ):
            return JobHandlerResult(
                outcome="skipped",
                result_code="FOSUN_MARKET_AUTHORITY_CHANGED_AFTER_FETCH",
            )
        try:
            page = await provider.normalize_bars(request, response)
        except MarketProviderPayloadError:
            return JobHandlerResult(
                outcome="skipped",
                result_code="FOSUN_MARKET_PAYLOAD_INVALID",
            )
        if not _fosun_page_matches_authority(page, response, frozen):
            return JobHandlerResult(
                outcome="skipped",
                result_code="FOSUN_MARKET_CONTRACT_MISMATCH",
            )
        if page.items:
            if any(item.instrument_id != instrument.id for item in page.items):
                raise RuntimeError("MARKET_PROVIDER_IDENTITY_MISMATCH")
            pending.append(
                (
                    page,
                    binding_at(frozen, authorized_at=post_check),
                    archived.raw_artifact,
                )
            )

    final_check = context.finished_at()
    if not await _matches_frozen_market_authority(
        context,
        loader=market_authority_loader,
        frozen=frozen,
        check_time=final_check,
    ):
        return JobHandlerResult(
            outcome="skipped",
            result_code="FOSUN_MARKET_AUTHORITY_CHANGED_AFTER_FETCH",
        )
    replayed = 0
    publish: list[tuple[ProviderPage[PriceBar], ProviderMarketBinding, DataArtifact]] = []
    for page, provider_binding, raw_artifact in pending:
        digest = sha256(serialize_price_bars(page.items)).hexdigest()
        exact = await context.session.scalar(
            select(PriceSeriesManifest.id)
            .join(
                DataArtifact,
                DataArtifact.id == PriceSeriesManifest.data_artifact_id,
            )
            .where(
                DataArtifact.sha256 == digest,
                PriceSeriesManifest.source == "fosun",
                PriceSeriesManifest.feed == "l1",
                PriceSeriesManifest.delivery_mode == frozen.delivery_mode,
                PriceSeriesManifest.adjustment == "raw",
                PriceSeriesManifest.interval == "1Day",
                PriceSeriesManifest.provider_capability_id
                == provider_binding.provider_capability_id,
                PriceSeriesManifest.provider_status_id
                == provider_binding.provider_status_id,
                PriceSeriesManifest.provider_status_version
                == provider_binding.provider_status_version,
                PriceSeriesManifest.provider_contract_hash
                == provider_binding.provider_contract_hash,
                PriceSeriesManifest.provider_authorized_at
                == provider_binding.provider_authorized_at,
            )
        )
        if exact is not None:
            replayed += 1
        else:
            any_artifact = await context.session.scalar(
                select(DataArtifact.id).where(DataArtifact.sha256 == digest)
            )
            if any_artifact is not None:
                return JobHandlerResult(
                    outcome="skipped",
                    result_code="FOSUN_MARKET_REPLAY_CONFLICT",
                )
            publish.append((page, provider_binding, raw_artifact))

    collected = 0
    source_conflict = False
    for page, provider_binding, raw_artifact in publish:
        result = await ingest_price_page(
            context.session,
            market_object_store,
            page,
            provider_binding=provider_binding,
            raw_artifact=raw_artifact,
            commit=False,
        )
        collected += 1
        source_conflict = source_conflict or result.price_authority is None
    if source_conflict:
        return JobHandlerResult(outcome="skipped", result_code="SOURCE_CONFLICT")
    if collected:
        return JobHandlerResult(outcome="succeeded", result_code="MARKET_COLLECTED")
    if replayed:
        return JobHandlerResult(outcome="succeeded", result_code="MARKET_REPLAYED")
    return JobHandlerResult(outcome="skipped", result_code="MARKET_NO_DATA")


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
                    Instrument.exchange.in_(
                        ("AMEX", "ARCA", "NASDAQ", "NYSE", "NYSEARCA", "XNYS")
                    ),
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
        company_facts, submissions = (
            await bundle.sec_client.fetch_company_facts_and_submission_history(
                instrument.cik
            )
        )
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
        accession_metadata = _submission_metadata(
            tuple(document.raw_payload for document in submissions)
        )
        evidence_by_accession: dict[str, EvidenceRef] = {}
        evidence_payloads: dict[str, bytes] = {}
        for accession, (accepted_at, _form) in accession_metadata.items():
            content = _sec_accession_content(
                company_facts.raw_payload,
                tuple(document.raw_payload for document in submissions),
                accession,
            )
            digest = sha256(content).hexdigest()
            location = await store.put_if_absent(
                key=f"evidence/sec/{accession}/{digest}.json",
                content=content,
            )
            evidence_payloads[accession] = content
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
            accession_facts = tuple(
                fact for fact in facts if fact.accession_number == accession
            )
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
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == calendar.id
                )
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
            if not all(isinstance(value, str) for value in (accession, accepted_at, form)):
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

"""Persisted executable proposal and calendar fixtures for Shadow tests."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import exchange_calendars
from nexus.backtest.engine import exchange_calendar_content_hash
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceSeriesManifest,
)
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.models.research import CorporateActionRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.shadow.canonical import shadow_source_query_content_hash
from nexus.shadow.start import ShadowStartResult, start_proposal_shadow
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from helpers.optimization import complete_comparison_metrics
from helpers.portfolio_api import INSTRUMENT_ID, PORTFOLIOS, seed_portfolio_list, settings

_BASELINE_ID = UUID("0198d500-1000-7000-8000-000000000041")
_OPTIMIZER_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")
_SECOND_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000009999")
_VALUATION_TIME = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
_NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class ShadowEligibleSeed:
    group_id: UUID
    proposal_ids: dict[str, UUID]
    instrument_ids: tuple[UUID, UUID]
    calendar_id: UUID
    calendar_sessions: tuple[date, ...]
    database_time: datetime


@dataclass(frozen=True)
class ShadowCalendarSeed:
    calendar_id: UUID
    sessions: tuple[date, ...]


@dataclass(frozen=True)
class ShadowStartedSeed:
    eligible: ShadowEligibleSeed
    start: ShadowStartResult


@dataclass(frozen=True)
class SeededShadowPriceAuthority:
    bar_id: UUID
    authority_id: UUID
    manifest_id: UUID


@dataclass(frozen=True)
class SeededShadowEvent:
    event_id: UUID
    evidence_hash: str


async def seed_complete_shadow_session_inputs(
    database_url: str,
    *,
    started: ShadowStartedSeed,
    ordinal: int,
    instrument_ids: tuple[UUID, ...] | None = None,
) -> datetime:
    """Insert a complete two-instrument raw Bar/event query authority fixture."""
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        return await _seed_complete_shadow_session_inputs(
            session,
            started=started,
            ordinal=ordinal,
            instrument_ids=instrument_ids,
        )


async def seed_complete_shadow_window(
    database_url: str,
    *,
    started: ShadowStartedSeed,
    through_ordinal: int,
    start_ordinal: int = 1,
    instrument_ids: tuple[UUID, ...] | None = None,
) -> tuple[datetime, ...]:
    """Insert a complete authority fixture window in one transaction."""
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        return tuple(
            [
                await _seed_complete_shadow_session_inputs(
                    session,
                    started=started,
                    ordinal=ordinal,
                    instrument_ids=instrument_ids,
                )
                for ordinal in range(start_ordinal, through_ordinal + 1)
            ]
        )


async def _seed_complete_shadow_session_inputs(
    session: AsyncSession,
    *,
    started: ShadowStartedSeed,
    ordinal: int,
    instrument_ids: tuple[UUID, ...] | None = None,
) -> datetime:
    session_date = started.eligible.calendar_sessions[ordinal - 1]
    query_instruments = (
        started.eligible.instrument_ids if instrument_ids is None else instrument_ids
    )
    decision_at = await session.scalar(
        select(ExchangeCalendarSessionRecord.decision_at).where(
            ExchangeCalendarSessionRecord.calendar_authority_id == started.eligible.calendar_id,
            ExchangeCalendarSessionRecord.session == session_date,
        )
    )
    assert isinstance(decision_at, datetime)
    decision_at = decision_at.astimezone(UTC)
    bars = tuple(
        [
            await seed_shadow_price_authority(
                session,
                instrument_id=instrument_id,
                session_date=session_date,
                available_at=decision_at - timedelta(minutes=1),
                open_price=str(100 + index),
                close_price=str(101 + index),
            )
            for index, instrument_id in enumerate(query_instruments)
        ]
    )
    await seed_shadow_query_proof(
        session,
        shadow_group_id=started.start.shadow_group_id,
        session_date=session_date,
        kind="price_bars",
        source="alpaca",
        delivery_mode="delayed",
        instrument_ids=query_instruments,
        complete=True,
        member_ids=tuple(item.bar_id for item in bars),
        available_at=decision_at - timedelta(seconds=10),
    )
    await seed_shadow_query_proof(
        session,
        shadow_group_id=started.start.shadow_group_id,
        session_date=session_date,
        kind="corporate_actions",
        source="fixture",
        delivery_mode="unknown",
        instrument_ids=query_instruments,
        complete=True,
        available_at=decision_at - timedelta(seconds=10),
    )
    return decision_at


async def seed_shadow_calendar(
    session: AsyncSession,
    *,
    database_time: datetime,
    market_date: date,
    session_count: int = 68,
    content_hash_override: str | None = None,
) -> ShadowCalendarSeed:
    """Insert one complete, content-addressed XNYS calendar authority."""
    official_calendar = exchange_calendars.get_calendar("XNYS")
    candidates = tuple(
        value.date()
        for value in official_calendar.sessions_in_range(
            market_date + timedelta(days=1),
            market_date + timedelta(days=180),
        )
    )
    calendar_sessions = candidates[:session_count]
    if len(calendar_sessions) != session_count:
        raise RuntimeError("Shadow fixture calendar range is too short")
    decision_times = {
        session_date: official_calendar.session_close(session_date).to_pydatetime()
        + timedelta(minutes=30)
        for session_date in calendar_sessions
    }
    calendar = ExchangeCalendarAuthorityRecord(
        id=uuid4(),
        exchange="XNYS",
        version=f"shadow-start-{uuid4()}",
        source="fixture",
        range_start=calendar_sessions[0],
        range_end=calendar_sessions[-1],
        query_parameters={"fixture": "shadow-start"},
        completeness_status="complete",
        content_hash=content_hash_override
        or exchange_calendar_content_hash(calendar_sessions, decision_times),
        published_at=database_time - timedelta(seconds=1),
    )
    session.add(calendar)
    await session.flush()
    session.add_all(
        ExchangeCalendarSessionRecord(
            calendar_authority_id=calendar.id,
            session=session_date,
            decision_at=decision_times[session_date],
        )
        for session_date in calendar_sessions
    )
    return ShadowCalendarSeed(calendar_id=calendar.id, sessions=calendar_sessions)


async def seed_started_shadow(
    database_url: str,
    *,
    all_cash_variant: str | None = None,
) -> ShadowStartedSeed:
    """Create and atomically start one executable Shadow root."""
    eligible = await seed_shadow_eligible_group(database_url, all_cash_variant=all_cash_variant)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        started = await start_proposal_shadow(
            session,
            actor_id="shadow-authority-fixture",
            idempotency_key=f"start-{uuid4()}",
            request_hash="9" * 64,
            group_id=eligible.group_id,
            expected_version=1,
        )
    return ShadowStartedSeed(eligible=eligible, start=started)


async def seed_shadow_price_authority(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    session_date: date,
    available_at: datetime,
    source: str = "alpaca",
    delivery_mode: str = "delayed",
    adjustment: str = "raw",
    revision: str = "1",
    open_price: str = "100",
    close_price: str = "101",
) -> SeededShadowPriceAuthority:
    """Insert one database-finalized one-session Bar authority."""
    manifest_id = uuid4()
    artifact_id = uuid4()
    artifact_hash = sha256(str(artifact_id).encode()).hexdigest()
    observed_at = available_at - timedelta(minutes=15)
    received_at = available_at + timedelta(seconds=1)
    feed = "sip" if source == "alpaca" else "fixture-feed"
    manifest = PriceSeriesManifest(
        id=manifest_id,
        evidence_ref_id=None,
        data_artifact_id=artifact_id,
        instrument=str(instrument_id),
        source=source,
        feed=feed,
        delivery_mode=delivery_mode,
        adjustment=adjustment,
        interval="1Day",
        start_at=observed_at,
        end_at=observed_at,
        row_count=1,
    )
    open_value = Decimal(open_price)
    close_value = Decimal(close_price)
    bar = PriceBarRecord(
        id=uuid4(),
        price_series_manifest_id=manifest_id,
        instrument_id=instrument_id,
        session=session_date,
        open=open_value,
        high=max(open_value, close_value) + Decimal("1"),
        low=min(open_value, close_value) - Decimal("1"),
        close=close_value,
        volume=Decimal("1000000"),
        observed_at=observed_at,
        available_at=available_at,
        received_at=received_at,
        revision=revision,
        quality_flags=[],
    )
    session.add(
        DataArtifact(
            id=artifact_id,
            uri=f"memory://sha256/{artifact_hash}",
            sha256=artifact_hash,
            byte_size=512,
            mime_type="application/vnd.apache.parquet",
            schema_version="price_bars_v1",
            source=source,
            observed_at=observed_at,
            available_at=available_at,
            received_at=received_at,
        )
    )
    session.add(manifest)
    await session.flush()
    session.add(bar)
    await session.flush()
    finalized = await finalize_price_bar_set(session, manifest)
    if finalized is None:
        raise RuntimeError("Shadow Bar fixture did not produce an authority")
    authority, _ = finalized
    return SeededShadowPriceAuthority(
        bar_id=bar.id,
        authority_id=authority.id,
        manifest_id=manifest_id,
    )


async def seed_shadow_query_proof(
    session: AsyncSession,
    *,
    shadow_group_id: UUID,
    session_date: date,
    kind: str,
    source: str,
    delivery_mode: str,
    instrument_ids: tuple[UUID, ...],
    complete: bool,
    available_at: datetime,
    member_ids: tuple[UUID, ...] = (),
    negative_instrument_ids: tuple[UUID, ...] = (),
) -> UUID:
    """Insert a sanitized query proof; no provider payload is retained."""
    query_hash = shadow_source_query_content_hash(
        shadow_group_id=shadow_group_id,
        session=session_date,
        kind=kind,
        source=source,
        delivery_mode=delivery_mode,
        instrument_ids=instrument_ids,
        complete=complete,
        member_ids=member_ids,
        negative_instrument_ids=negative_instrument_ids,
    )
    proof = DataSnapshot(
        id=uuid4(),
        source=source,
        provider_object_id=f"shadow-query:{uuid4()}",
        revision=query_hash,
        observed_at=available_at - timedelta(seconds=1),
        available_at=available_at,
        received_at=available_at + timedelta(microseconds=1),
        schema_version="shadow_source_query_v1",
        payload={
            "schema": "shadow_source_query_v1",
            "shadow_group_id": str(shadow_group_id),
            "session": session_date.isoformat(),
            "kind": kind,
            "source": source,
            "delivery_mode": delivery_mode,
            "instrument_ids": [
                str(value) for value in sorted(instrument_ids, key=lambda item: item.int)
            ],
            "complete": complete,
            "member_ids": [str(value) for value in sorted(member_ids, key=lambda item: item.int)],
            "negative_instrument_ids": [
                str(value) for value in sorted(negative_instrument_ids, key=lambda item: item.int)
            ],
            "query_hash": query_hash,
        },
    )
    session.add(proof)
    await session.flush()
    return proof.id


async def seed_shadow_event(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    source: str,
    provider_event_id: str,
    action_type: str,
    process_date: date,
    available_at: datetime,
    ex_date: date | None = None,
    record_date: date | None = None,
    payable_date: date | None = None,
    rate: str | None = None,
    revision: str = "1",
) -> SeededShadowEvent:
    """Insert one content-addressed corporate-action event authority."""
    evidence_hash = sha256(f"{source}:{provider_event_id}:{revision}".encode()).hexdigest()
    evidence = ResearchEvidenceRef(
        id=uuid4(),
        source=source,
        provider_object_id=provider_event_id,
        revision=revision,
        available_at=available_at,
        location=f"memory://sha256/{evidence_hash}",
        content_hash=evidence_hash,
        byte_length=256,
    )
    session.add(evidence)
    await session.flush()
    event = CorporateActionRecord(
        id=uuid4(),
        evidence_ref_id=evidence.id,
        instrument_id=instrument_id,
        source=source,
        provider_event_id=provider_event_id,
        action_type=action_type,
        status="complete",
        process_date=process_date,
        ex_date=ex_date,
        record_date=record_date,
        payable_date=payable_date,
        rate=None if rate is None else Decimal(rate),
        revision=revision,
    )
    session.add(event)
    await session.flush()
    return SeededShadowEvent(event_id=event.id, evidence_hash=evidence_hash)


async def seed_shadow_eligible_group(
    database_url: str,
    *,
    calendar_session_count: int = 68,
    calendar_content_hash_override: str | None = None,
    status: str = "compared",
    source_conflict: bool = False,
    partial: bool = False,
    requires_fx: bool = False,
    cost_complete: bool = True,
    authority_stale: bool = False,
    baseline_ready: bool = True,
    diagnostic_complete: bool = True,
    strategy_evidence_complete: bool = True,
    comparison_non_executable: bool = False,
    all_cash_variant: str | None = None,
) -> ShadowEligibleSeed:
    """Create exactly one compared group and one published future XNYS calendar."""
    await seed_portfolio_list(database_url)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        database_time = await session.scalar(select(func.transaction_timestamp()))
        assert isinstance(database_time, datetime)
        database_time = database_time.astimezone(UTC)
        market_date = database_time.astimezone(_NEW_YORK).date()

        second_instrument = Instrument(
            id=_SECOND_INSTRUMENT_ID,
            asset_type="equity",
            exchange="XNYS",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="us_equity",
        )
        data_snapshot = DataSnapshot(
            id=uuid4(),
            source="fixture",
            provider_object_id=f"shadow-start-{uuid4()}",
            revision="1",
            observed_at=_VALUATION_TIME,
            available_at=_VALUATION_TIME,
            received_at=_VALUATION_TIME,
            schema_version="1",
            payload={},
        )
        report = PortfolioDiagnosticReportRecord(
            id=uuid4(),
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=_BASELINE_ID,
            snapshot_as_of=_VALUATION_TIME,
            trading_day=_VALUATION_TIME.date(),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="OPTIMIZATION_CANDIDATE",
            diagnostic_input_hash="d" * 64,
        )
        selection = BaselineSelectionAuthorityRecord(
            id=uuid4(),
            policy_version="baseline_v1",
            data_snapshot_id=data_snapshot.id,
            decision_at=_VALUATION_TIME,
            available_at=_VALUATION_TIME,
            evidence_refs=["selection:shadow-start"],
            candidates=[
                {
                    "instrument_id": str(instrument_id),
                    "provenance_refs": ["candidate:shadow-start"],
                }
                for instrument_id in (INSTRUMENT_ID, _SECOND_INSTRUMENT_ID)
            ],
            selected_instrument_ids=[str(INSTRUMENT_ID), str(_SECOND_INSTRUMENT_ID)],
            evidence_hash="a" * 64,
        )
        session.add_all((second_instrument, data_snapshot, report, selection))
        await session.flush()

        group = OptimizationProposalGroupRecord(
            id=uuid4(),
            baseline_portfolio_snapshot_id=_BASELINE_ID,
            portfolio_scope="internal_paper",
            selection_authority_id=selection.id,
            selection_evidence_hash=selection.evidence_hash,
            status=status,
            version=1,
            policy_id=_OPTIMIZER_POLICY_ID,
            diagnostic_report_id=report.id,
            data_snapshot_id=data_snapshot.id,
            source_policy_id=SOURCE_POLICY_ID,
            cost_policy_id=COST_POLICY_ID,
            diagnostic_input_hash=report.diagnostic_input_hash,
            valuation_time=_VALUATION_TIME,
            baseline_ready=baseline_ready,
            diagnostic_complete=diagnostic_complete,
            source_conflict=source_conflict,
            partial=partial,
            requires_fx=requires_fx,
            cost_complete=cost_complete,
            authority_stale=authority_stale,
        )
        session.add(group)
        await session.flush()

        weights = {
            "hold_current": (("0.20", "0.25"), ("0.10", "0.05")),
            "minimum_change": (("0.20", "0.15"), ("0.10", "0.15")),
            "strategy_target": (("0.20", "0.125"), ("0.10", "0.125")),
        }
        proposal_ids: dict[str, UUID] = {}
        for variant in ("hold_current", "minimum_change", "strategy_target"):
            variant_strategy_evidence_complete = (
                strategy_evidence_complete if variant == "strategy_target" else True
            )
            proposal = OptimizationProposalRecord(
                id=uuid4(),
                proposal_group_id=group.id,
                variant=variant,
                baseline_portfolio_snapshot_id=_BASELINE_ID,
                analysis_only=False,
                strategy_evidence_complete=variant_strategy_evidence_complete,
                portfolio_scope="internal_paper",
            )
            session.add(proposal)
            await session.flush()
            proposal_ids[variant] = proposal.id
            for instrument_id, (current_weight, target_weight) in zip(
                (INSTRUMENT_ID, _SECOND_INSTRUMENT_ID),
                weights[variant],
                strict=True,
            ):
                if all_cash_variant in {variant, "all"}:
                    current_weight = target_weight = "0"
                persisted_target = (
                    current_weight if not variant_strategy_evidence_complete else target_weight
                )
                session.add(
                    ProposalPositionRecord(
                        id=uuid4(),
                        proposal_id=proposal.id,
                        instrument_id=instrument_id,
                        current_weight=Decimal(current_weight),
                        target_weight=Decimal(persisted_target),
                        action="maintain",
                        reason_code="shadow_fixture",
                    )
                )
            session.add(
                ProposalComparisonRecord(
                    id=uuid4(),
                    proposal_id=proposal.id,
                    metrics=complete_comparison_metrics(
                        report_id=report.id,
                        data_id=data_snapshot.id,
                        source_id=SOURCE_POLICY_ID,
                        optimizer_id=_OPTIMIZER_POLICY_ID,
                        cost_id=COST_POLICY_ID,
                    ),
                    tax_impact="unknown",
                    requires_fx=False,
                    non_executable=comparison_non_executable,
                    cost_complete=True,
                )
            )

        calendar = await seed_shadow_calendar(
            session,
            database_time=database_time,
            market_date=market_date,
            session_count=calendar_session_count,
            content_hash_override=calendar_content_hash_override,
        )
        return ShadowEligibleSeed(
            group_id=group.id,
            proposal_ids=proposal_ids,
            instrument_ids=(INSTRUMENT_ID, _SECOND_INSTRUMENT_ID),
            calendar_id=calendar.calendar_id,
            calendar_sessions=calendar.sessions,
            database_time=database_time,
        )

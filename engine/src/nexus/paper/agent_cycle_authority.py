"""Point-in-time root and candidate authority for Agent paper cycles."""

from datetime import UTC, date, datetime, time, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import select_authoritative_snapshot
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.paper.agent_cycle_models import CandidateManifestEntry
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    RiskQueryAuthorityRecord,
)


class CycleAuthority(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    account_id: UUID
    portfolio_id: UUID
    decision_session: date
    decision_time: datetime
    execution_session: date
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID | None = None


def _decision_source_policy_id(authority: CycleAuthority) -> UUID:
    return (
        PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
        if authority.execution_session == authority.decision_session
        else PAPER_DECISION_SOURCE_POLICY_ID
    )


async def load_quasi_realtime_cycle_authority(
    session: AsyncSession, *, account_id: UUID, now: datetime
) -> CycleAuthority:
    """Load one same-session AAPL cycle root from a server-owned UTC clock."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    local_now = now.astimezone(ZoneInfo("America/New_York"))
    if not time(7) <= local_now.time().replace(tzinfo=None) < time(16):
        raise ValueError("AGENT_PAPER_MARKET_WINDOW_CLOSED")
    account = await session.get(PaperAccountRecord, account_id)
    if account is None or account.capability != "simulation_write_internal":
        raise ValueError("AGENT_PAPER_ACCOUNT_INVALID")
    row = (
        await session.execute(
            select(ExchangeCalendarAuthorityRecord, ExchangeCalendarSessionRecord)
            .join(
                ExchangeCalendarSessionRecord,
                ExchangeCalendarSessionRecord.calendar_authority_id
                == ExchangeCalendarAuthorityRecord.id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.range_start <= local_now.date(),
                ExchangeCalendarAuthorityRecord.range_end >= local_now.date(),
                ExchangeCalendarAuthorityRecord.published_at <= now,
                ExchangeCalendarAuthorityRecord.created_at <= now,
                ExchangeCalendarSessionRecord.session == local_now.date(),
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(1)
        )
    ).one_or_none()
    if row is None:
        raise ValueError("AGENT_PAPER_CALENDAR_UNAVAILABLE")
    calendar, calendar_session = row
    existing_cycle = await session.scalar(
        select(AgentPaperCycleRecord.id).where(
            AgentPaperCycleRecord.account_id == account_id,
            AgentPaperCycleRecord.decision_session == calendar_session.session,
        )
    )
    if existing_cycle is not None:
        raise ValueError("AGENT_PAPER_CYCLE_EXISTS")
    universe = await session.scalar(
        select(UniverseSnapshot)
        .where(
            UniverseSnapshot.key == "us_liquid",
            UniverseSnapshot.version == "us_liquid_v1",
            UniverseSnapshot.source == "fosun_local_read_only",
            UniverseSnapshot.effective_from <= calendar_session.session,
            UniverseSnapshot.published_at <= now,
            UniverseSnapshot.created_at <= now,
        )
        .order_by(UniverseSnapshot.published_at.desc(), UniverseSnapshot.id)
        .limit(1)
    )
    if universe is None:
        raise ValueError("AGENT_PAPER_ROOT_AUTHORITY_UNAVAILABLE")
    return CycleAuthority(
        account_id=account.id,
        portfolio_id=account.portfolio_id,
        decision_session=calendar_session.session,
        decision_time=now,
        execution_session=calendar_session.session,
        calendar_authority_id=calendar.id,
        universe_snapshot_id=universe.id,
        quality_report_id=None,
    )


async def load_cycle_authority(
    session: AsyncSession, *, account_id: UUID, decision_time: datetime
) -> CycleAuthority:
    account = await session.get(PaperAccountRecord, account_id)
    if account is None:
        raise ValueError("AGENT_PAPER_ACCOUNT_INVALID")
    row = (
        await session.execute(
            select(ExchangeCalendarAuthorityRecord, ExchangeCalendarSessionRecord)
            .join(
                ExchangeCalendarSessionRecord,
                ExchangeCalendarSessionRecord.calendar_authority_id
                == ExchangeCalendarAuthorityRecord.id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= decision_time,
                ExchangeCalendarSessionRecord.decision_at == decision_time,
            )
            .order_by(ExchangeCalendarAuthorityRecord.published_at.desc())
            .limit(1)
        )
    ).one_or_none()
    if row is None:
        raise ValueError("AGENT_PAPER_CALENDAR_UNAVAILABLE")
    calendar, decision_session = row
    execution_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == calendar.id,
            ExchangeCalendarSessionRecord.session > decision_session.session,
        )
        .order_by(ExchangeCalendarSessionRecord.session)
        .limit(1)
    )
    universe = await session.scalar(
        select(UniverseSnapshot).where(
            UniverseSnapshot.key == "us_liquid",
            UniverseSnapshot.version == "us_liquid_v1",
            UniverseSnapshot.source == "fosun_local_read_only",
            UniverseSnapshot.effective_from <= decision_session.session,
            UniverseSnapshot.published_at <= decision_time,
        )
    )
    if execution_session is None or universe is None:
        raise ValueError("AGENT_PAPER_ROOT_AUTHORITY_UNAVAILABLE")
    return CycleAuthority(
        account_id=account.id,
        portfolio_id=account.portfolio_id,
        decision_session=decision_session.session,
        decision_time=decision_time,
        execution_session=execution_session,
        calendar_authority_id=calendar.id,
        universe_snapshot_id=universe.id,
        quality_report_id=None,
    )


async def prepare_candidates(
    session: AsyncSession, *, authority: CycleAuthority
) -> tuple[CandidateManifestEntry, ...]:
    decision_source_policy_id = _decision_source_policy_id(authority)
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias)
            .join(
                UniverseMember,
                UniverseMember.instrument_id == InstrumentAlias.instrument_id,
            )
            .where(
                UniverseMember.universe_snapshot_id == authority.universe_snapshot_id,
                UniverseMember.created_at <= authority.decision_time,
                InstrumentAlias.symbol == "AAPL",
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= authority.decision_session,
                (InstrumentAlias.valid_to.is_(None))
                | (InstrumentAlias.valid_to >= authority.decision_session),
                InstrumentAlias.created_at <= authority.decision_time,
            )
            .limit(2)
        )
    )
    if len(aliases) != 1:
        return ()
    alias = aliases[0]
    instrument = await session.get(Instrument, alias.instrument_id)
    member = await session.scalar(
        select(UniverseMember).where(
            UniverseMember.universe_snapshot_id == authority.universe_snapshot_id,
            UniverseMember.instrument_id == alias.instrument_id,
            UniverseMember.created_at <= authority.decision_time,
        )
    )
    snapshot = await select_authoritative_snapshot(
        session, instrument_id=alias.instrument_id, as_of=authority.decision_time
    )
    prior_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == authority.calendar_authority_id,
            ExchangeCalendarSessionRecord.session < authority.decision_session,
        )
        .order_by(ExchangeCalendarSessionRecord.session.desc())
        .limit(1)
    )
    if (
        instrument is None
        or instrument.created_at > authority.decision_time
        or instrument.currency != "USD"
        or instrument.exchange not in US_EQUITY_EXCHANGES
        or instrument.asset_type not in {"ETF", "common_stock"}
        or member is None
        or snapshot is None
        or snapshot.source != "nexus_research"
        or snapshot.schema_version != "research_filings_v1"
        or snapshot.created_at > authority.decision_time
        or prior_session is None
    ):
        return ()
    prior_daily = await session.scalar(
        select(PriceBarSetAuthority)
        .join(
            PriceSeriesManifest,
            PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
        )
        .join(
            PriceBarRecord,
            PriceBarRecord.price_series_manifest_id == PriceSeriesManifest.id,
        )
        .where(
            PriceBarSetAuthority.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
            PriceBarSetAuthority.available_at <= authority.decision_time,
            PriceBarSetAuthority.created_at <= authority.decision_time,
            PriceBarRecord.instrument_id == alias.instrument_id,
            PriceBarRecord.session == prior_session,
            PriceBarRecord.available_at <= authority.decision_time,
            PriceBarRecord.created_at <= authority.decision_time,
            PriceSeriesManifest.created_at <= authority.decision_time,
            PriceSeriesManifest.source == "alpaca",
            PriceSeriesManifest.feed == "sip",
            PriceSeriesManifest.delivery_mode == "delayed",
            PriceSeriesManifest.adjustment == "all",
            PriceSeriesManifest.interval == "1Day",
        )
        .order_by(PriceBarSetAuthority.available_at.desc(), PriceBarSetAuthority.id)
        .limit(1)
    )
    has_sector = await session.scalar(
        select(func.count())
        .select_from(SectorExposure)
        .where(
            SectorExposure.instrument_id == alias.instrument_id,
            SectorExposure.as_of <= authority.decision_session,
            SectorExposure.reviewed.is_(True),
            SectorExposure.created_at <= authority.decision_time,
        )
    )
    risk_rows = tuple(
        await session.scalars(
            select(RiskQueryAuthorityRecord).where(
                RiskQueryAuthorityRecord.source_policy_id == decision_source_policy_id,
                RiskQueryAuthorityRecord.universe_snapshot_id == authority.universe_snapshot_id,
                RiskQueryAuthorityRecord.decision_session == authority.decision_session,
                RiskQueryAuthorityRecord.kind.in_(("corporate_action", "suspension", "delisting")),
                RiskQueryAuthorityRecord.complete.is_(True),
                RiskQueryAuthorityRecord.available_at <= authority.decision_time,
                RiskQueryAuthorityRecord.created_at <= authority.decision_time,
            )
        )
    )
    risks = {row.kind: row for row in risk_rows}
    if (
        prior_daily is None
        or not has_sector
        or set(risks) != {"corporate_action", "suspension", "delisting"}
        or any(
            alias.instrument_id not in row.required_instrument_ids
            or alias.instrument_id in row.blocked_instrument_ids
            or (
                decision_source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
                and row.kind == "suspension"
                and authority.decision_time - row.available_at > timedelta(minutes=2)
            )
            for row in risks.values()
        )
    ):
        return ()
    quotes = tuple(
        await session.scalars(
            select(LatestQuote)
            .where(
                LatestQuote.instrument_id == alias.instrument_id,
                LatestQuote.source == "schwab",
                LatestQuote.feed == "l1",
                LatestQuote.delivery_mode == "real_time",
                LatestQuote.event_kind == "quote",
                LatestQuote.evidence_status == "verified",
                LatestQuote.received_at <= authority.decision_time,
                LatestQuote.received_at >= authority.decision_time - timedelta(seconds=60),
                LatestQuote.created_at <= authority.decision_time,
            )
            .order_by(LatestQuote.received_at.desc(), LatestQuote.id)
            .limit(2)
        )
    )
    if not quotes:
        return ()
    quote = quotes[0]
    artifact = await session.get(DataArtifact, quote.data_artifact_id)
    if (
        artifact is None
        or artifact.source != "schwab"
        or artifact.schema_version != "schwab_level_one_stream_v1"
        or artifact.created_at > authority.decision_time
        or artifact.received_at != quote.received_at
        or quote.observed_at > artifact.received_at
        or artifact.received_at - quote.observed_at > timedelta(seconds=60)
        or quote.bid is None
        or quote.ask is None
        or quote.bid <= 0
        or quote.ask <= 0
        or quote.bid > quote.ask
    ):
        return ()
    trade = await session.scalar(
        select(LatestQuote)
        .where(
            LatestQuote.instrument_id == alias.instrument_id,
            LatestQuote.data_artifact_id == artifact.id,
            LatestQuote.source == "schwab",
            LatestQuote.feed == "l1",
            LatestQuote.delivery_mode == "real_time",
            LatestQuote.event_kind == "trade",
            LatestQuote.evidence_status == "verified",
            LatestQuote.created_at <= authority.decision_time,
        )
        .order_by(LatestQuote.observed_at.desc(), LatestQuote.id)
        .limit(1)
    )
    if trade is not None and (
        trade.last is None or trade.last <= 0 or trade.observed_at > artifact.received_at
    ):
        return ()
    ordered_risks = (
        risks["corporate_action"].id,
        risks["suspension"].id,
        risks["delisting"].id,
    )
    return (
        CandidateManifestEntry(
            instrument_id=alias.instrument_id,
            symbol="AAPL",
            inclusion_reason=member.inclusion_reason,
            research_snapshot_id=snapshot.id,
            prior_daily_price_authority_id=prior_daily.id,
            decision_quote_id=quote.id,
            decision_trade_id=None if trade is None else trade.id,
            decision_artifact_id=artifact.id,
            decision_evidence_hash=artifact.sha256,
            risk_query_authority_ids=ordered_risks,
        ),
    )


async def validate_candidate_manifest(
    session: AsyncSession,
    *,
    authority: CycleAuthority,
    manifest: tuple[CandidateManifestEntry, ...],
) -> bool:
    """Read back the exact frozen authorities without selecting newer evidence."""
    if len(manifest) != 1:
        return False
    decision_source_policy_id = _decision_source_policy_id(authority)
    candidate = manifest[0]
    instrument = await session.get(Instrument, candidate.instrument_id)
    alias_count = await session.scalar(
        select(func.count())
        .select_from(InstrumentAlias)
        .where(
            InstrumentAlias.instrument_id == candidate.instrument_id,
            InstrumentAlias.symbol == "AAPL",
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.valid_from <= authority.decision_session,
            (InstrumentAlias.valid_to.is_(None))
            | (InstrumentAlias.valid_to >= authority.decision_session),
            InstrumentAlias.created_at <= authority.decision_time,
        )
    )
    member = await session.scalar(
        select(UniverseMember).where(
            UniverseMember.universe_snapshot_id == authority.universe_snapshot_id,
            UniverseMember.instrument_id == candidate.instrument_id,
            UniverseMember.created_at <= authority.decision_time,
        )
    )
    quote = await session.get(LatestQuote, candidate.decision_quote_id)
    trade = (
        None
        if candidate.decision_trade_id is None
        else await session.get(LatestQuote, candidate.decision_trade_id)
    )
    artifact = await session.get(DataArtifact, candidate.decision_artifact_id)
    prior_daily = await session.get(PriceBarSetAuthority, candidate.prior_daily_price_authority_id)
    risk_rows = tuple(
        row
        for row in await session.scalars(
            select(RiskQueryAuthorityRecord).where(
                RiskQueryAuthorityRecord.id.in_(candidate.risk_query_authority_ids)
            )
        )
    )
    snapshot = await session.get(DataSnapshot, candidate.research_snapshot_id)
    snapshot_evidence = await session.scalar(
        select(AgentDataSnapshotEvidenceRecord).where(
            AgentDataSnapshotEvidenceRecord.data_snapshot_id == candidate.research_snapshot_id,
            AgentDataSnapshotEvidenceRecord.instrument_id == candidate.instrument_id,
        )
    )
    prior_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == authority.calendar_authority_id,
            ExchangeCalendarSessionRecord.session < authority.decision_session,
        )
        .order_by(ExchangeCalendarSessionRecord.session.desc())
        .limit(1)
    )
    if (
        instrument is None
        or instrument.created_at > authority.decision_time
        or instrument.currency != "USD"
        or instrument.exchange not in US_EQUITY_EXCHANGES
        or instrument.asset_type not in {"ETF", "common_stock"}
        or alias_count != 1
        or member is None
        or member.inclusion_reason != candidate.inclusion_reason
        or quote is None
        or artifact is None
        or prior_daily is None
        or snapshot is None
        or snapshot_evidence is None
        or snapshot.source != "nexus_research"
        or snapshot.schema_version != "research_filings_v1"
        or snapshot.available_at > authority.decision_time
        or snapshot.received_at > authority.decision_time
        or snapshot.created_at > authority.decision_time
        or artifact.id != quote.data_artifact_id
        or artifact.sha256 != candidate.decision_evidence_hash
        or artifact.source != "schwab"
        or artifact.schema_version != "schwab_level_one_stream_v1"
        or artifact.created_at > authority.decision_time
        or quote.instrument_id != candidate.instrument_id
        or quote.source != "schwab"
        or quote.feed != "l1"
        or quote.delivery_mode != "real_time"
        or quote.event_kind != "quote"
        or quote.evidence_status != "verified"
        or quote.created_at > authority.decision_time
        or quote.bid is None
        or quote.ask is None
        or quote.bid <= 0
        or quote.ask <= 0
        or quote.bid > quote.ask
        or quote.received_at != artifact.received_at
        or quote.observed_at > artifact.received_at
        or artifact.received_at - quote.observed_at > timedelta(seconds=60)
        or artifact.received_at > authority.decision_time
        or len(risk_rows) != 3
        or {row.id for row in risk_rows} != set(candidate.risk_query_authority_ids)
        or {row.kind for row in risk_rows} != {"corporate_action", "suspension", "delisting"}
        or any(
            row.source_policy_id != decision_source_policy_id
            or row.universe_snapshot_id != authority.universe_snapshot_id
            or row.decision_session != authority.decision_session
            or not row.complete
            or row.available_at > authority.decision_time
            or row.created_at > authority.decision_time
            or candidate.instrument_id not in row.required_instrument_ids
            or candidate.instrument_id in row.blocked_instrument_ids
            or (
                decision_source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
                and row.kind == "suspension"
                and authority.decision_time - row.available_at > timedelta(minutes=2)
            )
            for row in risk_rows
        )
    ):
        return False
    if trade is not None and (
        trade.instrument_id != candidate.instrument_id
        or trade.data_artifact_id != artifact.id
        or trade.source != "schwab"
        or trade.feed != "l1"
        or trade.delivery_mode != "real_time"
        or trade.event_kind != "trade"
        or trade.evidence_status != "verified"
        or trade.created_at > authority.decision_time
        or trade.last is None
        or trade.last <= 0
        or trade.observed_at > artifact.received_at
    ):
        return False
    prior_manifest = await session.get(PriceSeriesManifest, prior_daily.price_series_manifest_id)
    prior_bar = await session.scalar(
        select(PriceBarRecord).where(
            PriceBarRecord.price_series_manifest_id == prior_daily.price_series_manifest_id,
            PriceBarRecord.instrument_id == candidate.instrument_id,
        )
    )
    return bool(
        prior_daily.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
        and prior_daily.available_at <= authority.decision_time
        and prior_daily.created_at <= authority.decision_time
        and prior_daily.first_session == prior_session
        and prior_daily.last_session == prior_session
        and prior_manifest is not None
        and prior_manifest.source == "alpaca"
        and prior_manifest.feed == "sip"
        and prior_manifest.delivery_mode == "delayed"
        and prior_manifest.adjustment == "all"
        and prior_manifest.interval == "1Day"
        and prior_manifest.created_at <= authority.decision_time
        and prior_bar is not None
        and prior_session is not None
        and prior_bar.session == prior_session
        and prior_bar.available_at <= authority.decision_time
        and prior_bar.created_at <= authority.decision_time
    )

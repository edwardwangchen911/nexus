"""Point-in-time root and candidate authority for Agent paper cycles."""

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import select_authoritative_snapshot
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.paper.agent_cycle_models import (
    CandidateAuthority,
    CandidateManifestEntry,
    select_candidate_manifest,
)
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import PriceBarRecord, PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID, PaperAccountRecord


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
    required = tuple(
        await session.scalars(
            select(UniverseMember.instrument_id)
            .where(UniverseMember.universe_snapshot_id == authority.universe_snapshot_id)
            .order_by(UniverseMember.instrument_id)
            .limit(6)
        )
    )
    if not required or len(required) > 5:
        return ()
    projection = await load_paper_projection(
        session, account_id=authority.account_id, as_of=authority.decision_time
    )
    holdings = {row.instrument_id for row in projection.positions if row.quantity > 0}
    if len(required) != len(set(required)) or len(set(required) - holdings) > 5:
        return ()
    candidates: list[CandidateAuthority] = []
    for instrument_id in required:
        instrument = await session.get(Instrument, instrument_id)
        member = await session.scalar(
            select(UniverseMember).where(
                UniverseMember.universe_snapshot_id == authority.universe_snapshot_id,
                UniverseMember.instrument_id == instrument_id,
                UniverseMember.created_at <= authority.decision_time,
            )
        )
        aliases = tuple(
            await session.scalars(
                select(InstrumentAlias).where(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= authority.decision_session,
                    (InstrumentAlias.valid_to.is_(None))
                    | (InstrumentAlias.valid_to >= authority.decision_session),
                    InstrumentAlias.created_at <= authority.decision_time,
                )
            )
        )
        snapshot = await select_authoritative_snapshot(
            session, instrument_id=instrument_id, as_of=authority.decision_time
        )
        has_price = await session.scalar(
            select(func.count())
            .select_from(PriceBarSetAuthority)
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
                PriceBarSetAuthority.created_at <= authority.decision_time,
                PriceBarRecord.instrument_id == instrument_id,
                PriceBarRecord.session == authority.decision_session,
                PriceBarRecord.available_at <= authority.decision_time,
                PriceBarRecord.created_at <= authority.decision_time,
                PriceSeriesManifest.created_at <= authority.decision_time,
                PriceSeriesManifest.source == "alpaca",
                PriceSeriesManifest.feed == "sip",
                PriceSeriesManifest.delivery_mode == "delayed",
                PriceSeriesManifest.adjustment == "all",
                PriceSeriesManifest.interval == "1Day",
            )
        )
        has_sector = await session.scalar(
            select(func.count())
            .select_from(SectorExposure)
            .where(
                SectorExposure.instrument_id == instrument_id,
                SectorExposure.as_of <= authority.decision_session,
                SectorExposure.reviewed.is_(True),
                SectorExposure.created_at <= authority.decision_time,
            )
        )
        if (
            instrument is None
            or instrument.created_at > authority.decision_time
            or instrument.currency != "USD"
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or instrument.asset_type not in {"ETF", "common_stock"}
            or member is None
            or len(aliases) != 1
            or snapshot is None
            or snapshot.source != "nexus_research"
            or snapshot.schema_version != "research_filings_v1"
            or snapshot.created_at > authority.decision_time
            or not has_price
            or not has_sector
        ):
            continue
        candidates.append(
            CandidateAuthority(
                instrument_id=instrument_id,
                symbol=aliases[0].symbol,
                inclusion_reason=member.inclusion_reason,
                research_snapshot_id=snapshot.id,
                paper_holding=instrument_id in holdings,
            )
        )
    manifest = select_candidate_manifest(tuple(candidates))
    if {row.instrument_id for row in manifest} | holdings != set(required):
        return ()
    return manifest

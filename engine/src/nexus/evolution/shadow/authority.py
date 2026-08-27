"""Point-in-time authority and balanced paper baseline for Strategy Shadow."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.canonical import canonical_decimal
from nexus.evolution.shadow.canonical import (
    initial_state_content_hash,
    session_evidence_content_hash,
)
from nexus.evolution.shadow.models import (
    INITIAL_NAV,
    StrategyShadowPositionV1,
    StrategyShadowSessionEvidenceV1,
    StrategyShadowStateV1,
)
from nexus.market.price_bar_authority import prepare_price_bar_set
from nexus.optimizer.cost_policy import COST_POLICY_ID, load_cost_policy_v1
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.data import (
    DataSnapshot,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import (
    Instrument,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionSnapshotRecord,
)
from nexus.quality.source_policy import SOURCE_POLICY_ID, load_source_policy_v1
from nexus.shadow.cost_policy import cost_model_from_policy
from nexus.strategy.baseline import BaselineInstrumentSnapshot, BaselineSectorExposure


class StrategyShadowAuthorityError(ValueError):
    """A required immutable Strategy Shadow authority is incomplete."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class BalancedPaperBaseline:
    snapshot_id: UUID
    account_id: UUID
    as_of: datetime
    snapshot_hash: str
    position_projection_hashes: tuple[str, ...]
    normalized_state: StrategyShadowStateV1
    initial_state_hash: str


@dataclass(frozen=True)
class LoadedStrategyShadowSession:
    record_id: UUID
    evidence: StrategyShadowSessionEvidenceV1
    maximum_available_at: datetime


def require_available_by(*, authority_id: UUID, available_at: datetime, cutoff: datetime) -> bool:
    """Return whether one immutable authority was visible at the exact cutoff."""
    if authority_id.int == 0:
        raise ValueError("authority identity cannot be nil")
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        raise ValueError("authority available_at must be timezone-aware")
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("authority cutoff must be timezone-aware")
    return available_at.astimezone(UTC) <= cutoff.astimezone(UTC)


async def load_balanced_paper_baseline(
    session: AsyncSession, *, now: datetime
) -> BalancedPaperBaseline:
    """Load the latest sealed internal-paper snapshot and normalize it to NAV 100."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise StrategyShadowAuthorityError("SHADOW_START_TIME_INVALID")
    snapshot = await session.scalar(
        select(PaperPerformanceSnapshotRecord)
        .join(
            PaperAccountRecord,
            PaperAccountRecord.id == PaperPerformanceSnapshotRecord.account_id,
        )
        .where(
            PaperAccountRecord.capability == "simulation_write_internal",
            PaperPerformanceSnapshotRecord.as_of <= now,
        )
        .order_by(
            PaperPerformanceSnapshotRecord.as_of.desc(),
            PaperPerformanceSnapshotRecord.id.desc(),
        )
        .limit(1)
    )
    if snapshot is None:
        raise StrategyShadowAuthorityError("PAPER_BASELINE_MISSING")
    latest_order_events = (
        select(
            PaperOrderEventRecord.order_id,
            func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
        )
        .where(PaperOrderEventRecord.occurred_at <= snapshot.as_of)
        .group_by(PaperOrderEventRecord.order_id)
        .subquery()
    )
    nonterminal_order = await session.scalar(
        select(PaperOrderRecord.id)
        .join(
            latest_order_events,
            latest_order_events.c.order_id == PaperOrderRecord.id,
        )
        .join(
            PaperOrderEventRecord,
            (PaperOrderEventRecord.order_id == latest_order_events.c.order_id)
            & (PaperOrderEventRecord.event_seq == latest_order_events.c.event_seq),
        )
        .where(
            PaperOrderRecord.account_id == snapshot.account_id,
            PaperOrderEventRecord.to_status.in_(
                (
                    "proposed",
                    "risk_approved",
                    "awaiting_approval",
                    "submitted",
                    "partially_filled",
                    "reconciling",
                )
            ),
        )
        .limit(1)
    )
    if nonterminal_order is not None:
        raise StrategyShadowAuthorityError("PAPER_BASELINE_NOT_SEALED")
    positions = tuple(
        await session.scalars(
            select(PaperPositionSnapshotRecord)
            .where(
                PaperPositionSnapshotRecord.account_id == snapshot.account_id,
                PaperPositionSnapshotRecord.as_of == snapshot.as_of,
            )
            .order_by(PaperPositionSnapshotRecord.instrument_id)
        )
    )
    if any(item.reserved_quantity != 0 for item in positions):
        raise StrategyShadowAuthorityError("PAPER_BASELINE_NOT_SEALED")
    reconstructed_nav = canonical_decimal(
        snapshot.cash
        + sum(
            (item.quantity * item.reference_close for item in positions),
            start=Decimal(0),
        )
    )
    if reconstructed_nav != canonical_decimal(snapshot.nav) or snapshot.nav <= 0:
        raise StrategyShadowAuthorityError("PAPER_BASELINE_UNBALANCED")
    scale = INITIAL_NAV / snapshot.nav
    normalized_positions = tuple(
        StrategyShadowPositionV1(
            instrument_id=item.instrument_id,
            quantity=canonical_decimal(item.quantity * scale),
            cost_basis=canonical_decimal(item.cost_basis * scale),
            realized_pnl=Decimal(0),
            cumulative_fees=Decimal(0),
            last_close=item.reference_close,
            untradable=False,
        )
        for item in positions
        if item.quantity > 0
    )
    state = StrategyShadowStateV1(
        cash=canonical_decimal(snapshot.cash * scale),
        nav=INITIAL_NAV,
        positions=normalized_positions,
        position_rank_states=(),
        hard_risk_violation=False,
    )
    projection_hashes = tuple(item.projection_hash for item in positions)
    state_hash = initial_state_content_hash(
        baseline_snapshot_id=snapshot.id,
        baseline_snapshot_hash=snapshot.content_hash,
        position_projection_hashes=projection_hashes,
        normalized_state=state,
    )
    return BalancedPaperBaseline(
        snapshot_id=snapshot.id,
        account_id=snapshot.account_id,
        as_of=snapshot.as_of,
        snapshot_hash=snapshot.content_hash,
        position_projection_hashes=projection_hashes,
        normalized_state=state,
        initial_state_hash=state_hash,
    )


async def load_strategy_shadow_session_evidence(
    session: AsyncSession,
    *,
    group_id: UUID,
    session_date: date,
    ordinal: int,
    calendar_authority_id: UUID,
    observed_at: datetime,
) -> LoadedStrategyShadowSession:
    """Load and revalidate one exact precomputed PIT strategy/session evidence set."""
    if observed_at.tzinfo is None or observed_at.utcoffset() != UTC.utcoffset(observed_at):
        raise StrategyShadowAuthorityError("SHADOW_OBSERVED_AT_INVALID")
    observed_at = observed_at.astimezone(UTC)
    calendar_member = await session.get(
        ExchangeCalendarSessionRecord,
        {
            "calendar_authority_id": calendar_authority_id,
            "session": session_date,
        },
    )
    if calendar_member is None or observed_at < calendar_member.decision_at:
        raise StrategyShadowAuthorityError("SHADOW_SESSION_NOT_DUE")
    rows = tuple(
        await session.scalars(
            select(DataSnapshot).where(
                DataSnapshot.schema_version == "strategy_shadow_evidence_v1",
                DataSnapshot.available_at <= observed_at,
            )
        )
    )
    matched: list[tuple[DataSnapshot, StrategyShadowSessionEvidenceV1]] = []
    for row in rows:
        if (
            row.payload.get("group_id") != str(group_id)
            or row.payload.get("session") != session_date.isoformat()
        ):
            continue
        try:
            evidence = StrategyShadowSessionEvidenceV1.model_validate(row.payload)
        except ValueError:
            continue
        if (
            evidence.group_id == group_id
            and evidence.session == session_date
            and evidence.session_ordinal == ordinal
        ):
            matched.append((row, evidence))
    content_hashes = {item.content_hash for _, item in matched}
    if len(content_hashes) > 1:
        raise StrategyShadowAuthorityError("SHADOW_SOURCE_CONFLICT")
    if len(matched) != 1:
        raise StrategyShadowAuthorityError("SHADOW_SESSION_EVIDENCE_MISSING")
    record, evidence = matched[0]
    if (
        evidence.content_hash != session_evidence_content_hash(evidence)
        or record.source != "internal"
        or record.revision != evidence.content_hash
        or record.observed_at > record.available_at
        or record.available_at > record.received_at
        or evidence.calendar_authority_id != calendar_authority_id
        or evidence.source_policy_id != SOURCE_POLICY_ID
        or evidence.cost_policy_id != COST_POLICY_ID
    ):
        raise StrategyShadowAuthorityError("SHADOW_SESSION_EVIDENCE_INVALID")
    await _validate_session_policies(session, evidence, observed_at)
    maximum = await _validate_strategy_inputs(session, evidence, observed_at)
    return LoadedStrategyShadowSession(
        record_id=record.id,
        evidence=evidence,
        maximum_available_at=max(maximum, record.available_at),
    )


async def _validate_session_policies(
    session: AsyncSession,
    evidence: StrategyShadowSessionEvidenceV1,
    observed_at: datetime,
) -> None:
    try:
        source = await load_source_policy_v1(session, instrument_tick_size=None)
        cost = await load_cost_policy_v1(session)
        cost_model_from_policy(cost, started_at=observed_at)
    except ValueError as error:
        raise StrategyShadowAuthorityError("SHADOW_POLICY_INVALID") from error
    if (
        source.record_id != evidence.source_policy_id
        or source.available_at > evidence.decision_time
    ):
        raise StrategyShadowAuthorityError("SHADOW_POLICY_INVALID")


async def _validate_strategy_inputs(
    session: AsyncSession,
    evidence: StrategyShadowSessionEvidenceV1,
    observed_at: datetime,
) -> datetime:
    universe = await session.get(UniverseSnapshot, evidence.universe_snapshot_id)
    if (
        universe is None
        or universe.effective_from > evidence.strategy_snapshot.decision_session
        or universe.published_at is None
        or universe.published_at > evidence.decision_time
    ):
        raise StrategyShadowAuthorityError("SHADOW_UNIVERSE_INVALID")
    member_ids = set(
        await session.scalars(
            select(UniverseMember.instrument_id).where(
                UniverseMember.universe_snapshot_id == universe.id
            )
        )
    )
    snapshot_ids = {item.instrument_id for item in evidence.strategy_snapshot.instruments}
    if member_ids != snapshot_ids or evidence.strategy_snapshot.current_positions:
        raise StrategyShadowAuthorityError("SHADOW_UNIVERSE_INVALID")
    available_times = [universe.published_at, evidence.strategy_available_at]
    bars_by_id = {item.instrument_id: item for item in evidence.execution_bars}
    for instrument_snapshot in evidence.strategy_snapshot.instruments:
        instrument = await session.get(Instrument, instrument_snapshot.instrument_id)
        bar_input = bars_by_id[instrument_snapshot.instrument_id]
        raw = await _load_price_set(session, bar_input.raw_price_authority_id, adjustment="raw")
        adjusted = await _load_price_set(
            session, bar_input.adjusted_price_authority_id, adjustment="adjusted"
        )
        raw_authority, raw_manifest, raw_rows = raw
        adjusted_authority, _, adjusted_rows = adjusted
        if (
            instrument is None
            or raw_authority.source_policy_id != SOURCE_POLICY_ID
            or adjusted_authority.source_policy_id != SOURCE_POLICY_ID
            or raw_authority.source != adjusted_authority.source
            or raw_authority.available_at > observed_at
            or adjusted_authority.available_at > evidence.decision_time
        ):
            raise StrategyShadowAuthorityError("SHADOW_PRICE_AUTHORITY_INVALID")
        current = [item for item in raw_rows if item.session == evidence.session]
        history_raw = tuple(
            item
            for item in raw_rows
            if item.session <= evidence.strategy_snapshot.decision_session
            and item.available_at <= evidence.decision_time
            and item.received_at <= evidence.decision_time
        )
        history_adjusted = tuple(
            item
            for item in adjusted_rows
            if item.session <= evidence.strategy_snapshot.decision_session
            and item.available_at <= evidence.decision_time
            and item.received_at <= evidence.decision_time
        )
        if (
            len(current) != 1
            or current[0].open != bar_input.open
            or current[0].close != bar_input.close
            or len(history_raw) < 200
            or len(history_adjusted) < 200
            or tuple(item.close for item in history_adjusted) != instrument_snapshot.closes
            or tuple(item.close * item.volume for item in history_raw)
            != instrument_snapshot.dollar_volumes
            or history_raw[-1].close != instrument_snapshot.raw_close
            or raw_manifest.instrument != str(instrument_snapshot.instrument_id)
            or _asset_type(instrument.asset_type) != instrument_snapshot.asset_type
        ):
            raise StrategyShadowAuthorityError("SHADOW_STRATEGY_DATA_MISMATCH")
        await _validate_sector_exposures(session, instrument_snapshot, evidence)
        available_times.extend((raw_authority.available_at, adjusted_authority.available_at))
    return max(available_times)


async def _load_price_set(
    session: AsyncSession, authority_id: UUID, *, adjustment: str
) -> tuple[PriceBarSetAuthority, PriceSeriesManifest, tuple[PriceBarRecord, ...]]:
    authority = await session.get(PriceBarSetAuthority, authority_id)
    manifest = (
        None
        if authority is None
        else await session.get(PriceSeriesManifest, authority.price_series_manifest_id)
    )
    if authority is None or manifest is None:
        raise StrategyShadowAuthorityError("SHADOW_PRICE_AUTHORITY_INVALID")
    prepared = await prepare_price_bar_set(session, manifest.id)
    expected_adjustment = manifest.adjustment != "raw"
    if (
        prepared is None
        or prepared.content_hash != authority.content_hash
        or prepared.available_at != authority.available_at
        or (adjustment == "raw") != (not expected_adjustment)
    ):
        raise StrategyShadowAuthorityError("SHADOW_PRICE_AUTHORITY_INVALID")
    rows = tuple(
        await session.scalars(
            select(PriceBarRecord)
            .where(PriceBarRecord.price_series_manifest_id == manifest.id)
            .order_by(PriceBarRecord.session)
        )
    )
    return authority, manifest, rows


async def _validate_sector_exposures(
    session: AsyncSession,
    instrument_snapshot: BaselineInstrumentSnapshot,
    evidence: StrategyShadowSessionEvidenceV1,
) -> None:
    rows = tuple(
        await session.scalars(
            select(SectorExposure)
            .where(
                SectorExposure.instrument_id == instrument_snapshot.instrument_id,
                SectorExposure.as_of <= evidence.strategy_snapshot.decision_session,
                SectorExposure.created_at <= evidence.decision_time,
                SectorExposure.reviewed.is_(True),
                SectorExposure.synthetic.is_(False),
            )
            .order_by(SectorExposure.as_of.desc(), SectorExposure.sector_code)
        )
    )
    if not rows:
        expected: tuple[BaselineSectorExposure, ...] = ()
    else:
        latest = rows[0].as_of
        expected = tuple(
            BaselineSectorExposure(sector=item.sector_code, weight=item.weight)
            for item in rows
            if item.as_of == latest
        )
    if expected != instrument_snapshot.sector_exposures:
        raise StrategyShadowAuthorityError("SHADOW_SECTOR_AUTHORITY_MISMATCH")


def _asset_type(value: str) -> str:
    return {
        "equity": "stock",
        "common_stock": "stock",
        "equity_etf": "equity_etf",
        "non_equity_etf": "non_equity_etf",
    }.get(value, value)

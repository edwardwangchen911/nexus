"""PostgreSQL fixtures for the isolated champion/challenger Strategy Shadow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from helpers.evolution_evaluation import SeededEvaluationPair, seed_evaluation_pair
from helpers.paper_batch import INSTRUMENTS
from helpers.paper_performance import seed_paper_performance
from helpers.shadow import seed_shadow_calendar
from nexus.domain.ids import new_uuid7
from nexus.evolution.evaluation import (
    persist_champion_comparison,
    persist_historical_promotion_decision,
)
from nexus.evolution.shadow.canonical import content_hash, session_evidence_content_hash
from nexus.evolution.shadow.metrics import calculate_strategy_shadow_metrics
from nexus.evolution.shadow.models import (
    CHECKPOINT_ORDINALS,
    INITIAL_NAV,
    StrategyShadowExecutionBarV1,
    StrategyShadowSessionEvidenceV1,
    StrategyShadowSessionPointV1,
    StrategyShadowStateV1,
)
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.paper.execution_service import cancel_internal_paper_order
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceSeriesManifest,
)
from nexus.persistence.models.evolution import (
    StrategyShadowCheckpointMetricRecord,
    StrategyShadowCheckpointRecord,
    StrategyShadowFillRecord,
    StrategyShadowGroupRecord,
    StrategyShadowLegRecord,
    StrategyShadowLegStateRecord,
    StrategyShadowOrderRecord,
    StrategyShadowSessionAuthorityRecord,
)
from nexus.persistence.models.market import SectorExposure, UniverseMember, UniverseSnapshot
from nexus.persistence.models.paper import (
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionSnapshotRecord,
)
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    BaselineSectorExposure,
    StrategyDataSnapshot,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

START_NOW = datetime(2026, 8, 28, 15, tzinfo=UTC)


@dataclass(frozen=True)
class StrategyShadowReadySeed:
    pair: SeededEvaluationPair
    calendar_id: UUID
    calendar_sessions: tuple[date, ...]
    baseline_snapshot_id: UUID
    now: datetime


@dataclass(frozen=True)
class StrategyShadowSessionEvidenceSeed:
    snapshot_id: UUID
    raw_authority_id: UUID
    adjusted_authority_id: UUID
    universe_snapshot_id: UUID
    observed_at: datetime


async def seed_strategy_shadow_ready(
    database_url: str,
    factory: async_sessionmaker[AsyncSession],
) -> StrategyShadowReadySeed:
    """Create one historical PASS experiment and one sealed balanced paper snapshot."""
    pair = await seed_evaluation_pair(factory)
    async with factory() as session, session.begin():
        comparison = await persist_champion_comparison(
            session,
            experiment_id=pair.experiment_id,
            champion_backtest_run_id=pair.champion_run_id,
            challenger_backtest_run_id=pair.challenger_run_id,
            now=START_NOW,
        )
    async with factory() as session, session.begin():
        await persist_historical_promotion_decision(
            session,
            comparison_id=comparison.id,
            promotion_policy_id=pair.promotion_policy_id,
            now=START_NOW,
        )

    performance = await seed_paper_performance(database_url)
    async with factory() as session, session.begin():
        await _cancel_remaining_open_orders(
            session,
            account_id=performance.execution.account_id,
            requested_at=performance.as_of - timedelta(seconds=1),
        )
    async with factory() as session, session.begin():
        publication = await publish_paper_performance_snapshot(
            session,
            account_id=performance.execution.account_id,
            as_of=performance.as_of,
        )
        calendar = await seed_shadow_calendar(
            session,
            database_time=START_NOW,
            market_date=START_NOW.date(),
        )
    return StrategyShadowReadySeed(
        pair=pair,
        calendar_id=calendar.calendar_id,
        calendar_sessions=calendar.sessions,
        baseline_snapshot_id=publication.snapshot_id,
        now=START_NOW,
    )


async def _cancel_remaining_open_orders(
    session: AsyncSession,
    *,
    account_id: UUID,
    requested_at: datetime,
) -> None:
    orders = tuple(
        await session.scalars(
            select(PaperOrderRecord)
            .where(PaperOrderRecord.account_id == account_id)
            .order_by(PaperOrderRecord.id)
        )
    )
    terminal = {"risk_rejected", "filled", "cancelled", "rejected"}
    for order in orders:
        latest = await session.scalar(
            select(PaperOrderEventRecord)
            .where(PaperOrderEventRecord.order_id == order.id)
            .order_by(PaperOrderEventRecord.event_seq.desc())
            .limit(1)
        )
        if latest is not None and latest.to_status not in terminal:
            await cancel_internal_paper_order(
                session,
                order_id=order.id,
                expected_event_seq=latest.event_seq,
                requested_at=requested_at,
                idempotency_key=f"strategy-shadow-seal-{order.id}",
            )


async def seed_strategy_shadow_session_evidence(
    session: AsyncSession,
    *,
    group_id: UUID,
    session_date: date,
    ordinal: int,
    observed_at: datetime,
) -> StrategyShadowSessionEvidenceSeed:
    """Freeze one exact 200-day decision snapshot plus its execution-session Bar."""
    group = await session.get(StrategyShadowGroupRecord, group_id)
    if group is None:
        raise RuntimeError("Strategy Shadow group fixture is missing")
    baseline = await session.get(PaperPerformanceSnapshotRecord, group.baseline_snapshot_id)
    if baseline is None:
        raise RuntimeError("Strategy Shadow baseline fixture is missing")
    positions = tuple(
        await session.scalars(
            select(PaperPositionSnapshotRecord)
            .where(
                PaperPositionSnapshotRecord.account_id == baseline.account_id,
                PaperPositionSnapshotRecord.as_of == baseline.as_of,
                PaperPositionSnapshotRecord.quantity > 0,
            )
            .order_by(PaperPositionSnapshotRecord.instrument_id)
        )
    )
    instrument_id = positions[0].instrument_id if positions else INSTRUMENTS[0][0]
    decision_session = session_date - timedelta(days=1)
    while decision_session.weekday() >= 5:
        decision_session -= timedelta(days=1)
    decision_time = datetime.combine(decision_session, time(21), tzinfo=UTC)
    strategy_available_at = decision_time - timedelta(seconds=1)
    history_sessions = _weekday_sessions_ending(decision_session, count=200)
    adjusted_closes = tuple(Decimal("100") + Decimal(index) / Decimal("10") for index in range(200))
    raw_closes = adjusted_closes
    volumes = tuple(Decimal("1000000") for _ in history_sessions)
    raw_authority_id = await _seed_price_authority(
        session,
        instrument_id=instrument_id,
        sessions=(*history_sessions, session_date),
        closes=(*raw_closes, raw_closes[-1] + Decimal("0.10")),
        volumes=(*volumes, Decimal("1000000")),
        adjustment="raw",
        historical_available_at=strategy_available_at,
        current_available_at=observed_at,
        fixture_key=f"strategy-shadow-raw-{group_id}-{ordinal}",
    )
    adjusted_authority_id = await _seed_price_authority(
        session,
        instrument_id=instrument_id,
        sessions=history_sessions,
        closes=adjusted_closes,
        volumes=volumes,
        adjustment="all",
        historical_available_at=strategy_available_at,
        current_available_at=strategy_available_at,
        fixture_key=f"strategy-shadow-adjusted-{group_id}-{ordinal}",
    )
    universe = UniverseSnapshot(
        id=new_uuid7(),
        key="strategy_shadow_fixture",
        version=f"{group_id}-{ordinal}",
        effective_from=decision_session,
        source="internal-fixture",
        content_hash=sha256(f"universe:{group_id}:{ordinal}".encode()).hexdigest(),
        published_at=None,
    )
    session.add(universe)
    await session.flush()
    session.add(
        UniverseMember(
            id=new_uuid7(),
            universe_snapshot_id=universe.id,
            instrument_id=instrument_id,
            inclusion_reason="strategy-shadow-test-authority",
        )
    )
    await session.flush()
    universe.published_at = strategy_available_at
    session.add(
        SectorExposure(
            id=new_uuid7(),
            instrument_id=instrument_id,
            sector_code="information_technology",
            weight=Decimal(1),
            as_of=decision_session,
            source_url=f"memory://strategy-shadow/sector/{group.id}/{ordinal}",
            content_hash=sha256(f"sector:{group.id}:{ordinal}".encode()).hexdigest(),
            synthetic=False,
            reviewed=True,
            created_at=strategy_available_at,
        )
    )
    strategy_snapshot = StrategyDataSnapshot(
        decision_session=decision_session,
        next_session=session_date,
        instruments=(
            BaselineInstrumentSnapshot(
                instrument_id=instrument_id,
                symbol="NEXUS",
                asset_type="stock",
                sector_exposures=(
                    BaselineSectorExposure(sector="information_technology", weight=Decimal(1)),
                ),
                raw_close=raw_closes[-1],
                closes=adjusted_closes,
                dollar_volumes=tuple(
                    close * volume for close, volume in zip(raw_closes, volumes, strict=True)
                ),
            ),
        ),
    )
    execution_close = raw_closes[-1] + Decimal("0.10")
    provisional = StrategyShadowSessionEvidenceV1(
        group_id=group.id,
        session=session_date,
        session_ordinal=ordinal,
        decision_time=decision_time,
        calendar_authority_id=group.calendar_authority_id,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=COST_POLICY_ID,
        universe_snapshot_id=universe.id,
        strategy_available_at=strategy_available_at,
        strategy_snapshot=strategy_snapshot,
        execution_bars=(
            StrategyShadowExecutionBarV1(
                instrument_id=instrument_id,
                raw_price_authority_id=raw_authority_id,
                adjusted_price_authority_id=adjusted_authority_id,
                open=execution_close,
                close=execution_close,
            ),
        ),
        content_hash="0" * 64,
    )
    evidence = provisional.model_copy(
        update={"content_hash": session_evidence_content_hash(provisional)}
    )
    snapshot = DataSnapshot(
        id=new_uuid7(),
        source="internal",
        provider_object_id=f"strategy-shadow-session:{group.id}:{ordinal}",
        revision=evidence.content_hash,
        observed_at=observed_at - timedelta(seconds=2),
        available_at=observed_at - timedelta(seconds=1),
        received_at=observed_at,
        schema_version=evidence.schema_version,
        payload=evidence.model_dump(mode="json"),
    )
    session.add(snapshot)
    await session.flush()
    return StrategyShadowSessionEvidenceSeed(
        snapshot_id=snapshot.id,
        raw_authority_id=raw_authority_id,
        adjusted_authority_id=adjusted_authority_id,
        universe_snapshot_id=universe.id,
        observed_at=observed_at,
    )


async def seed_complete_strategy_shadow_history(
    session: AsyncSession,
    *,
    group_id: UUID,
    calendar_sessions: tuple[date, ...],
    champion_final_nav: Decimal = Decimal("105"),
    challenger_final_nav: Decimal = Decimal("106"),
    challenger_full_fills: int = 63,
    challenger_risk_ordinal: int | None = None,
) -> None:
    """Persist a compact, internally consistent 63-session finalization fixture."""
    group = await session.get(StrategyShadowGroupRecord, group_id)
    if group is None:
        raise RuntimeError("Strategy Shadow group fixture is missing")
    legs = tuple(
        await session.scalars(
            select(StrategyShadowLegRecord)
            .where(StrategyShadowLegRecord.group_id == group.id)
            .order_by(StrategyShadowLegRecord.role)
        )
    )
    if len(legs) != 2:
        raise RuntimeError("Strategy Shadow leg fixture is incomplete")
    evaluation_sessions = calendar_sessions[:63]
    if len(evaluation_sessions) != 63:
        raise ValueError("Strategy Shadow finalization fixture requires 63 sessions")
    points: dict[UUID, list[StrategyShadowSessionPointV1]] = {leg.id: [] for leg in legs}
    for ordinal, session_date in enumerate(evaluation_sessions, start=1):
        observed_at = datetime.combine(session_date, time(23), tzinfo=UTC)
        authority_hash = content_hash(
            {
                "schema_version": "strategy_shadow_final_fixture_authority_v1",
                "group_id": str(group.id),
                "session": session_date.isoformat(),
                "ordinal": ordinal,
            }
        )
        authority = StrategyShadowSessionAuthorityRecord(
            id=new_uuid7(),
            group_id=group.id,
            session=session_date,
            session_ordinal=ordinal,
            calendar_authority_id=group.calendar_authority_id,
            source_policy_id=SOURCE_POLICY_ID,
            cost_policy_id=COST_POLICY_ID,
            authority_hash=authority_hash,
            available_at=observed_at,
            created_at=observed_at,
        )
        session.add(authority)
        await session.flush()
        for leg in legs:
            final_nav = champion_final_nav if leg.role == "champion" else challenger_final_nav
            nav = INITIAL_NAV + (final_nav - INITIAL_NAV) * Decimal(ordinal) / Decimal(63)
            risk = leg.role == "challenger" and challenger_risk_ordinal == ordinal
            state = StrategyShadowStateV1(
                cash=nav,
                nav=nav,
                positions=(),
                position_rank_states=(),
                hard_risk_violation=risk,
            )
            state_hash = content_hash(state)
            state_record = StrategyShadowLegStateRecord(
                id=new_uuid7(),
                leg_id=leg.id,
                session_authority_id=authority.id,
                session_ordinal=ordinal,
                cash=state.cash,
                nav=state.nav,
                positions=state.model_dump(mode="json"),
                hard_risk_violation=state.hard_risk_violation,
                state_hash=state_hash,
                transition_hash=content_hash(
                    {
                        "schema_version": "strategy_shadow_final_fixture_transition_v1",
                        "leg_id": str(leg.id),
                        "authority_hash": authority_hash,
                        "state_hash": state_hash,
                    }
                ),
                created_at=observed_at,
            )
            session.add(state_record)
            await session.flush()
            filled = leg.role == "challenger" and ordinal <= challenger_full_fills
            if filled:
                order_hash = content_hash(
                    {
                        "schema_version": "strategy_shadow_final_fixture_order_v1",
                        "state_id": str(state_record.id),
                    }
                )
                order = StrategyShadowOrderRecord(
                    id=new_uuid7(),
                    leg_state_id=state_record.id,
                    instrument_id=INSTRUMENTS[0][0],
                    side="BUY",
                    quantity=Decimal(1),
                    status="FILLED",
                    reason_code="STRATEGY_SHADOW_FULL_FILL",
                    order_hash=order_hash,
                    created_at=observed_at,
                )
                session.add(order)
                await session.flush()
                session.add(
                    StrategyShadowFillRecord(
                        id=new_uuid7(),
                        order_id=order.id,
                        quantity=Decimal(1),
                        price=Decimal(1),
                        fees=Decimal(0),
                        filled_at=observed_at,
                        fill_hash=content_hash(
                            {
                                "schema_version": "strategy_shadow_final_fixture_fill_v1",
                                "order_hash": order_hash,
                            }
                        ),
                        created_at=observed_at,
                    )
                )
            previous_nav = INITIAL_NAV if ordinal == 1 else points[leg.id][-1].nav
            points[leg.id].append(
                StrategyShadowSessionPointV1(
                    session=session_date,
                    ordinal=ordinal,
                    nav=state.nav,
                    turnover=Decimal(1) / previous_nav if filled else Decimal(0),
                    fees=Decimal(0),
                    filled_order_count=1 if filled else 0,
                    hard_risk_violation=risk,
                    authority_hash=authority_hash,
                )
            )
    await session.flush()
    for checkpoint_ordinal in CHECKPOINT_ORDINALS:
        checkpoint_hash = content_hash(
            {
                "schema_version": "strategy_shadow_final_fixture_checkpoint_v1",
                "group_id": str(group.id),
                "ordinal": checkpoint_ordinal,
            }
        )
        checkpoint = StrategyShadowCheckpointRecord(
            id=new_uuid7(),
            group_id=group.id,
            checkpoint_ordinal=checkpoint_ordinal,
            session=evaluation_sessions[checkpoint_ordinal - 1],
            authority_hash=checkpoint_hash,
            created_at=datetime.combine(
                evaluation_sessions[checkpoint_ordinal - 1], time(23), tzinfo=UTC
            ),
        )
        session.add(checkpoint)
        await session.flush()
        for leg in legs:
            metric = calculate_strategy_shadow_metrics(
                tuple(points[leg.id][:checkpoint_ordinal]),
                checkpoint_ordinal=checkpoint_ordinal,
            )
            session.add(
                StrategyShadowCheckpointMetricRecord(
                    id=new_uuid7(),
                    checkpoint_id=checkpoint.id,
                    leg_id=leg.id,
                    metric_report=metric.model_dump(mode="json"),
                    metric_hash=metric.metrics_hash,
                    created_at=checkpoint.created_at,
                )
            )


def _weekday_sessions_ending(end: date, *, count: int) -> tuple[date, ...]:
    sessions: list[date] = []
    cursor = end
    while len(sessions) < count:
        if cursor.weekday() < 5:
            sessions.append(cursor)
        cursor -= timedelta(days=1)
    return tuple(reversed(sessions))


async def _seed_price_authority(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    sessions: tuple[date, ...],
    closes: tuple[Decimal, ...],
    volumes: tuple[Decimal, ...],
    adjustment: str,
    historical_available_at: datetime,
    current_available_at: datetime,
    fixture_key: str,
) -> UUID:
    if not (len(sessions) == len(closes) == len(volumes)):
        raise ValueError("Strategy Shadow Bar fixture lengths must match")
    manifest_id = new_uuid7()
    artifact_id = new_uuid7()
    digest = sha256(fixture_key.encode()).hexdigest()
    first_observed = datetime.combine(sessions[0], time(20), tzinfo=UTC)
    last_observed = datetime.combine(sessions[-1], time(20), tzinfo=UTC)
    session.add(
        DataArtifact(
            id=artifact_id,
            uri=f"memory://strategy-shadow/{digest}",
            sha256=digest,
            byte_size=len(sessions) * 128,
            mime_type="application/vnd.apache.parquet",
            schema_version="price_bars_v1",
            source="fixture",
            observed_at=first_observed,
            available_at=current_available_at,
            received_at=current_available_at,
        )
    )
    manifest = PriceSeriesManifest(
        id=manifest_id,
        evidence_ref_id=None,
        data_artifact_id=artifact_id,
        instrument=str(instrument_id),
        source="fixture",
        feed="fixture",
        delivery_mode="fixture",
        adjustment=adjustment,
        interval="1Day",
        start_at=first_observed,
        end_at=last_observed,
        row_count=len(sessions),
    )
    session.add(manifest)
    await session.flush()
    last_index = len(sessions) - 1
    session.add_all(
        PriceBarRecord(
            id=new_uuid7(),
            price_series_manifest_id=manifest.id,
            instrument_id=instrument_id,
            session=session_date,
            open=close,
            high=close,
            low=close,
            close=close,
            volume=volume,
            observed_at=datetime.combine(session_date, time(20), tzinfo=UTC),
            available_at=(current_available_at if index == last_index else historical_available_at),
            received_at=(current_available_at if index == last_index else historical_available_at),
            revision="v1",
            quality_flags=[],
        )
        for index, (session_date, close, volume) in enumerate(
            zip(sessions, closes, volumes, strict=True)
        )
    )
    await session.flush()
    finalized = await finalize_price_bar_set(session, manifest)
    if finalized is None:
        raise RuntimeError("Strategy Shadow Bar fixture did not finalize")
    return finalized[0].id

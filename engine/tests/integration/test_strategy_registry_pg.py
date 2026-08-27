"""Pending-boundary internal-paper promotion and registry control events."""

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.evolution_registry import seed_promotable_registry
from helpers.paper_batch import INSTRUMENTS
from nexus.evolution.handover import HandoverEvaluationV1, evaluate_handover
from nexus.evolution.registry import (
    RegistryServiceError,
    activate_pending_transition,
    pause_auto_promotion,
    persist_handover_promotion_decision,
    promote_internal_paper_champion,
    resume_auto_promotion,
)
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
)
from nexus.persistence.session import session_factory
from nexus.risk.policy import RiskPolicy
from nexus.settings import EngineSettings
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    BaselineSectorExposure,
    StrategyDataSnapshot,
)
from nexus.strategy.forward import evaluate_registered_forward_strategy_decision
from sqlalchemy import func, select, text


async def _protected_counts(session) -> tuple[int, ...]:
    counts: list[int] = []
    for table in (
        "paper_orders",
        "paper_order_events",
        "paper_fills",
        "broker_order_snapshots",
        "broker_sync_requests",
    ):
        counts.append(int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0))
    return tuple(counts)


@pytest.mark.asyncio
async def test_pause_resume_promote_and_safe_boundary_activation_are_append_only(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_promotable_registry(migrated_database_url, factory)
    async with factory() as session:
        before = await _protected_counts(session)
    async with factory() as session, session.begin():
        paused = await pause_auto_promotion(
            session,
            expected_generation=seed.expected_generation,
            expected_version=seed.expected_version,
            idempotency_key="registry-pause-fixture",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        blocked = await promote_internal_paper_champion(
            session,
            experiment_id=seed.shadow.pair.experiment_id,
            expected_generation=paused.active_generation,
            expected_version=paused.version,
            effective_session=seed.effective_session,
            idempotency_key="registry-blocked-promotion-fixture",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        resumed = await resume_auto_promotion(
            session,
            expected_generation=paused.active_generation,
            expected_version=paused.version,
            idempotency_key="registry-resume-fixture",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        pending = await promote_internal_paper_champion(
            session,
            experiment_id=seed.shadow.pair.experiment_id,
            expected_generation=resumed.active_generation,
            expected_version=resumed.version,
            effective_session=seed.effective_session,
            idempotency_key="registry-promote-fixture",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        paused_pending = await pause_auto_promotion(
            session,
            expected_generation=pending.active_generation,
            expected_version=pending.version,
            idempotency_key="registry-pause-pending-fixture",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        early = await activate_pending_transition(
            session,
            expected_generation=paused_pending.active_generation,
            expected_version=paused_pending.version,
            decision_session=seed.shadow.calendar_sessions[62],
            now=seed.now,
        )
    async with factory() as session, session.begin():
        effective = await activate_pending_transition(
            session,
            expected_generation=early.active_generation,
            expected_version=early.version,
            decision_session=seed.effective_session,
            now=seed.now,
        )
    with pytest.raises(RegistryServiceError, match="REGISTRY_IDEMPOTENCY_CONFLICT"):
        async with factory() as session, session.begin():
            await pause_auto_promotion(
                session,
                expected_generation=effective.active_generation,
                expected_version=effective.version,
                idempotency_key="registry-promote-fixture",
                now=seed.now,
            )
    async with factory() as session, session.begin():
        replay = await promote_internal_paper_champion(
            session,
            experiment_id=seed.shadow.pair.experiment_id,
            expected_generation=seed.expected_generation,
            expected_version=resumed.version,
            effective_session=seed.effective_session,
            idempotency_key="registry-promote-fixture",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        alternate_replay = await promote_internal_paper_champion(
            session,
            experiment_id=seed.shadow.pair.experiment_id,
            expected_generation=effective.active_generation,
            expected_version=effective.version,
            effective_session=seed.effective_session,
            idempotency_key="registry-promote-alternate-replay-fixture",
            now=seed.now,
        )
    async with factory() as session:
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        projection = await session.get(
            StrategyExperimentStateProjectionRecord, seed.shadow.pair.family_id
        )
        event_count = int(
            await session.scalar(select(func.count()).select_from(StrategyRegistryEventRecord)) or 0
        )
        after = await _protected_counts(session)

    assert paused.status == "PAUSED" and paused.auto_promotion_paused is True
    assert blocked.status == "PAUSED"
    assert resumed.status == "EFFECTIVE" and resumed.auto_promotion_paused is False
    assert pending.status == "PENDING_BOUNDARY"
    assert pending.active_definition_id == seed.active_definition_id
    assert paused_pending.status == "PAUSED"
    assert early.status == "PENDING_BOUNDARY"
    assert effective.status == "EFFECTIVE"
    assert effective.registry_event_id == pending.registry_event_id
    assert effective.active_definition_id == seed.challenger_definition_id
    assert effective.active_generation == seed.expected_generation + 1
    assert replay.status == "EFFECTIVE"
    assert replay.registry_event_id == effective.registry_event_id
    assert alternate_replay.status == "EFFECTIVE"
    assert alternate_replay.registry_event_id == effective.registry_event_id
    assert head is not None and head.pending_event_id is None
    assert projection is not None and projection.current_state == "PROMOTED"
    assert event_count == 5
    assert after == before


@pytest.mark.asyncio
async def test_forward_decision_binds_activation_event_not_later_control_event(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_promotable_registry(migrated_database_url, factory)
    async with factory() as session, session.begin():
        pending = await promote_internal_paper_champion(
            session,
            experiment_id=seed.shadow.pair.experiment_id,
            expected_generation=seed.expected_generation,
            expected_version=seed.expected_version,
            effective_session=seed.effective_session,
            idempotency_key="registry-forward-promotion-fixture",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        effective = await activate_pending_transition(
            session,
            expected_generation=pending.active_generation,
            expected_version=pending.version,
            decision_session=seed.effective_session,
            now=seed.now,
        )

    closes = tuple(Decimal(100) + Decimal(index) / Decimal(10) for index in range(200))
    snapshot = StrategyDataSnapshot(
        decision_session=seed.effective_session,
        next_session=None,
        instruments=(
            BaselineInstrumentSnapshot(
                instrument_id=INSTRUMENTS[0][0],
                symbol="NEXUS",
                asset_type="stock",
                sector_exposures=(
                    BaselineSectorExposure(
                        sector="information_technology",
                        weight=Decimal(1),
                    ),
                ),
                raw_close=closes[-1],
                closes=closes,
                dollar_volumes=tuple(Decimal("100000000") for _ in closes),
            ),
        ),
    )
    decision_time = datetime.combine(seed.effective_session, time(23), tzinfo=UTC)
    inputs = {
        "snapshot": snapshot,
        "account_id": UUID(int=101),
        "portfolio_id": UUID(int=102),
        "decision_time": decision_time,
        "execution_session": seed.effective_session + timedelta(days=1),
        "source_policy_id": UUID(int=103),
        "calendar_authority_id": UUID(int=104),
        "universe_snapshot_id": UUID(int=105),
        "quality_report_id": UUID(int=106),
        "portfolio_projection_hash": "a" * 64,
        "authority_hash": "b" * 64,
    }
    async with factory() as session:
        first = await evaluate_registered_forward_strategy_decision(session, **inputs)
    assert first is not None
    assert first.strategy_definition_id == seed.challenger_definition_id
    assert first.registry_event_id == effective.registry_event_id
    assert first.champion_generation == seed.expected_generation + 1

    async with factory() as session, session.begin():
        paused = await pause_auto_promotion(
            session,
            expected_generation=effective.active_generation,
            expected_version=effective.version,
            idempotency_key="registry-forward-pause-fixture",
            now=decision_time + timedelta(seconds=1),
        )
    async with factory() as session:
        frozen = await evaluate_registered_forward_strategy_decision(session, **inputs)

    assert paused.registry_event_id != effective.registry_event_id
    assert frozen == first


@pytest.mark.asyncio
async def test_handover_persistence_rejects_real_snapshot_id_with_forged_weights(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_promotable_registry(migrated_database_url, factory)
    async with factory() as session:
        comparison = await session.scalar(
            select(StrategyChampionComparisonRecord).where(
                StrategyChampionComparisonRecord.experiment_id == seed.shadow.pair.experiment_id
            )
        )
        assert comparison is not None
        stored = await session.scalar(
            select(StrategyPromotionDecisionRecord).where(
                StrategyPromotionDecisionRecord.comparison_id == comparison.id,
                StrategyPromotionDecisionRecord.stage == "handover",
            )
        )
    assert stored is not None
    original = HandoverEvaluationV1.model_validate(stored.gate_report)
    position = original.snapshot.instruments[0]
    delta = Decimal("0.001")
    forged_snapshot = original.snapshot.model_copy(
        update={
            "current_cash_weight": original.snapshot.current_cash_weight + delta,
            "instruments": (
                position.model_copy(update={"current_weight": position.current_weight - delta}),
            ),
        }
    )
    forged = evaluate_handover(
        forged_snapshot,
        original.challenger,
        original.authority,
        RiskPolicy.v1(),
    )

    with pytest.raises(RegistryServiceError, match="HANDOVER_AUTHORITY_INVALID"):
        async with factory() as session, session.begin():
            await persist_handover_promotion_decision(
                session,
                comparison_id=comparison.id,
                promotion_policy_id=stored.promotion_policy_id,
                evaluation=forged,
                now=seed.now,
            )

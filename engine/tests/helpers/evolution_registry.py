"""Promotable Strategy Shadow and handover authorities for registry tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from uuid import UUID

from nexus.evolution.handover import (
    HandoverAuthorityV1,
    HandoverChallengerV1,
    HandoverInstrumentV1,
    HandoverSectorExposureV1,
    HandoverSnapshotV1,
    HandoverTargetV1,
    evaluate_handover,
)
from nexus.evolution.registry import persist_handover_promotion_decision
from nexus.evolution.shadow.authority import load_balanced_paper_baseline
from nexus.evolution.shadow.service import finalize_strategy_shadow, start_strategy_shadow
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyRegistryHeadRecord,
    StrategyShadowFinalizationRecord,
)
from nexus.persistence.models.paper import RiskPolicyRecord
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.risk.policy import RiskPolicy
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from helpers.evolution_shadow import (
    StrategyShadowReadySeed,
    seed_complete_strategy_shadow_history,
    seed_strategy_shadow_ready,
)
from helpers.paper_batch import INSTRUMENTS


@dataclass(frozen=True)
class PromotableRegistrySeed:
    shadow: StrategyShadowReadySeed
    group_id: UUID
    active_definition_id: UUID
    challenger_definition_id: UUID
    expected_generation: int
    expected_version: int
    effective_session: date
    now: datetime


async def seed_promotable_registry(
    database_url: str,
    factory: async_sessionmaker[AsyncSession],
) -> PromotableRegistrySeed:
    """Create complete Shadow PASS plus a PASS handover decision."""
    shadow = await seed_strategy_shadow_ready(database_url, factory)
    async with factory() as session, session.begin():
        started = await start_strategy_shadow(
            session,
            experiment_id=shadow.pair.experiment_id,
            now=shadow.now,
        )
    async with factory() as session, session.begin():
        await seed_complete_strategy_shadow_history(
            session,
            group_id=started.group_id,
            calendar_sessions=shadow.calendar_sessions,
        )
    finalized_at = datetime.combine(shadow.calendar_sessions[62], time(23), tzinfo=UTC)
    async with factory() as session, session.begin():
        await finalize_strategy_shadow(
            session,
            group_id=started.group_id,
            now=finalized_at,
        )
    async with factory() as session, session.begin():
        comparison = await session.scalar(
            select(StrategyChampionComparisonRecord).where(
                StrategyChampionComparisonRecord.experiment_id == shadow.pair.experiment_id
            )
        )
        risk_record = await session.scalar(
            select(RiskPolicyRecord).where(RiskPolicyRecord.active.is_(True))
        )
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        finalization = await session.scalar(
            select(StrategyShadowFinalizationRecord).where(
                StrategyShadowFinalizationRecord.group_id == started.group_id
            )
        )
        assert (
            comparison is not None
            and risk_record is not None
            and head is not None
            and finalization is not None
        )
        baseline = await load_balanced_paper_baseline(session, now=finalized_at)
        state = baseline.normalized_state
        snapshot_instruments = tuple(
            HandoverInstrumentV1(
                instrument_id=position.instrument_id,
                current_weight=position.quantity * position.last_close / state.nav,
                market="US",
                currency="USD",
                asset_type="common_stock",
                reference_close=position.last_close,
                adv20_usd=Decimal("100000000"),
                available_at=finalized_at,
                blocked=False,
                sector_exposures=(
                    HandoverSectorExposureV1(
                        sector="information_technology",
                        weight=Decimal(1),
                    ),
                ),
            )
            for position in state.positions
            if position.quantity > 0 and position.last_close is not None
        )
        evaluation = evaluate_handover(
            HandoverSnapshotV1(
                paper_snapshot_id=baseline.snapshot_id,
                nav=state.nav,
                current_cash_weight=state.cash / state.nav,
                drawdown_state="unlocked",
                instruments=snapshot_instruments,
                shadow_excess_return=Decimal("0.01"),
            ),
            HandoverChallengerV1(
                strategy_definition_id=shadow.pair.challenger_definition_id,
                target_cash_weight=Decimal("0.875"),
                targets=(
                    HandoverTargetV1(
                        instrument_id=INSTRUMENTS[0][0],
                        weight=Decimal("0.125"),
                    ),
                ),
            ),
            HandoverAuthorityV1(
                source_policy_id=SOURCE_POLICY_ID,
                cost_policy_id=COST_POLICY_ID,
                risk_policy_id=risk_record.id,
                decision_time=finalized_at,
                available_at=finalized_at,
                estimated_cost_rate=Decimal(0),
                complete=True,
                authority_hash=finalization.final_authority_hash,
            ),
            RiskPolicy.v1(),
        )
        assert evaluation.status == "PASS"
        await persist_handover_promotion_decision(
            session,
            comparison_id=comparison.id,
            promotion_policy_id=shadow.pair.promotion_policy_id,
            evaluation=evaluation,
            now=finalized_at,
        )
        expected_generation = head.active_generation
        expected_version = head.version
        active_definition_id = head.active_definition_id
    return PromotableRegistrySeed(
        shadow=shadow,
        group_id=started.group_id,
        active_definition_id=active_definition_id,
        challenger_definition_id=shadow.pair.challenger_definition_id,
        expected_generation=expected_generation,
        expected_version=expected_version,
        effective_session=shadow.calendar_sessions[63],
        now=finalized_at,
    )

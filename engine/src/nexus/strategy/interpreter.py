"""Pure deterministic evaluation of verified restricted strategy documents."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from functools import lru_cache
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    HeldRankObservation,
    PositionRankState,
    RankObservedDecision,
    StrategyDataSnapshot,
    StrategyDecision,
    StrategyTarget,
    _record_held_rank,
    _record_no_weekly_evaluation,
)
from nexus.strategy.dsl import (
    CompiledStrategy,
    ExpressionNode,
    StrategyDslError,
    baseline_feature_registry,
    evolution_feature_registry_v1,
    verify_compiled_strategy,
)


class CompiledStrategyEvaluationError(ValueError):
    """A compiled strategy cannot be evaluated inside the Phase 2 boundary."""


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CompiledStrategyTarget(_FrozenModel):
    instrument_id: UUID
    weight: Decimal
    rank: int | None
    ranking_value: Decimal


class CompiledStrategyDecision(_FrozenModel):
    strategy_version: str
    decision_session: date
    targets: tuple[CompiledStrategyTarget, ...]
    exits: tuple[UUID, ...]
    position_rank_states: tuple[PositionRankState, ...] = ()
    cash_weight: Decimal | None
    reason: Literal["WEEKLY_REBALANCE", "NOT_WEEK_END"]

    def as_legacy_baseline(self) -> StrategyDecision:
        if self.strategy_version != "baseline_v1":
            raise CompiledStrategyEvaluationError("LEGACY_BASELINE_CONVERSION_FORBIDDEN")
        return StrategyDecision(
            decision_session=self.decision_session,
            targets=tuple(
                StrategyTarget(
                    instrument_id=target.instrument_id,
                    weight=target.weight,
                    rank=target.rank,
                    momentum_126_skip5=target.ranking_value,
                )
                for target in self.targets
            ),
            exits=self.exits,
            position_rank_states=self.position_rank_states,
            cash_weight=self.cash_weight,
            reason=self.reason,
        )


def _feature_values(instrument: BaselineInstrumentSnapshot) -> dict[str, Decimal]:
    values = {"raw_close": instrument.raw_close}
    closes = instrument.closes
    if closes:
        values["adjusted_close"] = closes[-1]
    if len(instrument.dollar_volumes) >= 20:
        values["adv20_usd"] = sum(
            instrument.dollar_volumes[-20:], start=Decimal(0)
        ) / Decimal(20)
    if len(closes) >= 200:
        values["sma200"] = sum(closes[-200:], start=Decimal(0)) / Decimal(200)
    if len(closes) >= 127 and closes[-127] > 0:
        values["return_126"] = closes[-1] / closes[-127] - Decimal(1)
        values["momentum_126_skip5"] = closes[-6] / closes[-127] - Decimal(1)
    return values


def _effective_comparison_value(
    node: ExpressionNode, parameter_overrides: Mapping[str, Decimal]
) -> Decimal | None:
    if node.feature == "adv20_usd" and "min_adv20_usd" in parameter_overrides:
        return parameter_overrides["min_adv20_usd"]
    if node.feature == "raw_close" and "min_close" in parameter_overrides:
        return parameter_overrides["min_close"]
    return node.value


def _evaluate_expression(
    node: ExpressionNode,
    values: Mapping[str, Decimal],
    parameter_overrides: Mapping[str, Decimal],
) -> bool | None:
    if node.offset != 0:
        raise CompiledStrategyEvaluationError("OFFSET_NOT_SUPPORTED")
    if node.op in {"all", "any"}:
        results = tuple(
            _evaluate_expression(child, values, parameter_overrides) for child in node.args
        )
        if node.op == "all":
            if False in results:
                return False
            return True if all(item is True for item in results) else None
        if True in results:
            return True
        return False if all(item is False for item in results) else None
    if node.op == "not":
        if node.arg is None:
            raise CompiledStrategyEvaluationError("COMPILED_NODE_INVALID")
        result = _evaluate_expression(node.arg, values, parameter_overrides)
        return None if result is None else not result
    if node.feature is None or node.feature not in values:
        return None
    left = values[node.feature]
    right = (
        values.get(node.right_feature)
        if node.right_feature is not None
        else _effective_comparison_value(node, parameter_overrides)
    )
    if right is None:
        return None
    operations = {
        "gt": left > right,
        "gte": left >= right,
        "lt": left < right,
        "lte": left <= right,
        "eq": left == right,
    }
    try:
        return operations[node.op]
    except KeyError as error:
        raise CompiledStrategyEvaluationError("COMPILED_OPERATOR_INVALID") from error


@lru_cache(maxsize=256)
def _verify_strategy(strategy: CompiledStrategy) -> None:
    """Verify one immutable compiled document once per bounded process cache."""
    registry = (
        baseline_feature_registry()
        if strategy.strategy_version == "baseline_v1"
        else evolution_feature_registry_v1()
    )
    try:
        verify_compiled_strategy(strategy, registry)
    except StrategyDslError as error:
        raise CompiledStrategyEvaluationError(str(error)) from error


def evaluate_compiled_strategy(
    strategy: CompiledStrategy,
    snapshot: StrategyDataSnapshot,
    decision_session: date,
    *,
    _parameter_overrides: Mapping[str, Decimal] | None = None,
) -> CompiledStrategyDecision:
    """Evaluate with the unchanged serialized decision contract."""
    return _evaluate_compiled_strategy(
        strategy, snapshot, decision_session, _parameter_overrides=_parameter_overrides
    )


def evaluate_compiled_strategy_with_rank_observations(
    strategy: CompiledStrategy,
    snapshot: StrategyDataSnapshot,
    decision_session: date,
    *,
    _parameter_overrides: Mapping[str, Decimal] | None = None,
) -> RankObservedDecision[CompiledStrategyDecision]:
    """Observe actual held inputs without changing target or exit calculations."""
    observations: list[HeldRankObservation] = []
    decision = _evaluate_compiled_strategy(
        strategy,
        snapshot,
        decision_session,
        _parameter_overrides=_parameter_overrides,
        _observations=observations,
    )
    return RankObservedDecision(
        decision, tuple(sorted(observations, key=lambda item: item.instrument_id.int))
    )


def _evaluate_compiled_strategy(
    strategy: CompiledStrategy,
    snapshot: StrategyDataSnapshot,
    decision_session: date,
    *,
    _parameter_overrides: Mapping[str, Decimal] | None = None,
    _observations: list[HeldRankObservation] | None = None,
) -> CompiledStrategyDecision:
    """Evaluate one verified document without dynamic code, I/O, or historical model calls."""
    _verify_strategy(strategy)
    if snapshot.decision_session != decision_session:
        raise CompiledStrategyEvaluationError("DECISION_SESSION_MISMATCH")
    overrides = dict(_parameter_overrides or {})
    if not set(overrides) <= {"min_adv20_usd", "min_close", "target_weight"}:
        raise CompiledStrategyEvaluationError("PARAMETER_OVERRIDE_FORBIDDEN")
    next_session = snapshot.next_session
    if (
        next_session is not None
        and next_session.isocalendar()[:2] == decision_session.isocalendar()[:2]
    ):
        _record_no_weekly_evaluation(_observations, snapshot)
        return CompiledStrategyDecision(
            strategy_version=strategy.strategy_version,
            decision_session=decision_session,
            targets=(),
            exits=(),
            position_rank_states=snapshot.position_rank_states,
            cash_weight=None,
            reason="NOT_WEEK_END",
        )

    target_weight = overrides.get("target_weight", strategy.target_weight.weight)
    ranking_values: dict[UUID, Decimal] = {}
    feature_values: dict[UUID, dict[str, Decimal]] = {}
    trend_broken: set[UUID] = set()
    ranked: list[tuple[Decimal, BaselineInstrumentSnapshot]] = []
    for instrument in snapshot.instruments:
        values = _feature_values(instrument)
        feature_values[instrument.instrument_id] = values
        ranking_value = values.get(strategy.ranking.feature)
        if ranking_value is None:
            continue
        ranking_values[instrument.instrument_id] = ranking_value
        if _evaluate_expression(strategy.exits.trend_rule, values, overrides) is not True:
            trend_broken.add(instrument.instrument_id)
        if _evaluate_expression(strategy.root, values, overrides) is True:
            ranked.append((ranking_value, instrument))
    ranked.sort(
        key=(
            (lambda item: (-item[0], item[1].instrument_id.int))
            if strategy.ranking.direction == "desc"
            else (lambda item: (item[0], item[1].instrument_id.int))
        )
    )
    rank_by_instrument = {
        instrument.instrument_id: rank for rank, (_, instrument) in enumerate(ranked, start=1)
    }
    instrument_by_id = {instrument.instrument_id: instrument for instrument in snapshot.instruments}
    previous_streaks = {
        state.instrument_id: state.consecutive_outside_top20_weeks
        for state in snapshot.position_rank_states
    }
    selected: list[tuple[Decimal, BaselineInstrumentSnapshot]] = []
    etf_count = 0
    stocks_by_sector: dict[str, int] = {}
    sector_weights: dict[str, Decimal] = {}
    next_streaks: dict[UUID, int] = {}

    def add_if_within_caps(
        ranking_value: Decimal, instrument: BaselineInstrumentSnapshot
    ) -> bool:
        nonlocal etf_count
        projected = {
            exposure.sector: sector_weights.get(exposure.sector, Decimal(0))
            + target_weight * exposure.weight
            for exposure in instrument.sector_exposures
        }
        if any(weight > strategy.portfolio.max_sector_weight for weight in projected.values()):
            return False
        if instrument.asset_type != "stock":
            if etf_count >= strategy.portfolio.max_etfs:
                return False
        else:
            sector = instrument.sector_exposures[0].sector
            if stocks_by_sector.get(sector, 0) >= strategy.portfolio.max_stocks_per_sector:
                return False
        if instrument.asset_type != "stock":
            etf_count += 1
        else:
            sector = instrument.sector_exposures[0].sector
            stocks_by_sector[sector] = stocks_by_sector.get(sector, 0) + 1
        sector_weights.update(projected)
        selected.append((ranking_value, instrument))
        return True

    retained: list[tuple[int, UUID, BaselineInstrumentSnapshot, Decimal, int]] = []
    for instrument_id in snapshot.current_positions:
        retained_instrument = instrument_by_id.get(instrument_id)
        ranking_value = ranking_values.get(instrument_id)
        rank = rank_by_instrument.get(instrument_id)
        if (
            retained_instrument is None
            or ranking_value is None
            or instrument_id in trend_broken
        ):
            _record_held_rank(
                _observations,
                instrument_id,
                previous_streaks.get(instrument_id),
                reason="INSTRUMENT_UNAVAILABLE"
                if retained_instrument is None
                else "RANKING_VALUE_UNAVAILABLE"
                if ranking_value is None
                else "TREND_BROKEN",
            )
            continue
        streak = (
            0
            if rank is not None and rank <= strategy.ranking.exit_buffer_rank
            else previous_streaks.get(instrument_id, 0) + 1
        )
        _record_held_rank(_observations, instrument_id, previous_streaks.get(instrument_id), streak)
        if (
            not strategy.exits.ranking_buffer_enabled
            or streak >= strategy.ranking.exit_after_consecutive_evaluations
        ):
            continue
        retained.append(
            (rank or 1_000_000, instrument_id, retained_instrument, ranking_value, streak)
        )

    selection_limit = min(strategy.ranking.select_count, strategy.portfolio.max_positions)
    for _, instrument_id, instrument, ranking_value, streak in sorted(retained):
        if len(selected) == selection_limit:
            break
        if add_if_within_caps(ranking_value, instrument):
            next_streaks[instrument_id] = streak
    selected_ids = {instrument.instrument_id for _, instrument in selected}
    for ranking_value, instrument in ranked:
        if instrument.instrument_id in selected_ids or not instrument.eligible_for_new_position:
            continue
        if not add_if_within_caps(ranking_value, instrument):
            continue
        selected_ids.add(instrument.instrument_id)
        next_streaks[instrument.instrument_id] = 0
        if len(selected) == selection_limit:
            break
    selected.sort(
        key=lambda item: (
            rank_by_instrument.get(item[1].instrument_id, 1_000_000),
            item[1].instrument_id.int,
        )
    )
    targets = tuple(
        CompiledStrategyTarget(
            instrument_id=instrument.instrument_id,
            weight=target_weight,
            rank=rank_by_instrument.get(instrument.instrument_id),
            ranking_value=ranking_value,
        )
        for ranking_value, instrument in selected
    )
    selected_ids = {target.instrument_id for target in targets}
    return CompiledStrategyDecision(
        strategy_version=strategy.strategy_version,
        decision_session=decision_session,
        targets=targets,
        exits=tuple(
            sorted(set(snapshot.current_positions) - selected_ids, key=lambda value: value.int)
        ),
        position_rank_states=tuple(
            PositionRankState(
                instrument_id=instrument_id,
                consecutive_outside_top20_weeks=next_streaks[instrument_id],
            )
            for instrument_id in sorted(selected_ids, key=lambda value: value.int)
        ),
        cash_weight=Decimal(1) - target_weight * len(targets),
        reason="WEEKLY_REBALANCE",
    )

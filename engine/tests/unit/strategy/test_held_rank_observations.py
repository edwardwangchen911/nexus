"""Observe actual held rank state without changing legacy decisions."""

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.universe import EQUITY_SECTOR_CODES
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    PositionRankState,
    StrategyDataSnapshot,
    evaluate_baseline_v1,
    evaluate_baseline_v1_with_rank_observations,
)
from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy
from nexus.strategy.interpreter import (
    evaluate_compiled_strategy,
    evaluate_compiled_strategy_with_rank_observations,
)

DAY = date(2026, 8, 21)


def _instrument(number, *, sector="information_technology", close="210", skip5_close="180"):
    closes = [Decimal(100)] * 200
    closes[-6], closes[-1] = Decimal(skip5_close), Decimal(close)
    return BaselineInstrumentSnapshot(
        instrument_id=UUID(int=number),
        symbol=f"S{number}",
        asset_type="stock",
        sector_exposures=({"sector": sector, "weight": Decimal(1)},),
        raw_close=Decimal(close),
        closes=tuple(closes),
        dollar_volumes=(Decimal(60000000),) * 20,
    )


def snapshot(instruments, held, *, streak=1, next_session=DAY + timedelta(days=3)):
    return StrategyDataSnapshot(
        decision_session=DAY,
        next_session=next_session,
        instruments=instruments,
        current_positions=held,
        position_rank_states=tuple(
            PositionRankState(instrument_id=item, consecutive_outside_top20_weeks=streak)
            for item in held
        )
        if streak is not None
        else (),
    )


@pytest.fixture(params=["baseline", "compiled"])
def evaluate(request):
    def run(value):
        if request.param == "baseline":
            observed = evaluate_baseline_v1_with_rank_observations(value, value.decision_session)
            original = evaluate_baseline_v1(value, value.decision_session)
        else:
            strategy = compile_strategy(baseline_v1_document(), baseline_feature_registry())
            observed = evaluate_compiled_strategy_with_rank_observations(
                strategy, value, value.decision_session
            )
            original = evaluate_compiled_strategy(strategy, value, value.decision_session)
        assert observed.decision.model_dump(mode="json") == original.model_dump(mode="json")
        return observed

    return run


def test_rank_exit_retains_observation_of_still_held_position(evaluate):
    instruments = tuple(
        _instrument(i, sector=EQUITY_SECTOR_CODES[i % 11], skip5_close=str(300 - i))
        for i in range(1, 22)
    )
    result = evaluate(snapshot(instruments, (UUID(int=21),)))
    assert result.decision.exits == (UUID(int=21),)
    assert all(item.instrument_id != UUID(int=21) for item in result.decision.position_rank_states)
    assert len(result.held_rank_observations) == 1
    observation = result.held_rank_observations[0]
    assert (
        observation.input_streak,
        observation.evaluated_streak,
        observation.status,
        observation.reason,
    ) == (1, 2, "known", None)


def test_sector_cap_exit_does_not_discard_held_observation(evaluate):
    ids = tuple(UUID(int=i) for i in (1, 2, 3))
    result = evaluate(snapshot(tuple(_instrument(i) for i in (1, 2, 3)), ids))
    assert UUID(int=3) in result.decision.exits
    assert {item.instrument_id for item in result.held_rank_observations} == set(ids)
    assert all(item.evaluated_streak == 0 for item in result.held_rank_observations)


@pytest.mark.parametrize(
    "kind,reason",
    [
        ("trend", "TREND_BROKEN"),
        ("history", "RANKING_VALUE_UNAVAILABLE"),
        ("instrument", "INSTRUMENT_UNAVAILABLE"),
        ("input", "RANK_INPUT_MISSING"),
    ],
)
def test_uncomputed_or_missing_input_state_is_explicitly_unavailable(evaluate, kind, reason):
    instrument = _instrument(1, close="90" if kind == "trend" else "210")
    if kind == "history":
        instrument = instrument.model_copy(update={"closes": instrument.closes[:100]})
    value = snapshot(
        () if kind == "instrument" else (instrument,),
        (UUID(int=1),),
        streak=None if kind == "input" else 1,
    )
    observation = evaluate(value).held_rank_observations[0]
    assert observation.status == "unavailable"
    assert observation.reason == reason
    assert observation.evaluated_streak is None
    assert observation.input_streak == (None if kind == "input" else 1)


def test_non_week_end_does_not_evaluate_or_increment(evaluate):
    value = snapshot((_instrument(1),), (UUID(int=1),)).model_copy(
        update={"decision_session": date(2026, 8, 20), "next_session": DAY}
    )
    result = evaluate(value)
    assert result.decision.reason == "NOT_WEEK_END"
    assert result.decision.position_rank_states == value.position_rank_states
    observation = result.held_rank_observations[0]
    assert observation.input_streak == 1
    assert observation.evaluated_streak is None
    assert observation.status == "not_evaluated"
    assert observation.reason == "NOT_WEEK_END"


def test_new_target_is_not_an_actual_held_observation(evaluate):
    result = evaluate(snapshot((_instrument(1),), ()))
    assert result.decision.targets
    assert result.held_rank_observations == ()


def test_baseline_and_compiled_observations_match_for_the_same_actual_holdings():
    value = snapshot(
        tuple(_instrument(i) for i in (1, 2, 3)), tuple(UUID(int=i) for i in (1, 2, 3))
    )
    compiled = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    baseline = evaluate_baseline_v1_with_rank_observations(value, DAY)
    generic = evaluate_compiled_strategy_with_rank_observations(compiled, value, DAY)
    assert baseline.held_rank_observations == generic.held_rank_observations
    assert baseline.decision == generic.decision.as_legacy_baseline()


def test_two_explicit_held_evaluations_preserve_the_exit_streak(evaluate):
    instruments = tuple(
        _instrument(i, sector=EQUITY_SECTOR_CODES[i % 11], skip5_close=str(300 - i))
        for i in range(1, 22)
    )
    first = evaluate(snapshot(instruments, (UUID(int=21),), streak=0))
    assert first.held_rank_observations[0].evaluated_streak == 1
    assert UUID(int=21) not in first.decision.exits
    second = evaluate(
        snapshot(
            instruments, (UUID(int=21),), streak=first.held_rank_observations[0].evaluated_streak
        )
    )
    assert second.held_rank_observations[0].evaluated_streak == 2
    assert UUID(int=21) in second.decision.exits

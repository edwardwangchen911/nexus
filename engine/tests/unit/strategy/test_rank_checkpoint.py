from decimal import Decimal
from uuid import UUID

import pytest
from nexus.strategy.baseline import HeldRankObservation
from nexus.strategy.rank_checkpoint import _derive_rank_states
from nexus.strategy.rank_continuity import PositionContinuity

INSTRUMENT = UUID(int=1)


def continuity(*, kind="continuous", previous=10, current=10):
    return PositionContinuity(
        instrument_id=INSTRUMENT,
        quantity_at_previous=Decimal(previous),
        quantity_at_cutoff=Decimal(current),
        previous_event_seq=1,
        last_event_seq=2,
        previous_prefix_hash="a" * 64,
        prefix_hash="b" * 64,
        continuous_since_previous=kind == "continuous",
        opening_event_seq=1,
        opening_fill_id=UUID(int=2) if kind != "unavailable" else None,
        kind=kind,
        reason="NON_FILL_OPENING" if kind == "unavailable" else None,
    )


def test_continuous_holding_inherits_actual_observation_even_after_exit_intent():
    states = _derive_rank_states(
        (continuity(),),
        (HeldRankObservation(INSTRUMENT, 1, 2, "known", None),),
    )
    assert states[0].consecutive_outside_top20_weeks == 2


@pytest.mark.parametrize("kind", ["opened", "reopened"])
def test_only_proven_new_opening_resets_rank(kind):
    states = _derive_rank_states((continuity(kind=kind),), ())
    assert states[0].consecutive_outside_top20_weeks == 0


@pytest.mark.parametrize(
    "observations",
    [(), (HeldRankObservation(INSTRUMENT, None, None, "unavailable", "RANK_INPUT_MISSING"),)],
)
def test_continuous_unknown_cannot_be_reset(observations):
    with pytest.raises(ValueError, match="RANK_CHECKPOINT_STATE_UNAVAILABLE"):
        _derive_rank_states((continuity(),), observations)


def test_nonfill_opening_cannot_initialize_rank():
    with pytest.raises(ValueError, match="RANK_CHECKPOINT_STATE_UNAVAILABLE"):
        _derive_rank_states((continuity(kind="unavailable"),), ())


def test_closed_position_has_no_next_input_state():
    assert _derive_rank_states((continuity(kind="closed", current=0),), ()) == ()


def test_two_real_weekly_evaluations_across_a_missing_calendar_week():
    # design.md:587 says two successive weekly evaluations, not adjacent ISO weeks.
    from datetime import date

    from nexus.market.universe import EQUITY_SECTOR_CODES
    from nexus.strategy.baseline import BaselineInstrumentSnapshot, StrategyDataSnapshot
    from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy
    from nexus.strategy.interpreter import evaluate_compiled_strategy_with_rank_observations

    instruments = []
    for number in range(1, 22):
        closes = [Decimal(100)] * 200
        closes[-6], closes[-1] = Decimal(300 - number), Decimal(310)
        instruments.append(
            BaselineInstrumentSnapshot(
                instrument_id=UUID(int=number),
                symbol=f"S{number}",
                asset_type="stock",
                sector_exposures=(
                    {"sector": EQUITY_SECTOR_CODES[number % 11], "weight": Decimal(1)},
                ),
                raw_close=Decimal(310),
                closes=tuple(closes),
                dollar_volumes=(Decimal(60000000),) * 20,
            )
        )
    held = UUID(int=21)
    from dataclasses import replace

    position = replace(continuity(), instrument_id=held)
    compiled = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    from nexus.strategy.baseline import PositionRankState

    first_snapshot = StrategyDataSnapshot(
        decision_session=date(2026, 8, 21),
        next_session=date(2026, 8, 24),
        instruments=tuple(instruments),
        current_positions=(held,),
        position_rank_states=(
            PositionRankState(instrument_id=held, consecutive_outside_top20_weeks=0),
        ),
    )
    first = evaluate_compiled_strategy_with_rank_observations(
        compiled, first_snapshot, first_snapshot.decision_session
    )
    assert first.held_rank_observations[0].evaluated_streak == 1
    second_snapshot = first_snapshot.model_copy(
        update={
            "decision_session": date(2026, 9, 4),
            "next_session": date(2026, 9, 7),
            "position_rank_states": _derive_rank_states((position,), first.held_rank_observations),
        }
    )
    second = evaluate_compiled_strategy_with_rank_observations(
        compiled, second_snapshot, second_snapshot.decision_session
    )
    assert second.held_rank_observations[0].evaluated_streak == 2
    assert second.decision.exits == (held,)


def test_input_hash_binds_frozen_references_without_changing_legacy_hash():
    from datetime import date

    from nexus.strategy.baseline import StrategyDataSnapshot
    from nexus.strategy.rank_checkpoint import _hash, _snapshot_hash

    snapshot = StrategyDataSnapshot(
        decision_session=date(2026, 8, 28), next_session=date(2026, 8, 31), instruments=()
    )
    assert _snapshot_hash(snapshot, None) == _hash(snapshot.model_dump(mode="python"))
    assert _snapshot_hash(snapshot, "a" * 64) != _snapshot_hash(snapshot, "b" * 64)
    with pytest.raises(ValueError, match="REFERENCE_HASH_INVALID"):
        _snapshot_hash(snapshot, "not-a-hash")

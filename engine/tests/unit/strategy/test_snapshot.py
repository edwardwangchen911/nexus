"""Strict ordinary strategy preparation, independent of a backtest protocol."""

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.strategy.baseline import PositionRankState, evaluate_baseline_v1
from nexus.strategy.features import BacktestBar, SectorClassification
from nexus.strategy.snapshot import prepare_strategy_data_snapshot

ID = UUID(int=1)
HELD = UUID(int=2)
END = date(2026, 8, 21)


def inputs():
    days = []
    day = END
    while len(days) < 201:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    sessions = tuple(reversed(days)) + (date(2026, 8, 24),)
    clocks = {day: datetime.combine(day, time(21), UTC) for day in sessions}
    bars = tuple(
        BacktestBar(
            instrument_id=ID,
            session=day,
            open=Decimal(10 + index),
            high=Decimal(10 + index),
            low=Decimal(10 + index),
            close=Decimal(10 + index),
            volume=Decimal(10000000),
            observed_at=clocks[day],
            available_at=clocks[day],
            received_at=clocks[day],
        )
        for index, day in enumerate(sessions[:-1])
    )
    sector = SectorClassification(
        authority_id=UUID(int=100),
        instrument_id=ID,
        sector="information_technology",
        effective_session=sessions[0],
        published_at=clocks[sessions[0]],
        source_uri="memory://sector",
        content_hash="a" * 64,
        reviewed=True,
    )
    return dict(
        decision_session=END,
        next_session=sessions[-1],
        decision_time=clocks[END],
        calendar_sessions=sessions,
        decision_times=clocks,
        raw_bars=bars,
        adjusted_bars=bars,
        universe_members=(ID,),
        asset_types={ID: "stock"},
        sector_classifications=(sector,),
        sector_source="fixture",
        current_positions=(),
        position_rank_states=(),
    )


def test_complete_snapshot_preserves_feature_inputs_and_interpreter_result():
    values = inputs()
    result = prepare_strategy_data_snapshot(**values)
    instrument = result.instruments[0]
    assert len(instrument.closes) == 200
    assert instrument.closes == tuple(bar.close for bar in values["adjusted_bars"][-200:])
    assert instrument.dollar_volumes[-1] == Decimal(2100000000)
    decision = evaluate_baseline_v1(result, END)
    assert decision.targets[0].momentum_126_skip5 == Decimal(205) / Decimal(84) - 1
    assert decision.position_rank_states[0].consecutive_outside_top20_weeks == 0


@pytest.mark.parametrize(
    "change",
    [
        {"next_session": None},
        {"next_session": date(2026, 8, 25)},
        {"decision_time": datetime(2026, 8, 21, 21)},
        {"universe_members": ()},
        {"asset_types": {}},
        {"sector_classifications": ()},
        {"current_positions": (ID,)},
        {
            "position_rank_states": (
                PositionRankState(instrument_id=ID, consecutive_outside_top20_weeks=0),
            )
        },
    ],
)
def test_incomplete_inputs_fail_instead_of_silent_all_cash(change):
    values = inputs() | change
    with pytest.raises(ValueError):
        prepare_strategy_data_snapshot(**values)


@pytest.mark.parametrize("field", ["raw_bars", "adjusted_bars"])
def test_missing_middle_session_does_not_get_replaced_with_older_history(field):
    values = inputs()
    values[field] = values[field][:30] + values[field][31:]
    with pytest.raises(ValueError, match="history"):
        prepare_strategy_data_snapshot(**values)


def test_future_revision_cannot_change_current_features():
    values = inputs()
    bar = values["raw_bars"][-1]
    future = bar.model_copy(
        update={
            "close": Decimal(999),
            "available_at": values["decision_time"] + timedelta(seconds=1),
            "received_at": values["decision_time"] + timedelta(seconds=1),
        }
    )
    original = prepare_strategy_data_snapshot(**values)
    values["raw_bars"] += (future,)
    assert prepare_strategy_data_snapshot(**values) == original


def test_held_outside_universe_is_retained_as_input_not_new_candidate():
    values = inputs()
    values["current_positions"] = (HELD,)
    values["position_rank_states"] = (
        PositionRankState(instrument_id=HELD, consecutive_outside_top20_weeks=1),
    )
    values["asset_types"][HELD] = "stock"
    for field in ("raw_bars", "adjusted_bars"):
        values[field] += tuple(
            bar.model_copy(update={"instrument_id": HELD}) for bar in values[field]
        )
    values["sector_classifications"] += (
        values["sector_classifications"][0].model_copy(
            update={"instrument_id": HELD, "authority_id": UUID(int=101)}
        ),
    )
    result = prepare_strategy_data_snapshot(**values)
    assert {item.instrument_id for item in result.instruments} == {ID, HELD}
    assert not next(
        item for item in result.instruments if item.instrument_id == HELD
    ).eligible_for_new_position
    assert result.position_rank_states == values["position_rank_states"]


def test_duplicate_rank_state_is_not_silently_collapsed():
    values = inputs()
    state = PositionRankState(instrument_id=ID, consecutive_outside_top20_weeks=1)
    with pytest.raises(ValueError, match="rank"):
        prepare_strategy_data_snapshot(
            **(values | {"current_positions": (ID,), "position_rank_states": (state, state)})
        )


def test_same_cutoff_revision_selection_is_order_independent():
    values = inputs()
    original = values["raw_bars"][-1]
    later = original.model_copy(
        update={"revision": "2", "close": Decimal(211), "high": Decimal(211)}
    )
    one = prepare_strategy_data_snapshot(**(values | {"raw_bars": values["raw_bars"] + (later,)}))
    two = prepare_strategy_data_snapshot(**(values | {"raw_bars": (later,) + values["raw_bars"]}))
    assert one == two
    assert one.instruments[0].raw_close == Decimal(211)


@pytest.mark.parametrize(
    "update",
    [
        {"published_at": datetime(2026, 8, 22, tzinfo=UTC)},
        {"reviewed": False, "synthetic": False},
    ],
)
def test_future_or_unreviewed_sector_is_rejected(update):
    values = inputs()
    values["sector_classifications"] = (
        values["sector_classifications"][0].model_copy(update=update),
    )
    with pytest.raises(ValueError, match="sector"):
        prepare_strategy_data_snapshot(**values)


def test_stale_etf_sector_allows_existing_input_but_not_new_candidate():
    values = inputs()
    values["asset_types"] = {ID: "equity_etf"}
    values["sector_source"] = "issuer"
    result = prepare_strategy_data_snapshot(**values)
    assert not result.instruments[0].eligible_for_new_position


def test_non_week_end_keeps_explicit_rank_state():
    values = inputs()
    # Thursday has a real Friday successor and enough prior sessions.
    values["decision_session"] = values["calendar_sessions"][-3]
    values["next_session"] = END
    values["decision_time"] = values["decision_times"][values["decision_session"]]
    values["current_positions"] = (ID,)
    values["position_rank_states"] = (
        PositionRankState(instrument_id=ID, consecutive_outside_top20_weeks=1),
    )
    snapshot = prepare_strategy_data_snapshot(**values)
    decision = evaluate_baseline_v1(snapshot, snapshot.decision_session)
    assert decision.reason == "NOT_WEEK_END"
    assert decision.position_rank_states == values["position_rank_states"]


def test_unranked_features_cannot_be_evaluated_as_zero_rank():
    from nexus.strategy.snapshot import prepare_unranked_strategy_snapshot

    values = inputs()
    values.pop("position_rank_states")
    values["current_positions"] = (ID,)
    features = prepare_unranked_strategy_snapshot(**values)
    assert features.current_positions == (ID,)
    with pytest.raises(ValueError, match="STRATEGY_RANK_INPUT_REQUIRED"):
        evaluate_baseline_v1(features, END)
    with pytest.raises(ValueError, match="exactly cover"):
        features.with_rank_states(())
    ranked = features.with_rank_states(
        (PositionRankState(instrument_id=ID, consecutive_outside_top20_weeks=2),)
    )
    assert ranked.position_rank_states[0].consecutive_outside_top20_weeks == 2

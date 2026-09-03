"""Shared snapshot semantics under the normal pytest import path."""

from datetime import date
from uuid import UUID

from nexus.backtest.engine import _prepare_dataset, _strategy_snapshot
from nexus.strategy.baseline import evaluate_baseline_v1
from nexus.strategy.snapshot import prepare_strategy_data_snapshot
from test_engine import _bar, _dataset, _history_sessions


def test_preparation_matches_existing_backtest_snapshot_and_decision():
    instrument = UUID(int=1)
    end, next_session = date(2026, 8, 21), date(2026, 8, 24)
    history = _history_sessions(end)
    sessions = (*history, end, next_session)
    data = _dataset(
        bars=tuple(_bar(instrument, day, "8") for day in history) + (_bar(instrument, end, "10"),),
        sessions=sessions,
        universe_members=(instrument,),
    )
    old = _strategy_snapshot(
        data=data,
        prepared=_prepare_dataset(data),
        decision_session=end,
        next_session=next_session,
        current_positions=(),
        position_rank_streaks={},
    )
    new = prepare_strategy_data_snapshot(
        decision_session=end,
        next_session=next_session,
        decision_time=data.authority.exchange_calendar.decision_times[end],
        calendar_sessions=sessions,
        decision_times=data.authority.exchange_calendar.decision_times,
        raw_bars=data.bars,
        adjusted_bars=data.adjusted_bars,
        universe_members=(instrument,),
        asset_types=data.asset_types,
        sector_classifications=data.sector_classifications,
        sector_source="fixture",
        current_positions=(),
        position_rank_states=(),
    )
    assert new == old
    assert evaluate_baseline_v1(new, end) == evaluate_baseline_v1(old, end)

"""Pure, fail-closed strategy inputs; callers retain source and purpose authorization."""

from collections.abc import Mapping
from datetime import date, datetime
from typing import Literal
from uuid import UUID

from nexus.domain.time import require_utc
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    BaselineSectorExposure,
    PositionRankState,
    StrategyDataSnapshot,
)
from nexus.strategy.features import (
    BacktestBar,
    SectorClassification,
    align_feature_window,
    newer_bar,
    normalized_sector_weights,
    sector_exposures_complete,
    select_sector_exposures,
)


def prepare_strategy_data_snapshot(
    *,
    decision_session: date,
    next_session: date,
    decision_time: datetime,
    calendar_sessions: tuple[date, ...],
    decision_times: Mapping[date, datetime],
    raw_bars: tuple[BacktestBar, ...],
    adjusted_bars: tuple[BacktestBar, ...],
    universe_members: tuple[UUID, ...],
    asset_types: Mapping[UUID, Literal["stock", "equity_etf", "non_equity_etf"]],
    sector_classifications: tuple[SectorClassification, ...],
    sector_source: str,
    current_positions: tuple[UUID, ...],
    position_rank_states: tuple[PositionRankState, ...],
) -> StrategyDataSnapshot:
    """Assemble declared facts without I/O, decisions, hashes, or execution permission.

    The real reader must verify source identities, raw/all adjustment semantics,
    USD pricing and share-volume units before supplying these facts for ADV20.
    This function does not establish those qualifications from numeric values.
    Rank states must come from the same account/strategy history; they are never
    inferred here. An empty account supplies an explicit empty state tuple.
    """
    cutoff = require_utc(decision_time)
    if (
        not calendar_sessions
        or tuple(sorted(set(calendar_sessions))) != calendar_sessions
        or set(decision_times) != set(calendar_sessions)
        or any(day.weekday() >= 5 for day in calendar_sessions)
        or decision_session not in decision_times
    ):
        raise ValueError("strategy calendar is incomplete or ambiguous")
    clocks = tuple(require_utc(decision_times[day]) for day in calendar_sessions)
    if (
        tuple(sorted(set(clocks))) != clocks
        or any(clock.date() != day for day, clock in zip(calendar_sessions, clocks, strict=True))
        or decision_times[decision_session] != cutoff
    ):
        raise ValueError("strategy calendar decision time mismatch")
    index = calendar_sessions.index(decision_session)
    if index + 1 >= len(calendar_sessions) or calendar_sessions[index + 1] != next_session:
        raise ValueError("strategy calendar successor is missing or mismatched")
    if not universe_members or len(set(universe_members)) != len(universe_members):
        raise ValueError("strategy universe must be nonempty and unique")
    if len(set(current_positions)) != len(current_positions):
        raise ValueError("strategy current positions must be unique")
    rank_ids = tuple(state.instrument_id for state in position_rank_states)
    if len(set(rank_ids)) != len(rank_ids) or set(rank_ids) != set(current_positions):
        raise ValueError("strategy rank states must exactly cover current positions")
    required = set(universe_members) | set(current_positions)
    if not required <= set(asset_types):
        raise ValueError("strategy asset types do not cover required instruments")
    expected_sessions = calendar_sessions[max(0, index - 199) : index + 1]
    if len(expected_sessions) != 200:
        raise ValueError("strategy history requires 200 trading sessions")

    def visible(bars: tuple[BacktestBar, ...]) -> dict[UUID, dict[date, BacktestBar]]:
        selected: dict[UUID, dict[date, BacktestBar]] = {}
        for bar in bars:
            if bar.instrument_id not in required:
                continue
            if bar.session not in decision_times:
                raise ValueError("strategy price session is outside the calendar")
            if max(bar.available_at, decision_times[bar.session]) > cutoff:
                continue
            state = selected.setdefault(bar.instrument_id, {})
            state[bar.session] = newer_bar(state.get(bar.session), bar)
        return selected

    raw = visible(raw_bars)
    adjusted = visible(adjusted_bars)
    instruments: list[BaselineInstrumentSnapshot] = []
    for instrument_id in sorted(required, key=lambda item: item.int):
        raw_window, adjusted_window = align_feature_window(
            calendar_sessions,
            decision_session,
            raw.get(instrument_id, {}),
            adjusted.get(instrument_id, {}),
        )
        if tuple(bar.session for bar in raw_window) != expected_sessions:
            raise ValueError("strategy history is incomplete or misaligned")
        asset_type = asset_types[instrument_id]
        if asset_type not in ("stock", "equity_etf", "non_equity_etf"):
            raise ValueError("strategy asset type is unsupported")
        exposures = select_sector_exposures(
            classifications=sector_classifications,
            instrument_id=instrument_id,
            asset_type=asset_type,
            sector_source=sector_source,
            session=decision_session,
            cutoff=cutoff,
        )
        if not sector_exposures_complete(asset_type, exposures):
            raise ValueError("strategy sector exposures are incomplete")
        stale = (
            asset_type == "equity_etf"
            and sector_source != "fixture"
            and bool(exposures)
            and (decision_session - exposures[0].effective_session).days > 30
        )
        instruments.append(
            BaselineInstrumentSnapshot(
                instrument_id=instrument_id,
                symbol=str(instrument_id),
                asset_type=asset_type,
                sector_exposures=tuple(
                    BaselineSectorExposure(sector=sector, weight=weight)
                    for sector, weight in normalized_sector_weights(exposures)
                ),
                raw_close=raw_window[-1].close,
                closes=tuple(bar.close for bar in adjusted_window),
                dollar_volumes=tuple(bar.close * bar.volume for bar in raw_window),
                eligible_for_new_position=instrument_id in universe_members and not stale,
            )
        )
    return StrategyDataSnapshot(
        decision_session=decision_session,
        next_session=next_session,
        instruments=tuple(instruments),
        current_positions=tuple(sorted(current_positions, key=lambda item: item.int)),
        position_rank_states=tuple(
            sorted(position_rank_states, key=lambda item: item.instrument_id.int)
        ),
    )

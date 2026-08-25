"""The exact, deterministic historical ``baseline_v1`` policy."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.market.universe import EQUITY_SECTOR_CODES

_TARGET_WEIGHT = Decimal("0.125")
_MIN_ADV20_USD = Decimal("50000000")
_MIN_CLOSE = Decimal("5")


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BaselineSectorExposure(_FrozenModel):
    sector: str
    weight: Decimal

    @field_validator("sector")
    @classmethod
    def _canonical_sector(cls, value: str) -> str:
        if value not in EQUITY_SECTOR_CODES:
            raise ValueError("sector must use the canonical NEXUS_SECTOR_V1 code")
        return value

    @field_validator("weight", mode="before")
    @classmethod
    def _exact_weight(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("sector weights must not contain binary floats")
        return value

    @model_validator(mode="after")
    def _valid_weight(self) -> BaselineSectorExposure:
        if not Decimal(0) < self.weight <= Decimal(1):
            raise ValueError("sector weights must be positive and no greater than one")
        return self


class BaselineInstrumentSnapshot(_FrozenModel):
    instrument_id: UUID
    symbol: str
    asset_type: Literal["stock", "equity_etf", "non_equity_etf"]
    sector_exposures: tuple[BaselineSectorExposure, ...]
    raw_close: Decimal
    closes: tuple[Decimal, ...]
    dollar_volumes: tuple[Decimal, ...]
    eligible_for_new_position: bool = True

    @field_validator("closes", "dollar_volumes", mode="before")
    @classmethod
    def _exact_decimals(cls, value: object) -> object:
        if isinstance(value, tuple | list) and any(isinstance(item, float) for item in value):
            raise TypeError("strategy data must not contain binary floats")
        return value

    @field_validator("raw_close", mode="before")
    @classmethod
    def _exact_raw_close(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("strategy data must not contain binary floats")
        return value

    @model_validator(mode="after")
    def _valid_sector_exposures(self) -> BaselineInstrumentSnapshot:
        sectors = [exposure.sector for exposure in self.sector_exposures]
        if len(sectors) != len(set(sectors)):
            raise ValueError("sector exposures must be unique")
        total = sum((exposure.weight for exposure in self.sector_exposures), start=Decimal(0))
        if self.asset_type == "stock" and (len(self.sector_exposures) != 1 or total != Decimal(1)):
            raise ValueError("stocks require one full-weight sector exposure")
        if self.asset_type == "equity_etf" and (
            not sectors or not Decimal("0.995") <= total <= Decimal("1.005")
        ):
            raise ValueError("equity ETF sector exposures must sum to 100% within tolerance")
        if self.asset_type == "non_equity_etf" and sectors:
            raise ValueError("non-equity ETFs do not carry equity-sector exposures")
        return self


class PositionRankState(_FrozenModel):
    instrument_id: UUID
    consecutive_outside_top20_weeks: int = Field(ge=0)


class StrategyDataSnapshot(_FrozenModel):
    decision_session: date
    next_session: date | None
    instruments: tuple[BaselineInstrumentSnapshot, ...]
    current_positions: tuple[UUID, ...] = ()
    position_rank_states: tuple[PositionRankState, ...] = ()

    @model_validator(mode="after")
    def _unique_instruments(self) -> StrategyDataSnapshot:
        ids = [item.instrument_id for item in self.instruments]
        if len(ids) != len(set(ids)):
            raise ValueError("instrument snapshots must be unique")
        position_ids = list(self.current_positions)
        if len(position_ids) != len(set(position_ids)):
            raise ValueError("current positions must be unique")
        state_ids = [state.instrument_id for state in self.position_rank_states]
        if len(state_ids) != len(set(state_ids)) or not set(state_ids) <= set(position_ids):
            raise ValueError("position rank states must be unique current positions")
        return self


class StrategyTarget(_FrozenModel):
    instrument_id: UUID
    weight: Decimal
    rank: int | None
    momentum_126_skip5: Decimal


class StrategyDecision(_FrozenModel):
    strategy_version: Literal["baseline_v1"] = "baseline_v1"
    decision_session: date
    targets: tuple[StrategyTarget, ...]
    exits: tuple[UUID, ...]
    position_rank_states: tuple[PositionRankState, ...] = ()
    cash_weight: Decimal | None
    reason: Literal["WEEKLY_REBALANCE", "NOT_WEEK_END"]


def evaluate_baseline_v1(
    snapshot: StrategyDataSnapshot,
    decision_session: date,
    *,
    _min_adv20_usd: Decimal = _MIN_ADV20_USD,
) -> StrategyDecision:
    """Rank the point-in-time universe and apply all baseline portfolio caps."""
    if snapshot.decision_session != decision_session:
        raise ValueError("decision session does not match snapshot")
    next_session = snapshot.next_session
    if (
        next_session is not None
        and next_session.isocalendar()[:2] == decision_session.isocalendar()[:2]
    ):
        return StrategyDecision(
            decision_session=decision_session,
            targets=(),
            exits=(),
            position_rank_states=snapshot.position_rank_states,
            cash_weight=None,
            reason="NOT_WEEK_END",
        )
    ranked: list[tuple[Decimal, BaselineInstrumentSnapshot]] = []
    momentum_by_instrument: dict[UUID, Decimal] = {}
    trend_broken: set[UUID] = set()
    for instrument in snapshot.instruments:
        if len(instrument.closes) < 200 or len(instrument.dollar_volumes) < 20:
            continue
        closes = instrument.closes
        adv20 = sum(instrument.dollar_volumes[-20:], start=Decimal(0)) / Decimal(20)
        sma200 = sum(closes[-200:], start=Decimal(0)) / Decimal(200)
        prior_126 = closes[-127]
        if prior_126 <= 0:
            continue
        return_126 = closes[-1] / prior_126 - Decimal(1)
        momentum = closes[-6] / prior_126 - Decimal(1)
        momentum_by_instrument[instrument.instrument_id] = momentum
        if closes[-1] <= sma200:
            trend_broken.add(instrument.instrument_id)
        if (
            adv20 < _min_adv20_usd
            or instrument.raw_close < _MIN_CLOSE
            or closes[-1] <= sma200
            or return_126 <= 0
        ):
            continue
        ranked.append((momentum, instrument))
    ranked.sort(key=lambda item: (-item[0], item[1].instrument_id.int))

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
    portfolio_sector_weights: dict[str, Decimal] = {}
    next_streaks: dict[UUID, int] = {}

    def add_if_within_caps(momentum: Decimal, instrument: BaselineInstrumentSnapshot) -> bool:
        nonlocal etf_count
        projected_sector_weights = {
            exposure.sector: portfolio_sector_weights.get(exposure.sector, Decimal(0))
            + _TARGET_WEIGHT * exposure.weight
            for exposure in instrument.sector_exposures
        }
        if any(weight > Decimal("0.30") for weight in projected_sector_weights.values()):
            return False
        if instrument.asset_type != "stock":
            if etf_count >= 2:
                return False
        else:
            sector = instrument.sector_exposures[0].sector
            count = stocks_by_sector.get(sector, 0)
            if count >= 2:
                return False
        if instrument.asset_type != "stock":
            etf_count += 1
        else:
            sector = instrument.sector_exposures[0].sector
            stocks_by_sector[sector] = stocks_by_sector.get(sector, 0) + 1
        portfolio_sector_weights.update(projected_sector_weights)
        selected.append((momentum, instrument))
        return True

    retained_candidates: list[tuple[int, UUID, BaselineInstrumentSnapshot, Decimal, int]] = []
    for instrument_id in snapshot.current_positions:
        retained_instrument = instrument_by_id.get(instrument_id)
        retained_momentum = momentum_by_instrument.get(instrument_id)
        rank = rank_by_instrument.get(instrument_id)
        if (
            retained_instrument is None
            or retained_momentum is None
            or instrument_id in trend_broken
        ):
            continue
        streak = (
            0 if rank is not None and rank <= 20 else previous_streaks.get(instrument_id, 0) + 1
        )
        if streak >= 2:
            continue
        retained_candidates.append(
            (
                rank or 1_000_000,
                instrument_id,
                retained_instrument,
                retained_momentum,
                streak,
            )
        )

    for _, instrument_id, instrument, momentum, streak in sorted(retained_candidates):
        if len(selected) == 6:
            break
        if add_if_within_caps(momentum, instrument):
            next_streaks[instrument_id] = streak

    selected_ids = {instrument.instrument_id for _, instrument in selected}
    for momentum, instrument in ranked:
        if instrument.instrument_id in selected_ids:
            continue
        if not instrument.eligible_for_new_position:
            continue
        if not add_if_within_caps(momentum, instrument):
            continue
        selected_ids.add(instrument.instrument_id)
        next_streaks[instrument.instrument_id] = 0
        if len(selected) == 6:
            break

    selected.sort(
        key=lambda item: (
            rank_by_instrument.get(item[1].instrument_id, 1_000_000),
            item[1].instrument_id.int,
        )
    )

    targets = tuple(
        StrategyTarget(
            instrument_id=instrument.instrument_id,
            weight=_TARGET_WEIGHT,
            rank=rank_by_instrument.get(instrument.instrument_id),
            momentum_126_skip5=momentum,
        )
        for momentum, instrument in selected
    )
    selected_ids = {target.instrument_id for target in targets}
    exits = tuple(
        sorted(set(snapshot.current_positions) - selected_ids, key=lambda value: value.int)
    )
    cash_weight = Decimal(1) - _TARGET_WEIGHT * len(targets)
    return StrategyDecision(
        decision_session=decision_session,
        targets=targets,
        exits=exits,
        position_rank_states=tuple(
            PositionRankState(
                instrument_id=instrument_id,
                consecutive_outside_top20_weeks=next_streaks[instrument_id],
            )
            for instrument_id in sorted(selected_ids, key=lambda value: value.int)
        ),
        cash_weight=cash_weight,
        reason="WEEKLY_REBALANCE",
    )

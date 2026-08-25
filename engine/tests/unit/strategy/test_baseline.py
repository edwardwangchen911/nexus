"""Exact deterministic baseline_v1 selection policy."""

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.market.universe import EQUITY_SECTOR_CODES
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    BaselineSectorExposure,
    StrategyDataSnapshot,
    evaluate_baseline_v1,
)


def _instrument(
    number: int,
    *,
    asset_type: str = "stock",
    sector: str = "information_technology",
    close: str = "210",
    raw_close: str | None = None,
    dollar_volume: str = "60000000",
    skip5_close: str = "180",
    start_close: str = "100",
    sector_exposures: tuple[tuple[str, str], ...] | None = None,
) -> BaselineInstrumentSnapshot:
    closes = [Decimal(start_close)] * 200
    closes[-127] = Decimal(start_close)
    closes[-6] = Decimal(skip5_close)
    closes[-1] = Decimal(close)
    resolved_asset_type = "equity_etf" if asset_type == "etf" else asset_type
    resolved_sector = (
        EQUITY_SECTOR_CODES[number % len(EQUITY_SECTOR_CODES)]
        if resolved_asset_type == "equity_etf" and sector == "information_technology"
        else sector
    )
    resolved_exposures = sector_exposures or ((resolved_sector, "1"),)
    return BaselineInstrumentSnapshot(
        instrument_id=UUID(int=number),
        symbol=f"S{number}",
        asset_type=resolved_asset_type,
        sector_exposures=tuple(
            BaselineSectorExposure(sector=sector_name, weight=Decimal(weight))
            for sector_name, weight in resolved_exposures
        ),
        raw_close=Decimal(raw_close or close),
        closes=tuple(closes),
        dollar_volumes=tuple(Decimal(dollar_volume) for _ in range(20)),
    )


def test_baseline_v1_applies_exact_filters_ranking_and_caps() -> None:
    friday = date(2026, 8, 21)
    instruments = (
        _instrument(1, asset_type="etf", skip5_close="195"),
        _instrument(2, asset_type="etf", skip5_close="194"),
        _instrument(3, asset_type="etf", skip5_close="193"),
        _instrument(4, sector="information_technology", skip5_close="192"),
        _instrument(5, sector="information_technology", skip5_close="191"),
        _instrument(6, sector="information_technology", skip5_close="190"),
        _instrument(7, sector="health_care", skip5_close="189"),
        _instrument(8, sector="industrials", skip5_close="188"),
        _instrument(9, sector="financials", skip5_close="187"),
        _instrument(10, dollar_volume="49999999", skip5_close="999"),
        _instrument(11, close="4.99", skip5_close="999"),
        _instrument(12, close="90", start_close="100", skip5_close="999"),
        _instrument(13, close="100", start_close="100", skip5_close="99"),
    )
    snapshot = StrategyDataSnapshot(
        decision_session=friday,
        next_session=friday + timedelta(days=3),
        instruments=instruments,
        current_positions=(UUID(int=3), UUID(int=99)),
    )

    decision = evaluate_baseline_v1(snapshot, friday)

    assert tuple(target.instrument_id.int for target in decision.targets) == (1, 3, 4, 5, 7, 8)
    assert tuple(target.weight for target in decision.targets) == (Decimal("0.125"),) * 6
    assert decision.cash_weight == Decimal("0.25")
    assert tuple(instrument.int for instrument in decision.exits) == (99,)
    assert decision.strategy_version == "baseline_v1"


def test_baseline_v1_only_decides_on_last_session_of_iso_week() -> None:
    thursday = date(2026, 8, 20)
    decision = evaluate_baseline_v1(
        StrategyDataSnapshot(
            decision_session=thursday,
            next_session=date(2026, 8, 21),
            instruments=(_instrument(1),),
        ),
        thursday,
    )

    assert decision.targets == ()
    assert decision.exits == ()
    assert decision.cash_weight is None
    assert decision.reason == "NOT_WEEK_END"


def test_absolute_price_filter_uses_raw_close_not_future_adjusted_close() -> None:
    friday = date(2026, 8, 21)
    eligible = _instrument(
        1,
        raw_close="10",
        close="1",
        skip5_close="0.9",
        start_close="0.5",
    )
    below_raw_threshold = _instrument(2, raw_close="4.99")

    decision = evaluate_baseline_v1(
        StrategyDataSnapshot(
            decision_session=friday,
            next_session=friday + timedelta(days=3),
            instruments=(eligible, below_raw_threshold),
        ),
        friday,
    )

    assert tuple(target.instrument_id for target in decision.targets) == (
        eligible.instrument_id,
    )


def test_baseline_v1_retains_a_held_top20_name_and_fills_only_vacant_slots() -> None:
    """Selling every name outside the top six would defeat the specified rank buffer."""
    friday = date(2026, 8, 21)
    instruments = tuple(
        _instrument(
            number,
            sector=EQUITY_SECTOR_CODES[number % len(EQUITY_SECTOR_CODES)],
            skip5_close=str(300 - number),
        )
        for number in range(1, 22)
    )

    decision = evaluate_baseline_v1(
        StrategyDataSnapshot(
            decision_session=friday,
            next_session=friday + timedelta(days=3),
            instruments=instruments,
            current_positions=(UUID(int=7),),
        ),
        friday,
    )

    assert tuple(target.instrument_id.int for target in decision.targets) == (1, 2, 3, 4, 5, 7)
    assert decision.exits == ()
    assert decision.cash_weight == Decimal("0.25")


def test_baseline_v1_exits_after_second_week_outside_top20_and_fills_the_slot() -> None:
    """The second consecutive out-of-buffer evaluation must produce the deterministic exit."""
    friday = date(2026, 8, 21)
    instruments = tuple(
        _instrument(
            number,
            sector=EQUITY_SECTOR_CODES[number % len(EQUITY_SECTOR_CODES)],
            skip5_close=str(300 - number),
        )
        for number in range(1, 22)
    )

    decision = evaluate_baseline_v1(
        StrategyDataSnapshot(
            decision_session=friday,
            next_session=friday + timedelta(days=3),
            instruments=instruments,
            current_positions=(UUID(int=21),),
            position_rank_states=(
                {
                    "instrument_id": UUID(int=21),
                    "consecutive_outside_top20_weeks": 1,
                },
            ),
        ),
        friday,
    )

    assert tuple(target.instrument_id.int for target in decision.targets) == (1, 2, 3, 4, 5, 6)
    assert tuple(instrument.int for instrument in decision.exits) == (21,)
    assert all(state.instrument_id != UUID(int=21) for state in decision.position_rank_states)


def test_baseline_v1_explicitly_retains_first_week_ineligible_position_without_rank() -> None:
    friday = date(2026, 8, 21)
    held = _instrument(99, dollar_volume="1000", sector="utilities")
    candidates = tuple(
        _instrument(
            number,
            sector=EQUITY_SECTOR_CODES[number % len(EQUITY_SECTOR_CODES)],
            skip5_close=str(300 - number),
        )
        for number in range(1, 7)
    )

    decision = evaluate_baseline_v1(
        StrategyDataSnapshot(
            decision_session=friday,
            next_session=friday + timedelta(days=3),
            instruments=(*candidates, held),
            current_positions=(held.instrument_id,),
        ),
        friday,
    )

    retained = next(
        target for target in decision.targets if target.instrument_id == held.instrument_id
    )
    assert retained.rank is None
    assert retained.instrument_id not in decision.exits


def test_baseline_v1_applies_etf_sector_lookthrough_to_the_thirty_percent_cap() -> None:
    friday = date(2026, 8, 21)
    instruments = (
        _instrument(
            1,
            asset_type="etf",
            skip5_close="299",
            sector_exposures=(("information_technology", "0.60"), ("health_care", "0.40")),
        ),
        _instrument(2, sector="information_technology", skip5_close="298"),
        _instrument(3, sector="information_technology", skip5_close="297"),
        _instrument(4, sector="financials", skip5_close="296"),
        _instrument(5, sector="industrials", skip5_close="295"),
        _instrument(6, sector="utilities", skip5_close="294"),
        _instrument(7, sector="materials", skip5_close="293"),
    )

    decision = evaluate_baseline_v1(
        StrategyDataSnapshot(
            decision_session=friday,
            next_session=friday + timedelta(days=3),
            instruments=instruments,
        ),
        friday,
    )

    assert tuple(target.instrument_id.int for target in decision.targets) == (1, 2, 4, 5, 6, 7)
    assert UUID(int=3) not in {target.instrument_id for target in decision.targets}

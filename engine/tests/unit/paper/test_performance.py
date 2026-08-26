from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.performance import (
    MarkedPosition,
    PositionAdjustment,
    allocate_average_cost,
    fold_position_adjustments,
    project_paper_performance,
)
from pydantic import ValidationError

ACCOUNT_ID = UUID("64000000-0000-0000-0000-000000000001")
INSTRUMENT_ID = UUID("64000000-0000-0000-0000-000000000002")
NOW = datetime(2026, 8, 27, 20, tzinfo=UTC)


def _event(
    sequence: int,
    quantity: str,
    cost: str,
    *,
    event_type: str = "fill",
) -> PositionAdjustment:
    return PositionAdjustment(
        event_id=UUID(int=0x64000000000000000000000000000100 + sequence),
        account_id=ACCOUNT_ID,
        instrument_id=INSTRUMENT_ID,
        event_seq=sequence,
        event_type=event_type,
        quantity_delta=Decimal(quantity),
        cost_delta=Decimal(cost),
        occurred_at=NOW + timedelta(seconds=sequence),
        payload_hash=f"{sequence}" * 64,
    )


def test_position_fold_tracks_average_cost_across_buys_and_partial_sell() -> None:
    after_buys = fold_position_adjustments((_event(1, "10", "1000"), _event(2, "5", "600")))
    allocated = allocate_average_cost(after_buys, Decimal("6"))
    final = fold_position_adjustments(
        (
            _event(1, "10", "1000"),
            _event(2, "5", "600"),
            _event(3, "-6", str(-allocated)),
        )
    )

    assert after_buys.quantity == Decimal("15.000000000000")
    assert after_buys.cost_basis == Decimal("1600.000000000000")
    assert after_buys.average_cost == Decimal("106.666666666667")
    assert allocated == Decimal("640.000000000000")
    assert final.quantity == Decimal("9.000000000000")
    assert final.cost_basis == Decimal("960.000000000000")
    assert final.average_cost == Decimal("106.666666666667")


def test_full_exit_allocates_exact_remaining_cost_without_rounding_residue() -> None:
    position = fold_position_adjustments((_event(1, "3", "100"),))

    assert allocate_average_cost(position, Decimal("3")) == Decimal("100.000000000000")
    with pytest.raises(ValueError, match="exceeds position"):
        allocate_average_cost(position, Decimal("4"))


def test_correction_event_is_folded_and_bound_into_canonical_source_hash() -> None:
    original = _event(1, "5", "500")
    correction = _event(2, "-2", "-210", event_type="correction")
    corrected = fold_position_adjustments((original, correction))
    replay = fold_position_adjustments((original, correction))
    changed_fact = fold_position_adjustments(
        (original, correction.model_copy(update={"payload_hash": "f" * 64}))
    )

    assert corrected.quantity == Decimal("3.000000000000")
    assert corrected.cost_basis == Decimal("290.000000000000")
    assert corrected.average_cost == Decimal("96.666666666667")
    assert corrected.source_fact_hash == replay.source_fact_hash
    assert corrected.source_fact_hash != changed_fact.source_fact_hash


@pytest.mark.parametrize(
    ("events", "message"),
    (
        ((_event(2, "1", "10"),), "continuous"),
        ((_event(1, "1", "10"), _event(2, "-2", "-20")), "negative quantity"),
        ((_event(1, "1", "10"), _event(2, "-1", "-9")), "zero quantity"),
        ((_event(1, "1", "-10"),), "negative cost"),
    ),
)
def test_position_fold_rejects_gaps_oversells_and_inconsistent_cost(
    events: tuple[PositionAdjustment, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        fold_position_adjustments(events)


def test_performance_projects_nav_pnl_peak_and_drawdown_from_exact_facts() -> None:
    performance = project_paper_performance(
        account_id=ACCOUNT_ID,
        as_of=NOW,
        cash=Decimal("9000"),
        realized_pnl=Decimal("50"),
        fees=Decimal("5"),
        previous_peak_nav=Decimal("10000"),
        positions=(
            MarkedPosition(
                instrument_id=INSTRUMENT_ID,
                quantity=Decimal("9"),
                cost_basis=Decimal("960"),
                reference_close=Decimal("110"),
                price_authority_id=UUID(int=50),
                price_content_hash="a" * 64,
            ),
        ),
        source_fact_hashes=("b" * 64, "c" * 64),
    )

    assert performance.cash == Decimal("9000.000000000000")
    assert performance.market_value == Decimal("990.000000000000")
    assert performance.nav == Decimal("9990.000000000000")
    assert performance.realized_pnl == Decimal("50.000000000000")
    assert performance.unrealized_pnl == Decimal("30.000000000000")
    assert performance.fees == Decimal("5.000000000000")
    assert performance.peak_nav == Decimal("10000.000000000000")
    assert performance.drawdown_ratio == Decimal("0.001000000000")
    assert performance.content_hash != "0" * 64


def test_performance_fails_closed_on_missing_marks_negative_cash_or_float() -> None:
    with pytest.raises(ValueError, match="at least one position"):
        project_paper_performance(
            account_id=ACCOUNT_ID,
            as_of=NOW,
            cash=Decimal("10000"),
            realized_pnl=Decimal(0),
            fees=Decimal(0),
            previous_peak_nav=None,
            positions=(),
            source_fact_hashes=("a" * 64,),
            require_positions=True,
        )

    with pytest.raises(ValueError, match="cash"):
        project_paper_performance(
            account_id=ACCOUNT_ID,
            as_of=NOW,
            cash=Decimal("-1"),
            realized_pnl=Decimal(0),
            fees=Decimal(0),
            previous_peak_nav=None,
            positions=(),
            source_fact_hashes=("a" * 64,),
        )

    with pytest.raises(ValidationError):
        MarkedPosition(
            instrument_id=INSTRUMENT_ID,
            quantity=1.5,
            cost_basis=Decimal("1"),
            reference_close=Decimal("1"),
            price_authority_id=UUID(int=50),
            price_content_hash="a" * 64,
        )

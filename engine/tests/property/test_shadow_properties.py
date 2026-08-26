"""Property contracts for deterministic Proposal Shadow calculations."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st
from nexus.shadow.authority import BoundShadowBar, ShadowSessionInput
from nexus.shadow.canonical import simulation_state_content_hash, target_weights_content_hash
from nexus.shadow.metrics import calculate_checkpoint_metrics
from nexus.shadow.models import (
    SHADOW_VARIANTS,
    ShadowFinalGate,
    ShadowNavPoint,
    ShadowVariant,
)
from nexus.shadow.service import simulate_shadow_portfolio_session
from nexus.shadow.state_machine import decide_terminal_action
from nexus.simulation.canonical import canonical_decimal
from nexus.simulation.costs import CostModel
from nexus.simulation.models import SimulationState, TargetWeight


@given(st.permutations(SHADOW_VARIANTS))
def test_checkpoint_output_order_is_fixed_for_any_mapping_insertion_order(
    insertion_order: list[ShadowVariant],
) -> None:
    sessions = tuple(date(2026, 1, 5) + timedelta(days=index) for index in range(5))
    points = tuple(ShadowNavPoint(session=session, nav=Decimal("100")) for session in sessions)
    nav = {variant: points for variant in insertion_order}

    result = calculate_checkpoint_metrics(
        nav_by_variant=nav,
        turnover_by_variant={variant: Decimal("0") for variant in insertion_order},
        fees_by_variant={variant: Decimal("0") for variant in insertion_order},
        ordinal=5,
    )

    assert tuple(item.variant for item in result) == SHADOW_VARIANTS


@given(st.lists(st.decimals(min_value="-0.5", max_value="0.5", places=4), min_size=5, max_size=5))
def test_metric_bounds_hold_for_any_bounded_return_path(returns: list[Decimal]) -> None:
    nav = Decimal("100")
    points: list[ShadowNavPoint] = []
    for index, daily_return in enumerate(returns):
        nav *= Decimal(1) + daily_return
        points.append(ShadowNavPoint(session=date(2026, 1, 5) + timedelta(days=index), nav=nav))
    series = tuple(points)
    result = calculate_checkpoint_metrics(
        nav_by_variant={variant: series for variant in SHADOW_VARIANTS},
        turnover_by_variant={variant: Decimal("1") for variant in SHADOW_VARIANTS},
        fees_by_variant={variant: Decimal("0.1") for variant in SHADOW_VARIANTS},
        ordinal=5,
    )

    for item in result:
        metrics = item.metrics
        assert Decimal("-1") <= metrics.max_drawdown.value <= Decimal("0")  # type: ignore[operator]
        assert metrics.annualized_volatility.value >= 0  # type: ignore[operator]
        assert metrics.cumulative_turnover.value >= 0  # type: ignore[operator]
        assert metrics.fee_drag_index_points.value >= 0  # type: ignore[operator]


@given(st.integers(min_value=0, max_value=62), st.booleans(), st.booleans())
def test_no_terminal_result_before_sixty_three_sessions(
    completed: int,
    initial_complete: bool,
    authority_valid: bool,
) -> None:
    gate = ShadowFinalGate(
        completed_sessions=completed,
        complete_checkpoint_ordinals=tuple(ordinal for ordinal in (5, 21) if completed >= ordinal),
        checkpoint_metric_rows=6 if completed >= 21 else 3 if completed >= 5 else 0,
        initial_execution_complete=initial_complete,
        authority_binding_valid=authority_valid,
        has_data_gap=False,
        grace_exhausted=False,
    )

    assert decide_terminal_action(gate) == "continue"


@given(st.permutations((1, 2, 3)))
def test_target_weight_hash_is_deterministic_for_input_order(order: list[int]) -> None:
    weights = tuple(
        TargetWeight(instrument_id=UUID(int=value), weight=Decimal("0.2")) for value in order
    )

    assert target_weights_content_hash(weights, Decimal("0.4")) == target_weights_content_hash(
        tuple(reversed(weights)), Decimal("0.4")
    )


@given(
    st.integers(min_value=1, max_value=99),
    st.decimals(min_value="1", max_value="500", places=4),
    st.decimals(min_value="1", max_value="500", places=4),
)
def test_persisted_precision_state_reconstructs_exact_nav(
    weight_percent: int,
    open_price: Decimal,
    close_price: Decimal,
) -> None:
    instrument_id = UUID(int=1)
    bar = BoundShadowBar(
        price_bar_record_id=UUID(int=11),
        price_bar_set_authority_id=UUID(int=12),
        instrument_id=instrument_id,
        usage="execution_and_mark",
        source="fixture",
        delivery_mode="fixture",
        revision="1",
        available_at=datetime(2026, 1, 5, 21, tzinfo=UTC),
        content_hash="a" * 64,
        open=open_price,
        close=close_price,
    )
    authority = ShadowSessionInput(
        status="complete",
        reason=None,
        session=date(2026, 1, 5),
        ordinal=1,
        observed_at=datetime(2026, 1, 5, 22, tzinfo=UTC),
        late_backfill=False,
        authority_hash="b" * 64,
        bars=(bar,),
        missing_execution_inputs=(),
        events=(),
        source_summary=(),
        delivery_mode_summary=("fixture",),
        maximum_available_at=bar.available_at,
    )
    result = simulate_shadow_portfolio_session(
        previous_state=SimulationState(cash=Decimal("100"), instruments=(), receivables=()),
        authority=authority,
        targets=(
            TargetWeight(
                instrument_id=instrument_id,
                weight=Decimal(weight_percent) / Decimal(100),
            ),
        ),
        cost_model=CostModel.zero(),
        signal_session=date(2026, 1, 2),
    )
    instrument = result.state.instruments[0]
    reconstructed = canonical_decimal(
        result.state.cash + instrument.quantity * instrument.last_close  # type: ignore[operator]
    )

    assert reconstructed == result.nav
    assert simulation_state_content_hash(result.state) == simulation_state_content_hash(
        SimulationState(
            cash=result.state.cash,
            instruments=result.state.instruments,
            receivables=result.state.receivables,
        )
    )

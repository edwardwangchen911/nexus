"""Property checks for Strategy Shadow deterministic metrics."""

from datetime import date, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st
from nexus.evolution.shadow.metrics import calculate_strategy_shadow_metrics
from nexus.evolution.shadow.models import StrategyShadowSessionPointV1


@given(
    st.lists(
        st.decimals(
            min_value=Decimal("90"),
            max_value=Decimal("110"),
            places=6,
            allow_nan=False,
            allow_infinity=False,
        ),
        min_size=5,
        max_size=5,
    )
)
def test_metric_hash_is_deterministic_for_exact_ordered_facts(
    navs: list[Decimal],
) -> None:
    points = tuple(
        StrategyShadowSessionPointV1(
            session=date(2026, 9, 1) + timedelta(days=index),
            ordinal=index + 1,
            nav=nav,
            turnover=Decimal("0"),
            fees=Decimal("0"),
            filled_order_count=0,
            hard_risk_violation=False,
            authority_hash=f"{index + 1:064x}",
        )
        for index, nav in enumerate(navs)
    )

    first = calculate_strategy_shadow_metrics(points, checkpoint_ordinal=5)
    second = calculate_strategy_shadow_metrics(points, checkpoint_ordinal=5)

    assert first == second
    assert first.metrics_hash == second.metrics_hash

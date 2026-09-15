from decimal import Decimal

from nexus.paper.read_service import market_benchmark_row_from_closes


def test_market_benchmark_uses_close_path_for_return_and_max_drawdown() -> None:
    row = market_benchmark_row_from_closes(
        initial_capital=Decimal("10000"),
        closes=(Decimal("100"), Decimal("120"), Decimal("90"), Decimal("110")),
    )

    assert row.key == "market_benchmark"
    assert row.data_state == "READY"
    assert Decimal(row.nav or "0") == Decimal("11000")
    assert Decimal(row.total_return_ratio or "0") == Decimal("0.1")
    assert Decimal(row.max_drawdown_ratio or "0") == Decimal("0.25")
    assert row.turnover_ratio == "0"
    assert row.total_cost == "0"
    assert row.reason_code == "PERSISTED_MARKET_BENCHMARK"


def test_market_benchmark_stays_unavailable_without_two_session_closes() -> None:
    row = market_benchmark_row_from_closes(
        initial_capital=Decimal("10000"),
        closes=(Decimal("100"),),
    )

    assert row.data_state == "UNAVAILABLE"
    assert row.nav is None
    assert row.reason_code == "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE"

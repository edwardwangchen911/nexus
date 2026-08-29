from nexus.diagnostics.assembler import _market_quality


def test_current_fosun_raw_series_remains_partial() -> None:
    assert (
        _market_quality(
            policy_version="source_policy_v2",
            benchmark_available=True,
            selected_count=2,
            position_count=2,
        )
        == "PARTIAL"
    )


def test_historical_adjusted_series_can_pass() -> None:
    assert (
        _market_quality(
            policy_version="source_policy_v1",
            benchmark_available=True,
            selected_count=2,
            position_count=2,
        )
        == "PASS"
    )

"""Versioned public regulatory-fee references for scenario analysis."""

from datetime import UTC, datetime, timedelta

from nexus.optimizer.scenario_cost_evidence import regulatory_fee_reference


def test_returns_normalized_reference_only_inside_verified_coverage() -> None:
    evidence = regulatory_fee_reference(datetime(2026, 9, 10, tzinfo=UTC))

    assert evidence is not None
    assert evidence.version == "us_regulatory_fee_reference_2026_verified_20260911_v1"
    assert evidence.available_at == datetime(2026, 2, 28, tzinfo=UTC)
    assert evidence.coverage_start == datetime(2026, 4, 4, tzinfo=UTC)
    assert evidence.verified_through == datetime(2026, 9, 11, 23, 59, 59, tzinfo=UTC)
    assert evidence.evidence_hash == (
        "56c13aeb742a7ad616834408ac63dea4210f22fa6c7443ccbb7c082782a60138"
    )
    assert {source.authority for source in evidence.sources} == {"SEC", "FINRA", "FOSUN"}
    assert evidence.source_material_status == "normalized_summary_only"
    assert evidence.customer_charge_status == "broker_pass_through_unverified"


def test_rejects_pre_effective_and_future_valuation_times() -> None:
    assert regulatory_fee_reference(datetime(2026, 4, 3, 23, 59, 59, tzinfo=UTC)) is None
    boundary = datetime(2026, 9, 11, 23, 59, 59, tzinfo=UTC)
    assert regulatory_fee_reference(boundary) is not None
    assert regulatory_fee_reference(boundary + timedelta(microseconds=1)) is None

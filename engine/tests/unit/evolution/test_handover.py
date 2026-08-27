"""Deterministic internal-paper handover economics and hard-risk gates."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.evolution.handover import (
    HandoverAuthorityV1,
    HandoverChallengerV1,
    HandoverInstrumentV1,
    HandoverSectorExposureV1,
    HandoverSnapshotV1,
    HandoverTargetV1,
    evaluate_handover,
)
from nexus.risk.policy import RiskPolicy

SNAPSHOT_ID = UUID("019d0000-0000-7000-8000-000000007001")
DEFINITION_ID = UUID("019d0000-0000-7000-8000-000000007002")
INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-000000007003")
SOURCE_ID = UUID("019d0000-0000-7000-8000-000000007004")
COST_ID = UUID("019d0000-0000-7000-8000-000000007005")
RISK_ID = UUID("019d0000-0000-7000-8000-000000007006")
NOW = datetime(2026, 10, 1, 21, tzinfo=UTC)


def _snapshot(*, excess: str = "0.01") -> HandoverSnapshotV1:
    return HandoverSnapshotV1(
        paper_snapshot_id=SNAPSHOT_ID,
        nav=Decimal("100"),
        current_cash_weight=Decimal("0.75"),
        drawdown_state="unlocked",
        instruments=(
            HandoverInstrumentV1(
                instrument_id=INSTRUMENT_ID,
                current_weight=Decimal("0.25"),
                market="US",
                currency="USD",
                asset_type="common_stock",
                reference_close=Decimal("100"),
                adv20_usd=Decimal("100000000"),
                available_at=NOW,
                blocked=False,
                sector_exposures=(
                    HandoverSectorExposureV1(sector="information_technology", weight=Decimal(1)),
                ),
            ),
        ),
        shadow_excess_return=Decimal(excess),
    )


def _challenger(*, weight: str = "0.125") -> HandoverChallengerV1:
    target = Decimal(weight)
    return HandoverChallengerV1(
        strategy_definition_id=DEFINITION_ID,
        target_cash_weight=Decimal(1) - target,
        targets=(HandoverTargetV1(instrument_id=INSTRUMENT_ID, weight=target),),
    )


def _authority(*, rate: str = "0.01", late: bool = False) -> HandoverAuthorityV1:
    return HandoverAuthorityV1(
        source_policy_id=SOURCE_ID,
        cost_policy_id=COST_ID,
        risk_policy_id=RISK_ID,
        decision_time=NOW,
        available_at=NOW + timedelta(seconds=1) if late else NOW,
        estimated_cost_rate=Decimal(rate),
        complete=True,
        authority_hash="a" * 64,
    )


def test_handover_passes_only_when_shadow_excess_covers_cost_and_risk() -> None:
    result = evaluate_handover(_snapshot(), _challenger(), _authority(), RiskPolicy.v1())

    assert result.status == "PASS"
    assert result.turnover == Decimal("0.125000000000")
    assert result.estimated_handover_cost_ratio == Decimal("0.001250000000")
    assert result.economic_residual == Decimal("0.008750000000")
    assert result.risk_violations == ()
    assert result.live_execution_enabled is False


def test_handover_cost_or_hard_risk_fails_and_late_authority_is_unknown() -> None:
    expensive = evaluate_handover(
        _snapshot(excess="0.001"), _challenger(), _authority(), RiskPolicy.v1()
    )
    concentrated = evaluate_handover(
        _snapshot(), _challenger(weight="0.40"), _authority(), RiskPolicy.v1()
    )
    late = evaluate_handover(_snapshot(), _challenger(), _authority(late=True), RiskPolicy.v1())

    assert expensive.status == "FAIL"
    assert expensive.reason_code == "HANDOVER_GATES_FAILED"
    assert concentrated.status == "FAIL"
    assert "SINGLE_NAME_LIMIT" in concentrated.risk_violations
    assert late.status == "UNKNOWN"
    assert late.reason_code == "PROMOTION_WAITING_HANDOVER"
    assert late.turnover is None

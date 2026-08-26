from decimal import Decimal

import pytest
from nexus.risk.policy import RiskPolicy, select_active_policy, verify_policy_content_hash
from pydantic import ValidationError


def test_v1_policy_has_the_reviewed_hash_and_safe_limits() -> None:
    policy = RiskPolicy.v1()

    assert policy.version == "risk_policy_v1"
    assert policy.content_hash == (
        "70f395ac13218d3e19b65e1717b3e2fdcbc31eaa5614ff60613f1c509dd7280b"
    )
    assert policy.single_name_limit == Decimal("0.15")
    assert policy.sector_limit == Decimal("0.30")
    assert policy.minimum_cash_ratio == Decimal("0.10")
    assert policy.drawdown_lock_ratio == Decimal("0.08")
    assert policy.drawdown_recovery_ratio == Decimal("0.06")
    assert policy.minimum_buy_notional == Decimal("100")
    assert policy.buy_reserve_markup == Decimal("0.01")
    assert policy.commission == Decimal("0")
    assert policy.supported_market == "US"
    assert policy.supported_currency == "USD"
    assert policy.supported_asset_types == ("ETF", "common_stock")


def test_policy_hash_verification_detects_any_limit_change() -> None:
    policy = RiskPolicy.v1()
    changed = policy.model_copy(update={"single_name_limit": Decimal("0.16")})

    assert verify_policy_content_hash(policy) is policy
    with pytest.raises(ValueError, match="RISK_POLICY_INVALID"):
        verify_policy_content_hash(changed)


def test_policy_rejects_binary_float_and_inverted_drawdown_thresholds() -> None:
    values = RiskPolicy.v1().model_dump()
    values["content_hash"] = "a" * 64
    values["single_name_limit"] = 0.15
    with pytest.raises(ValidationError, match="binary float"):
        RiskPolicy(**values)

    values = RiskPolicy.v1().model_dump()
    values["content_hash"] = "a" * 64
    values["drawdown_recovery_ratio"] = Decimal("0.08")
    with pytest.raises(ValidationError, match="strictly below"):
        RiskPolicy(**values)


def test_policy_rejects_unsafe_or_unknown_scope() -> None:
    values = RiskPolicy.v1().model_dump()
    values["content_hash"] = "a" * 64
    values["minimum_cash_ratio"] = Decimal("0")
    with pytest.raises(ValidationError, match="minimum cash"):
        RiskPolicy(**values)

    values = RiskPolicy.v1().model_dump()
    values["content_hash"] = "a" * 64
    values["supported_currency"] = "HKD"
    with pytest.raises(ValidationError, match="USD"):
        RiskPolicy(**values)


def test_select_active_policy_requires_exactly_one_verified_record() -> None:
    active = RiskPolicy.v1()
    inactive = active.model_copy(
        update={
            "active": False,
        }
    )

    assert select_active_policy((inactive, active)) is active
    with pytest.raises(ValueError, match="RISK_POLICY_INVALID"):
        select_active_policy(())
    with pytest.raises(ValueError, match="RISK_POLICY_INVALID"):
        select_active_policy((active, active))

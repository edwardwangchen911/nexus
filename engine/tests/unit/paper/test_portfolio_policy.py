import hashlib
from datetime import time
from decimal import Decimal

import pytest
from nexus.paper.portfolio_policy import (
    PORTFOLIO_ALLOCATION_POLICY_HASH,
    PORTFOLIO_ALLOCATION_V1,
    PortfolioAllocationPolicyV1,
    canonical_portfolio_allocation_policy_hash,
)
from pydantic import ValidationError

_BASELINE_POLICY_VALUES: dict[str, object] = {
    "version": "portfolio_allocation_v1",
    "core_target": Decimal("0.10"),
    "tactical_target": Decimal("0.05"),
    "target_cash_floor": Decimal("0.20"),
    "single_name_target_cap": Decimal("0.15"),
    "max_increases": 2,
    "adjustment_step": Decimal("0.05"),
    "turnover_cap": Decimal("0.10"),
    "minimum_target_delta": Decimal("0.02"),
    "minimum_notional_usd": Decimal("100"),
    "rebalance_drift": Decimal("0.03"),
    "research_valid_days": 7,
    "decision_local_time": time(10, 0),
    "decision_timezone": "America/New_York",
}
_EXPECTED_CANONICAL_POLICY_JSON = (
    '{"adjustment_step":"0.050000000000","core_target":"0.100000000000",'
    '"decision_local_time":"10:00:00","decision_timezone":"America/New_York",'
    '"max_increases":2,"minimum_notional_usd":"100.000000000000",'
    '"minimum_target_delta":"0.020000000000","rebalance_drift":"0.030000000000",'
    '"research_valid_days":7,"schema":"portfolio_allocation_policy_sha256_v1",'
    '"single_name_target_cap":"0.150000000000","tactical_target":"0.050000000000",'
    '"target_cash_floor":"0.200000000000","turnover_cap":"0.100000000000",'
    '"version":"portfolio_allocation_v1"}'
)
_EXPECTED_CANONICAL_POLICY_HASH = (
    "d7b719f47edbf3cb7ebb40dbe555a51f717ca581162a72d05c23391b8b083dd7"
)


def test_portfolio_allocation_v1_is_exact_and_content_addressed() -> None:
    """Catches a policy-value drift or a hash that omits frozen content."""
    policy = PORTFOLIO_ALLOCATION_V1

    assert policy.core_target == Decimal("0.10")
    assert policy.tactical_target == Decimal("0.05")
    assert policy.target_cash_floor == Decimal("0.20")
    assert policy.single_name_target_cap == Decimal("0.15")
    assert policy.max_increases == 2
    assert policy.adjustment_step == Decimal("0.05")
    assert policy.turnover_cap == Decimal("0.10")
    assert policy.minimum_target_delta == Decimal("0.02")
    assert policy.minimum_notional_usd == Decimal("100")
    assert policy.rebalance_drift == Decimal("0.03")
    assert policy.research_valid_days == 7
    assert policy.decision_local_time.isoformat() == "10:00:00"
    assert policy.decision_timezone == "America/New_York"
    assert canonical_portfolio_allocation_policy_hash(policy) == (
        PORTFOLIO_ALLOCATION_POLICY_HASH
    )


def test_portfolio_allocation_policy_rejects_binary_float() -> None:
    """Catches accidental binary-float coercion at the policy boundary."""
    with pytest.raises(TypeError):
        PortfolioAllocationPolicyV1(core_target=0.10)


def test_portfolio_allocation_policy_hash_is_sensitive_to_each_serialized_field() -> None:
    """Catches canonical hashes that silently omit one policy field."""
    policy = PortfolioAllocationPolicyV1.model_validate(_BASELINE_POLICY_VALUES)
    mutations: dict[str, object] = {
        "core_target": Decimal("0.11"),
        "tactical_target": Decimal("0.06"),
        "target_cash_floor": Decimal("0.21"),
        "single_name_target_cap": Decimal("0.14"),
        "max_increases": 3,
        "adjustment_step": Decimal("0.04"),
        "turnover_cap": Decimal("0.09"),
        "minimum_target_delta": Decimal("0.01"),
        "minimum_notional_usd": Decimal("101"),
        "rebalance_drift": Decimal("0.04"),
        "research_valid_days": 8,
        "decision_local_time": time(10, 30),
    }

    for field, value in mutations.items():
        changed_values = dict(_BASELINE_POLICY_VALUES)
        changed_values[field] = value
        changed = PortfolioAllocationPolicyV1.model_validate(changed_values)
        assert canonical_portfolio_allocation_policy_hash(changed) != (
            canonical_portfolio_allocation_policy_hash(policy)
        ), field


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("version", "portfolio_allocation_v2"),
        ("decision_timezone", "America/Chicago"),
    ),
)
def test_portfolio_allocation_policy_rejects_alternate_fixed_literals(
    field: str, value: str
) -> None:
    """Catches accidental widening of the frozen version and timezone contracts."""
    changed_values = dict(_BASELINE_POLICY_VALUES)
    changed_values[field] = value

    with pytest.raises(ValidationError):
        PortfolioAllocationPolicyV1.model_validate(changed_values)


def test_portfolio_allocation_policy_hash_matches_hand_authored_payload() -> None:
    """Catches omission of fixed literals from production canonical serialization."""
    expected_digest = hashlib.sha256(
        _EXPECTED_CANONICAL_POLICY_JSON.encode("utf-8")
    ).hexdigest()

    assert expected_digest == _EXPECTED_CANONICAL_POLICY_HASH
    assert canonical_portfolio_allocation_policy_hash(PORTFOLIO_ALLOCATION_V1) == (
        _EXPECTED_CANONICAL_POLICY_HASH
    )

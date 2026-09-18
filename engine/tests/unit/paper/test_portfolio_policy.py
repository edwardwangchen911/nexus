from datetime import time
from decimal import Decimal

import pytest
from nexus.paper.portfolio_policy import (
    PORTFOLIO_ALLOCATION_POLICY_HASH,
    PORTFOLIO_ALLOCATION_V1,
    PortfolioAllocationPolicyV1,
    canonical_portfolio_allocation_policy_hash,
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
    policy = PORTFOLIO_ALLOCATION_V1
    mutations: dict[str, object] = {
        "version": "portfolio_allocation_v2",
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
        "decision_timezone": "America/Chicago",
    }

    for field, value in mutations.items():
        changed = policy.model_copy(update={field: value})
        assert canonical_portfolio_allocation_policy_hash(changed) != (
            canonical_portfolio_allocation_policy_hash(policy)
        ), field

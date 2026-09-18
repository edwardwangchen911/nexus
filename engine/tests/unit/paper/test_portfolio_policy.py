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
    assert canonical_portfolio_allocation_policy_hash(policy) == (
        PORTFOLIO_ALLOCATION_POLICY_HASH
    )


def test_portfolio_allocation_policy_rejects_binary_float() -> None:
    """Catches accidental binary-float coercion at the policy boundary."""
    with pytest.raises(TypeError):
        PortfolioAllocationPolicyV1(core_target=0.10)

"""Frozen deterministic portfolio-allocation policy v1."""

from __future__ import annotations

import hashlib
import json
from datetime import time
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator

from nexus.risk.models import exact_decimal

_V1_CONTENT_HASH = "461253c4d8b5aeb5db834f0d11f85c092ef46c03fb01838389516b202e67e949"
_DECIMAL_QUANTUM = Decimal("0.000000000001")


class PortfolioAllocationPolicyV1(BaseModel):
    """Immutable, exact-decimal allocation parameters for portfolio synthesis."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal["portfolio_allocation_v1"] = "portfolio_allocation_v1"
    core_target: Decimal = Decimal("0.10")
    tactical_target: Decimal = Decimal("0.05")
    target_cash_floor: Decimal = Decimal("0.20")
    single_name_target_cap: Decimal = Decimal("0.15")
    max_increases: int = 2
    adjustment_step: Decimal = Decimal("0.05")
    turnover_cap: Decimal = Decimal("0.10")
    minimum_target_delta: Decimal = Decimal("0.02")
    minimum_notional_usd: Decimal = Decimal("100")
    rebalance_drift: Decimal = Decimal("0.03")
    research_valid_days: int = 7
    decision_local_time: time = time(10, 0)

    @field_validator(
        "core_target",
        "tactical_target",
        "target_cash_floor",
        "single_name_target_cap",
        "adjustment_step",
        "turnover_cap",
        "minimum_target_delta",
        "minimum_notional_usd",
        "rebalance_drift",
        mode="before",
    )
    @classmethod
    def _decimal_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        if isinstance(value, float):
            raise TypeError(f"{info.field_name or 'policy value'} must not contain a binary float")
        return exact_decimal(value, field=info.field_name or "policy value")


PORTFOLIO_ALLOCATION_V1 = PortfolioAllocationPolicyV1()


def _decimal_string(value: Decimal) -> str:
    return format(value.quantize(_DECIMAL_QUANTUM), ".12f")


def canonical_portfolio_allocation_policy_hash(
    policy: PortfolioAllocationPolicyV1,
) -> str:
    payload = {
        "schema": "portfolio_allocation_policy_sha256_v1",
        "version": policy.version,
        "core_target": _decimal_string(policy.core_target),
        "tactical_target": _decimal_string(policy.tactical_target),
        "target_cash_floor": _decimal_string(policy.target_cash_floor),
        "single_name_target_cap": _decimal_string(policy.single_name_target_cap),
        "max_increases": policy.max_increases,
        "adjustment_step": _decimal_string(policy.adjustment_step),
        "turnover_cap": _decimal_string(policy.turnover_cap),
        "minimum_target_delta": _decimal_string(policy.minimum_target_delta),
        "minimum_notional_usd": _decimal_string(policy.minimum_notional_usd),
        "rebalance_drift": _decimal_string(policy.rebalance_drift),
        "research_valid_days": policy.research_valid_days,
        "decision_local_time": policy.decision_local_time.isoformat(),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


PORTFOLIO_ALLOCATION_POLICY_HASH = _V1_CONTENT_HASH

"""Immutable v1 hard-risk policy and deterministic drawdown transitions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from nexus.risk.models import DrawdownState, Hash64, OrderSide, exact_decimal

_V1_CONTENT_HASH = "70f395ac13218d3e19b65e1717b3e2fdcbc31eaa5614ff60613f1c509dd7280b"


class RiskPolicy(BaseModel):
    """A verified, versioned policy; active selection is not part of policy content."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal["risk_policy_v1"]
    active: bool
    single_name_limit: Decimal
    sector_limit: Decimal
    minimum_cash_ratio: Decimal
    drawdown_lock_ratio: Decimal
    drawdown_recovery_ratio: Decimal
    minimum_buy_notional: Decimal
    buy_reserve_markup: Decimal
    commission: Decimal
    supported_market: Literal["US"]
    supported_currency: Literal["USD"]
    supported_asset_types: tuple[Literal["ETF", "common_stock"], ...]
    content_hash: Hash64

    @field_validator(
        "single_name_limit",
        "sector_limit",
        "minimum_cash_ratio",
        "drawdown_lock_ratio",
        "drawdown_recovery_ratio",
        "minimum_buy_notional",
        "buy_reserve_markup",
        "commission",
        mode="before",
    )
    @classmethod
    def _amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "policy value")

    @model_validator(mode="after")
    def _safe_thresholds(self) -> RiskPolicy:
        ratios = (
            self.single_name_limit,
            self.sector_limit,
            self.minimum_cash_ratio,
            self.drawdown_lock_ratio,
            self.drawdown_recovery_ratio,
        )
        if any(not Decimal(0) < value <= Decimal(1) for value in ratios):
            raise ValueError("risk ratios and minimum cash must be positive and at most one")
        if self.single_name_limit > self.sector_limit:
            raise ValueError("single-name limit cannot exceed sector limit")
        if self.drawdown_recovery_ratio >= self.drawdown_lock_ratio:
            raise ValueError("drawdown recovery must be strictly below drawdown lock")
        if self.minimum_buy_notional <= 0:
            raise ValueError("minimum buy notional must be positive")
        if not Decimal(0) <= self.buy_reserve_markup <= Decimal("0.10"):
            raise ValueError("buy reserve markup must be between zero and ten percent")
        if self.commission < 0:
            raise ValueError("commission cannot be negative")
        if tuple(sorted(self.supported_asset_types)) != ("ETF", "common_stock"):
            raise ValueError("supported asset types must be exactly ETF and common_stock")
        return self

    @classmethod
    def v1(cls) -> RiskPolicy:
        return cls(
            version="risk_policy_v1",
            active=True,
            single_name_limit=Decimal("0.15"),
            sector_limit=Decimal("0.30"),
            minimum_cash_ratio=Decimal("0.10"),
            drawdown_lock_ratio=Decimal("0.08"),
            drawdown_recovery_ratio=Decimal("0.06"),
            minimum_buy_notional=Decimal("100"),
            buy_reserve_markup=Decimal("0.01"),
            commission=Decimal("0"),
            supported_market="US",
            supported_currency="USD",
            supported_asset_types=("ETF", "common_stock"),
            content_hash=_V1_CONTENT_HASH,
        )


def _decimal_string(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000000000001")), ".12f")


def policy_content_hash(policy: RiskPolicy) -> str:
    payload = {
        "schema": "risk_policy_sha256_v1",
        "version": policy.version,
        "single_name_limit": _decimal_string(policy.single_name_limit),
        "sector_limit": _decimal_string(policy.sector_limit),
        "minimum_cash_ratio": _decimal_string(policy.minimum_cash_ratio),
        "drawdown_lock_ratio": _decimal_string(policy.drawdown_lock_ratio),
        "drawdown_recovery_ratio": _decimal_string(policy.drawdown_recovery_ratio),
        "minimum_buy_notional": _decimal_string(policy.minimum_buy_notional),
        "buy_reserve_markup": _decimal_string(policy.buy_reserve_markup),
        "commission": _decimal_string(policy.commission),
        "supported_market": policy.supported_market,
        "supported_currency": policy.supported_currency,
        "supported_asset_types": sorted(policy.supported_asset_types),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def verify_policy_content_hash(policy: RiskPolicy) -> RiskPolicy:
    if policy.content_hash != policy_content_hash(policy):
        raise ValueError("RISK_POLICY_INVALID")
    return policy


def select_active_policy(policies: Sequence[RiskPolicy]) -> RiskPolicy:
    active = tuple(policy for policy in policies if policy.active)
    if len(active) != 1:
        raise ValueError("RISK_POLICY_INVALID")
    return verify_policy_content_hash(active[0])


def _drawdown(value: object) -> Decimal:
    result = exact_decimal(value, field="drawdown")
    if not Decimal(0) <= result <= Decimal(1):
        raise ValueError("drawdown must be between zero and one")
    return result


def transition_drawdown(
    state: DrawdownState,
    current_drawdown: Decimal,
    *,
    user_acknowledged: bool,
    policy: RiskPolicy | None = None,
) -> DrawdownState:
    """Fold one trusted drawdown observation into the hard-lock state."""
    verified = verify_policy_content_hash(policy or RiskPolicy.v1())
    drawdown = _drawdown(current_drawdown)
    if state == "unlocked":
        return "locked" if drawdown >= verified.drawdown_lock_ratio else "unlocked"
    if state == "locked":
        if drawdown < verified.drawdown_recovery_ratio:
            return "recovery_eligible"
        return "locked"
    if drawdown >= verified.drawdown_recovery_ratio:
        return "locked"
    if user_acknowledged:
        return "unlocked"
    return "recovery_eligible"


def drawdown_allows_candidate(
    state: DrawdownState,
    *,
    side: OrderSide,
    is_reduction: bool,
) -> bool:
    if state == "unlocked":
        return True
    return side == "sell" and is_reduction

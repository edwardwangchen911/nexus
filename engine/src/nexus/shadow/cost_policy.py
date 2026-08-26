"""Fail-closed adaptation of the frozen Task 12 cost authority."""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from nexus.optimizer.cost_policy import (
    COST_POLICY_EVIDENCE_REFS,
    COST_POLICY_KEY,
    COST_POLICY_VERSION,
)
from nexus.persistence.models.optimization import CostPolicyRecord
from nexus.simulation.costs import CostModel

_PARAMETER_KEYS = frozenset(
    {"commission_bps", "slippage_bps", "tax_treatment"}
)


def cost_model_from_policy(
    policy: CostPolicyRecord,
    *,
    started_at: datetime,
) -> CostModel:
    """Return the one admitted v1 cost model, never UI or environment values."""
    if (
        started_at.tzinfo is None
        or started_at.utcoffset() != UTC.utcoffset(started_at)
        or policy.key != COST_POLICY_KEY
        or policy.version != COST_POLICY_VERSION
        or policy.schema_version != "1"
        or set(policy.parameters) != _PARAMETER_KEYS
        or policy.parameters.get("tax_treatment") != "unknown"
        or tuple(policy.evidence_refs) != COST_POLICY_EVIDENCE_REFS
        or policy.available_at > started_at
    ):
        raise ValueError("COST_POLICY_UNSUPPORTED")
    try:
        commission = _decimal(policy.parameters["commission_bps"])
        slippage = _decimal(policy.parameters["slippage_bps"])
        return CostModel(
            commission_bps=commission,
            spread_bps=Decimal("0"),
            slippage_bps=slippage,
        )
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("COST_POLICY_UNSUPPORTED") from None


def _decimal(value: object) -> Decimal:
    if isinstance(value, float | bool) or not isinstance(value, str | int | Decimal):
        raise TypeError("cost policy decimals are invalid")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("cost policy decimals must be finite")
    return result

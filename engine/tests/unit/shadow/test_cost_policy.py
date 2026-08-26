"""Versioned adaptation of the immutable optimizer cost authority."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from nexus.persistence.models.optimization import CostPolicyRecord
from nexus.shadow.cost_policy import cost_model_from_policy

_STARTED_AT = datetime(2026, 8, 26, 12, tzinfo=UTC)


def _policy(**overrides: object) -> CostPolicyRecord:
    values: dict[str, object] = {
        "id": uuid4(),
        "key": "cost_model",
        "version": "cost_policy_v1",
        "schema_version": "1",
        "parameters": {
            "commission_bps": "1.25",
            "slippage_bps": "2.50",
            "tax_treatment": "unknown",
        },
        "evidence_refs": ["cost_policy_v1_methodology"],
        "available_at": _STARTED_AT - timedelta(days=1),
    }
    values.update(overrides)
    return CostPolicyRecord(**values)  # type: ignore[arg-type]


def test_cost_policy_v1_adapts_exact_decimal_costs_with_zero_spread() -> None:
    model = cost_model_from_policy(_policy(), started_at=_STARTED_AT)

    assert model.commission_bps == Decimal("1.25")
    assert model.spread_bps == Decimal("0")
    assert model.slippage_bps == Decimal("2.50")


@pytest.mark.parametrize(
    "overrides",
    (
        {"key": "transaction_cost"},
        {"version": "cost_policy_v2"},
        {"schema_version": "2"},
        {"evidence_refs": []},
        {"evidence_refs": ["different-methodology"]},
        {"available_at": _STARTED_AT + timedelta(seconds=1)},
    ),
)
def test_cost_policy_rejects_wrong_or_future_authority(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="COST_POLICY_UNSUPPORTED"):
        cost_model_from_policy(_policy(**overrides), started_at=_STARTED_AT)


@pytest.mark.parametrize(
    "parameters",
    (
        {"commission_bps": "1", "slippage_bps": "2"},
        {
            "commission_bps": "1",
            "slippage_bps": "2",
            "tax_treatment": "unknown",
            "spread_bps": "3",
        },
        {
            "commission_bps": 1.0,
            "slippage_bps": "2",
            "tax_treatment": "unknown",
        },
        {
            "commission_bps": "NaN",
            "slippage_bps": "2",
            "tax_treatment": "unknown",
        },
        {
            "commission_bps": "1",
            "slippage_bps": "2",
            "tax_treatment": "estimated",
        },
    ),
)
def test_cost_policy_rejects_noncanonical_parameter_documents(
    parameters: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="COST_POLICY_UNSUPPORTED"):
        cost_model_from_policy(
            _policy(parameters=parameters), started_at=_STARTED_AT
        )


def test_cost_policy_requires_utc_start_time() -> None:
    with pytest.raises(ValueError, match="COST_POLICY_UNSUPPORTED"):
        cost_model_from_policy(
            _policy(), started_at=datetime(2026, 8, 26, 12)
        )

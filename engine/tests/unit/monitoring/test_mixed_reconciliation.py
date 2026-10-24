from uuid import UUID

import pytest
from nexus.monitoring.batch_inputs import BatchInputs
from nexus.monitoring.mixed_context import MixedMonitorContext
from nexus.monitoring.mixed_reconciliation import frozen_predecessor


def test_each_kind_uses_its_own_frozen_parent_and_absent_kind_has_no_parent():
    primary, secondary = UUID(int=1), UUID(int=2)
    context = MixedMonitorContext.model_construct(
        plan_id=primary,
        materials=BatchInputs.model_construct(
            parents=(
                {"kind": "price", "plan_id": str(primary)},
                {"kind": "disclosure", "plan_id": str(secondary)},
            )
        ),
    )
    assert frozen_predecessor(context, "price") == primary
    assert frozen_predecessor(context, "disclosure") == secondary
    empty = context.model_copy(
        update={
            "materials": BatchInputs.model_construct(
                parents=({"kind": "disclosure", "plan_id": str(secondary)},)
            )
        }
    )
    assert frozen_predecessor(empty, "price") is None
    duplicate = context.model_copy(
        update={
            "materials": BatchInputs.model_construct(
                parents=(
                    {"kind": "price", "plan_id": str(primary)},
                    {"kind": "price", "plan_id": str(secondary)},
                )
            )
        }
    )
    with pytest.raises(ValueError, match="MONITOR_PLAN_CONFLICT"):
        frozen_predecessor(duplicate, "price")

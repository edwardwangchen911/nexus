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


@pytest.mark.parametrize(
    "bound,changed,late_record,late_effective,expected",
    [
        (True, False, False, False, False),
        (True, True, False, False, True),
        (False, False, False, False, False),
        (False, False, True, False, True),
        (False, False, False, True, True),
    ],
)
def test_unfrozen_kind_is_not_absence_and_postfreeze_plan_is_a_conflict(
    bound, changed, late_record, late_effective, expected
):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from nexus.monitoring.mixed_reconciliation import frozen_predecessor_changed

    at = datetime(2026, 10, 23, 14, tzinfo=UTC)
    original = UUID(int=1)
    context = MixedMonitorContext.model_construct(
        materials=BatchInputs.model_construct(
            prepared_at=at,
            parents=({"kind": "price", "plan_id": str(original)},) if bound else (),
        )
    )
    current = SimpleNamespace(
        id=UUID(int=2) if changed else original,
        created_at=at + timedelta(seconds=1 if late_record else -1),
        effective_at=at + timedelta(seconds=1 if late_effective else -1),
    )
    assert frozen_predecessor_changed(context, "price", current) is expected
    assert frozen_predecessor_changed(context, "price", None) is bound

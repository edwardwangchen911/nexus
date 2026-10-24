from types import SimpleNamespace
from uuid import UUID

import pytest


def test_daily_replacement_uses_current_frozen_predecessors_not_historical_parent():
    from nexus.monitoring.daily_v2_reconciliation import frozen_daily_predecessor

    current, historical = UUID(int=3), UUID(int=1)
    context = SimpleNamespace(
        model_dump=lambda **_: {
            "predecessors": {
                "price": {"plan_id": str(current), "generation": 3},
                "disclosure": {"plan_id": None, "generation": None},
            },
            "trigger_checks": [{"plan_id": str(historical)}],
        }
    )
    assert frozen_daily_predecessor(context, "price") == (current, 3)
    assert frozen_daily_predecessor(context, "disclosure") == (None, None)
    with pytest.raises(ValueError, match="MONITOR_DAILY_PREDECESSOR_INVALID"):
        frozen_daily_predecessor(context, "unknown")

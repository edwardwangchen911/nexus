"""Stage result labels never infer a fill from a risk rejection."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.paper.economic_execution import _summary
from nexus.paper.economic_later_window import _states


def test_rejected_plan_side_is_not_reported_as_filled():
    window = SimpleNamespace(
        id=uuid4(),
        qualification={
            "plan_stage": "post_sale_v1",
            "order_states": [
                {"lane": "incumbent", "status": "risk_rejected"},
                {"lane": "challenger", "status": "submitted"},
            ],
        },
    )
    result = _summary(uuid4(), window, [{"lane": "challenger", "action": "filled"}], replayed=False)
    assert result["status"] == "partial"
    assert result["comparison_status"] == "incomplete"
    assert result["lanes"] == [
        {"lane": "incumbent", "status": "risk_rejected", "order_count": 0},
        {"lane": "challenger", "status": "filled", "order_count": 1},
    ]


@pytest.mark.asyncio
async def test_new_submitted_stage_cannot_hide_existing_native_authority():
    now = datetime(2027, 1, 4, 14, tzinfo=UTC)
    event = SimpleNamespace(
        event_seq=3,
        to_status="submitted",
        source_fill_id=None,
        payload_hash="a" * 64,
        created_at=now,
        occurred_at=now,
    )
    order = SimpleNamespace(risk_outcome="approved")
    link = SimpleNamespace(order_id=uuid4(), lane="incumbent")

    class Session:
        async def scalars(self, query):
            return [event]

        async def get(self, model, key):
            return order

        async def scalar(self, query):
            if "paper_order_events" in str(query):
                return event
            assert "paper_execution_window_authorities" in str(query)
            return uuid4()

    with pytest.raises(ValueError, match="ECONOMIC_WINDOW_NOT_READY"):
        await _states(Session(), SimpleNamespace(eligible_at=now), plan_links=[link])


@pytest.mark.parametrize("action", ["unknown", "unavailable", "not_attempted"])
def test_rejected_plan_side_preserves_unknown_or_failed_execution(action):
    window = SimpleNamespace(
        id=uuid4(),
        qualification={"order_states": [{"lane": "incumbent", "status": "risk_rejected"}]},
    )
    result = _summary(uuid4(), window, [{"lane": "incumbent", "action": action}], replayed=False)
    expected = "unknown" if action == "unknown" else "unavailable"
    assert result["status"] == expected
    assert result["lanes"][0]["status"] == expected

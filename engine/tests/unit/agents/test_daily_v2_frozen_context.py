from datetime import UTC, datetime

import pytest
from nexus.agents.paper_context import AgentPaperResearchContext
from nexus.monitoring.daily_v2_context import DailyEventGap
from pydantic import ValidationError
from unit.agents.test_paper_context import _context


def test_daily_v2_none_preserves_legacy_bytes():
    old = _context("a" * 64)
    restored = AgentPaperResearchContext.model_validate(
        {
            **old.model_dump(mode="json"),
            "pending_monitoring_v2": None,
            "pending_monitoring_v2_gap": None,
        }
    )
    assert restored.model_dump_json() == old.model_dump_json()
    assert "pending_monitoring_v2" not in restored.model_dump(mode="json")


def test_daily_v2_gap_is_frozen_and_does_not_grant_event_coverage():
    old = _context("a" * 64)
    gap = DailyEventGap(
        observed_at=old.decision_time, pending_count=33, reason="MONITOR_PENDING_OVERFLOW"
    )
    value = AgentPaperResearchContext.model_validate(
        {
            **old.model_dump(mode="json"),
            "pending_monitoring_v2_gap": gap,
        }
    )
    assert value.pending_monitoring is None and value.pending_monitoring_v2 is None
    with pytest.raises(ValidationError):
        AgentPaperResearchContext.model_validate(
            {
                **value.model_dump(mode="json"),
                "pending_monitoring_gap": {"observed_at": old.decision_time, "pending_count": 33},
            }
        )
    with pytest.raises(ValidationError):
        AgentPaperResearchContext.model_validate(
            {
                **value.model_dump(mode="json"),
                "pending_monitoring_v2_gap": gap.model_copy(
                    update={"observed_at": datetime(2030, 1, 1, tzinfo=UTC)}
                ),
            }
        )

from datetime import UTC, datetime

import pytest
from nexus.api import research_monitoring
from nexus.api.schemas import research_monitoring as schemas
from nexus.main import create_engine_app
from nexus.settings import EngineSettings


@pytest.mark.parametrize(
    "code,reason",
    [
        ("MONITOR_KIND_EVENTS_UNRESOLVED", "events_unresolved"),
        ("MONITOR_DAILY_EVENT_REVIEW_UNVERIFIED", "review_unverified"),
        ("MONITOR_DAILY_REPLACEMENT_REJECTED", "replacement_rejected"),
    ],
)
def test_coverage_reasons_are_specific_and_not_audit_inference(code, reason):
    assert research_monitoring._REASONS[code] == reason


def test_disclosure_view_is_separate_readonly_contract():
    view = schemas.ResearchDisclosureMonitoringView(observed_at=datetime.now(UTC), state="unknown")
    assert view.execution_locked and view.trigger_count == 0 and view.triggers == ()
    schema = create_engine_app(EngineSettings.for_test()).openapi()["components"]["schemas"]
    assert "disclosure_monitoring" in schema["ResearchRunResponse"]["properties"]
    assert "baseline_as_of" in schema["ResearchDisclosureMonitoringView"]["properties"]
    assert schema["ResearchDisclosureMonitorTrigger"]["properties"]["event_review"]["enum"] == [
        "addressed",
        "unresolved",
        "unverified",
        "not_performed",
    ]


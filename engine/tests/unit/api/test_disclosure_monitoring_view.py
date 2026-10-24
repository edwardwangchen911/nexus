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


def test_disclosure_reader_only_declared_by_sec_prompt_version():
    from types import SimpleNamespace

    from nexus.agents.committee import DISCLOSURE_SCOPE_PROMPT_VERSION
    from nexus.api.routes.research import _declares_disclosure_monitoring

    for manifest in (
        {},
        {"portfolio": None},
        {"portfolio": {"version": "local-portfolio-synthesis-v7"}},
    ):
        assert not _declares_disclosure_monitoring(SimpleNamespace(prompt_manifest=manifest))
    assert _declares_disclosure_monitoring(
        SimpleNamespace(prompt_manifest={"portfolio": {"version": DISCLOSURE_SCOPE_PROMPT_VERSION}})
    )


@pytest.mark.asyncio
async def test_future_native_settlement_cannot_be_shown_as_known_at_old_read_time():
    from datetime import timedelta
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from nexus.api.research_disclosure_monitoring import _usage

    now = datetime(2026, 10, 9, 14, tzinfo=UTC)
    run = SimpleNamespace(
        id=uuid4(),
        created_at=now,
        status="completed",
        settled_cost_usd=0,
        input_tokens=10,
        output_tokens=5,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=SimpleNamespace(finished_at=now + timedelta(seconds=1)))
    )
    assert await _usage(session, run, now) == "unknown"

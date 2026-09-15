"""Closed read-only contracts for the decision-first workbench."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.api.schemas.workbench import (
    WorkbenchAttentionResponse,
    WorkbenchResponse,
)
from nexus.main import create_engine_app
from nexus.settings import EngineSettings
from pydantic import ValidationError

NOW = datetime(2026, 9, 15, 1, tzinfo=UTC)
CYCLE_ID = UUID("72000000-0000-0000-0000-000000000001")


def _attention(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "service",
        "severity": "warning",
        "title_code": "AGENT_PAPER_LOOP_OFFLINE",
        "reason_code": "HEARTBEAT_STALE",
        "safe_action": "wait",
        "href": "/workspace",
        "cycle_id": None,
        "order_id": None,
    }
    payload.update(changes)
    return payload


def _workbench(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "mode": "internal_paper",
        "execution_locked": True,
        "loop": {
            "status": "unknown",
            "started_at": None,
            "last_seen_at": None,
            "stale_after_seconds": 30,
        },
        "session": {
            "decision_session": None,
            "status": "calendar_unavailable",
            "cycle_count": 0,
            "completed_count": 0,
            "latest_result_code": None,
            "last_completed_decision_at": None,
            "next_eligible_at": None,
            "next_eligible_reason": "CALENDAR_UNAVAILABLE",
            "execution_locked": True,
        },
        "latest_cycle": None,
        "active_orders": (),
        "account": None,
        "attention": (),
    }
    payload.update(changes)
    return payload


def test_workbench_schema_is_strict_bounded_and_execution_locked() -> None:
    response = WorkbenchResponse.model_validate(_workbench())
    assert response.execution_locked is True
    assert response.loop.status == "unknown"

    with pytest.raises(ValidationError):
        WorkbenchResponse.model_validate(_workbench(execution_locked=False))
    with pytest.raises(ValidationError):
        WorkbenchResponse.model_validate(_workbench(unexpected="value"))
    with pytest.raises(ValidationError):
        WorkbenchResponse.model_validate(
            _workbench(attention=tuple(_attention() for _ in range(11)))
        )


@pytest.mark.parametrize("href", ("https://example.com", "//example.com", "workspace"))
def test_attention_rejects_non_local_actions(href: str) -> None:
    with pytest.raises(ValidationError):
        WorkbenchAttentionResponse.model_validate(_attention(href=href))


def test_attention_rejects_extra_or_unbounded_codes() -> None:
    with pytest.raises(ValidationError):
        WorkbenchAttentionResponse.model_validate(_attention(secret="hidden"))
    with pytest.raises(ValidationError):
        WorkbenchAttentionResponse.model_validate(_attention(reason_code="x" * 101))


def test_openapi_exposes_only_read_only_workbench_and_owned_cycle_detail() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()
    paths = document["paths"]

    assert set(paths["/internal/v1/workbench"]) == {"get"}
    assert paths["/internal/v1/workbench"]["get"]["operationId"] == (
        "getInternalPaperWorkbench"
    )
    assert paths["/internal/v1/workbench"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"] == {"$ref": "#/components/schemas/WorkbenchEnvelope"}

    detail = paths["/internal/v1/paper/cycles/{cycle_id}"]
    assert set(detail) == {"get"}
    assert detail["get"]["operationId"] == "getInternalAgentPaperCycleDetail"
    assert detail["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/AgentPaperCycleDetailEnvelope"}
    assert "requestBody" not in detail["get"]

    schemas = document["components"]["schemas"]
    assert schemas["WorkbenchResponse"]["properties"]["execution_locked"] == {
        "const": True,
        "title": "Execution Locked",
        "type": "boolean",
    }
    assert schemas["WorkbenchResponse"]["properties"]["attention"]["maxItems"] == 10
    assert schemas["AgentPaperCycleDetailResponse"]["properties"]["orders"][
        "maxItems"
    ] == 50
    wire = str(
        {
            name: schema
            for name, schema in schemas.items()
            if name.startswith("Workbench")
        }
    ).lower()
    assert not any(
        forbidden in wire
        for forbidden in (
            "account_id",
            "database_url",
            "secret",
            "credential",
            "token",
            "api_key",
        )
    )

from datetime import UTC, datetime

import httpx
import pytest
from helpers.paper_api import app, headers

_PATH = "/internal/v1/live-evaluation"
_NOW = datetime(2026, 9, 17, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_empty_facts_return_locked_not_ready_evaluation(
    migrated_database_url: str,
) -> None:
    engine_app = app(migrated_database_url, now=_NOW)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=engine_app),
            base_url="http://testserver",
        ) as client:
            unauthenticated = await client.get(_PATH)
            query_path = f"{_PATH}?unlock=true"
            rejected_query = await client.get(
                query_path,
                headers=headers("GET", query_path),
            )
            response = await client.get(_PATH, headers=headers("GET", _PATH))

        assert unauthenticated.status_code == 401
        assert rejected_query.status_code == 422
        assert response.status_code == 200
        assert response.json()["data"] == {
            "status": "not_ready",
            "execution_locked": True,
            "live_write": False,
            "user_decision_required": True,
            "gates": [
                {
                    "key": "internal_paper_execution",
                    "status": "not_ready",
                    "reason_code": "AGENT_PAPER_EXECUTION_EVIDENCE_MISSING",
                    "evidence": [
                        {"key": "agent_orders", "count": 0},
                        {"key": "agent_fills", "count": 0},
                        {"key": "balanced_agent_fills", "count": 0},
                    ],
                },
                {
                    "key": "oos_agent_version",
                    "status": "not_ready",
                    "reason_code": "AGENT_VERSION_PASS_MISSING",
                    "evidence": [
                        {"key": "version_evaluations", "count": 0},
                        {"key": "pass_decisions", "count": 0},
                    ],
                },
                {
                    "key": "broker_simulation_lifecycle",
                    "status": "not_ready",
                    "reason_code": "BROKER_SIMULATION_LIFECYCLE_MISSING",
                    "evidence": [
                        {"key": "matched_bindings", "count": 0},
                        {"key": "completed_intents", "count": 0},
                        {"key": "filled_order_events", "count": 0},
                        {"key": "matched_reconciliations", "count": 0},
                    ],
                },
                {
                    "key": "broker_fee_variance",
                    "status": "unknown",
                    "reason_code": "BROKER_FEE_EVIDENCE_UNSUPPORTED",
                    "evidence": [{"key": "broker_fee_observations", "count": 0}],
                },
                {
                    "key": "broker_recovery_drill",
                    "status": "not_ready",
                    "reason_code": "BROKER_RECOVERY_EVIDENCE_MISSING",
                    "evidence": [
                        {"key": "result_unknown_attempts", "count": 0},
                        {"key": "reconciled_unknown_commands", "count": 0},
                    ],
                },
            ],
        }
        assert response.json()["meta"]["as_of"] == "2026-09-17T01:00:00Z"
    finally:
        await engine_app.state.session_factory.kw["bind"].dispose()

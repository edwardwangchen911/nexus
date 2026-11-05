"""Read the same persisted independent owner through API, BFF and optional browser."""

import json
import os
from decimal import Decimal
from pathlib import Path

import httpx
from helpers.monitoring_readview_browser import stage_facts, verify_monitoring_browser
from helpers.paper_api import headers
from helpers.paper_api import settings as api_settings
from nexus.main import create_engine_app
from sqlalchemy import text


async def facts(db):
    value = {"stages": await stage_facts(db)}
    for table in (
        "manual_paper_requests",
        "manual_paper_request_events",
        "research_request_manifests",
        "research_request_members",
        "agent_research_runs",
        "agent_research_outputs",
        "agent_daily_budget_reservations",
        "single_sol_budget_scopes",
        "single_sol_budget_credits",
        "jobs",
        "job_attempts",
        "job_leases",
    ):
        value[table] = tuple(
            await db.scalars(text(f"SELECT row_to_json(t)::text FROM {table} t ORDER BY 1"))
        )
    return value


async def verify_budgeted_readback(factory, settings, view, *, outcome):
    assert settings.database_url is not None
    application = create_engine_app(
        api_settings(settings.database_url).model_copy(
            update={
                "agent_paper_account_id": settings.agent_paper_account_id,
            }
        )
    )
    async with factory() as db:
        before = await facts(db)
        now = await db.scalar(text("SELECT clock_timestamp()"))
    application.state.utc_clock = lambda: now
    member = view.research.members[0]
    request_path = f"/internal/v1/paper/manual-requests/{view.request_id}"
    research_path = f"/internal/v1/research-runs/{member.run_id}"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test"
        ) as client:
            for _ in range(2):
                response = await client.get(request_path, headers=headers("GET", request_path))
                assert response.status_code == 200, response.text
                receipt = response.json()["data"]
                assert receipt["request_id"] == str(view.request_id)
                assert receipt["status"] == view.status
                assert receipt["research_contract"] == "single_sol_research_v1"
                assert receipt["execution_locked"] is True
                assert receipt.get("independent_disposition") is None
                assert receipt["research"]["members"][0]["usage_known"] == member.usage_known
                response = await client.get(research_path, headers=headers("GET", research_path))
                assert response.status_code == 200, response.text
                research = response.json()["data"]
                assert research["run_id"] == str(member.run_id)
                assert research["status"] == member.status
                if outcome != "complete":
                    assert research["abstention_reason"] == (
                        "model_output_limit_reached"
                        if outcome == "output_limit"
                        else "model_usage_uncertain"
                    )
                if outcome == "timeout":
                    assert research["settled_cost_usd"] is None
                else:
                    assert Decimal(research["settled_cost_usd"]) == Decimal(
                        "0.0897" if outcome == "complete" else "0.0299"
                    )
            endpoint = "/internal/v1/workbench"
            response = await client.get(endpoint, headers=headers("GET", endpoint))
            assert response.status_code == 200, response.text
            activity = response.json()["data"]["activity"]
            scope = next(s for s in activity["scopes"] if s["mode"] == "research_only")
            assert scope["request_id"] == str(view.request_id), scope
            assert scope["status"] == view.status and scope["portfolio_status"] == "not_applicable"
            assert len(scope["candidates"]) == 1
            candidate = scope["candidates"][0]
            assert candidate["run_id"] == str(member.run_id)
            assert candidate["read_status"] == "verified", candidate
            if outcome != "complete":
                assert candidate["reason_code"] == research["abstention_reason"]
            if outcome == "timeout":
                assert scope["occupancy"] == "unresolved"
        output = os.environ.get("NEXUS_TEST_BUDGETED_BROWSER_DIR")
        if output:
            directory = Path(output).resolve() / outcome
            directory.mkdir(parents=True, exist_ok=False)
            (directory / "budgeted.json").write_text(
                json.dumps(
                    {
                        "request_id": str(view.request_id),
                        "key": "new-explicit-v5",
                        "run_id": str(member.run_id),
                        "instrument_id": str(member.instrument_id),
                        "outcome": outcome,
                        "receipt": receipt,
                        "research": research,
                        "activity_scope": scope,
                    }
                )
            )
            await verify_monitoring_browser(
                application,
                directory=directory,
                spec_path="tests/e2e/budgeted-single-sol-live.spec.ts",
            )
        async with factory() as db:
            assert await facts(db) == before
    finally:
        await application.state.session_factory.kw["bind"].dispose()

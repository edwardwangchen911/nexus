"""Read the new root and its native successor without changing business facts."""

import json
import os
from pathlib import Path

import httpx
from helpers.monitoring_readview_browser import verify_monitoring_browser
from helpers.paper_api import app, headers
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, Job, OutboxEvent
from sqlalchemy import select


async def verify_readview(database_url, factory, monkeypatch, pointer, child_id, batch_id, now):
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)
    application = app(database_url, now=now)
    guarded = (
        AgentResearchRunRecord,
        Job,
        MonitorPlan,
        MonitorTrigger,
        MonitorCoverageReceipt,
        OutboxEvent,
        AuditEvent,
        IdempotencyRecord,
    )

    async def facts():
        async with factory() as session:
            identities = {model: set(await session.scalars(select(model.id))) for model in guarded}
            budgets = [
                (r.id, r.state, r.reserved_usd, r.settled_usd)
                for r in await session.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            ]
            return identities, budgets

    before = await facts()
    from nexus.paper.read_service import PaperReadService
    from nexus.workbench.activity_read import _scope

    async with factory() as session:
        scope = await _scope(
            session,
            PaperReadService(session, now),
            pointer.account_id,
            "research_and_monitor",
            now,
            now.date(),
        )
        assert scope.status == "completed" and scope.portfolio_status == "not_applicable"
        assert len(scope.candidates) == 3
    path = f"/internal/v1/research-runs/{pointer.source_run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    price, disclosure = data["monitoring"], data["disclosure_monitoring"]
    for view in (price, disclosure):
        assert view["execution_locked"] and view["state"] == "superseded", view
        assert view["replacement_run_id"] == str(child_id)
    assert disclosure["trigger_count"] == len(disclosure["triggers"]) == 2
    assert {r["batch_id"] for r in disclosure["triggers"]} == {str(batch_id)}
    assert all(
        r["event_review"] == "addressed"
        and r["usage_status"] == "known"
        and r["run_id"] == str(child_id)
        for r in disclosure["triggers"]
    )
    browser_root = os.environ.get("NEXUS_TEST_MONITOR_BROWSER_DIR")
    if browser_root:
        directory = Path(browser_root).resolve() / "independent-addressed"
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "ready.json").write_text(
            json.dumps(
                dict(
                    run_id=str(pointer.source_run_id),
                    instrument_id=str(pointer.instrument_id),
                    reason="addressed",
                    independent=True,
                    replacement_run_id=str(child_id),
                    response=response.json(),
                )
            )
        )
        await verify_monitoring_browser(application, directory=directory)
    assert await facts() == before

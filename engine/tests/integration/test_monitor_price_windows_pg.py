"""Historical price windows traverse the exact Run GET in an isolated database."""

import json
import os
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from helpers.paper_batch import CALENDAR_ID
from helpers.portfolio_api import app, headers
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_increment_pg import ready_trigger
from integration.test_monitor_view_pg import business_facts
from nexus.audit.service import append_audit_event
from nexus.monitoring.storage import record_check
from nexus.paper.formal_budget_protection import SCHEMA, _hash
from nexus.persistence.models.agents import FormalBudgetBindingRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan
from nexus.persistence.models.paper import AgentPaperCandidateBatchRecord


@pytest.mark.asyncio
async def test_exact_run_reports_one_missing_window_without_changing_business(
    migrated_database_url, monkeypatch
):
    factory, settings, trigger, parent, _ = await ready_trigger(
        migrated_database_url, monkeypatch, monitoring_lifetime_days=4
    )
    # Monday at 10:00 NY: a complete interval after Friday's install.
    monday = AT + timedelta(days=3)
    now = monday + timedelta(minutes=15)
    await _set_test_insert_clock(factory, monday + timedelta(minutes=10, seconds=1))
    async with factory.begin() as session:
        first = await session.get(MonitorCheck, trigger.check_id)
        plan = await session.get(MonitorPlan, first.plan_id)
        batch = await session.get(
            AgentPaperCandidateBatchRecord, settings.agent_paper_candidate_batch_id
        )
        assert plan.effective_at < monday and plan.valid_until > now
        tuesday = monday + timedelta(days=1)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=tuesday.date(),
                decision_at=tuesday.replace(hour=21),
                market_open_at=tuesday.replace(hour=13, minute=30),
                market_close_at=tuesday.replace(hour=20),
                created_at=AT,
            )
        )
        for at in (monday, monday + timedelta(minutes=10)):
            await record_check(
                session,
                plan_id=plan.id,
                scheduled_at=at,
                checked_at=at + timedelta(seconds=1),
                observation=None,
            )
    path = f"/internal/v1/research-runs/{parent.id}"
    application = app(migrated_database_url)
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)

    async def read(client, extended=True):
        request_headers = headers("GET", path)
        if extended:
            request_headers["X-Nexus-Monitor-View"] = "1"
        response = await client.get(path, headers=request_headers)
        assert response.status_code == 200
        return response.json()["data"]

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://nexus.test"
        ) as client:
            legacy = await read(client, False)
            assert "price_check_windows" not in legacy and "monitoring_applicability" not in legacy
            unbound = (await read(client))["price_check_windows"]
            assert (
                not unbound["scope_complete"] and unbound["recorded_outside_verified_windows"] == 3
            )
            # A signed scope is available only after its own audited visibility.
            payload = {
                "schema": SCHEMA,
                "batch_id": str(batch.id),
                "batch_hash": batch.batch_hash,
                "account_id": str(plan.account_id),
                "calendar_id": str(CALENDAR_ID),
                "enabled": True,
            }
            digest = _hash(payload)
            await _set_test_insert_clock(factory, monday)
            async with factory.begin() as session:
                audit = await append_audit_event(
                    session,
                    actor_id="fixture",
                    action="formal_budget_binding_apply",
                    schema_version=SCHEMA,
                    payload={"binding_hash": digest, "binding_payload": payload},
                    occurred_at=monday,
                )
                session.add(
                    FormalBudgetBindingRecord(
                        budget_domain="agent_global_v1",
                        business_date=monday.date(),
                        sequence=1,
                        previous_id=None,
                        idempotency_key="fixture-price-scope",
                        content_hash=digest,
                        schema_version=SCHEMA,
                        daily_limit_usd=3,
                        payload=payload,
                        applied_at=monday,
                        created_at=monday,
                        audit_event_id=audit.id,
                    )
                )
            before = await business_facts(factory)
            data = await read(client)
            report = data["price_check_windows"]
            assert report["expected"] == 3 and report["recorded"] == 2, report
            assert report["missing"] == [
                (monday + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
            ]
            assert data["monitoring"]["latest_check"]["result"] == "unknown"
            assert data["monitoring_applicability"]["price"] == "applicable"
            capture = os.getenv("NEXUS_TEST_PRICE_WINDOW_BROWSER_DIR")
            if capture:
                from helpers.monitoring_readview_browser import verify_monitoring_browser

                directory = Path(capture)
                directory.mkdir(exist_ok=True, parents=True)
                (directory / "ready.json").write_text(
                    json.dumps(
                        {
                            "run_id": str(parent.id),
                            "instrument_id": str(parent.instrument_id),
                            "missing": report["missing"][0],
                        }
                    )
                )
                await verify_monitoring_browser(
                    application,
                    directory=directory,
                    spec_path="tests/e2e/price-window-readview-live.spec.ts",
                )
            assert await business_facts(factory) == before
    finally:
        await factory.kw["bind"].dispose()

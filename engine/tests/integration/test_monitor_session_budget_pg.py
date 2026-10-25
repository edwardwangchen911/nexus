"""A new trading session resets finite calls, never historical unknown occupancy."""

import argparse
import json
from datetime import timedelta
from uuid import UUID

import pytest
from helpers.mixed_stage import add_mixed_fields
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_batches_pg import installed
from integration.test_monitor_increment_pg import append_fresh_inputs
from nexus.agents import codex_staged
from nexus.monitoring.batches import scan_stock_batch
from nexus.ops.research_monitor import parser, run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorIncrement
from nexus.persistence.models.runtime import Job, JobLease
from nexus.settings import AgentRuntimeMode
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select, text
from unit.agents.test_codex_sdk import _restore_view


async def preserved_rows(factory):
    async with factory() as session:
        return {
            name: set(await session.scalars(text(f'SELECT to_jsonb(t)::text FROM "{name}" t')))
            for name in (
                "agent_research_runs",
                "agent_research_outputs",
                "agent_research_events",
                "agent_research_output_evidence",
                "agent_daily_budget_reservations",
                "formal_budget_bindings",
                "agent_portfolio_synthesis_budget_reservations",
                "portfolio_decision_budget_reservations",
                "research_monitor_increments",
                "research_monitor_triggers",
                "research_monitor_coverage",
                "jobs",
                "job_leases",
                "job_attempts",
                "research_monitor_plans",
                "data_snapshots",
                "outbox_events",
                "audit_events",
            )
        }


async def workload_counts(factory):
    async with factory() as session:
        return tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (
                    AgentResearchRunRecord,
                    Job,
                    MonitorIncrement,
                    AgentDailyBudgetReservationRecord,
                )
            ]
        )


async def stock_batch(factory, settings, instrument_id, clock, now):
    clock[0] = now
    await _set_test_insert_clock(factory, now)
    await append_fresh_inputs(factory, instrument_id, now, decision_session=now.date())
    async with factory() as session:
        output = await session.scalar(
            select(AgentResearchOutputRecord)
            .join(
                AgentResearchRunRecord,
                AgentResearchRunRecord.id == AgentResearchOutputRecord.run_id,
            )
            .where(
                AgentResearchRunRecord.instrument_id == instrument_id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
            )
            .order_by(AgentResearchRunRecord.decision_time)
            .limit(1)
        )
    installed_plan = await run(
        settings, parser().parse_args(["install", "--kind", "price", "--output-id", str(output.id)])
    )
    assert installed_plan["plan_id"]
    async with factory() as session, session.begin():
        batch = await scan_stock_batch(
            session,
            account_id=settings.agent_paper_account_id,
            instrument_id=instrument_id,
            now=now,
        )
        assert batch["new_triggers"] == batch["notifications"] == 1, batch
    prepared = await run(
        settings, argparse.Namespace(action="prepare-batch", batch_id=UUID(batch["batch_id"]))
    )
    assert prepared["status"] == "prepared", prepared
    return argparse.Namespace(
        action="admit-batch",
        batch_id=UUID(batch["batch_id"]),
        materials_snapshot_id=UUID(prepared["snapshot_id"]),
        calendar_authority_id=CALENDAR_ID,
    )


async def verify_budget_rejection_readback(factory, database_url, monkeypatch, *, batch_id, now):
    """Read a real normal-entry rejection, never stamp the blocked reason in a fixture."""
    import os
    from pathlib import Path

    import httpx
    from helpers.monitoring_readview_browser import verify_monitoring_browser
    from helpers.paper_api import app, headers
    from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
    from nexus.persistence.models.runtime import AuditEvent, OutboxEvent

    async with factory() as session:
        trigger = await session.scalar(
            select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
        )
        assert trigger.status == "blocked" and trigger.reason == "MONITOR_DAILY_LIMIT_REACHED"
        assert trigger.research_run_id is None
        check = await session.get(MonitorCheck, trigger.check_id)
        plan = await session.get(MonitorPlan, check.plan_id)
        assert check.instrument_id == plan.instrument_id == trigger.instrument_id
        event = await session.get(OutboxEvent, trigger.outbox_id)
        assert event is not None and event.status == "queued" and event.attempt_count == 0
        assert str(trigger.id) in event.payload["trigger_ids"]
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(
                    AuditEvent.action == "research.monitor.batch_admission_blocked",
                    AuditEvent.payload["batch_id"].as_string() == str(batch_id),
                )
            )
            == 1
        )
        source_run_id, instrument_id, trigger_id = (
            plan.source_run_id,
            plan.instrument_id,
            trigger.id,
        )
    before = await preserved_rows(factory)
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)
    application = app(database_url, now=now)
    path = f"/internal/v1/research-runs/{source_run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200, response.text
    monitoring = response.json()["data"]["monitoring"]
    assert monitoring["execution_locked"] is True
    assert monitoring["trigger_count"] == 1
    saved = monitoring["triggers"][0]
    assert saved["trigger_id"] == str(trigger_id)
    assert saved["status"] == "blocked" and saved["blocked_reason"] == "budget"
    assert saved["run_id"] is None and saved["usage_status"] == "not_started"
    assert saved["notification_status"] == "queued"
    browser_root = os.environ.get("NEXUS_TEST_MONITOR_BROWSER_DIR")
    if browser_root:
        directory = Path(browser_root).resolve() / "budget-limit"
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "ready.json").write_text(
            json.dumps(
                {
                    "run_id": str(source_run_id),
                    "instrument_id": str(instrument_id),
                    "trigger_id": str(trigger_id),
                    "response": response.json(),
                }
            )
        )
        await verify_monitoring_browser(
            application, directory=directory, spec_path="tests/e2e/monitoring-budget-live.spec.ts"
        )
    assert await preserved_rows(factory) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["settled", "capacity", "unknown"])
async def test_normal_sealed_batches_reset_calls_only_for_new_session(
    migrated_database_url, monkeypatch, tmp_path, outcome
):
    factory, settings, store, captured, pointer = await installed(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        added=0,
        kinds=("price",),
        monitoring_lifetime_days=4,
    )
    monkeypatch.setattr("nexus.monitoring.filing_archive.configured_exhibit_store", lambda s: store)
    try:
        async with factory() as session:
            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            friday = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(
                seconds=1
            )
            instruments = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord.instrument_id)
                    .distinct()
                    .order_by(AgentResearchRunRecord.instrument_id)
                )
            )
        others = tuple(i for i in instruments if i != pointer.instrument_id)
        assert len(others) == 2
        settings = settings.model_copy(
            update={
                "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
                "agent_paper_cycle_enabled": True,
                "agent_paper_formal_model_config_id": parent.model_config_id,
                "agent_monitor_daily_sdk_call_limit": 3,
            }
        )
        monday = (AT + timedelta(days=3)).replace(hour=13, minute=36)
        captured["clock"][0] = friday
        await _set_test_insert_clock(factory, friday)
        async with factory() as session, session.begin():
            for day in (monday, monday + timedelta(days=1)):
                if (
                    await session.scalar(
                        select(ExchangeCalendarSessionRecord.session).where(
                            ExchangeCalendarSessionRecord.calendar_authority_id == CALENDAR_ID,
                            ExchangeCalendarSessionRecord.session == day.date(),
                        )
                    )
                    is None
                ):
                    session.add(
                        ExchangeCalendarSessionRecord(
                            calendar_authority_id=CALENDAR_ID,
                            session=day.date(),
                            decision_at=day.replace(hour=21),
                            market_open_at=day.replace(hour=13, minute=30),
                            market_close_at=day.replace(hour=20),
                        )
                    )
        first_args = await stock_batch(
            factory, settings, pointer.instrument_id, captured["clock"], friday
        )
        first = await run(settings, first_args)
        assert first["status"] == "admitted" and first["reserved_sdk_calls"] == 3, first
        assert first["execution_locked"] is True
        async with factory() as session:
            member = await session.scalar(
                select(MonitorIncrement).where(MonitorIncrement.batch_id == first_args.batch_id)
            )
            assert member.session_date == friday.date() and member.daily_sdk_call_limit == 3
        native = codex_staged.invoke_codex
        stages = []

        async def controlled(payload, **kwargs):
            wire = _restore_view(payload)
            stages.append((captured["clock"][0].date(), wire["stage"]))
            if outcome == "unknown" and wire["stage"] == "critique":
                raise TimeoutError("controlled unknown usage")
            raw, usage = await native(payload, **kwargs)
            body = json.loads(raw)
            add_mixed_fields(body, wire)
            return json.dumps(body), usage

        monkeypatch.setattr(codex_staged, "invoke_codex", controlled)
        if outcome == "capacity":
            from nexus.ops.monitor_dispatch import dispatch_configured_once

            capacity_checks = []

            async def reject_capacity(session, configured, research, **kwargs):
                job = await session.get(Job, member.job_id)
                assert research.id == member.run_id and job.status == "queued"
                assert job.attempt_count == 0
                capacity_checks.append(research.id)
                raise ValueError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")

            with monkeypatch.context() as preflight:
                preflight.setattr(
                    "nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", reject_capacity
                )
                terminal = await dispatch_configured_once(
                    settings, trigger_id=member.trigger_id, calendar_authority_id=CALENDAR_ID
                )
            assert terminal["status"] == "failed" and capacity_checks == [member.run_id]
        else:
            assert await run_agent_worker_once(
                settings, job_ids=(member.job_id,), formal_stage_capacity=True
            )
            if outcome == "unknown":
                captured["clock"][0] += timedelta(minutes=2)
                await _set_test_insert_clock(factory, captured["clock"][0])
            terminal = await run(
                settings, argparse.Namespace(action="finish", trigger_id=member.trigger_id)
            )
            assert terminal["status"] == ("unknown" if outcome == "unknown" else "completed"), (
                terminal
            )
        assert len(stages) == {"settled": 3, "capacity": 0, "unknown": 2}[outcome]
        async with factory() as session:
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert (
                hold.state
                == {"settled": "settled", "capacity": "released", "unknown": "reserved"}[outcome]
            )
            assert (await session.get(MonitorIncrement, member.id)).reserved_sdk_calls == 3
            first_run = await session.get(AgentResearchRunRecord, member.run_id)
            assert (first_run.settled_cost_usd is None) is (outcome == "unknown")
            assert (hold.settled_usd is None) is (outcome != "settled")
        frozen = await preserved_rows(factory)
        same_args = await stock_batch(
            factory, settings, others[0], captured["clock"], captured["clock"][0]
        )
        counts = await workload_counts(factory)
        higher = settings.model_copy(update={"agent_monitor_daily_sdk_call_limit": 9})
        same = await run(higher, same_args)
        assert same.get("error_code") == (
            "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"
            if outcome == "unknown"
            else "MONITOR_DAILY_CALL_LIMIT"
        ), same
        assert await workload_counts(factory) == counts
        if outcome == "settled":
            await verify_budget_rejection_readback(
                factory,
                migrated_database_url,
                monkeypatch,
                batch_id=same_args.batch_id,
                now=captured["clock"][0],
            )
            assert len(stages) == 3
        next_args = await stock_batch(factory, settings, others[1], captured["clock"], monday)
        from nexus.paper.agent_cycle_authority import (
            load_quasi_realtime_cycle_authority,
            prepare_candidates,
        )
        from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

        async with factory() as session:
            batch = await load_agent_paper_candidate_batch(
                session, batch_id=settings.agent_paper_candidate_batch_id
            )
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=pointer.account_id,
                now=monday,
                expected_calendar_authority_id=CALENDAR_ID,
                expected_universe_snapshot_id=batch.universe_snapshot_id,
            )
            candidates = await prepare_candidates(
                session,
                authority=authority,
                requested_instrument_ids=(others[1],),
                max_instruments=1,
            )
            assert len(candidates) == 1, "Monday current inputs must qualify before testing quota"
            from nexus.persistence.models.data import PriceBarSetAuthority
            from nexus.persistence.models.market import LatestQuote
            from nexus.persistence.models.paper import RiskQueryAuthorityRecord

            selected = candidates[0]
            prior = await session.get(PriceBarSetAuthority, selected.prior_daily_price_authority_id)
            assert prior.first_session == prior.last_session == friday.date()
            assert prior.available_at <= monday
            quote = await session.get(LatestQuote, selected.decision_quote_id)
            assert quote.instrument_id == others[1] and quote.received_at == monday
            risks = tuple(
                await session.scalars(
                    select(RiskQueryAuthorityRecord).where(
                        RiskQueryAuthorityRecord.id.in_(selected.risk_query_authority_ids)
                    )
                )
            )
            assert len(risks) == 3 and {r.kind for r in risks} == {
                "corporate_action",
                "suspension",
                "delisting",
            }
            assert all(
                r.decision_session == monday.date() and r.available_at == monday for r in risks
            )
        counts = await workload_counts(factory)
        following = await run(higher, next_args)
        assert following["execution_locked"] is True
        if outcome == "unknown":
            assert following.get("error_code") == "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN", following
            assert await workload_counts(factory) == counts
        else:
            assert following["status"] == "admitted" and following["reserved_sdk_calls"] == 3, (
                following
            )
            repeated = await run(higher, next_args)
            assert (
                repeated["run_id"] == following["run_id"]
                and repeated["job_id"] == following["job_id"]
            )
            assert await workload_counts(factory) == tuple(n + 1 for n in counts)
            async with factory() as session:
                current = await session.scalar(
                    select(MonitorIncrement).where(MonitorIncrement.batch_id == next_args.batch_id)
                )
                assert current.session_date == monday.date() and current.daily_sdk_call_limit == 9
                assert current.run_id != member.run_id and current.snapshot_id != member.snapshot_id
            assert await run_agent_worker_once(
                higher, job_ids=(current.job_id,), formal_stage_capacity=True
            )
            done = await run(
                higher, argparse.Namespace(action="finish", trigger_id=current.trigger_id)
            )
            assert done["status"] == "completed", done
        after = await preserved_rows(factory)
        for name, rows in frozen.items():
            assert rows <= after[name], name
        assert len(stages) == {"settled": 6, "capacity": 3, "unknown": 2}[outcome]
        from nexus.paper.manual_request import require_manual_account_available

        async with factory() as session, session.begin():
            if outcome == "unknown":
                with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        session, account_id=pointer.account_id, now=monday
                    )
            else:
                await require_manual_account_available(
                    session, account_id=pointer.account_id, now=monday
                )
    finally:
        await factory.kw["bind"].dispose()

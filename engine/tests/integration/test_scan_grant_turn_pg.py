"""A real scan and original formal Worker serialize before scope/claim locks."""

import asyncio
from contextvars import ContextVar
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_sol_native_preparation_pg import (
    test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes as prepare_formal,
)
from integration.test_sol_native_stage_pg import install_sol_http
from nexus import worker
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.ops import monitor_disclosure_refresh, monitor_price_refresh, research_monitor
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorCheck
from nexus.persistence.models.paper import AgentPaperCycleRecord
from sqlalchemy import select, text
from unit.agents.test_monitoring_producer import proposal_wire


@pytest.mark.asyncio
async def test_original_formal_stages_finish_once_without_waiting_with_scope_lock(
    migrated_database_url, monkeypatch
):
    from integration import test_candidate_multistock_path as candidate

    def response(value, payload):
        if payload["stage"] == "research":
            for role in value["roles"]:
                if role["role"] == "market_state":
                    role["monitoring_proposal"] = proposal_wire(candidate.AT)
        elif payload["stage"] == "critique":
            for role in value["roles"]:
                if role["role"] == "evidence_audit":
                    role["vote"] = "support"
                role["monitoring_review"] = {
                    "proposal_hash": payload["monitoring_proposal"]["content_hash"],
                    "decision": "accepted",
                    "rationale": "核对冻结原件。",
                    "citations": [0],
                }
        else:
            value["monitoring_adoption_hash"] = payload["monitoring_proposal"]["content_hash"]

    calls, clients = install_sol_http(monkeypatch, None, model_response=response)

    async def validate(factory, settings, cycle_id):
        async with factory() as db:
            cycle = await db.get(AgentPaperCycleRecord, cycle_id)
            routes = (
                await db.execute(
                    text("select run_id,job_id from research_stage_route order by run_id")
                )
            ).all()
        async with factory() as db:
            model_id = (await db.get(AgentResearchRunRecord, routes[0][0])).model_config_id
        async with factory.begin() as db:
            next_day = cycle.decision_session + timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += timedelta(days=1)
            if not await db.scalar(
                select(ExchangeCalendarSessionRecord).where(
                    ExchangeCalendarSessionRecord.calendar_authority_id == CALENDAR_ID,
                    ExchangeCalendarSessionRecord.session == next_day,
                )
            ):
                base = candidate.AT.replace(hour=14, minute=30, second=0, microsecond=0)
                shift = timedelta(days=(next_day - cycle.decision_session).days)
                db.add(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=CALENDAR_ID,
                        session=next_day,
                        decision_at=base + shift + timedelta(hours=6, minutes=30),
                        market_open_at=base + shift,
                        market_close_at=base + shift + timedelta(hours=6, minutes=30),
                        created_at=candidate.AT - timedelta(seconds=1),
                    )
                )
        live = settings.model_copy(
            update={
                "agent_paper_account_id": cycle.account_id,
                "agent_paper_candidate_batch_id": cycle.candidate_batch_id,
                "agent_paper_cycle_enabled": True,
                "object_store_endpoint": "http://127.0.0.1:9000",
                "agent_paper_formal_model_config_id": model_id,
            }
        )
        assert await worker.run_agent_worker_once(
            live, job_ids=(routes[0][1],), formal_stage_capacity=True
        )
        async with factory() as db:
            first = await db.get(AgentResearchRunRecord, routes[0][0])
            assert first.status == "completed", (first.status, calls)
        reconcile_args = research_monitor.parser().parse_args(["reconcile"])
        reconciled = await research_monitor.run(live, reconcile_args)
        async with factory() as db:
            coverage = (
                await db.execute(text("select kind,status,reason from research_monitor_coverage"))
            ).all()
            assert await db.scalar(text("select count(*) from research_monitor_plans")) == 1, (
                reconciled,
                coverage,
            )

        entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        scan_task = None
        scan_started = False
        early_scope = []
        granting = ContextVar("granting", default=False)
        sec_scope = monitor_disclosure_refresh._scope
        price_locks = monitor_price_refresh._locks
        pause_phase = None

        async def pause_sec(*args, **kwargs):
            value = await sec_scope(*args, **kwargs)
            if pause_phase == "disclosure" and not entered.is_set():
                entered.set()
                await release.wait()
            return value

        async def pause_price(*args, **kwargs):
            await price_locks(*args, **kwargs)
            if pause_phase == "price" and not entered.is_set():
                entered.set()
                await release.wait()

        member, authorize, cleanup = (
            StagedProtocol._member,
            StagedProtocol.authorize,
            StagedProtocol.cleanup,
        )

        async def actual_scan():
            try:
                now = candidate.AT + timedelta(seconds=1)
                await research_monitor.refresh_configured_observations(
                    live,
                    calendar_authority_id=CALENDAR_ID,
                    scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
                    environment={},
                    repo_root=Path("/tmp"),
                )
                return await research_monitor.run(
                    live,
                    research_monitor.parser().parse_args(
                        ["scan", "--calendar-authority-id", str(CALENDAR_ID)]
                    ),
                )
            finally:
                finished.set()

        async def after_cleanup(protocol, ticket, **kwargs):
            nonlocal scan_task, scan_started, pause_phase, entered, release, finished
            await cleanup(protocol, ticket, **kwargs)
            if ticket.run_id == routes[1][0] and ticket.stage in (1, 2):
                if scan_task is not None:
                    await scan_task
                entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
                pause_phase = "disclosure" if ticket.stage == 1 else "price"
                scan_started = True
                scan_task = asyncio.create_task(actual_scan())
                await asyncio.wait_for(entered.wait(), 10)

        async def observe_member(protocol, db, claim):
            if scan_started and claim.job_id == routes[1][1] and granting.get():
                from nexus.paper.target_service import try_lock_portfolio_account

                async with factory.begin() as probe:
                    if not await try_lock_portfolio_account(probe, cycle.account_id):
                        early_scope.append(claim.job_id)
            return await member(protocol, db, claim)

        async def overlap_authorize(protocol, claim, **kwargs):
            if claim.job_id != routes[1][1] or kwargs["stage"] not in (2, 3):
                return await authorize(protocol, claim, **kwargs)

            async def granting_ticket():
                token = granting.set(True)
                try:
                    return await authorize(protocol, claim, **kwargs)
                finally:
                    granting.reset(token)

            task = asyncio.create_task(granting_ticket())
            # One scheduler turn, not a business-time assertion. An unguarded
            # authorize enters _member synchronously before its first SQL await.
            await asyncio.sleep(0)
            release.set()
            return await task

        monkeypatch.setattr(monitor_disclosure_refresh, "_scope", pause_sec)
        monkeypatch.setattr(monitor_price_refresh, "_locks", pause_price)
        monkeypatch.setattr(StagedProtocol, "cleanup", after_cleanup)
        monkeypatch.setattr(StagedProtocol, "_member", observe_member)
        monkeypatch.setattr(StagedProtocol, "authorize", overlap_authorize)
        try:
            assert await worker.run_agent_worker_once(
                live, job_ids=(routes[1][1],), formal_stage_capacity=True
            )
        finally:
            release.set()
            if scan_task is not None:
                scanned = await scan_task
        assert not early_scope, "grant entered scope/claim transaction while scan owned account"
        assert scanned["status"] == "scanned" and scanned["checks"] == 1
        async with factory() as db:
            run = await db.get(AgentResearchRunRecord, routes[1][0])
            failure = (
                await db.execute(
                    text(
                        "select state,details from agent_research_events "
                        "where run_id=:r order by sequence"
                    ),
                    {"r": run.id},
                )
            ).all()
            assert run.status == "completed" and run.settled_cost_usd == Decimal("0.0897"), (
                calls,
                failure,
            )
            assert (
                await db.execute(
                    text(
                        "select stage,count(*) from research_stage_authorization "
                        "where run_id=:run group by stage order by stage"
                    ),
                    {"run": run.id},
                )
            ).all() == [(1, 1), (2, 1), (3, 1)]
            checks = tuple(await db.scalars(select(MonitorCheck)))
            assert len(checks) == 1 and checks[0].scheduled_at == candidate.AT.replace(
                minute=candidate.AT.minute // 5 * 5, second=0, microsecond=0
            )
        assert calls == ["research", "critique", "synthesis"] * 2
        assert all(c.is_closed for c in clients)

    await prepare_formal(
        migrated_database_url, monkeypatch, after_prepared=validate, monitoring=True
    )

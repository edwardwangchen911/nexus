"""Normal Sol dispatch and untouched closeout retain original ownership and unknown holds."""

from datetime import timedelta
from functools import partial
from pathlib import Path

import httpx
import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_sol_native_preparation_pg import (
    test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes as prepare_formal,
)
from integration.test_sol_native_preparation_pg import (
    test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor as prepare_increment,
)
from integration.test_sol_native_stage_pg import install_sol_http
from nexus.ops import agent_paper_loop as loop
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job
from pydantic import SecretStr
from sqlalchemy import func, select, text

CODE = "PLATFORM_HTTP_CREDENTIAL_CHANGED"


async def frozen(session):
    return {
        table: tuple(
            await session.scalars(text(f"SELECT to_jsonb(t)::text FROM {table} t ORDER BY id"))
        )
        for table in ("agent_research_runs", "agent_daily_budget_reservations", "jobs")
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "locked", "unknown"])
async def test_formal_native_preclaim_closes_only_untouched_pool(
    migrated_database_url, monkeypatch, fault
):
    from nexus import worker
    from nexus.ops.agent_capacity_rejection import close_capacity_rejected_cycle_once
    from nexus.paper.agent_cycle import _events

    async def validate(factory, settings, cycle_id):
        async with factory() as session:
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            jobs = tuple(await session.scalars(select(Job).order_by(Job.id)))
        settings = settings.model_copy(update={"agent_paper_account_id": cycle.account_id})

        async def provider(payload):
            if fault == "unknown":
                raise httpx.ReadTimeout("synthetic transport unknown")
            raise AssertionError("preclaim refusal must never send")

        calls, clients = install_sol_http(monkeypatch, None, on_request=provider)
        if fault == "unknown":
            assert await worker.run_agent_worker_once(
                settings, job_ids=(jobs[0].id,), formal_stage_capacity=True
            )
        async with factory() as session:
            before = await frozen(session)

        async def close():
            await close_capacity_rejected_cycle_once(
                settings,
                account_id=cycle.account_id,
                cycle_id=cycle_id,
                job_id=jobs[0].id,
                code=CODE,
            )

        if fault == "locked":
            async with factory.begin() as session:
                await session.scalar(select(Job).where(Job.id == jobs[0].id).with_for_update())
                with pytest.raises(ValueError, match="AGENT_PAPER_LOOP_RESULT_UNKNOWN"):
                    await close()
        elif fault == "unknown":
            with pytest.raises(ValueError, match="AGENT_PAPER_LOOP_RESULT_UNKNOWN"):
                await close()
        else:
            changed = settings.model_copy(
                update={"openai_api_key": SecretStr("sk-proj-" + "changed-synthetic" * 5)}
            )
            assert (
                await loop.execute_agent_paper_loop_action(
                    changed,
                    account_id=cycle.account_id,
                    repo_root=Path.cwd(),
                    action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
                    snapshot=loop.AgentPaperLoopSnapshot(
                        now=cycle.decision_time,
                        cycle_id=cycle_id,
                        cycle_status="researching",
                        research_job_id=jobs[0].id,
                        research_job_status="queued",
                    ),
                )
                is None
            )
            await close()  # Exact repeated receipt; no new cancellation or lease.
        async with factory() as session:
            events = await _events(session, cycle_id)
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            holds = tuple(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            if fault:
                assert await frozen(session) == before
                assert events[-1].to_status == "researching"
                assert all(h.state != "released" for h in holds)
                if fault == "unknown":
                    assert any(r.status == "failed" and r.settled_cost_usd is None for r in runs)
            else:
                assert events[-1].to_status == "unavailable"
                assert events[-1].fact_ids["failure_code"] == CODE
                assert events[-1].fact_ids["unused_research_cancelled"] == 3
                assert all(
                    r.input_tokens == r.output_tokens == r.settled_cost_usd == 0 for r in runs
                )
                assert all(h.state == "released" for h in holds)
                assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 3
                assert (
                    await session.scalar(text("SELECT count(*) FROM research_stage_authorization"))
                    == 0
                )
        assert calls == (["research"] if fault == "unknown" else [])
        assert all(c.is_closed for c in clients)

    await prepare_formal(migrated_database_url, monkeypatch, after_prepared=validate)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["completed", "preclaim", "unknown", "untyped"])
async def test_native_increment_normal_host_preserves_once_only_dispatch(
    migrated_database_url, monkeypatch, outcome
):
    from nexus.monitoring.admission import require_monitor_account_available
    from nexus.ops import manual_paper_host
    from nexus.ops.monitor_dispatch import dispatch_configured_once, inspect_monitor_queue
    from nexus.persistence.models.monitoring import MonitorTrigger

    async def validate(factory, settings, member, trigger, parent):
        settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
        if outcome == "preclaim":
            settings = settings.model_copy(
                update={"openai_api_key": SecretStr("sk-proj-" + "changed-synthetic" * 5)}
            )

        async def provider(payload):
            if outcome == "preclaim":
                raise AssertionError("preclaim refusal must never send")
            if outcome == "unknown" and payload["stage"] == "critique":
                raise httpx.ReadTimeout("synthetic transport unknown")

        calls, clients = install_sol_http(monkeypatch, None, on_request=provider)
        async with factory() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            before = tuple(
                await session.scalars(
                    text(
                        "SELECT to_jsonb(o)::text FROM agent_research_outputs o "
                        "WHERE run_id=:id ORDER BY sequence"
                    ),
                    {"id": parent.id},
                )
            )
        normal = loop.AgentPaperLoopSnapshot(
            now=now,
            calendar_authority_id=CALENDAR_ID,
            market_open_at=now - timedelta(hours=1),
            decision_window_start_at=now - timedelta(minutes=30),
            decision_window_end_at=now + timedelta(hours=4),
            next_market_open_at=now + timedelta(days=3),
            next_decision_window_start_at=now + timedelta(days=3),
        )
        selected = await inspect_monitor_queue(
            settings, account_id=settings.agent_paper_account_id, snapshot=normal
        )
        assert selected.monitor_trigger_id == trigger.id

        async def environment(*args, **kwargs):
            values = settings.model_dump(mode="json", by_alias=True)
            values["openai_api_key"] = settings.openai_api_key.get_secret_value()
            assert (
                loop._FileEngineSettings.model_validate(values).openai_api_key
                == settings.openai_api_key
            )
            return values, str(CALENDAR_ID)

        async def normal_inspection(*args, **kwargs):
            return normal

        async def no_evaluation(*args, **kwargs):
            return None

        async def no_manual(*args, **kwargs):
            return kwargs["snapshot"]

        monkeypatch.setattr(loop, "_load_bound_loop_environment", environment)
        monkeypatch.setattr(loop, "inspect_agent_paper_loop", normal_inspection)
        monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", no_evaluation)
        monkeypatch.setattr(manual_paper_host, "inspect_manual_close", no_manual)
        monkeypatch.setattr(manual_paper_host, "inspect_manual_queue", no_manual)
        dispatch = partial(
            loop.execute_agent_paper_loop_action,
            settings,
            account_id=settings.agent_paper_account_id,
            repo_root=Path.cwd(),
            env_file=Path("isolated.env"),
            action=loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT,
            snapshot=selected,
        )
        if outcome == "untyped":
            attempts = []

            async def uncertain(*args, **kwargs):
                attempts.append(kwargs["job_ids"])
                raise ValueError(CODE)

            monkeypatch.setattr("nexus.worker.run_agent_worker_once", uncertain)
            with pytest.raises(ValueError, match=CODE):
                await dispatch()
            async with factory() as session:
                job = await session.get(Job, member.job_id)
                run = await session.get(AgentResearchRunRecord, member.run_id)
                hold = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == run.id
                    )
                )
                assert job.status == "queued" and job.attempt_count == 0
                assert run.settled_cost_usd is None and hold.state == "reserved"
            again = await dispatch_configured_once(
                settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
            )
            assert again["status"] == "unconfirmed" and attempts == [(member.job_id,)] and not calls
            return
        result = await dispatch()
        assert result == (
            loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN if outcome == "unknown" else None
        )
        expected = {"completed": "completed", "preclaim": "failed", "unknown": "unknown"}[outcome]
        async with factory() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            row = await session.get(MonitorTrigger, trigger.id)
            assert row.status == expected
            run = await session.get(AgentResearchRunRecord, member.run_id)
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            if outcome == "unknown":
                assert run.settled_cost_usd is None and hold.state != "released"
                with pytest.raises(ValueError):
                    await require_monitor_account_available(
                        session, account_id=settings.agent_paper_account_id, now=now
                    )
            else:
                await require_monitor_account_available(
                    session, account_id=settings.agent_paper_account_id, now=now
                )
            assert (
                tuple(
                    await session.scalars(
                        text(
                            "SELECT to_jsonb(o)::text FROM agent_research_outputs o "
                            "WHERE run_id=:id ORDER BY sequence"
                        ),
                        {"id": parent.id},
                    )
                )
                == before
            )
            frozen_after = await frozen(session)
        again = await dispatch_configured_once(
            settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
        )
        assert again["status"] == expected
        async with factory() as session:
            assert await frozen(session) == frozen_after
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='research.monitor.worker_dispatched'"
                    )
                )
                == 1
            )
        assert (
            calls
            == (
                {
                    "completed": ["research", "critique", "synthesis"],
                    "preclaim": [],
                    "unknown": ["research", "critique"],
                }[outcome]
            )
        )
        assert all(c.is_closed for c in clients)

    await prepare_increment(migrated_database_url, monkeypatch, after_admitted=validate)

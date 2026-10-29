"""Host automatic dispatch to original native Worker, with no model replay."""

from dataclasses import replace
from datetime import timedelta

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_monitor_increment_pg import ready_trigger
from nexus.ops import agent_paper_loop as loop
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorIncrement
from nexus.persistence.models.runtime import AuditEvent, Job
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True])
async def test_automatic_single_stock_dispatch_settles_original_job_only(
    migrated_database_url, monkeypatch, unknown
):
    from nexus.ops.monitor_dispatch import dispatch_configured_once, inspect_monitor_queue

    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
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
    assert loop.choose_loop_action(selected) == loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT
    daily = replace(normal, cycle_status="researching", research_job_status="queued")
    assert (
        await inspect_monitor_queue(
            settings, account_id=settings.agent_paper_account_id, snapshot=daily
        )
    ).monitor_trigger_id is None
    from nexus.agents import codex_staged

    original = codex_staged.invoke_codex
    calls = []

    async def invoke(payload, **kwargs):
        calls.append(payload["stage"])
        if unknown and payload["stage"] == "critique":
            raise TimeoutError("controlled unknown")
        return await original(payload, **kwargs)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    # Exercise the normal host execution entry, not only the dispatch helper.
    from pathlib import Path

    from nexus.ops import manual_paper_host

    async def environment(*args, **kwargs):
        return settings.model_dump(mode="json", by_alias=True), str(CALENDAR_ID)

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
    action_result = await loop.execute_agent_paper_loop_action(
        settings,
        account_id=settings.agent_paper_account_id,
        repo_root=Path.cwd(),
        env_file=Path("isolated.env"),
        action=loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT,
        snapshot=selected,
    )
    assert action_result == (loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN if unknown else None)
    async with factory() as session:
        from nexus.persistence.models.monitoring import MonitorTrigger

        completed_trigger = await session.get(MonitorTrigger, trigger.id)
        result = {"status": completed_trigger.status}
    assert result["status"] == ("unknown" if unknown else "completed"), result
    assert calls == (["research", "critique"] if unknown else ["research", "critique", "synthesis"])
    again = await dispatch_configured_once(
        settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
    )
    assert again["status"] == result["status"]
    assert len(calls) == (2 if unknown else 3)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 4
        member = await session.scalar(select(MonitorIncrement))
        child = await session.get(AgentResearchRunRecord, member.run_id)
        job = await session.get(Job, member.job_id)
        assert child.instrument_id == parent.instrument_id
        assert job.attempt_count == 1 and job.manual_retry_count == 0
        assert (child.settled_cost_usd is None) is unknown
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "research.monitor.dispatch_checked")
            )
            == 1
        )
    assert await loop.confirm_agent_paper_shutdown(
        settings,
        account_id=settings.agent_paper_account_id,
        expected_calendar_authority_id=CALENDAR_ID,
        action=loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT,
        before_snapshot=selected,
        action_result=None,
    ) is (not unknown)
    final = await inspect_monitor_queue(
        settings, account_id=settings.agent_paper_account_id, snapshot=normal
    )
    assert final.monitor_trigger_id == (trigger.id if unknown else None)
    if unknown:
        assert loop.choose_loop_action(final) == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["timeout", "not_claimed"])
async def test_dispatch_interruption_before_native_claim_never_replays(
    migrated_database_url, monkeypatch, outcome
):
    from nexus.ops.monitor_dispatch import dispatch_configured_once

    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
    calls = []

    async def interrupted(*args, **kwargs):
        calls.append(kwargs["job_ids"])
        if outcome == "timeout":
            raise TimeoutError("controlled preclaim observation lost")
        return False

    monkeypatch.setattr("nexus.worker.run_agent_worker_once", interrupted)
    if outcome == "timeout":
        with pytest.raises(TimeoutError):
            await dispatch_configured_once(
                settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
            )
    else:
        first = await dispatch_configured_once(
            settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
        )
        assert first["status"] == "unconfirmed"
    result = await dispatch_configured_once(
        settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
    )
    assert result["status"] == "unconfirmed"
    assert len(calls) == 1
    async with factory() as session:
        member = await session.scalar(select(MonitorIncrement))
        job = await session.get(Job, member.job_id)
        assert job.status == "queued" and job.attempt_count == 0
        assert await session.scalar(select(func.count()).select_from(MonitorIncrement)) == 1
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_failed_eligibility_is_bounded_without_erasing_trigger(
    migrated_database_url, monkeypatch
):
    from uuid import uuid4

    from nexus.monitoring import admission
    from nexus.ops.monitor_dispatch import dispatch_configured_once, inspect_monitor_queue

    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
    bad = settings.model_copy(update={"agent_paper_formal_model_config_id": uuid4()})
    calls = []
    native = admission.admit_trigger

    async def tracked(*args, **kwargs):
        calls.append(kwargs["trigger_id"])
        return await native(*args, **kwargs)

    monkeypatch.setattr(admission, "admit_trigger", tracked)
    first = await dispatch_configured_once(
        bad, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
    )
    assert first["status"] == "blocked"
    again = await dispatch_configured_once(
        settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
    )
    assert again["status"] == "checked" and len(calls) == 1
    async with factory() as session:
        from nexus.persistence.models.monitoring import MonitorTrigger

        persisted = await session.get(MonitorTrigger, trigger.id)
        assert (
            persisted.status == "blocked"
            and persisted.research_run_id is None
            and persisted.outbox_id == trigger.outbox_id
        )
        assert await session.scalar(select(func.count()).select_from(MonitorIncrement)) == 0
    normal = loop.AgentPaperLoopSnapshot(
        now=now,
        calendar_authority_id=CALENDAR_ID,
        market_open_at=now - timedelta(hours=1),
        decision_window_start_at=now - timedelta(minutes=30),
        decision_window_end_at=now + timedelta(hours=4),
        next_decision_window_start_at=now + timedelta(days=3),
    )
    assert (
        await inspect_monitor_queue(
            settings, account_id=settings.agent_paper_account_id, snapshot=normal
        )
    ).monitor_trigger_id is None
    later = replace(normal, now=now + timedelta(minutes=5))
    assert (
        await inspect_monitor_queue(
            settings, account_id=settings.agent_paper_account_id, snapshot=later
        )
    ).monitor_trigger_id == trigger.id
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_native_worker_capacity_rejection_closes_without_model_or_false_unknown(
    migrated_database_url, monkeypatch
):
    from nexus.ops.monitor_dispatch import confirm_monitor_shutdown, dispatch_configured_once
    from nexus.paper.manual_request import require_manual_account_available
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
    checks = []

    async def reject(session, settings, run, **kwargs):
        job = await session.scalar(select(Job).where(Job.payload["run_id"].astext == str(run.id)))
        assert job.status == "queued" and job.attempt_count == 0
        checks.append(run.id)
        raise ValueError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")

    async def forbidden(*args, **kwargs):
        raise AssertionError("preclaim rejection called a model")

    monkeypatch.setattr("nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", reject)
    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", forbidden)
    result = await dispatch_configured_once(
        settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
    )
    assert result["status"] == "failed" and result["reason"] == "MONITOR_CAPACITY_REJECTED"
    assert len(checks) == 1
    again = await dispatch_configured_once(
        settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
    )
    assert again["status"] == "failed" and len(checks) == 1
    async with factory() as session, session.begin():
        member = await session.scalar(select(MonitorIncrement))
        run = await session.get(AgentResearchRunRecord, member.run_id)
        job = await session.get(Job, member.job_id)
        budget = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run.id
            )
        )
        assert job.status == "skipped" and job.attempt_count == 1
        assert (run.input_tokens, run.output_tokens, run.settled_cost_usd) == (0, 0, 0)
        assert budget.state == "released" and member.reserved_sdk_calls == 3
        observed_at = await session.scalar(select(func.clock_timestamp()))
        await require_manual_account_available(
            session, account_id=settings.agent_paper_account_id, now=observed_at
        )
    assert await confirm_monitor_shutdown(
        settings, account_id=settings.agent_paper_account_id, trigger_id=trigger.id
    )
    await factory.kw["bind"].dispose()

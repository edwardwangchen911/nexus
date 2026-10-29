"""Real Worker/committee/SDK code with a controlled SDK process, isolated DB only."""

import pytest
from helpers.staged_worker import run_staged_worker
from integration import test_monitor_mixed_increment_pg as mixed
from nexus.worker import run_agent_worker_once


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, "veto", "unresolved", "rollback", True])
async def test_staged_worker_preserves_normal_close_replacement_and_next_admission(
    migrated_database_url, monkeypatch, tmp_path, outcome
):
    await exercise(migrated_database_url, monkeypatch, tmp_path, outcome)


async def exercise(
    database_url, monkeypatch, tmp_path, outcome, phase_hook=None, independent=False, controls=None
):
    from helpers.staged_worker import require_staged_monitor_account_available

    monkeypatch.setattr(
        "nexus.monitoring.admission.require_monitor_account_available",
        require_staged_monitor_account_available,
    )
    captured = {}
    prepare = mixed.prepared_materials

    async def capture(*args, **kwargs):
        result = await prepare(*args, **kwargs)
        captured["fixture"] = result
        result[3]["sdk_phase_hook"] = phase_hook
        result[3].update(controls or {})
        if result[3].get("select_increment_protocol", True):
            from helpers.staged_routes import install_increment_preparation

            install_increment_preparation(monkeypatch)
        return result

    async def staged(settings, **kwargs):
        if "fixture" not in captured:
            return await run_agent_worker_once(settings, **kwargs)
        return await run_staged_worker(
            settings, monkeypatch=monkeypatch, fixture=captured["fixture"], **kwargs
        )

    # The generation helper imports Worker by value. Bind this test's adapter
    # explicitly so a module first imported under a prior patch cannot retain
    # the prior disposable database after that fixture has been dropped.
    from helpers import monitor_generation

    monkeypatch.setattr(monitor_generation, "run_agent_worker_once", staged)
    monkeypatch.setattr(mixed, "prepared_materials", capture)
    monkeypatch.setattr("nexus.worker.run_agent_worker_once", staged)
    # Existing assertions are reused intact: once-only Job, exact shared budget,
    # final output, mixed-event audit veto, original predecessor CAS, normal
    # finish/reconcile/shutdown and next-account admission. No live model I/O.
    await mixed.test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
        database_url, monkeypatch, tmp_path, unknown=outcome, independent=independent
    )


@pytest.mark.asyncio
async def test_actual_sdk_response_is_visible_before_cleanup_and_scan_commits(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops.research_monitor import scan_configured_once
    from nexus.persistence.models.monitoring import MonitorCheck
    from sqlalchemy import select, text

    phases = []
    first_checks = None

    async def phase_hook(phase, factory, settings):
        nonlocal first_checks
        phases.append(phase)
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            )
            assert await observer.scalar(text("SELECT count(*) FROM stage_proto_observation")) == (
                phase == "cleanup"
            )
            assert await observer.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 0
            assert (
                await observer.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND pid<>pg_backend_pid() AND state='idle in transaction'"
                    )
                )
                == 0
            )
            before = set(await observer.scalars(select(MonitorCheck.id)))
            if first_checks is None:
                first_checks = before
        result = await scan_configured_once(settings, calendar_authority_id=CALENDAR_ID)
        assert result["new_triggers"] == result["notifications"] == 0
        async with factory() as observer:
            after = set(await observer.scalars(select(MonitorCheck.id)))
            assert after - first_checks
            if phase == "cleanup":
                # Same five-minute slot: a second committed scan is deduplicated.
                assert after == before

    await exercise(migrated_database_url, monkeypatch, tmp_path, "unresolved", phase_hook)
    assert phases == ["model", "cleanup"]


@pytest.mark.asyncio
async def test_independent_parent_uses_exact_increment_owner_without_fake_cycle(
    migrated_database_url, monkeypatch, tmp_path
):
    await exercise(migrated_database_url, monkeypatch, tmp_path, "unresolved", independent=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["price", "disclosure"])
async def test_normal_new_generation_blocks_old_predecessor_replacement(
    migrated_database_url, monkeypatch, tmp_path, kind
):
    await exercise(migrated_database_url, monkeypatch, tmp_path, "superseded_" + kind)


class ObservedCloseout(BaseException):
    pass


@pytest.mark.asyncio
async def test_known_usage_cleanup_failure_cannot_release_normal_admission(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.staged_worker import require_staged_monitor_account_available
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from sqlalchemy import select, text

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            run = await session.get(AgentResearchRunRecord, member.run_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert run.status == "failed"
            assert (run.input_tokens, run.output_tokens) == (100, 100)
            assert budget.state == "settled" and run.settled_cost_usd == budget.settled_usd
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_observation")) == 1
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 0
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_staged_monitor_account_available(
                    session, account_id=member.account_id, now=captured["clock"][0]
                )
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"sdk_cleanup_failure": True, "post_worker_hook": closed},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["commit", "rollback", "payload", "tokens"])
async def test_sdk_observation_fault_never_publishes_or_spends_unknown_and_still_closes(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchOutputRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from sqlalchemy import select, text

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            run = await session.get(AgentResearchRunRecord, member.run_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert captured.get("sdk_close_calls", 0) >= 1
            assert captured.get("sdk_run_calls", 0) == 1
            assert run.status == "failed"
            assert budget.state == "reserved" and run.settled_cost_usd is None
            assert not await session.scalar(
                select(AgentResearchOutputRecord.id).where(
                    AgentResearchOutputRecord.run_id == run.id,
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            assert not await session.scalar(
                text("SELECT known FROM stage_proto_terminal WHERE run_id=:run"), {"run": run.id}
            )
        raise ObservedCloseout

    control = {"post_worker_hook": closed}
    if fault in {"commit", "rollback"}:
        control["observation_commit_fault"] = fault == "commit"
    else:
        control["observation_mutation"] = fault
    with pytest.raises(ObservedCloseout):
        await exercise(migrated_database_url, monkeypatch, tmp_path, "unresolved", controls=control)


@pytest.mark.asyncio
async def test_stop_observed_after_authorization_prevents_actual_sdk_send(
    migrated_database_url, monkeypatch, tmp_path
):
    async def closed(factory, settings, captured):
        assert captured.get("sdk_run_calls", 0) == 0
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"stop_before_sdk": True, "post_worker_hook": closed},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True])
async def test_terminal_commit_uncertainty_preserves_native_closeout_path(
    migrated_database_url, monkeypatch, tmp_path, server_commits
):
    from datetime import timedelta

    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
    from sqlalchemy import select

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            job = await session.get(Job, member.job_id)
            run = await session.get(AgentResearchRunRecord, member.run_id)
            attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert job.status == ("succeeded" if server_commits else "running")
            assert run.status == ("completed" if server_commits else "prepared")
            assert attempt.outcome == ("succeeded" if server_commits else "running")
            assert budget.state == ("settled" if server_commits else "reserved")
            expiry = lease.expires_at
        if not server_commits:
            assert await captured["protocol"].close(
                captured["claim"], now=expiry + timedelta(seconds=1), orphan=True
            )
            # Record the remaining public-consumer gap explicitly. Known orphan
            # accounting is safe, but dead_letter is not a normal admission proof.
            from helpers.staged_worker import require_staged_monitor_account_available
            from sqlalchemy import text

            async with factory.begin() as session:
                assert (
                    await session.scalar(
                        text("SELECT stage_proto_inflight(:scope)"), {"scope": member.id}
                    )
                    == 0
                )
                with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_staged_monitor_account_available(
                        session, account_id=member.account_id, now=expiry + timedelta(seconds=1)
                    )
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"terminal_commit_fault": server_commits, "post_worker_hook": closed},
        )


@pytest.mark.asyncio
async def test_known_prefix_then_observed_stop_settles_once_without_later_send(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from sqlalchemy import select, text

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            run = await session.get(AgentResearchRunRecord, member.run_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert captured["sdk_run_calls"] == 1
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            assert run.status == "failed" and (run.input_tokens, run.output_tokens) == (100, 100)
            assert budget.state == "settled" and run.settled_cost_usd == budget.settled_usd
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"stop_after_cleanup": True, "post_worker_hook": closed},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True])
async def test_authorization_commit_uncertainty_has_no_generic_failure_or_send(
    migrated_database_url, monkeypatch, tmp_path, server_commits
):
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.models.runtime import Job, JobAttempt
    from sqlalchemy import select, text

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            job = await session.get(Job, member.job_id)
            assert captured.get("sdk_run_calls", 0) == 0
            assert job.status == "running"
            assert not isinstance(captured.get("adapter_error"), KeyError)
            assert job.attempt_count == 1
            run = await session.get(AgentResearchRunRecord, member.run_id)
            attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert run.status == "prepared" and attempt.outcome == "running"
            assert budget.state == "reserved" and budget.settled_usd is None
            assert await session.scalar(
                text("SELECT count(*) FROM stage_proto_authorization")
            ) == int(server_commits)
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 0
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"authorization_commit_fault": server_commits, "post_worker_hook": closed},
        )


@pytest.mark.asyncio
async def test_untrusted_observation_then_terminal_rollback_cannot_settle_as_orphan(
    migrated_database_url, monkeypatch, tmp_path
):
    from datetime import timedelta

    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.models.runtime import JobLease
    from sqlalchemy import select

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == member.job_id))
            expiry = lease.expires_at
        assert not await captured["protocol"].close(
            captured["claim"], now=expiry + timedelta(seconds=1), orphan=True
        )
        async with factory() as session:
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert budget.state == "reserved" and budget.settled_usd is None
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={
                "observation_mutation": "tokens",
                "terminal_commit_fault": False,
                "post_worker_hook": closed,
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_sdk_original_error_survives_failed_stop_and_cleanup_still_runs(
    migrated_database_url, monkeypatch, tmp_path, cancelled
):
    import asyncio

    from helpers.staged_protocol import StagedProtocol
    from nexus.agents.codex_sdk import CodexInvocationError
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.monitoring import MonitorIncrement
    from sqlalchemy import select, text

    original = (
        asyncio.CancelledError("controlled cancellation")
        if cancelled
        else CodexInvocationError("CODEX_RESPONSE_INCOMPLETE", provider_call_started=True)
    )

    async def failed_stop(self, scope):
        raise RuntimeError("controlled stop write failure")

    monkeypatch.setattr(StagedProtocol, "stop", failed_stop)

    async def closed(factory, settings, captured):
        assert captured["invocation_error"] is original
        assert "SCOPE_STOP_UNCONFIRMED" in original.__notes__
        assert captured["protocol"].local_stopped
        assert captured["sdk_run_calls"] == 1 and captured["sdk_close_calls"] == 1
        if cancelled:
            assert captured["adapter_error"] is original
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert budget.state == "reserved" and budget.settled_usd is None
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={
                "sdk_original_error": original,
                "post_worker_hook": closed,
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_first", [True, False])
async def test_stop_and_final_publication_obey_committed_scope_order(
    migrated_database_url, monkeypatch, tmp_path, stop_first
):
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from sqlalchemy import select, text

    before = {}

    async def prepare(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            before["scope"] = member.id
            before["plans"] = await session.scalar(
                text("SELECT count(*) FROM research_monitor_plans")
            )
            before["receipts"] = await session.scalar(
                text("SELECT count(*) FROM research_monitor_mixed_event_receipts")
            )
        if stop_first:
            await captured["protocol"].stop(member.id)

    async def closed(factory, settings, captured):
        if not stop_first:
            await captured["protocol"].stop(before["scope"])
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            run = await session.get(AgentResearchRunRecord, member.run_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert captured["sdk_run_calls"] == 3
            assert run.status == ("failed" if stop_first else "completed")
            assert budget.state == "settled" and (run.input_tokens, run.output_tokens) == (300, 300)
            count = await session.scalar(
                text(
                    "SELECT count(*) FROM agent_research_outputs "
                    "WHERE run_id=:run AND status='completed'"
                ),
                {"run": run.id},
            )
            assert count == (0 if stop_first else 1)
            assert (
                await session.scalar(text("SELECT count(*) FROM research_monitor_plans"))
                == before["plans"]
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_monitor_mixed_event_receipts")
                )
                == before["receipts"]
            )
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"before_final_hook": prepare, "post_worker_hook": closed},
        )


@pytest.mark.asyncio
async def test_terminal_sql_failure_never_enters_generic_worker_failure(
    migrated_database_url, monkeypatch, tmp_path
):
    from datetime import timedelta

    from helpers.staged_worker import StagedFinalizationFailed
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
    from sqlalchemy import select, text
    from sqlalchemy.exc import DBAPIError

    async def prepare(factory, settings, captured):
        async with factory.begin() as session:
            await session.execute(
                text(
                    "CREATE FUNCTION reject_stage_terminal() RETURNS trigger "
                    "LANGUAGE plpgsql AS $$ "
                    "BEGIN RAISE EXCEPTION 'controlled terminal SQL rejection'; END $$"
                )
            )
            await session.execute(
                text(
                    "CREATE TRIGGER reject_stage_terminal BEFORE INSERT ON stage_proto_terminal "
                    "FOR EACH ROW EXECUTE FUNCTION reject_stage_terminal()"
                )
            )

    async def closed(factory, settings, captured):
        error = captured["adapter_error"]
        assert isinstance(error, StagedFinalizationFailed)
        assert isinstance(error.__cause__, DBAPIError)
        assert error.__cause__.orig.sqlstate == "P0001"
        assert "controlled terminal SQL rejection" in str(error.__cause__.orig)
        async with factory.begin() as session:
            member = await session.scalar(select(MonitorIncrement))
            job = await session.get(Job, member.job_id)
            run = await session.get(AgentResearchRunRecord, member.run_id)
            attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert captured["sdk_run_calls"] == 3
            assert job.status == "running" and attempt.outcome == "running"
            assert (
                run.status == "prepared"
                and budget.state == "reserved"
                and budget.settled_usd is None
            )
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 0
            for table in ("authorization", "observation", "verification", "cleanup"):
                assert await session.scalar(text("SELECT count(*) FROM stage_proto_" + table)) == 3
            await session.execute(
                text("DROP TRIGGER reject_stage_terminal ON stage_proto_terminal")
            )
            expiry = lease.expires_at
        assert await captured["protocol"].close(
            captured["claim"], now=expiry + timedelta(seconds=1), orphan=True
        )
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"before_final_hook": prepare, "post_worker_hook": closed},
        )

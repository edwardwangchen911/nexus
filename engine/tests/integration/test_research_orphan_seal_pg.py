"""Native interrupted research through explicit seal, close and fresh admission."""

import asyncio
from argparse import Namespace
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_formal_research_orphan_pg import (
    test_formal_research_orphan_preserves_completed_and_unknown as recover_case,
)
from nexus.ops import manual_paper
from nexus.paper import history_seal, manual_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, Job, JobAttempt, JobLease
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


async def original_facts(session):
    """Whole original facts, including known sibling and unused sibling budgets."""
    return {
        model.__tablename__: sorted(
            history_seal._row_hash(row) for row in await session.scalars(select(model))
        )
        for model in (
            AgentPaperCycleRecord,
            AgentPaperCycleEventRecord,
            AgentResearchRunRecord,
            AgentResearchOutputRecord,
            AgentResearchEventRecord,
            AgentDailyBudgetReservationRecord,
            Job,
            JobAttempt,
            JobLease,
        )
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "same_day",
        "cross_day",
        "no_ack",
        "default_policy",
        "sdk_policy",
        "wrong_run",
        "worker",
        "fence",
        "job_key",
        "event_hash",
        "terminal_hash",
        "stage_input",
        "live_lease",
        "extra_event",
        "budget_released",
        "usage",
        "request_hash",
        "source_evidence",
        "process_live",
        "late_process",
        "close_rollback",
        "concurrent",
        "late_fact",
        "repeat_with_new_producer",
    ],
)
async def test_orphan_normal_seal_closes_manual_and_admits_fresh_request(
    migrated_database_url, monkeypatch, tmp_path, case, *, known_prefix=None, legacy=False
):
    cross_day = case in {"cross_day", "cross_day_no_ack"}
    # Existing regression runs normal HTTP/manual/Worker paths, interrupts only the
    # second Worker, resumes twice and proves known/unknown/unused accounting.
    await recover_case(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "manual",
        known_prefix=known_prefix,
        legacy=legacy,
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            request = await session.scalar(select(ManualPaperRequest))
            run = await session.scalar(
                select(AgentResearchRunRecord).where(AgentResearchRunRecord.status == "failed")
            )
            now = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
            if cross_day:
                now += timedelta(days=3)
            before = await original_facts(session)
        await _set_test_insert_clock(sessions, now)
        scalar = AsyncSession.scalar

        async def database_clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return now
            return await scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", database_clock)
        monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *a: None)
        lifecycle = []

        @contextmanager
        def lifecycle_lock(*_args):
            owner = asyncio.current_task()
            lifecycle.append(owner)
            try:
                yield
            finally:
                lifecycle.remove(owner)

        monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lifecycle_lock)
        monkeypatch.setattr(history_seal, "require_research_processes_quiescent", lambda: None)
        settings = EngineSettings(
            database_url=migrated_database_url, agent_paper_account_id=cycle.account_id
        )
        args = Namespace(
            action="seal-failed-research",
            repo_root=Path.cwd(),
            cycle_id=cycle.id,
            run_ids=(run.id,),
            accept_unknown_usage=True,
            same_day=not cross_day,
            expired_attempt=not cross_day,
        )
        async with sessions() as session:
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(run.id))
            )
            lease = await session.get(JobLease, job.id)
            if lease is None:
                lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            claim = SimpleNamespace(
                job_id=job.id,
                attempt_no=1,
                lease_token=lease.lease_token,
                fence_token=lease.fence_token,
                worker_id=lease.worker_id,
            )
        mutations = {
            "usage": (
                "agent_research_runs",
                "input_tokens=coalesce(input_tokens,0)+1",
                "id",
                run.id,
            ),
            "request_hash": ("agent_research_runs", "input_hash=repeat('0',64)", "id", run.id),
            "source_evidence": (
                "agent_research_runs",
                "evidence_hash=repeat('0',64)",
                "id",
                run.id,
            ),
            "worker": ("job_attempts", "worker_id='wrong-worker'", "job_id", job.id),
            "fence": ("job_leases", "fence_token=fence_token+1", "job_id", job.id),
            "job_key": ("jobs", "job_key=repeat('0',64)", "id", job.id),
            "event_hash": ("agent_research_events", "event_hash=repeat('0',64)", "run_id", run.id),
            "terminal_hash": (
                "agent_research_outputs",
                "content_hash=repeat('0',64)",
                "run_id",
                run.id,
            ),
            "stage_input": (
                "agent_research_outputs",
                "payload=payload || '{\"stage_input\":{}}'::jsonb",
                "run_id",
                run.id,
            ),
            "live_lease": (
                "job_leases",
                "expires_at=expires_at+interval '1 day'",
                "job_id",
                job.id,
            ),
            "budget_released": (
                "agent_daily_budget_reservations",
                "state='released'",
                "run_id",
                run.id,
            ),
        }
        if case in mutations:
            table, change, key, value = mutations[case]
            async with sessions.begin() as session:
                # Inject corruption only into the isolated DB, beyond its immutable guard.
                await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
                await session.execute(
                    text(f"UPDATE {table} SET {change} WHERE {key}=:id"), {"id": value}
                )
                await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
        if case == "extra_event":
            async with sessions.begin() as session:
                session.add(
                    AgentResearchEventRecord(
                        run_id=run.id, sequence=2, state="failed", details={}, event_hash="a" * 64
                    )
                )
        if case in {"no_ack", "cross_day_no_ack"}:
            args.accept_unknown_usage = False
        elif case == "default_policy":
            args.same_day = args.expired_attempt = False
        elif case == "sdk_policy":
            args.expired_attempt = False
        elif case == "wrong_run":
            args.run_ids = (UUID(int=1),)
        process_checks = []

        def quiescent():
            if args.expired_attempt:
                assert asyncio.current_task() in lifecycle
            process_checks.append(True)
            if case == "process_live" or (case == "late_process" and len(process_checks) == 2):
                raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")

        monkeypatch.setattr(history_seal, "require_research_processes_quiescent", quiescent)
        if case == "close_rollback":
            close = manual_request.close_failed_manual_request

            async def reject_after_close(*a, **kw):
                await close(*a, **kw)
                raise ValueError("controlled close rollback")

            monkeypatch.setattr(manual_request, "close_failed_manual_request", reject_after_close)
        failures = set(mutations) | {
            "no_ack",
            "cross_day_no_ack",
            "default_policy",
            "sdk_policy",
            "wrong_run",
            "extra_event",
            "process_live",
            "late_process",
            "close_rollback",
        }
        if case in failures:
            async with sessions() as session:
                before_rejection = await original_facts(session)
            with pytest.raises(ValueError, match="MANUAL_|controlled close rollback"):
                await manual_paper.run(settings, args)
            async with sessions() as session:
                assert await original_facts(session) == before_rejection
                assert (
                    await manual_request.read_manual_request(session, request)
                ).status == "active"
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(AuditEvent)
                        .where(AuditEvent.action == "paper.legacy_research.sealed")
                    )
                    == 0
                )
            return
        if case == "concurrent":
            receipts = await asyncio.gather(
                manual_paper.run(settings, args), manual_paper.run(settings, args)
            )
            assert sorted(r.replayed for r in receipts) == [False, True]
            receipt = next(r for r in receipts if not r.replayed)
        else:
            receipt = await manual_paper.run(settings, args)
        assert receipt.sealed_research == 1 and not receipt.replayed
        assert (await manual_paper.run(settings, args)).replayed
        assert not lifecycle
        import httpx
        from helpers.paper_api import headers
        from helpers.paper_api import settings as api_settings
        from nexus.main import create_engine_app

        application = create_engine_app(
            api_settings(migrated_database_url).model_copy(
                update={"agent_paper_account_id": cycle.account_id}
            )
        )
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application), base_url="http://testserver"
            ) as client:
                for path, expected_status, nested in (
                    (f"/internal/v1/paper/manual-requests/{request.id}", "unavailable", False),
                    (f"/internal/v1/paper/management/cycles/{cycle.id}", "unavailable", True),
                ):
                    response = await client.get(path, headers=headers("GET", path))
                    assert response.status_code == 200
                    data = response.json()["data"]
                    if nested:
                        data = data["cycle"]
                    assert data["status"] == expected_status and data["execution_locked"]
        finally:
            await application.state.session_factory.kw["bind"].dispose()
        async with sessions.begin() as session:
            assert await original_facts(session) == before
            assert (
                await manual_request.read_manual_request(session, request)
            ).status == "unavailable"
            await manual_request.require_manual_account_available(
                session, account_id=cycle.account_id, now=now
            )
            assert await history_seal.legacy_research_sealed(
                session, cycle_id=cycle.id, run_id=run.id, now=now
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "paper.legacy_research.sealed")
                )
                == 1
            )
            # Account admission is distinct from source freshness and market time.
            fresh = await manual_request.admit_manual_request(
                session,
                account_id=cycle.account_id,
                batch_id=request.candidate_batch_id,
                model_config_id=request.model_config_id,
                mode="research_and_paper",
                idempotency_key="independent-after-orphan-seal",
            )
            assert fresh.request_id != request.id and fresh.status == "queued"
            assert await original_facts(session) == before
            assert await history_seal.legacy_research_sealed(
                session, cycle_id=cycle.id, run_id=run.id, now=now
            )
        if case == "repeat_with_new_producer":
            async with sessions.begin() as session:
                view, claimed = await manual_request.claim_manual_request(
                    session, request_id=fresh.request_id
                )
                assert claimed and view.status == "preparing"
            monkeypatch.setattr(
                history_seal,
                "require_research_processes_quiescent",
                lambda: (_ for _ in ()).throw(
                    AssertionError("new producer must not block old receipt")
                ),
            )
            assert (await manual_paper.run(settings, args)).replayed
        async with sessions() as session:
            # Same proof still valid after UTC midnight; original policy is frozen.
            assert await history_seal.legacy_research_sealed(
                session, cycle_id=cycle.id, run_id=run.id, now=now + timedelta(days=1)
            )
        if case == "late_fact":
            async with sessions.begin() as session:
                session.add(
                    AgentResearchEventRecord(
                        run_id=run.id, sequence=2, state="failed", details={}, event_hash="a" * 64
                    )
                )
            async with sessions() as session:
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await history_seal.legacy_research_sealed(
                        session, cycle_id=cycle.id, run_id=run.id, now=now
                    )
        from nexus.jobs.manual_retry import ManualRetryError, retry_manual_safe_job
        from nexus.jobs.repository import LeaseLostError, checkpoint_job, execute_and_finalize_job

        async def forbidden(*a):
            raise AssertionError("late execution reached business callback")

        for operation in ("checkpoint", "finalize"):
            with pytest.raises(LeaseLostError):
                async with sessions.begin() as session:
                    if operation == "checkpoint":
                        await checkpoint_job(session, claim=claim, now=now, apply=forbidden)
                    else:
                        await execute_and_finalize_job(
                            session, claim=claim, checked_at=now, execute=forbidden
                        )
        with pytest.raises(ManualRetryError, match="RETRY_NOT_PERMITTED"):
            async with sessions.begin() as session:
                await retry_manual_safe_job(
                    session,
                    job_id=job.id,
                    actor_id="isolated",
                    expected_version=job.version,
                    idempotency_key="never-replay",
                    now=now,
                )
    finally:
        await engine.dispose()

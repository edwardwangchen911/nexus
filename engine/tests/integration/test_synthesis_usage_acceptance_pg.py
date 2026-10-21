"""Explicit acceptance preserves failed synthesis facts and only records an audit."""

import asyncio
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_candidate_multistock_path import FIVE
from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
from nexus.agents.codex_sdk import CodexInvocationError
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.agents.portfolio_synthesis_worker import run_synthesis_job_once
from nexus.ops import agent_paper_once, manual_paper
from nexus.paper import manual_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, Job, JobAttempt, JobLease
from nexus.settings import EngineSettings
from sqlalchemy import func, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

ACTION = "paper.synthesis.unknown_usage_accepted"
BLOCKED = "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
PRESERVED = (
    AgentPaperCycleRecord,
    AgentPaperCycleEventRecord,
    AgentResearchRunRecord,
    AgentResearchOutputRecord,
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    Job,
    JobAttempt,
    JobLease,
)


async def _facts(session):
    """Compare full synthetic records, without weakening a business field assertion."""
    result = {}
    for model in PRESERVED:
        rows = list(await session.scalars(select(model)))
        result[model.__tablename__] = sorted(
            [tuple(repr(getattr(row, col.key)) for col in inspect(model).columns) for row in rows]
        )
    return result


async def _audit_count(session):
    return await session.scalar(
        select(func.count()).select_from(AuditEvent).where(AuditEvent.action == ACTION)
    )


async def _failed_case(url, monkeypatch):
    from integration import test_portfolio_same_day_research_pg as normal_seed

    original_seed = normal_seed.seed_normal_path

    async def current_dated_seed(*args, **kwargs):
        return await original_seed(
            *args, **{**kwargs, "decision_time": datetime(2026, 10, 19, 14, tzinfo=UTC)}
        )

    monkeypatch.setattr(normal_seed, "seed_normal_path", current_dated_seed)
    engine, sessions, args = await _seed_normal_synthesis(
        url, monkeypatch, candidate_instruments=FIVE[:3], partial_research=True
    )
    async with sessions.begin() as session:
        # The reusable research fixture supplies completed synthetic outputs.
        # Complete its synthetic bookkeeping before any recovery operation;
        # the failed synthesis itself always traverses the actual Worker.
        for research in await session.scalars(select(AgentResearchRunRecord)):
            reserve = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == research.id
                )
            )
            reserve.state = "settled"
            reserve.settled_usd = research.settled_cost_usd
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(research.id))
            )
            token = uuid4()
            session.add(
                JobLease(
                    job_id=job.id,
                    worker_id="seed-completed-research",
                    lease_token=token,
                    fence_token=1,
                    acquired_at=job.started_at,
                    heartbeat_at=job.started_at,
                    expires_at=job.finished_at + timedelta(seconds=1),
                )
            )
            session.add(
                JobAttempt(
                    job_id=job.id,
                    worker_id="seed-completed-research",
                    lease_token=token,
                    fence_token=1,
                    attempt_no=1,
                    started_at=job.started_at,
                    finished_at=job.finished_at,
                    outcome="succeeded",
                    error_code=None,
                )
            )
        run = (await admit_portfolio_synthesis(session, **args)).run
        cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
        job_id = await session.scalar(
            select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
        )
    calls = []

    class UnknownTransport:
        def supports(self, model):
            return True

        async def invoke(self, payload):
            calls.append(payload["input_hash"])
            raise CodexInvocationError(
                "CODEX_TIMEOUT", phase="turn_run", provider_call_started=True
            )

    await run_synthesis_job_once(
        sessions,
        transport=UnknownTransport(),
        worker_id="acceptance-test",
        clock=lambda: args["now"] + timedelta(seconds=1),
        job_ids=(job_id,),
    )
    now = args["now"] + timedelta(seconds=122)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(agent_paper_once, "datetime", Clock)
    original_scalar = AsyncSession.scalar

    async def db_clock(self, statement, *a, **kw):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return now
        return await original_scalar(self, statement, *a, **kw)

    monkeypatch.setattr(AsyncSession, "scalar", db_clock)
    settings = EngineSettings(
        database_url=url, agent_runtime_mode="codex_sdk", agent_paper_account_id=cycle.account_id
    )
    result = await agent_paper_once.resume_once(
        settings,
        agent_paper_once.AgentPaperResumeRequest(
            action="resume", cycle_id=cycle.id, idempotency_key="unknown-synthesis-resume"
        ),
    )
    assert result.status == "unavailable"
    assert result.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
    assert len(calls) == 1
    return (
        engine,
        sessions,
        settings,
        dict(account_id=cycle.account_id, cycle_id=cycle.id, run_id=run.id, now=now),
        calls,
    )


def _acceptance(monkeypatch):
    from nexus.paper import synthesis_usage_acceptance as acceptance

    monkeypatch.setattr(acceptance, "require_preparation_producers_stopped", lambda *_a, **_k: None)
    return acceptance


@pytest.mark.asyncio
async def test_normal_failed_synthesis_fixture_is_unknown_and_blocks_account(
    migrated_database_url, monkeypatch
):
    engine, sessions, _, scope, calls = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            run = await session.get(AgentPortfolioSynthesisRunRecord, scope["run_id"])
            reserve = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
                )
            )
            assert (
                run.status == "failed"
                and run.input_tokens is run.output_tokens is run.settled_cost_usd is None
            )
            assert reserve.state == "reserved" and reserve.settled_usd is None
            assert len(list(await session.scalars(select(AgentResearchRunRecord)))) == 3
            with pytest.raises(ValueError, match=BLOCKED):
                await manual_request.require_manual_account_available(
                    session, account_id=scope["account_id"], now=scope["now"]
                )
            assert len(calls) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_normal_cli_check_accept_replay_preserves_facts_and_releases_only_gate(
    migrated_database_url, monkeypatch, tmp_path
):
    acceptance = _acceptance(monkeypatch)
    engine, sessions, settings, scope, calls = await _failed_case(
        migrated_database_url, monkeypatch
    )
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_a: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lambda *_a: nullcontext())
    try:
        async with sessions.begin() as session:
            before = await _facts(session)
            assert await _audit_count(session) == 0
            with pytest.raises(ValueError, match=BLOCKED):
                await manual_request.require_manual_account_available(
                    session, account_id=scope["account_id"], now=scope["now"]
                )
        base = ["--repo-root", str(tmp_path), "--env-file", str(tmp_path / "fixture.env")]
        ids = ["--cycle-id", str(scope["cycle_id"]), "--run-id", str(scope["run_id"])]
        check = manual_paper.parser().parse_args(base + ["check-synthesis-acceptance"] + ids)
        checked = await manual_paper.run(settings, check)
        assert checked.status == "eligible_at_check" and checked.check_only
        async with sessions() as session:
            assert await _facts(session) == before and await _audit_count(session) == 0
        accept = manual_paper.parser().parse_args(
            base + ["accept-failed-synthesis-usage"] + ids + ["--accept-unknown-usage"]
        )
        accepted = await manual_paper.run(settings, accept)
        assert accepted.status == "accepted_usage_unknown" and not accepted.replayed
        assert (
            accepted.usage == "unknown" and accepted.budget_preserved and accepted.execution_locked
        )
        repeated = await manual_paper.run(settings, accept)
        assert repeated.replayed
        async with sessions.begin() as session:
            assert await _facts(session) == before and await _audit_count(session) == 1
            assert await acceptance.synthesis_usage_accepted(
                session, cycle_id=scope["cycle_id"], run_id=scope["run_id"], now=scope["now"]
            )
            await manual_request.require_manual_account_available(
                session, account_id=scope["account_id"], now=scope["now"]
            )
        async with sessions.begin() as session:
            await manual_request.require_settled_account_cycle_history(
                session,
                account_id=scope["account_id"],
                now=scope["now"],
                allow_scheduled_completion=True,
            )
        assert len(calls) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["confirmation", "wrong_run", "live_lease", "producer"])
async def test_rejects_without_writing_or_releasing_unknown_budget(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, calls = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            before = await _facts(session)
            args = dict(
                scope,
                repo_root=tmp_path,
                env_file=tmp_path / "fixture.env",
                accept_unknown_usage=fault != "confirmation",
            )
            if fault == "wrong_run":
                args["run_id"] = uuid4()
            if fault == "live_lease":
                args["now"] -= timedelta(seconds=121)
            if fault == "producer":

                def busy(*a, **kw):
                    raise ValueError(BLOCKED)

                monkeypatch.setattr(acceptance, "require_preparation_producers_stopped", busy)
            with pytest.raises(ValueError):
                await acceptance.accept_failed_synthesis_usage(session, **args)
            assert await _facts(session) == before and await _audit_count(session) == 0
        assert len(calls) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_acceptance_rolls_back_with_outer_transaction(
    migrated_database_url, monkeypatch, tmp_path
):
    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, _ = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions() as session:
            before = await _facts(session)
        with pytest.raises(RuntimeError, match="synthetic rollback"):
            async with sessions.begin() as session:
                await acceptance.accept_failed_synthesis_usage(
                    session,
                    **scope,
                    repo_root=tmp_path,
                    env_file=tmp_path / "fixture.env",
                    accept_unknown_usage=True,
                )
                raise RuntimeError("synthetic rollback")
        async with sessions.begin() as session:
            assert await _facts(session) == before and await _audit_count(session) == 0
            with pytest.raises(ValueError, match=BLOCKED):
                await manual_request.require_manual_account_available(
                    session, account_id=scope["account_id"], now=scope["now"]
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_accept_is_exact_once(migrated_database_url, monkeypatch, tmp_path):
    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, _ = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions() as session:
            before = await _facts(session)

        async def accept():
            async with sessions.begin() as session:
                return await acceptance.accept_failed_synthesis_usage(
                    session,
                    **scope,
                    repo_root=tmp_path,
                    env_file=tmp_path / "fixture.env",
                    accept_unknown_usage=True,
                )

        first, second = await asyncio.gather(accept(), accept())
        assert sorted([first.replayed, second.replayed]) == [False, True]
        async with sessions() as session:
            assert await _audit_count(session) == 1 and await _facts(session) == before
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_late_attempt_invalidates_acceptance_without_rewriting_it(
    migrated_database_url, monkeypatch, tmp_path
):
    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, calls = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            await acceptance.accept_failed_synthesis_usage(
                session,
                **scope,
                repo_root=tmp_path,
                env_file=tmp_path / "fixture.env",
                accept_unknown_usage=True,
            )
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(scope["run_id"]))
            )
            original = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            # A new contradictory persisted fact must revoke eligibility even if
            # the original terminal Run/Job/Output and acceptance remain unchanged.
            session.add(
                JobAttempt(
                    job_id=job.id,
                    attempt_no=2,
                    lease_token=uuid4(),
                    fence_token=original.fence_token + 1,
                    worker_id="late-fact",
                    started_at=scope["now"],
                    finished_at=scope["now"],
                    outcome="failed",
                    error_code="CODEX_TIMEOUT",
                )
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match=BLOCKED):
                await acceptance.synthesis_usage_accepted(
                    session, cycle_id=scope["cycle_id"], run_id=scope["run_id"], now=scope["now"]
                )
            with pytest.raises(ValueError, match=BLOCKED):
                await manual_request.require_manual_account_available(
                    session, account_id=scope["account_id"], now=scope["now"]
                )
            assert await _audit_count(session) == 1
            reserve = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == scope["run_id"]
                )
            )
            assert reserve.state == "reserved" and reserve.settled_usd is None
        assert len(calls) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_accepted_history_allows_distinct_normal_manual_key_without_dispatch(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.agents.codex_bounded_budget import bounded_stage_config, bounded_stage_config_hash
    from nexus.persistence.models.agents import AgentModelConfigRecord

    acceptance = _acceptance(monkeypatch)
    engine, sessions, settings, scope, calls = await _failed_case(
        migrated_database_url, monkeypatch
    )
    try:
        async with sessions.begin() as session:
            config = bounded_stage_config()
            model = AgentModelConfigRecord(
                model_id=str(config["model_id"]),
                version=str(config["version"]),
                config_hash=bounded_stage_config_hash(),
                max_input_tokens=int(config["max_input_tokens"]),
                max_output_tokens=int(config["max_output_tokens"]),
                max_tool_calls=int(config["max_tool_calls"]),
                input_usd_per_million=Decimal(str(config["input_usd_per_million"])),
                output_usd_per_million=Decimal(str(config["output_usd_per_million"])),
                pricing_available_at=datetime.fromisoformat(str(config["pricing_available_at"])),
                pricing_expires_at=datetime.fromisoformat(str(config["pricing_expires_at"])),
                created_at=scope["now"] - timedelta(minutes=1),
            )
            session.add(model)
            await session.flush()
            cycle = await session.get(AgentPaperCycleRecord, scope["cycle_id"])
            before = await _facts(session)
            request_args = dict(
                account_id=scope["account_id"],
                batch_id=cycle.candidate_batch_id,
                model_config_id=model.id,
                mode="research_only",
                idempotency_key="distinct-request-after-explicit-synthesis-acceptance",
            )
            with pytest.raises(ValueError, match=BLOCKED):
                await manual_request.admit_manual_request(session, **request_args)
            await acceptance.accept_failed_synthesis_usage(
                session,
                **scope,
                repo_root=tmp_path,
                env_file=tmp_path / "fixture.env",
                accept_unknown_usage=True,
            )
            queued = await manual_request.admit_manual_request(session, **request_args)
            assert queued.status == "queued"
            replayed = await manual_request.admit_manual_request(session, **request_args)
            assert replayed.request_id == queued.request_id
            assert await acceptance.synthesis_usage_accepted(
                session, cycle_id=scope["cycle_id"], run_id=scope["run_id"], now=scope["now"]
            )
            with pytest.raises(ValueError, match=BLOCKED):
                await manual_request._require_cycle_jobs_settled(
                    session, cycle_id=scope["cycle_id"], now=scope["now"]
                )
            assert await _facts(session) == before
            assert await _audit_count(session) == 1

        class NeverDispatch:
            def supports(self, model):
                raise AssertionError("accepted failure cannot be dispatched")

            async def invoke(self, payload):
                raise AssertionError("accepted failure cannot call model")

        async with sessions() as session:
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(scope["run_id"]))
            )
        assert not await run_synthesis_job_once(
            sessions,
            transport=NeverDispatch(),
            worker_id="accepted-no-replay",
            clock=lambda: scope["now"],
            job_ids=(job_id,),
        )
        assert len(calls) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_normal_management_api_reads_disposition_without_mutating_failed_facts(
    migrated_database_url, monkeypatch, tmp_path
):
    import httpx
    from helpers.paper_api import app, headers
    from nexus.paper.read_service import PaperReadService

    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, _ = await _failed_case(migrated_database_url, monkeypatch)
    try:

        async def read(expected):
            async with sessions() as session:
                before = await _facts(session)
                audit_count = await _audit_count(session)
                detail = await PaperReadService(session, scope["now"]).management_cycle_detail(
                    scope["cycle_id"]
                )
                assert detail is not None
                assert detail.synthesis.status == "failed" and detail.cycle.status == "unavailable"
                assert detail.synthesis_usage_disposition.status == expected
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app(migrated_database_url, now=scope["now"])),
                base_url="http://testserver",
            ) as client:
                response = await client.get(
                    f"/internal/v1/paper/management/cycles/{scope['cycle_id']}",
                    headers=headers(
                        "GET", f"/internal/v1/paper/management/cycles/{scope['cycle_id']}"
                    ),
                )
                assert response.status_code == 200
                data = response.json()["data"]
                assert data["synthesis"]["status"] == "failed"
                assert data["synthesis_usage_disposition"] == dict(
                    status=expected, usage="unknown", budget_preserved=True, execution_allowed=False
                )
            async with sessions() as session:
                assert await _facts(session) == before
                assert await _audit_count(session) == audit_count

        await read("not_accepted")
        async with sessions.begin() as session:
            await acceptance.accept_failed_synthesis_usage(
                session,
                **scope,
                repo_root=tmp_path,
                env_file=tmp_path / "fixture.env",
                accept_unknown_usage=True,
            )
        await read("accepted")
        async with sessions.begin() as session:
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(scope["run_id"]))
            )
            original = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            session.add(
                JobAttempt(
                    job_id=job.id,
                    attempt_no=2,
                    lease_token=uuid4(),
                    fence_token=original.fence_token + 1,
                    worker_id="late-api-fact",
                    started_at=scope["now"],
                    finished_at=scope["now"],
                    outcome="failed",
                    error_code="CODEX_TIMEOUT",
                )
            )
        await read("invalid")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("write", ["job_update", "attempt_update", "attempt_insert"])
async def test_acceptance_fence_blocks_concurrent_writers_until_commit(
    migrated_database_url, monkeypatch, tmp_path, write
):
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, _ = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as accepting:
            before = await _facts(accepting)
            await acceptance.accept_failed_synthesis_usage(
                accepting,
                **scope,
                repo_root=tmp_path,
                env_file=tmp_path / "fixture.env",
                accept_unknown_usage=True,
            )
            job_id = await accepting.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(scope["run_id"]))
            )
            sql = {
                "job_update": (
                    "UPDATE jobs SET status='dead_letter',version=version+1 WHERE id=:job"
                ),
                "attempt_update": "UPDATE job_attempts SET worker_id=worker_id WHERE job_id=:job",
                "attempt_insert": (
                    "INSERT INTO job_attempts SELECT * FROM job_attempts WHERE job_id=:job"
                ),
            }[write]
            with pytest.raises(DBAPIError) as rejected:
                async with engine.begin() as competing:
                    await competing.execute(text("SET LOCAL lock_timeout='500ms'"))
                    await competing.execute(text(sql), {"job": job_id})
            assert rejected.value.orig.sqlstate == "55P03"
            assert await _facts(accepting) == before
            assert await _audit_count(accepting) == 1
        async with sessions.begin() as session:
            assert await acceptance.synthesis_usage_accepted(
                session, cycle_id=scope["cycle_id"], run_id=scope["run_id"], now=scope["now"]
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_existing_writer_rejects_acceptance_without_audit_or_business_change(
    migrated_database_url, monkeypatch, tmp_path
):
    from sqlalchemy import text

    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, _ = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions() as session:
            before = await _facts(session)
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(scope["run_id"]))
            )
        async with engine.connect() as writer:
            transaction = await writer.begin()
            try:
                await writer.execute(
                    text("UPDATE jobs SET status='dead_letter',version=version+1 WHERE id=:job"),
                    {"job": job_id},
                )
                with pytest.raises(ValueError):
                    async with sessions.begin() as session:
                        await acceptance.accept_failed_synthesis_usage(
                            session,
                            **scope,
                            repo_root=tmp_path,
                            env_file=tmp_path / "fixture.env",
                            accept_unknown_usage=True,
                        )
            finally:
                await transaction.rollback()
        async with sessions() as session:
            assert await _audit_count(session) == 0 and await _facts(session) == before
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_accepted_unknown_synthesis_still_consumes_shared_budget(
    migrated_database_url, monkeypatch, tmp_path
):
    """Synthetic paid fixture proves the common guard, not subscription billing."""
    from nexus.agents.budget import BudgetExhausted
    from nexus.agents.persistence import _shared_daily_budget_used, reserve_daily_budget

    acceptance = _acceptance(monkeypatch)
    engine, sessions, _, scope, _ = await _failed_case(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            before = await _facts(session)
            reserve = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == scope["run_id"]
                )
            )
            used_before = await _shared_daily_budget_used(
                session, business_date=scope["now"].date()
            )
            assert reserve.state == "reserved" and reserve.settled_usd is None
            assert used_before >= reserve.reserved_usd > 0
            # Hold the same ceiling across acceptance. A new positive-cost
            # synthetic request must be rejected before any reservation/Job insert.
            budget_args = dict(
                run_id=uuid4(),
                now=scope["now"],
                daily_limit_usd=used_before,
                worst_case_usd=Decimal("0.00001"),
                pricing_version="synthetic-budget-guard",
                pricing_fresh=True,
            )
            with pytest.raises(BudgetExhausted, match="daily research budget exhausted"):
                await reserve_daily_budget(session, **budget_args)
            await acceptance.accept_failed_synthesis_usage(
                session,
                **scope,
                repo_root=tmp_path,
                env_file=tmp_path / "fixture.env",
                accept_unknown_usage=True,
            )
            await manual_request.require_manual_account_available(
                session, account_id=scope["account_id"], now=scope["now"]
            )
            with pytest.raises(BudgetExhausted, match="daily research budget exhausted"):
                await reserve_daily_budget(session, **budget_args)
            assert (
                await _shared_daily_budget_used(session, business_date=scope["now"].date())
                == used_before
            )
            assert await _facts(session) == before
            assert await _audit_count(session) == 1
    finally:
        await engine.dispose()


@pytest.fixture
def recovery_database_url(temporary_database_factory, migrated_database_template):
    """Own one strict recovery-named clone; never address a formal database."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    name = "nexus_recovery_" + uuid4().hex
    admin = temporary_database_factory.admin_url

    async def command(statement):
        engine = create_async_engine(admin, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                await connection.execute(text(statement))
        finally:
            await engine.dispose()

    asyncio.run(command(f'CREATE DATABASE "{name}" TEMPLATE "{migrated_database_template.name}"'))
    try:
        yield admin.set(database=name).render_as_string(hide_password=False)
    finally:
        asyncio.run(command(f'DROP DATABASE "{name}" WITH (FORCE)'))


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["none", "source_configuration", "inspector_script"])
async def test_isolated_normal_cli_uses_owner_and_rolls_back_identity_change(
    recovery_database_url, monkeypatch, tmp_path, fault
):
    import subprocess

    from sqlalchemy.engine import make_url

    engine, sessions, settings, scope, _ = await _failed_case(recovery_database_url, monkeypatch)
    from nexus.paper import synthesis_usage_acceptance as acceptance

    owner = tmp_path / "owner"
    owner.mkdir()
    subprocess.run(["git", "init", "-q", str(owner)], check=True)
    (owner / ".gitignore").write_text("*.env\n")
    source = owner / "source.env"
    source.write_text(
        "DATABASE_URL="
        + make_url(recovery_database_url)
        .set(database="postgres")
        .render_as_string(hide_password=False)
        + "\n"
    )
    source.chmod(0o600)
    candidate = tmp_path / "candidate"
    script = candidate / "scripts/macos/agent-paper-loop-service.py"
    script.parent.mkdir(parents=True)
    script.write_text("# trusted test inspector\n")
    subprocess.run(["git", "init", "-q", str(candidate)], check=True)
    subprocess.run(["git", "-C", str(candidate), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(candidate),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    revision = subprocess.check_output(
        ["git", "-C", str(candidate), "rev-parse", "HEAD"], text=True
    ).strip()
    settings = settings.model_copy(update={"release_revision": revision})
    guards = []

    def producer(root, env, *, inspector_repo_root=None, inspector_revision=None):
        assert root == owner and env == source and inspector_repo_root == candidate
        assert inspector_revision == revision
        guards.append(True)
        if fault == "inspector_script" and len(guards) == 3:
            script.write_text("# drift during transaction\n")
        if fault == "source_configuration" and len(guards) == 3:
            # The last guard occurs after the audit insertion, before CLI commit.
            source.write_text(source.read_text() + "# changed\n")

    monkeypatch.setattr(acceptance, "require_preparation_producers_stopped", producer)
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_a: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lambda *_a: nullcontext())
    base = ["--repo-root", str(candidate), "--env-file", str(candidate / "isolated.env")]
    ids = [
        "--cycle-id",
        str(scope["cycle_id"]),
        "--run-id",
        str(scope["run_id"]),
        "--recovery-producer-repo-root",
        str(owner),
        "--recovery-producer-env-file",
        str(source),
    ]
    try:
        async with sessions() as session:
            before = await _facts(session)
        check = manual_paper.parser().parse_args(base + ["check-synthesis-acceptance"] + ids)
        assert (await manual_paper.run(settings, check)).status == "eligible_at_check"
        accept = manual_paper.parser().parse_args(
            base + ["accept-failed-synthesis-usage"] + ids + ["--accept-unknown-usage"]
        )
        if fault != "none":
            from nexus.paper.manual_preparation_close import PreparationCloseDiagnosticError

            error = (
                PreparationCloseDiagnosticError
                if fault == "inspector_script"
                else acceptance.SynthesisUsageDiagnosticError
            )
            with pytest.raises(error):
                await manual_paper.run(settings, accept)
        else:
            assert (await manual_paper.run(settings, accept)).status == "accepted_usage_unknown"
            assert (await manual_paper.run(settings, accept)).replayed
        async with sessions() as session:
            assert await _facts(session) == before
            assert await _audit_count(session) == (0 if fault != "none" else 1)
            if fault != "none":
                with pytest.raises(ValueError, match=BLOCKED):
                    await manual_request.require_manual_account_available(
                        session, account_id=scope["account_id"], now=scope["now"]
                    )
            else:
                await manual_request.require_manual_account_available(
                    session, account_id=scope["account_id"], now=scope["now"]
                )
            await session.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_acceptance_proof_survives_nullable_diagnostic_column(
    migrated_database_url, monkeypatch, tmp_path
):
    """An actual pre-diagnostic receipt remains valid without rewriting its facts."""
    import hashlib
    import json

    from nexus.agents import portfolio_synthesis_worker

    acceptance = _acceptance(monkeypatch)
    # Old workers emitted no diagnostic; preserve that first-insert behavior.
    with monkeypatch.context() as legacy_worker:
        legacy_worker.setattr(
            portfolio_synthesis_worker, "freeze_failure_observation", lambda _: None
        )
        engine, sessions, _, scope, calls = await _failed_case(migrated_database_url, monkeypatch)

    def legacy_hash(row):
        values = {
            col.key: getattr(row, col.key)
            for col in inspect(type(row)).columns
            if col.key != "failure_diagnostic"
        }
        normalized = json.loads(json.dumps(values, default=str))
        return hashlib.sha256(
            json.dumps(
                normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()

    try:
        async with sessions.begin() as session:
            output = await session.scalar(select(AgentPortfolioSynthesisOutputRecord))
            assert output.failure_diagnostic is None
            before = await _facts(session)
            with monkeypatch.context() as legacy_reader:
                legacy_reader.setattr(acceptance, "_row_hash", legacy_hash)
                await acceptance.accept_failed_synthesis_usage(
                    session,
                    **scope,
                    repo_root=tmp_path,
                    env_file=tmp_path / "fixture.env",
                    accept_unknown_usage=True,
                )
        async with sessions.begin() as session:
            assert await acceptance.synthesis_usage_accepted(
                session, cycle_id=scope["cycle_id"], run_id=scope["run_id"], now=scope["now"]
            )
            await manual_request.require_manual_account_available(
                session, account_id=scope["account_id"], now=scope["now"]
            )
            assert await _facts(session) == before
            assert await _audit_count(session) == 1
            run = await session.get(AgentPortfolioSynthesisRunRecord, scope["run_id"])
            reserve = await session.scalar(select(AgentPortfolioSynthesisBudgetReservationRecord))
            assert run.status == "failed" and run.input_tokens is None
            assert reserve.state == "reserved" and reserve.settled_usd is None
        assert len(calls) == 1
    finally:
        await engine.dispose()

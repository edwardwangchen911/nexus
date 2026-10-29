"""Real lock waits and physical pooled connection isolation; no live services."""

import asyncio
import hashlib
from contextlib import contextmanager
from datetime import timedelta
from uuid import UUID

import pytest
from helpers.staged_protocol import ProtocolRejected
from integration.test_staged_protocol_pg import prepared
from sqlalchemy import select, text


async def wait_until_blocked(factory, blocker_pid):
    async with asyncio.timeout(10):
        while True:
            async with factory() as observer:
                if await observer.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND :pid=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'"
                    ),
                    {"pid": blocker_pid},
                ):
                    return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_after_cleanup", [False, True])
async def test_normal_terminal_has_one_time_and_public_closeout(
    migrated_database_url, monkeypatch, tmp_path, stop_after_cleanup
):
    from helpers.staged_public import install_public_readers, read_terminal_proof
    from integration.test_staged_worker_pg import ObservedCloseout, exercise
    from nexus.monitoring.admission import finish_increment, require_monitor_account_available
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.models.runtime import Job, JobAttempt
    from sqlalchemy.ext.asyncio import AsyncSession

    install_public_readers(monkeypatch)
    original_execute = AsyncSession.execute
    held = {}

    async def advance_after_terminal(self, statement, *args, **kwargs):
        result = await original_execute(self, statement, *args, **kwargs)
        if str(statement).startswith("INSERT INTO stage_proto_terminal"):
            held["clock"][0] += timedelta(seconds=1)
            held["advanced"] = True
        return result

    monkeypatch.setattr(AsyncSession, "execute", advance_after_terminal)

    async def before_final(factory, settings, captured):
        held["clock"] = captured["clock"]

    async def closed(factory, settings, captured):
        assert held["advanced"] and "adapter_error" not in captured
        async with factory.begin() as session:
            member = await session.scalar(select(MonitorIncrement))
            job = await session.get(Job, member.job_id)
            attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            terminal_at = await session.scalar(
                text("SELECT closed_at FROM stage_proto_terminal WHERE run_id=:run"),
                {"run": member.run_id},
            )
            assert terminal_at == job.finished_at == attempt.finished_at
            assert job.status == ("failed" if stop_after_cleanup else "succeeded")
            proof = await read_terminal_proof(
                session, run_id=member.run_id, now=captured["clock"][0]
            )
            assert proof.known and proof.cleanup_confirmed and not proof.orphan
            result = await finish_increment(
                session,
                account_id=member.account_id,
                trigger_id=member.trigger_id,
                now=captured["clock"][0],
            )
            assert result.status == ("failed" if stop_after_cleanup else "completed")
            await require_monitor_account_available(
                session, account_id=member.account_id, now=captured["clock"][0]
            )
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={
                "stop_after_cleanup": stop_after_cleanup,
                "before_final_hook": before_final,
                "post_worker_hook": closed,
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["observation", "verification", "cleanup"])
async def test_fresh_clock_does_not_repair_invalid_evidence_time(migrated_database_url, kind):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    current = [now]
    protocol.clock = lambda: current[0]
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"wire")
        if kind == "verification":
            current[0] = now + timedelta(seconds=1)
            await protocol.observe(ticket, now=current[0], payload=b"response", tokens=(1, 1))
        current[0] = now + timedelta(seconds=2)
        invalid_time = now - timedelta(seconds=1)
        with pytest.raises(ProtocolRejected, match=f"{kind.upper()}_BINDING_INVALID"):
            if kind == "observation":
                await protocol.observe(ticket, now=invalid_time, payload=b"response", tokens=(1, 1))
            elif kind == "verification":
                await protocol.verify(
                    ticket,
                    now=invalid_time,
                    wire_hash=hashlib.sha256(b"wire").hexdigest(),
                    payload=b"response",
                    tokens=(1, 1),
                )
            else:
                await protocol.cleanup(ticket, now=invalid_time, confirmed=True)
        async with factory() as session:
            assert await session.scalar(text(f"SELECT count(*) FROM stage_proto_{kind}")) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_verification_window_is_not_extended_by_scope_wait(migrated_database_url):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    current = [now]
    protocol.clock = lambda: current[0]
    task = None
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"wire")
        await protocol.observe(ticket, now=now, payload=b"response", tokens=(1, 1))
        async with factory() as blocker:
            await blocker.begin()
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(
                text("SELECT id FROM stage_proto_scope WHERE id=:id FOR UPDATE"), {"id": scope}
            )
            task = asyncio.create_task(
                protocol.verify(
                    ticket,
                    now=now,
                    wire_hash=hashlib.sha256(b"wire").hexdigest(),
                    payload=b"response",
                    tokens=(1, 1),
                )
            )
            await wait_until_blocked(factory, pid)
            current[0] = now + timedelta(seconds=111)
            await blocker.rollback()
        with pytest.raises(ProtocolRejected, match="VERIFICATION_WINDOW_CLOSED"):
            await task
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_verification")) == 0
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_observation")) == 1
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_window", [False, True])
async def test_session_exit_after_deadline_grants_no_ticket(
    migrated_database_url, monkeypatch, stage_window
):
    from sqlalchemy.ext.asyncio import AsyncSession

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    current = [now]
    protocol.clock = lambda: current[0]
    cutoff = now + timedelta(seconds=10 if stage_window else 100)
    commit = protocol._commit
    original_exit = AsyncSession.__aexit__
    held = {}

    async def committed(session, scope_id):
        await commit(session, scope_id)
        held["session"] = session

    async def late_exit(self, *args):
        result = await original_exit(self, *args)
        if self is held.get("session"):
            current[0] = cutoff + timedelta(seconds=1)
        return result

    monkeypatch.setattr(protocol, "_commit", committed)
    monkeypatch.setattr(AsyncSession, "__aexit__", late_exit)
    try:
        with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
            await protocol.authorize(claims[0], stage=1, now=now, wire=b"wire", not_after=cutoff)
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_observation")) == 0
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state<>'reserved'"
                    )
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_window", [False, True])
async def test_commit_return_after_deadline_grants_no_ticket(
    migrated_database_url, monkeypatch, stage_window
):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    current = [now]
    protocol.clock = lambda: current[0]
    commit = protocol._commit
    cutoff = now + timedelta(seconds=10 if stage_window else 100)

    async def late_ack(session, scope_id):
        await commit(session, scope_id)
        current[0] = cutoff + timedelta(seconds=1)

    monkeypatch.setattr(protocol, "_commit", late_ack)
    try:
        with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
            await protocol.authorize(claims[0], stage=1, now=now, wire=b"wire", not_after=cutoff)
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_observation")) == 0
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state<>'reserved'"
                    )
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("locked", ["scope", "job", "budget"])
async def test_authorization_rechecks_deadline_after_real_lock_wait(migrated_database_url, locked):
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.runtime import JobLease

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    current = [now]
    protocol.clock = lambda: current[0]
    claim = claims[0]
    task = None
    try:
        async with factory() as blocker:
            await blocker.begin()
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            table, column, identity = {
                "scope": ("stage_proto_scope", "id", scope),
                "job": ("jobs", "id", claim.job_id),
                "budget": (
                    "agent_daily_budget_reservations",
                    "run_id",
                    UUID(claim.payload["run_id"]),
                ),
            }[locked]
            await blocker.execute(
                text(f"SELECT 1 FROM {table} WHERE {column}=:id FOR UPDATE"), {"id": identity}
            )
            task = asyncio.create_task(protocol.authorize(claim, stage=1, now=now, wire=b"frozen"))
            await wait_until_blocked(factory, pid)
            current[0] = now + timedelta(seconds=101)
            await blocker.rollback()
        with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
            await task
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
            )
            assert all(
                b.state == "reserved" and b.settled_usd is None
                for b in await observer.scalars(select(AgentDailyBudgetReservationRecord))
            )
            lease = await observer.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
            assert lease.expires_at == claim.expires_at
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "python_error", "sql_error", "cancel"])
async def test_cancel_marker_cannot_escape_same_physical_connection(
    migrated_database_url, monkeypatch, tmp_path, outcome
):
    from helpers import staged_public
    from integration.test_staged_roots_pg import (
        test_normal_three_stock_roots_use_one_staged_protocol as roots,
    )
    from integration.test_staged_worker_pg import ObservedCloseout
    from nexus.jobs.models import QueueName
    from nexus.jobs.repository import claim_next_job
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.runtime import Job
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    async def before_worker(captured, settings, options):
        engine = create_async_engine(migrated_database_url, pool_size=1, max_overflow=0)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        original = (
            asyncio.CancelledError("controlled cancel")
            if outcome == "cancel"
            else ValueError("controlled error")
        )
        errors = []

        async def native_probe(session, **kwargs):
            assert (
                await session.scalar(text("SELECT current_setting('nexus.stage_protocol',true)"))
                == "staged-experiment-v1"
            )
            if outcome == "sql_error":
                try:
                    await session.execute(text("SELECT * FROM missing_pool_probe"))
                except DBAPIError as error:
                    errors.append(error)
                    raise
            if outcome != "success":
                raise original
            return 0

        monkeypatch.setattr(staged_public, "_native_cancel_unused", native_probe)
        try:
            if outcome in {"success", "python_error"}:
                async with factory.begin() as session:
                    pid = await session.scalar(text("SELECT pg_backend_pid()"))
                    await session.execute(text("SET LOCAL nexus.stage_protocol='outer-probe'"))
                    if outcome == "success":
                        assert await staged_public._cancel_unused(session) == 0
                    else:
                        with pytest.raises(ValueError) as raised:
                            await staged_public._cancel_unused(session)
                        assert raised.value is original
                    assert (
                        await session.scalar(
                            text("SELECT current_setting('nexus.stage_protocol',true)")
                        )
                        == "outer-probe"
                    )
            else:
                with pytest.raises(
                    DBAPIError if outcome == "sql_error" else asyncio.CancelledError
                ) as raised:
                    async with factory.begin() as session:
                        pid = await session.scalar(text("SELECT pg_backend_pid()"))
                        await session.execute(text("SET LOCAL nexus.stage_protocol='outer-probe'"))
                        await staged_public._cancel_unused(session)
                assert raised.value is (errors[0] if outcome == "sql_error" else original)
                if outcome == "sql_error":
                    assert "PROTOCOL_MARKER_RESTORE_UNCONFIRMED" in raised.value.__notes__
            async with factory.begin() as session:
                assert await session.scalar(text("SELECT pg_backend_pid()")) == pid
                assert (
                    await session.scalar(
                        text("SELECT current_setting('nexus.stage_protocol',true)")
                    )
                    or ""
                ) == ""
                job = await session.scalar(
                    select(Job).where(Job.job_type == "agent.paper.research").order_by(Job.id)
                )
                run = await session.get(
                    AgentResearchRunRecord, __import__("uuid").UUID(job.payload["run_id"])
                )

                async def exact(_session, selected):
                    return selected.id == job.id

                with pytest.raises(DBAPIError, match="staged worker required"):
                    async with session.begin_nested():
                        await claim_next_job(
                            session,
                            queue=QueueName.GENERAL,
                            worker_id="unselected-pool-borrower",
                            now=captured["clock"][0],
                            lease_seconds=330,
                            job_ids=(job.id,),
                            job_types=("agent.paper.research",),
                            locked_job_guard=exact,
                            research_budget_profile=run.pricing_version,
                        )
                assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
                assert (
                    await session.scalar(text("SELECT count(*) FROM stage_proto_authorization"))
                    == 0
                )
            assert captured.get("sdk_run_calls", 0) == 0
        finally:
            await engine.dispose()
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )


@pytest.mark.asyncio
async def test_actual_worker_lock_wait_cannot_rebase_sdk_work_deadline(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.staged_protocol import StagedProtocol
    from helpers.staged_worker import WorkerBindings, require_staged_monitor_account_available
    from integration.test_staged_worker_pg import ObservedCloseout, exercise
    from nexus.persistence.models.runtime import JobLease

    authorize = StagedProtocol.authorize
    held = {}
    bind = WorkerBindings.bind

    @contextmanager
    def observe_sdk_deadline(self, **callbacks):
        if "stage" in callbacks:
            stage = callbacks["stage"]

            async def observed(payload, **options):
                held["original_deadline"] = options["deadline_at"]
                held["parent_deadline"] = options["parent_deadline_at"]
                return await stage(payload, **options)

            callbacks["stage"] = observed
        with bind(self, **callbacks):
            yield

    monkeypatch.setattr(WorkerBindings, "bind", observe_sdk_deadline)

    async def blocked(self, claim, **kwargs):
        # Read the actual unchanged SDK stage allocation, distinct from its
        # parent budget. The parent timer must remain valid throughout the wait.
        deadline = held["original_deadline"]
        assert deadline + 1 < held["parent_deadline"]
        async with self.factory() as blocker:
            await blocker.begin()
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            member = (
                (
                    await blocker.execute(
                        text("SELECT * FROM stage_proto_member WHERE job_id=:job"),
                        {"job": claim.job_id},
                    )
                )
                .mappings()
                .one()
            )
            held["member"] = dict(member)
            await blocker.execute(
                text("SELECT id FROM stage_proto_scope WHERE id=:id FOR UPDATE"),
                {"id": member["scope_id"]},
            )
            task = asyncio.create_task(authorize(self, claim, **kwargs))
            try:
                await wait_until_blocked(self.factory, pid)
                elapsed = asyncio.Event()
                timer = asyncio.get_running_loop().call_at(deadline + 0.05, elapsed.set)
                try:
                    await elapsed.wait()
                finally:
                    timer.cancel()
                await blocker.rollback()
                return await task
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    monkeypatch.setattr(StagedProtocol, "authorize", blocked)

    async def before_final(factory, settings, captured):
        assert asyncio.get_running_loop().time() >= held["original_deadline"]
        assert captured.get("sdk_run_calls", 0) == 0
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
            current = (
                (
                    await session.execute(
                        text("SELECT * FROM stage_proto_member WHERE job_id=:job"),
                        {"job": captured["claim"].job_id},
                    )
                )
                .mappings()
                .one()
            )
            assert dict(current) == held["member"]
            lease = await session.scalar(
                select(JobLease).where(JobLease.job_id == captured["claim"].job_id)
            )
            assert lease.expires_at == captured["claim"].expires_at
        held["pre_final_verified"] = True

    async def after_worker(factory, settings, captured):
        from nexus.persistence.models.agents import (
            AgentDailyBudgetReservationRecord,
            AgentResearchRunRecord,
        )
        from nexus.persistence.models.monitoring import MonitorIncrement
        from nexus.persistence.models.runtime import Job

        assert held["pre_final_verified"]
        assert "adapter_error" not in captured, repr(captured.get("adapter_error"))
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            run = await session.get(AgentResearchRunRecord, member.run_id)
            job = await session.get(Job, member.job_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert job.status == "failed" and run.status == "failed"
            assert run.settled_cost_usd is None and budget.state == "reserved"
            assert budget.settled_usd is None
            assert (
                await session.scalar(text("SELECT count(*) FROM stage_proto_terminal WHERE known"))
                == 0
            )
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 1
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
            controls={"before_final_hook": before_final, "post_worker_hook": after_worker},
        )

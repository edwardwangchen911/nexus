"""Owned offline OS children through the real Worker/SDK lifecycle; no provider I/O."""

import asyncio
import subprocess
import sys
from contextlib import contextmanager, suppress
from datetime import timedelta
from types import SimpleNamespace

import pytest
from helpers.staged_public import install_public_readers, read_terminal_proof
from integration.test_staged_worker_pg import exercise
from nexus.agents import codex_sdk
from nexus.monitoring.admission import finish_increment, require_monitor_account_available
from nexus.paper.manual_host_exit import host_drain_scope
from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
from nexus.persistence.models.monitoring import MonitorIncrement
from nexus.persistence.models.runtime import Job, JobLease
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("responded", [False, True])
@pytest.mark.parametrize(
    "exit_mode", ["graceful", "forced", "unconfirmed", "poll-error", "resistant-close"]
)
async def test_repeated_cancel_keeps_owned_process_cleanup_and_original_budget(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    responded,
    exit_mode,
    block_observation=False,
    fault_phase=None,
    server_commits=False,
):
    install_public_readers(monkeypatch)
    started, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    held = {}
    task = None
    bounded_close = codex_sdk._close_portfolio_client
    from helpers.staged_worker import WorkerBindings

    native_bind = WorkerBindings.bind

    @contextmanager
    def record_original_work_window(self, **callbacks):
        stage = callbacks.get("stage")
        if stage is not None:

            async def recorded(payload, **options):
                held["stage_deadline"] = options["deadline_at"]
                held["parent_deadline"] = options["parent_deadline_at"]
                return await stage(payload, **options)

            callbacks["stage"] = recorded
        with native_bind(self, **callbacks):
            yield

    monkeypatch.setattr(WorkerBindings, "bind", record_original_work_window)

    async def original_window(client, run_task, *, deadline):
        held.setdefault("deadlines", []).append(deadline)
        held["bounded_enter"] = asyncio.get_running_loop().time()
        return await bounded_close(client, run_task, deadline=deadline)

    monkeypatch.setattr(codex_sdk, "_close_portfolio_client", original_window)

    async def setup(client, thread, response, captured):
        process = subprocess.Popen(
            [sys.executable, "-u", "-c", "print('READY',flush=True); input()"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            cwd=tmp_path,
            env={},
            start_new_session=True,
        )
        held.update(process=process, captured=captured, factory=captured["protocol"].factory)
        now = captured["protocol"].clock()
        held["lease_deadline"] = (
            asyncio.get_running_loop().time() + (captured["claim"].expires_at - now).total_seconds()
        )
        assert await asyncio.wait_for(asyncio.to_thread(process.stdout.readline), 10) == b"READY\n"

        if fault_phase is not None:
            from helpers.commit_ack_fault import lose_commit_ack
            from helpers.staged_protocol import StagedProtocol
            from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

            engine = held["fault_engine"] = create_async_engine(held["factory"].kw["bind"].url)
            dedicated = StagedProtocol(async_sessionmaker(engine), clock=captured["protocol"].clock)
            target = getattr(dedicated, fault_phase)

            async def fault(*args, **kwargs):
                with lose_commit_ack(engine, server_commits=server_commits) as evidence:
                    held["fault_evidence"] = evidence
                    return await target(*args, **kwargs)

            monkeypatch.setattr(captured["protocol"], fault_phase, fault)

        if block_observation:
            locker = held["locker"] = held["factory"]()
            await locker.begin()
            held["blocker_pid"] = await locker.scalar(text("SELECT pg_backend_pid()"))
            await locker.execute(text("SELECT id FROM stage_proto_scope FOR UPDATE"))

        def refused_kill():
            raise OSError("controlled owned-child termination unavailable")

        def failed_poll():
            if closing.is_set():
                raise OSError("controlled owned-child exit observation unavailable")
            return process.poll()

        observed_process = (
            SimpleNamespace(pid=process.pid, poll=process.poll, kill=refused_kill)
            if exit_mode == "unconfirmed"
            else process
        )
        if exit_mode == "poll-error":
            observed_process = SimpleNamespace(pid=process.pid, poll=failed_poll, kill=process.kill)
        client._client._sync = SimpleNamespace(_proc=observed_process)
        original_run = thread.run.side_effect

        async def run(*args, **kwargs):
            held["run_task"] = asyncio.current_task()
            result = await original_run(*args, **kwargs)
            started.set()
            if not responded:
                assert await asyncio.to_thread(process.stdout.readline) == b""
                raise ConnectionError("owned test process closed before response")
            return result

        async def close():
            held["close_task"] = asyncio.current_task()
            captured["sdk_close_calls"] = captured.get("sdk_close_calls", 0) + 1
            # Match the SDK's ownership hazard: close clears _proc before wait.
            client._client._sync._proc = None
            closing.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    if exit_mode != "resistant-close":
                        raise
            process.stdin.write(b"EXIT\n")
            process.stdin.flush()
            await asyncio.to_thread(process.wait)

        thread.run.side_effect = run
        client.close.side_effect = close

    try:
        with host_drain_scope() as drain:
            task = asyncio.create_task(
                exercise(
                    migrated_database_url,
                    monkeypatch,
                    tmp_path,
                    "unresolved",
                    controls={"sdk_client_setup": setup},
                )
            )
            startup = asyncio.create_task(started.wait())
            try:
                done, _ = await asyncio.wait(
                    (task, startup), timeout=40, return_when=asyncio.FIRST_COMPLETED
                )
                if task in done:
                    await task
                assert startup in done and started.is_set(), "SDK run was not reached"
            finally:
                startup.cancel()
                await asyncio.gather(startup, return_exceptions=True)
            if not responded:
                task.cancel("original owned cancellation")
            await asyncio.wait_for(closing.wait(), 10)
            if block_observation:
                from integration.test_staged_deadline_pool_pg import wait_until_blocked

                await wait_until_blocked(held["factory"], held["blocker_pid"])
            for attempt in range(2):
                task.cancel(
                    "original owned cancellation"
                    if responded and attempt == 0
                    else "later owned cancellation"
                )
                await asyncio.sleep(0)
            done, _ = await asyncio.wait((task,), timeout=1)
            assert not done, "parent returned before owned process exit"
            assert held["process"].poll() is None
            if exit_mode == "graceful":
                release.set()
            done, _ = await asyncio.wait((task,), timeout=15)
            assert task in done, "cleanup outlived its fixed envelope"
            with pytest.raises(asyncio.CancelledError) as cancelled:
                await task
            assert str(cancelled.value) == "original owned cancellation"
            assert held["captured"]["invocation_error"] is cancelled.value
            if exit_mode == "graceful":
                assert held["process"].poll() == 0
                assert drain.confirmed is (not block_observation and fault_phase is None)
            elif exit_mode in ("forced", "poll-error", "resistant-close"):
                assert held["process"].poll() is not None and not drain.confirmed
            else:
                assert held["process"].poll() is None and not drain.confirmed
            assert held["run_task"].done()
            if exit_mode == "resistant-close":
                assert not held["close_task"].done()
            else:
                assert held["close_task"].done()
            await asyncio.sleep(0)
            assert {t for t in held["captured"]["cleanup_owned_tasks"] if t.done()}.issubset(
                held["captured"]["cleanup_consumed_tasks"]
            )
            assert len(held["deadlines"]) == 1
            assert held["deadlines"][0] <= min(
                held["stage_deadline"] + 7,
                held["parent_deadline"],
                held["lease_deadline"],
                held["bounded_enter"] + 7,
            )
            assert asyncio.get_running_loop().time() <= held["deadlines"][0] + 2
        if block_observation:
            # Parent returned before the external holder releases observation/stop.
            await held["locker"].rollback()
            await held["locker"].close()
        captured, factory = held["captured"], held["factory"]
        known = (
            responded
            and not block_observation
            and fault_phase != "observe"
            and (fault_phase != "verify" or server_commits)
        )
        observation_exists = (
            responded and not block_observation and (fault_phase != "observe" or server_commits)
        )
        cleanup_known = (
            exit_mode == "graceful"
            and not block_observation
            and (fault_phase != "cleanup" or server_commits)
        )
        if fault_phase is not None:
            assert held["fault_evidence"].boundary_calls == 1
            assert held["fault_evidence"].server_commit_completed is server_commits
        assert captured["sdk_run_calls"] == captured["sdk_close_calls"] == 1
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            job = await session.get(Job, member.job_id)
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert job.status == "running" and job.attempt_count == 1
            assert budget.state == "reserved" and budget.settled_usd is None
            assert lease.expires_at == captured["claim"].expires_at
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            assert await session.scalar(
                text("SELECT count(*) FROM stage_proto_observation")
            ) == int(observation_exists)
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == int(
                cleanup_known
            )
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 0
            if block_observation:
                assert captured["protocol"].local_stopped
                assert "SCOPE_STOP_UNCONFIRMED" in cancelled.value.__notes__
            elif fault_phase == "stop":
                assert (
                    await session.scalar(text("SELECT stopped FROM stage_proto_scope"))
                    is server_commits
                )
                assert "SCOPE_STOP_UNCONFIRMED" in cancelled.value.__notes__
            else:
                assert await session.scalar(text("SELECT stopped FROM stage_proto_scope"))
        if exit_mode == "resistant-close":
            # Test teardown may release the resistant close, but never turn it
            # into a business cleanup success after the caller's cutoff.
            release.set()
            done, pending = await asyncio.wait(captured["cleanup_owned_tasks"], timeout=5)
            assert not pending
            await asyncio.sleep(0)
            assert set(done).issubset(captured["cleanup_consumed_tasks"])
            async with factory() as session:
                assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 0
        now = lease.expires_at + timedelta(seconds=1)
        assert await captured["protocol"].close(captured["claim"], now=now, orphan=True) is known
        async with factory.begin() as session:
            proof = await read_terminal_proof(session, run_id=member.run_id, now=now)
            assert proof.known is known
            assert proof.cleanup_confirmed is cleanup_known
            result = await finish_increment(
                session, account_id=member.account_id, trigger_id=member.trigger_id, now=now
            )
            assert result.status == (
                ("failed" if known else "unknown") if cleanup_known else "admitted"
            )
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert budget.state == ("settled" if known else "reserved")
            assert (budget.settled_usd is None) is not known
            if known and cleanup_known:
                await require_monitor_account_available(
                    session, account_id=member.account_id, now=now
                )
            else:
                with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_monitor_account_available(
                        session, account_id=member.account_id, now=now
                    )
    finally:
        if "locker" in held:
            await held["locker"].rollback()
            await held["locker"].close()
        release.set()
        process = held.get("process")
        if process is not None:
            if process.poll() is None:
                process.kill()
            await asyncio.to_thread(process.wait)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if "fault_engine" in held:
            await held["fault_engine"].dispose()
        if process is not None:
            with suppress(BrokenPipeError):
                process.stdin.close()
            process.stdout.close()


@pytest.mark.asyncio
async def test_blocked_observation_cannot_extend_process_or_stop_cleanup(
    migrated_database_url, monkeypatch, tmp_path
):
    await test_repeated_cancel_keeps_owned_process_cleanup_and_original_budget(
        migrated_database_url, monkeypatch, tmp_path, True, "graceful", block_observation=True
    )


@pytest.mark.asyncio
async def test_cancel_suppressing_cleanup_cannot_publish_after_original_deadline(
    migrated_database_url, monkeypatch, tmp_path, commit_mode=None
):
    entered, late = asyncio.Event(), asyncio.Event()
    held = {}

    async def setup(client, thread, response, captured):
        held.update(captured=captured, factory=captured["protocol"].factory)
        if commit_mode is not None:
            from helpers.staged_protocol import StagedProtocol
            from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
            from sqlalchemy.util import await_only

            engine = held["fault_engine"] = create_async_engine(held["factory"].kw["bind"].url)
            dedicated = StagedProtocol(async_sessionmaker(engine), clock=captured["protocol"].clock)
            dialect = engine.sync_engine.dialect
            native_commit = dialect.do_commit

            def delayed_commit(connection):
                dialect.do_commit = native_commit
                held["commit_calls"] = held.get("commit_calls", 0) + 1
                if commit_mode:
                    native_commit(connection)
                else:
                    dialect.do_rollback(connection)
                entered.set()
                try:
                    await_only(late.wait())
                except asyncio.CancelledError:
                    await_only(late.wait())
                if not commit_mode:
                    raise RuntimeError("controlled original COMMIT confirmation lost")

            dialect.do_commit = delayed_commit
            monkeypatch.setattr(captured["protocol"], "cleanup", dedicated.cleanup)

    async def hook(phase, factory, settings):
        if phase == "cleanup" and commit_mode is None:
            entered.set()
            try:
                await late.wait()
            except asyncio.CancelledError:
                await late.wait()

    drain_scope = host_drain_scope()
    drain = drain_scope.__enter__()
    task = asyncio.create_task(
        exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            phase_hook=hook,
            controls={"sdk_client_setup": setup},
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 40)
        task.cancel("original cleanup cancellation")
        done, _ = await asyncio.wait((task,), timeout=12)
        assert task in done
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not drain.confirmed
        late.set()
        owned = held["captured"]["cleanup_owned_tasks"]
        done, pending = await asyncio.wait(owned, timeout=5)
        assert not pending
        async with held["factory"]() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == int(
                commit_mode is True
            )
            if commit_mode is not None:
                assert held["commit_calls"] == 1
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id
                    == select(MonitorIncrement.run_id).scalar_subquery()
                )
            )
            assert budget.state == "reserved" and budget.settled_usd is None
            member = await session.scalar(select(MonitorIncrement))
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=member.account_id, now=held["captured"]["clock"][0]
                )
        assert not drain.confirmed
    finally:
        late.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if "fault_engine" in held:
            await held["fault_engine"].dispose()
        drain_scope.__exit__(None, None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault_phase", ["observe", "verify", "cleanup", "stop"])
@pytest.mark.parametrize("server_commits", [False, True])
async def test_process_cleanup_preserves_exact_commit_unknown_readback(
    migrated_database_url, monkeypatch, tmp_path, fault_phase, server_commits
):
    await test_repeated_cancel_keeps_owned_process_cleanup_and_original_budget(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        True,
        "graceful",
        fault_phase=fault_phase,
        server_commits=server_commits,
    )


@pytest.mark.asyncio
async def test_original_sdk_error_survives_repeated_cancel_and_auxiliary_failures(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.staged_protocol import StagedProtocol
    from nexus.agents.codex_sdk import CodexInvocationError

    original = CodexInvocationError("CODEX_RESPONSE_INCOMPLETE", provider_call_started=True)
    closing = asyncio.Event()
    held = {}

    class Inspected(Exception):
        pass

    async def failed_stop(self, scope):
        raise RuntimeError("controlled stop failure")

    monkeypatch.setattr(StagedProtocol, "stop", failed_stop)

    async def setup(client, thread, response, captured):
        process = held["process"] = subprocess.Popen(
            [sys.executable, "-u", "-c", "print('READY',flush=True); input()"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            cwd=tmp_path,
            env={},
            start_new_session=True,
        )
        assert await asyncio.to_thread(process.stdout.readline) == b"READY\n"
        client._client._sync = SimpleNamespace(_proc=process)

        async def close():
            captured["sdk_close_calls"] = captured.get("sdk_close_calls", 0) + 1
            client._client._sync._proc = None
            closing.set()
            await asyncio.Event().wait()

        client.close.side_effect = close

    async def inspected(factory, settings, captured):
        assert captured["invocation_error"] is original
        assert "SCOPE_STOP_UNCONFIRMED" in original.__notes__
        assert "SDK_CLEANUP_UNCONFIRMED" in original.__notes__
        assert captured["sdk_run_calls"] == captured["sdk_close_calls"] == 1
        assert held["process"].poll() is not None
        await asyncio.sleep(0)
        assert all(t.done() for t in captured["cleanup_owned_tasks"])
        assert set(captured["cleanup_owned_tasks"]).issubset(captured["cleanup_consumed_tasks"])
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 0
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id
                    == select(MonitorIncrement.run_id).scalar_subquery()
                )
            )
            assert budget.state == "reserved" and budget.settled_usd is None
        raise Inspected

    with host_drain_scope() as drain:
        task = asyncio.create_task(
            exercise(
                migrated_database_url,
                monkeypatch,
                tmp_path,
                "unresolved",
                controls={
                    "sdk_client_setup": setup,
                    "sdk_original_error": original,
                    "post_worker_hook": inspected,
                },
            )
        )
        try:
            waiter = asyncio.create_task(closing.wait())
            try:
                done, _ = await asyncio.wait(
                    (task, waiter), timeout=40, return_when=asyncio.FIRST_COMPLETED
                )
                if task in done:
                    await task
                assert waiter in done and closing.is_set(), "SDK cleanup was not reached"
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
            for _ in range(2):
                task.cancel("later cancellation cannot replace SDK error")
                await asyncio.sleep(0)
            done, _ = await asyncio.wait((task,), timeout=12)
            assert task in done
            with pytest.raises(Inspected):
                await task
            assert not drain.confirmed
        finally:
            process = held.get("process")
            if process is not None:
                if process.poll() is None:
                    process.kill()
                await asyncio.to_thread(process.wait)
                with suppress(BrokenPipeError):
                    process.stdin.close()
                process.stdout.close()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True])
async def test_late_commit_confirmation_does_not_grant_new_cleanup_window(
    migrated_database_url, monkeypatch, tmp_path, server_commits
):
    await test_cancel_suppressing_cleanup_cannot_publish_after_original_deadline(
        migrated_database_url, monkeypatch, tmp_path, commit_mode=server_commits
    )

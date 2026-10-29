"""Final-review counterexamples: real Worker/PG; no external model or host."""

import asyncio

import pytest
from helpers.staged_public import install_public_readers
from helpers.staged_worker import run_staged_worker
from integration.test_staged_dispatch_pg import DispatchObserved, roots
from integration.test_staged_worker_pg import ObservedCloseout, exercise
from nexus.agents.codex_sdk import CodexInvocationError
from nexus.agents.research_dispatch import ResearchDispatch, current_dispatch
from nexus.paper import manual_request
from nexus.paper.manual_host_exit import host_drain_scope
from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.monitoring import MonitorIncrement
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("ambient", [False, True])
async def test_work_error_and_successful_cleanup_have_separate_exception_ownership(
    migrated_database_url, monkeypatch, tmp_path, ambient
):
    original = CodexInvocationError("CODEX_RESPONSE_INCOMPLETE", provider_call_started=True)

    async def closed(factory, settings, captured):
        assert drain.confirmed, "work error was incorrectly classified as cleanup failure"
        if ambient:
            assert "invocation_error" not in captured
            assert captured["sdk_run_calls"] == captured["sdk_close_calls"] == 3
        else:
            assert captured["invocation_error"] is original
            assert original.failure_facts is None or original.failure_facts.cleanup_kind is None
            assert captured["sdk_run_calls"] == captured["sdk_close_calls"] == 1
            async with factory() as session:
                member = await session.scalar(select(MonitorIncrement))
                budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == member.run_id
                    )
                )
                assert budget.state == "reserved" and budget.settled_usd is None
                assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 1
        raise ObservedCloseout

    controls = {"post_worker_hook": closed}
    with host_drain_scope() as drain:
        if ambient:
            try:
                raise original
            except CodexInvocationError:
                with pytest.raises(ObservedCloseout):
                    await exercise(
                        migrated_database_url,
                        monkeypatch,
                        tmp_path,
                        "unresolved",
                        controls=controls,
                    )
        else:
            controls["sdk_original_error"] = original
            with pytest.raises(ObservedCloseout):
                await exercise(
                    migrated_database_url, monkeypatch, tmp_path, "unresolved", controls=controls
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_at", ["observe_return", "verification_read"])
async def test_cancel_suppression_cannot_start_or_publish_late_verification(
    migrated_database_url, monkeypatch, tmp_path, wait_at
):
    entered, release = asyncio.Event(), asyncio.Event()
    held = {}

    async def delay():
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()

    async def setup(client, thread, response, captured):
        protocol = captured["protocol"]
        held.update(captured=captured, factory=protocol.factory)
        if wait_at == "observe_return":
            native = protocol.observe

            async def observe(*args, **kwargs):
                result = await native(*args, **kwargs)
                await delay()
                return result

            monkeypatch.setattr(protocol, "observe", observe)
        else:
            native = protocol._member
            calls = 0

            async def member(*args, **kwargs):
                nonlocal calls
                result = await native(*args, **kwargs)
                calls += 1
                if calls == 2:  # observation committed; verification holds its own real read lock.
                    await delay()
                return result

            monkeypatch.setattr(protocol, "_member", member)

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
        waiter = asyncio.create_task(entered.wait())
        try:
            done, _ = await asyncio.wait(
                (task, waiter), timeout=40, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                await task
            assert entered.is_set()
            task.cancel("original observation cancellation")
            done, _ = await asyncio.wait((task,), timeout=12)
            assert task in done
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not drain.confirmed
            release.set()
            _, pending = await asyncio.wait(held["captured"]["cleanup_owned_tasks"], timeout=5)
            assert not pending
            async with held["factory"]() as session:
                assert (
                    await session.scalar(text("SELECT count(*) FROM stage_proto_observation")) == 1
                )
                assert (
                    await session.scalar(text("SELECT count(*) FROM stage_proto_verification")) == 0
                )
                member = await session.scalar(select(MonitorIncrement))
                budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == member.run_id
                    )
                )
                assert budget.state == "reserved" and budget.settled_usd is None
        finally:
            release.set()
            waiter.cancel()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, waiter, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fails",
    [
        "observation",
        "native_close",
        "native_close_wait_exit",
        "commit_stop_wait",
        "sql_rollback_wait",
    ],
)
async def test_cleanup_first_failure_stops_shared_dispatch_before_other_cleanup_finishes(
    migrated_database_url, monkeypatch, tmp_path, fails
):
    from nexus.ops.manual_paper_host import _dispatch_two_flight_members

    native_admit = manual_request.admit_manual_request

    async def admit(session, **kwargs):
        return await native_admit(session, **{**kwargs, "two_flight": True})

    monkeypatch.setattr(manual_request, "admit_manual_request", admit)
    install_public_readers(monkeypatch)
    both, fail_now, failed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    finish_cleanup, finish_peer, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()
    started, captures = [], {}
    fault_engines = []
    original = RuntimeError("controlled first cleanup child failure")
    native_stop = ResearchDispatch.stop

    def stop(self, *args, **kwargs):
        native_stop(self, *args, **kwargs)
        stopped.set()

    monkeypatch.setattr(ResearchDispatch, "stop", stop)

    async def before_worker(shared, settings, _options):
        factory = shared["fixture"][0]
        async with factory() as session:
            row = await session.scalar(select(ManualPaperRequest))
            view = await manual_request.read_manual_request(session, row)
        ids = [member.run_id for member in view.research.members]

        async def worker(config, **options):
            member = next(m for m in view.research.members if m.job_id in options["job_ids"])
            captured = {"clock": shared["clock"]}
            captures[member.run_id] = captured

            async def phase(name, _factory, _settings):
                if name == "model":
                    started.append(member.run_id)
                    if len(started) == 2:
                        both.set()
                    await fail_now.wait()
                    if member.run_id == ids[1]:
                        await finish_peer.wait()
                        assert current_dispatch().stopped
                        raise CodexInvocationError(
                            "CODEX_RESEARCH_STOPPED", provider_call_started=True
                        )

            async def setup(client, thread, response, state):
                if member.run_id != ids[0]:
                    return
                native_close = client.close.side_effect
                native_observe = state["protocol"].observe
                if fails == "native_close_wait_exit":
                    from types import SimpleNamespace

                    client._client._sync._proc = SimpleNamespace(
                        poll=lambda: 0 if finish_cleanup.is_set() else None,
                        kill=lambda: None,
                    )
                if fails in ("commit_stop_wait", "sql_rollback_wait"):
                    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

                    engine = create_async_engine(factory.kw["bind"].url)
                    fault_engines.append(engine)
                    state["protocol"].factory = async_sessionmaker(engine)
                    native_stop_scope = state["protocol"].stop

                    async def gated_stop(scope):
                        failed.set()
                        await finish_cleanup.wait()
                        await native_stop_scope(scope)

                    if fails == "commit_stop_wait":
                        monkeypatch.setattr(state["protocol"], "stop", gated_stop)
                    else:
                        from sqlalchemy import event
                        from sqlalchemy.util import await_only

                        def rollback_gate(connection):
                            failed.set()
                            await_only(finish_cleanup.wait())

                        event.listen(engine.sync_engine, "rollback", rollback_gate)

                        async def broken_member(session, claim):
                            await session.execute(text("SELECT 1/0"))
                            raise AssertionError("database error was not raised")

                        monkeypatch.setattr(state["protocol"], "_member", broken_member)

                async def close():
                    if fails in ("native_close", "native_close_wait_exit"):
                        if fails == "native_close":
                            await native_close()
                        else:
                            state["sdk_close_calls"] = state.get("sdk_close_calls", 0) + 1
                        failed.set()
                        raise original
                    if fails not in ("commit_stop_wait", "sql_rollback_wait"):
                        await finish_cleanup.wait()
                    await native_close()

                async def observe(*args, **kwargs):
                    if fails == "observation":
                        failed.set()
                        raise original
                    if fails == "sql_rollback_wait":
                        return await native_observe(*args, **kwargs)
                    if fails == "commit_stop_wait":
                        engine = state["protocol"].factory.kw["bind"]
                        dialect, native_commit = (
                            engine.sync_engine.dialect,
                            engine.sync_engine.dialect.do_commit,
                        )

                        def reject_commit(connection):
                            dialect.do_commit = native_commit
                            dialect.do_rollback(connection)
                            raise original

                        dialect.do_commit = reject_commit
                        try:
                            return await native_observe(*args, **kwargs)
                        finally:
                            dialect.do_commit = native_commit
                    await finish_cleanup.wait()
                    return await native_observe(*args, **kwargs)

                client.close.side_effect = close
                monkeypatch.setattr(state["protocol"], "observe", observe)

            captured.update(sdk_phase_hook=phase, sdk_client_setup=setup)
            return await run_staged_worker(
                config,
                monkeypatch=monkeypatch,
                fixture=(factory, None, None, captured, None, None, None),
                **options,
            )

        monkeypatch.setattr("nexus.ops.agent_paper_once.run_agent_worker_once", worker)
        task = asyncio.create_task(_dispatch_two_flight_members(settings, view=view))
        try:
            await asyncio.wait_for(both.wait(), 40)
            fail_now.set()
            await asyncio.wait_for(failed.wait(), 10)
            # Deterministic other-task gate stays closed; do not wait for the
            # seven-second expiry to manufacture a shared stop.
            await asyncio.wait_for(stopped.wait(), 2)
            assert not finish_cleanup.is_set() and not task.done()
            assert started == ids[:2]
            async with factory() as session:
                assert (
                    await session.scalar(text("SELECT count(*) FROM stage_proto_authorization"))
                    == 2
                )
                assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 0
            finish_peer.set()
            finish_cleanup.set()
            if fails == "sql_rollback_wait":
                from sqlalchemy.exc import DBAPIError

                with pytest.raises(DBAPIError) as raised:
                    await task
                assert raised.value.orig.sqlstate == "22012"
            else:
                with pytest.raises(RuntimeError) as raised:
                    await task
                assert raised.value is original
            assert started == ids[:2]
            assert all(c["sdk_run_calls"] == 1 for c in captures.values())
            async with factory() as session:
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM agent_daily_budget_reservations "
                            "WHERE state='reserved' AND settled_usd IS NULL"
                        )
                    )
                    == 3
                )
        finally:
            fail_now.set()
            finish_peer.set()
            finish_cleanup.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            for engine in fault_engines:
                await engine.dispose()
        raise DispatchObserved

    with pytest.raises(DispatchObserved):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ambient_cancelled", [False, True])
async def test_ambient_error_cannot_swallow_or_replace_current_cleanup_failure(
    migrated_database_url, monkeypatch, tmp_path, ambient_cancelled
):
    from nexus.monitoring.admission import require_monitor_account_available

    install_public_readers(monkeypatch)

    old = (
        asyncio.CancelledError("old caller cancellation")
        if ambient_cancelled
        else CodexInvocationError("CODEX_RESPONSE_INCOMPLETE", provider_call_started=True)
    )
    old_fields, old_args = dict(old.__dict__), old.args
    from nexus.agents import codex_sdk

    native_core = codex_sdk._invoke_codex
    invocation_states = []

    async def measured_core(*args, **kwargs):
        try:
            return await native_core(*args, **kwargs)
        finally:
            state = kwargs["invocation_state"]
            # A fixed diagnostic fact, not a replacement for any business clock.
            state.phase_elapsed_ms["client_close"] = 4321
            invocation_states.append(state)

    monkeypatch.setattr(codex_sdk, "_invoke_codex", measured_core)
    current = CodexInvocationError(
        "CODEX_UNEXPECTED_EXCEPTION", phase="client_close", provider_call_started=True
    )

    async def setup(client, thread, response, captured):
        native_close = client.close.side_effect

        async def close():
            await native_close()  # Original owned process exited; this close then fails.
            raise current

        client.close.side_effect = close

    async def inspected(factory, settings, captured):
        assert captured["invocation_error"] is current
        assert current.phase_elapsed_ms["client_close"] == 4321
        assert invocation_states[0].phase_elapsed_ms["client_close"] == 4321
        assert old.__dict__ == old_fields and old.args == old_args
        assert not drain.confirmed
        assert captured["sdk_run_calls"] == captured["sdk_close_calls"] == 1
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 0
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert budget.state == "settled" and budget.settled_usd is not None
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=member.account_id, now=captured["clock"][0]
                )
        raise ObservedCloseout

    with host_drain_scope() as drain:
        try:
            raise old
        except BaseException:
            with pytest.raises(ObservedCloseout):
                await exercise(
                    migrated_database_url,
                    monkeypatch,
                    tmp_path,
                    "unresolved",
                    controls={"sdk_client_setup": setup, "post_worker_hook": inspected},
                )

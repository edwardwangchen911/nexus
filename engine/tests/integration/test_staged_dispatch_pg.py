"""Native two-flight controller with selected preparation and the real SDK boundary."""

import asyncio

import pytest
from helpers.staged_public import install_public_readers
from helpers.staged_worker import run_staged_worker
from integration.test_staged_roots_pg import (
    test_normal_three_stock_roots_use_one_staged_protocol as roots,
)
from nexus.agents.research_dispatch import current_dispatch
from nexus.paper import manual_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import select, text


class DispatchObserved(BaseException):
    pass


def fail_controller_dispose(monkeypatch):
    from types import SimpleNamespace

    from nexus.ops import manual_paper_host

    native_factory = manual_paper_host.session_factory
    error = RuntimeError("controlled controller dispose failure")

    def factory(settings):
        actual = native_factory(settings)

        async def dispose():
            await actual.kw["bind"].dispose()
            raise error

        def sessions(*args, **kwargs):
            return actual(*args, **kwargs)

        sessions.kw = {"bind": SimpleNamespace(dispose=dispose)}
        return sessions

    monkeypatch.setattr(manual_paper_host, "session_factory", factory)
    return error


@pytest.mark.asyncio
@pytest.mark.parametrize("dispose_fails", [False, True])
async def test_native_two_flight_keeps_cleanup_slot_and_replaces_only_completed_peer(
    migrated_database_url, monkeypatch, tmp_path, dispose_fails
):
    from nexus.ops.manual_paper_host import _dispatch_two_flight_members

    native_admit = manual_request.admit_manual_request

    async def admit(session, **kwargs):
        return await native_admit(session, **{**kwargs, "two_flight": True})

    monkeypatch.setattr(manual_request, "admit_manual_request", admit)
    install_public_readers(monkeypatch)
    started, captures = [], {}
    two_started, third_started = asyncio.Event(), asyncio.Event()
    release_models, release_cleanup = asyncio.Event(), asyncio.Event()
    peer_before_final, first_cleanup_entered = asyncio.Event(), asyncio.Event()

    async def before_worker(shared, settings, _options):
        factory = shared["fixture"][0]
        async with factory() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            view = await manual_request.read_manual_request(session, row)
        run_ids = [member.run_id for member in view.research.members]

        async def worker(config, **options):
            member = next(m for m in view.research.members if m.job_id in options["job_ids"])
            captured = {"clock": shared["clock"]}
            captures[member.run_id] = captured

            async def phase(name, factory, _settings):
                state = current_dispatch(member.run_id)
                assert state is not None and not state.stopped
                assert len(state.active) <= 2
                if name == "model":
                    started.append(member.run_id)
                    if len(started) == 2:
                        two_started.set()
                    if len(started) == 3:
                        async with factory() as session:
                            assert (
                                await session.scalar(
                                    text("SELECT count(*) FROM stage_proto_terminal")
                                )
                                == 1
                            )
                            assert (
                                await session.scalar(
                                    text(
                                        "SELECT count(*) FROM stage_proto_cleanup WHERE run_id=:run"
                                    ),
                                    {"run": run_ids[0]},
                                )
                                == 0
                            )
                            assert (
                                await session.scalar(
                                    text("SELECT stage_proto_inflight(:scope)"), {"scope": row.id}
                                )
                                == 2
                            )
                        third_started.set()
                    await release_models.wait()
                    if member.run_id == run_ids[0]:
                        await peer_before_final.wait()
                elif member.run_id == run_ids[0]:
                    first_cleanup_entered.set()
                    await release_cleanup.wait()

            async def before_final(_factory, _protocol, _claim):
                if member.run_id == run_ids[1]:
                    # Align the ownership crossing, without requiring all three
                    # peer stages to fit inside another child's cleanup window.
                    peer_before_final.set()
                    await first_cleanup_entered.wait()

            captured["before_final_hook"] = before_final
            captured["sdk_phase_hook"] = phase
            fixture = (factory, None, None, captured, None, None, None)
            return await run_staged_worker(
                config, monkeypatch=monkeypatch, fixture=fixture, **options
            )

        monkeypatch.setattr("nexus.ops.agent_paper_once.run_agent_worker_once", worker)
        disposal = fail_controller_dispose(monkeypatch) if dispose_fails else None
        task = asyncio.create_task(_dispatch_two_flight_members(settings, view=view))
        waiter = asyncio.create_task(two_started.wait())
        third = None
        try:
            async with asyncio.timeout(90):
                await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
                if task.done():
                    await task
                assert two_started.is_set(), "two selected stocks never overlapped at SDK"
                assert started == run_ids[:2]
                release_models.set()
                third = asyncio.create_task(third_started.wait())
                await asyncio.wait({task, third}, return_when=asyncio.FIRST_COMPLETED)
                if task.done():
                    await task
                assert third_started.is_set(), "completed peer did not yield its slot"
                release_cleanup.set()
                if dispose_fails:
                    with pytest.raises(RuntimeError) as raised:
                        await task
                    assert raised.value is disposal
                else:
                    await task
                await third
        finally:
            release_models.set()
            release_cleanup.set()
            peer_before_final.set()
            first_cleanup_entered.set()
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            if third is not None:
                third.cancel()
                await asyncio.gather(third, return_exceptions=True)
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert started == run_ids
        assert all(c.get("sdk_run_calls") == 3 for c in captures.values())
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 3
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 9
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations WHERE state='settled'"
                    )
                )
                == 3
            )
        raise DispatchObserved

    with pytest.raises(DispatchObserved):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("caller_cancel", [False, True])
@pytest.mark.parametrize("dispose_fails", [False, True])
async def test_native_two_flight_failure_drains_both_without_replacement_or_budget_release(
    migrated_database_url, monkeypatch, tmp_path, caller_cancel, dispose_fails
):
    from nexus.agents.codex_sdk import CodexInvocationError
    from nexus.ops.manual_paper_host import _dispatch_two_flight_members

    native_admit = manual_request.admit_manual_request

    async def admit(session, **kwargs):
        return await native_admit(session, **{**kwargs, "two_flight": True})

    monkeypatch.setattr(manual_request, "admit_manual_request", admit)
    install_public_readers(monkeypatch)
    original = CodexInvocationError("CODEX_RESPONSE_INCOMPLETE", provider_call_started=True)
    both_started, first_cleanup = asyncio.Event(), asyncio.Event()
    peer_exited, release_cleanup = asyncio.Event(), asyncio.Event()
    release = asyncio.Event()
    started, captures, states = [], {}, []

    async def before_worker(shared, settings, _options):
        factory = shared["fixture"][0]
        async with factory() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            view = await manual_request.read_manual_request(session, row)
        run_ids = [member.run_id for member in view.research.members]

        async def worker(config, **options):
            member = next(m for m in view.research.members if m.job_id in options["job_ids"])
            captured = {"clock": shared["clock"]}
            captures[member.run_id] = captured

            async def phase(name, _factory, _settings):
                state = current_dispatch(member.run_id)
                assert state is not None
                if name == "model":
                    states.append(state)
                    started.append(member.run_id)
                    if len(started) == 2:
                        both_started.set()
                    await release.wait()
                    if member.run_id == run_ids[0] and not caller_cancel:
                        raise original
                    if not caller_cancel:
                        await first_cleanup.wait()
                        assert state.stopped
                elif not caller_cancel and member.run_id == run_ids[0]:
                    # The SDK's native invocation boundary must stop the shared
                    # state BEFORE this cleanup wait; the hook never stops it.
                    assert state.stopped
                    first_cleanup.set()
                    await release_cleanup.wait()

            captured["sdk_phase_hook"] = phase
            fixture = (factory, None, None, captured, None, None, None)
            try:
                return await run_staged_worker(
                    config, monkeypatch=monkeypatch, fixture=fixture, **options
                )
            finally:
                if member.run_id == run_ids[1]:
                    peer_exited.set()

        monkeypatch.setattr("nexus.ops.agent_paper_once.run_agent_worker_once", worker)
        if dispose_fails:
            fail_controller_dispose(monkeypatch)
        task = asyncio.create_task(_dispatch_two_flight_members(settings, view=view))
        waiter = asyncio.create_task(both_started.wait())
        try:
            async with asyncio.timeout(90):
                await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
                if task.done():
                    await task
                assert both_started.is_set()
                if caller_cancel:
                    task.cancel()
                    # Both calls have entered their real SDK boundary. Cancellation
                    # must drain cleanup before the shared dispatcher returns.
                    with pytest.raises(asyncio.CancelledError) as raised:
                        await task
                else:
                    release.set()
                    await peer_exited.wait()
                    assert first_cleanup.is_set() and not task.done()
                    assert run_ids[0] in states[0].active
                    assert started == run_ids[:2]
                    async with factory() as session:
                        assert (
                            await session.scalar(
                                text("SELECT count(*) FROM stage_proto_authorization")
                            )
                            == 2
                        )
                        assert (
                            await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup"))
                            == 1
                        )
                        assert (
                            await session.scalar(text("SELECT count(*) FROM stage_proto_terminal"))
                            == 0
                        )
                    release_cleanup.set()
                    with pytest.raises(CodexInvocationError) as raised:
                        await task
                    assert raised.value is original
                if dispose_fails:
                    assert "RESEARCH_DISPATCH_CLEANUP_UNCONFIRMED" in raised.value.__notes__
        finally:
            release.set()
            release_cleanup.set()
            waiter.cancel()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, waiter, return_exceptions=True)

        assert started == run_ids[:2]
        assert states[0] is states[1] and states[0].stopped and not states[0].active
        assert all(c["sdk_run_calls"] == 1 and c["sdk_close_calls"] == 1 for c in captures.values())
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 2
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 2
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 0
            for captured in captures.values():
                from nexus.persistence.models.runtime import Job, JobAttempt, JobLease

                claim = captured["claim"]
                job = await session.get(Job, claim.job_id)
                attempt = (
                    await session.scalars(
                        select(JobAttempt).where(JobAttempt.job_id == claim.job_id)
                    )
                ).one()
                lease = (
                    await session.scalars(select(JobLease).where(JobLease.job_id == claim.job_id))
                ).one()
                assert job.status == "running" and job.attempt_count == 1
                assert job.finished_at is None and attempt.finished_at is None
                assert attempt.outcome == "running"
                assert attempt.worker_id == lease.worker_id == claim.worker_id
                assert attempt.lease_token == lease.lease_token == claim.lease_token
                assert attempt.fence_token == lease.fence_token == claim.fence_token
                assert lease.expires_at == claim.expires_at
                assert (
                    await session.scalar(
                        text("SELECT status FROM agent_research_runs WHERE id=:run"),
                        {"run": next(run for run, value in captures.items() if value is captured)},
                    )
                ) == "prepared"
            assert await session.scalar(text("SELECT bool_and(stopped) FROM stage_proto_scope"))
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='reserved' AND settled_usd IS NULL"
                    )
                )
                == 3
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM jobs WHERE status='queued' AND id=:job"),
                    {"job": view.research.members[2].job_id},
                )
                == 1
            )
        raise DispatchObserved

    with pytest.raises(DispatchObserved):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )

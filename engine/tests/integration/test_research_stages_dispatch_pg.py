"""Selected executor failure stops native two-flight before rollback return."""

import asyncio
from datetime import timedelta

import pytest
from helpers import staged_routes
from integration.test_research_stages_runtime_pg import controlled_sdk
from integration.test_staged_roots_pg import (
    test_normal_three_stock_roots_use_one_staged_protocol as roots,
)
from nexus import worker
from nexus.agents import codex_sdk
from nexus.agents.research_dispatch import _RUN, current_dispatch
from nexus.paper import manual_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.util import await_only


class Observed(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_final", [False, True])
async def test_selected_two_flight_stops_before_rollback_return(
    migrated_database_url, monkeypatch, tmp_path, fail_final
):
    from nexus.ops.manual_paper_host import _dispatch_two_flight_members

    calls = controlled_sdk(monkeypatch)

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(staged_routes, "ensure_routes", noop)
    monkeypatch.setattr(staged_routes, "register_prepared_runs", noop)
    admit = manual_request.admit_manual_request

    async def two_flight(session, **kwargs):
        return await admit(session, **{**kwargs, "two_flight": True})

    monkeypatch.setattr(manual_request, "admit_manual_request", two_flight)

    async def before_worker(shared, settings, options):
        factory = shared["fixture"][0]
        async with factory() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            view = await manual_request.read_manual_request(session, row)
        ids = [m.run_id for m in view.research.members]
        base, anchor, at = worker.datetime, asyncio.get_running_loop().time(), shared["clock"][0]

        class Clock(base):
            @classmethod
            def now(cls, tz=None):
                shared["clock"][0] = max(
                    shared["clock"][0],
                    at + timedelta(seconds=asyncio.get_running_loop().time() - anchor),
                )
                return shared["clock"][0] if tz is None else shared["clock"][0].astimezone(tz)

        monkeypatch.setattr(worker, "datetime", Clock)
        both, rollback_entered, release_rollback, release_peer, peer_finished = (
            asyncio.Event() for _ in range(5)
        )
        seen, state = [], {}
        create = codex_sdk.AsyncCodex

        def sdk(config):
            client = create(config)
            thread = client.thread_start.return_value
            native = thread.run.side_effect
            rid = _RUN.get()

            async def model(*args, **kwargs):
                dispatch = current_dispatch()
                state["dispatch"] = dispatch
                if rid not in seen:
                    assert not dispatch.stopped, "normal Prepared rollback stopped dispatch"
                    seen.append(rid)
                    if len(seen) == 2:
                        both.set()
                if fail_final and rid == ids[0]:
                    await both.wait()
                if fail_final and rid == ids[1]:
                    await release_peer.wait()
                return await native(*args, **kwargs)

            thread.run.side_effect = model
            return client

        monkeypatch.setattr(codex_sdk, "AsyncCodex", sdk)
        execute = AsyncSession.execute
        patched_dialects = []

        async def fault(session, statement, *args, **kwargs):
            if (
                fail_final
                and _RUN.get() == ids[0]
                and str(statement).startswith("INSERT INTO research_stage_terminal")
            ):
                dialect = session.get_bind().dialect
                rollback = dialect.do_rollback

                def gated(connection):
                    rollback(connection)
                    if _RUN.get() == ids[0] and not rollback_entered.is_set():
                        rollback_entered.set()
                        await_only(release_rollback.wait())

                monkeypatch.setattr(dialect, "do_rollback", gated)
                patched_dialects.append((dialect, rollback))
                try:
                    await execute(session, text("SELECT 1/0"))
                except DBAPIError as error:
                    state["error"] = error
                    raise
            return await execute(session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "execute", fault)
        native_worker = worker.run_agent_worker_once

        async def run(config, **kwargs):
            rid = _RUN.get()
            try:
                return await native_worker(config, **kwargs)
            finally:
                if rid == ids[1]:
                    peer_finished.set()

        monkeypatch.setattr("nexus.ops.agent_paper_once.run_agent_worker_once", run)
        task = asyncio.create_task(_dispatch_two_flight_members(settings, view=view))
        try:
            async with asyncio.timeout(90):
                if fail_final:
                    await rollback_entered.wait()
                    assert state["dispatch"].stopped, (
                        "rollback released DB locks before shared stop"
                    )
                    assert state["dispatch"].first_error is state["error"]
                    release_peer.set()
                    await peer_finished.wait()
                    async with factory() as session:
                        assert (
                            await session.scalar(
                                text(
                                    "SELECT count(*) FROM research_stage_authorization "
                                    "WHERE run_id=:run"
                                ),
                                {"run": ids[1]},
                            )
                            == 1
                        )
                        assert (
                            await session.scalar(
                                text(
                                    "SELECT count(*) FROM jobs "
                                    "WHERE payload->>'run_id'=:run AND attempt_count>0"
                                ),
                                {"run": str(ids[2])},
                            )
                            == 0
                        )
                        assert (
                            await session.scalar(
                                text(
                                    "SELECT count(*) FROM research_stage_authorization "
                                    "WHERE run_id=:run"
                                ),
                                {"run": ids[0]},
                            )
                            == 3
                        )
                        assert (
                            await session.scalar(
                                text(
                                    "SELECT count(*) FROM research_stage_terminal WHERE run_id=:run"
                                ),
                                {"run": ids[0]},
                            )
                            == 0
                        )
                        assert (
                            await session.scalar(
                                text(
                                    "SELECT state FROM agent_daily_budget_reservations "
                                    "WHERE run_id=:run"
                                ),
                                {"run": ids[0]},
                            )
                            == "reserved"
                        )
                    release_rollback.set()
                    with pytest.raises(DBAPIError) as caught:
                        await task
                    assert caught.value is state["error"]
                    assert seen == ids[:2]
                    assert len(calls) == 4
                    async with factory() as session:
                        row = (
                            await session.execute(
                                text(
                                    "SELECT j.status, j.attempt_count, a.outcome, "
                                    "a.finished_at, a.error_code FROM jobs j "
                                    "JOIN job_attempts a ON a.job_id=j.id "
                                    "WHERE j.payload->>'run_id'=:run"
                                ),
                                {"run": str(ids[0])},
                            )
                        ).one()
                        assert tuple(row) == ("running", 1, "running", None, None)
                else:
                    await task
                    assert not state["dispatch"].stopped
                    assert len(calls) == 9 and seen == ids
                    async with factory() as session:
                        assert (
                            await session.scalar(
                                text("SELECT count(*) FROM research_stage_terminal WHERE known")
                            )
                            == 3
                        )
        finally:
            release_peer.set()
            release_rollback.set()
            await asyncio.gather(task, return_exceptions=True)
            for dialect, original in patched_dialects:
                monkeypatch.setattr(dialect, "do_rollback", original)
        raise Observed

    with pytest.raises(Observed):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )

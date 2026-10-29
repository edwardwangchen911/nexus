"""Safe failure facts through the actual Worker, PostgreSQL and normal reader."""

import json
from datetime import timedelta

import pytest
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
from nexus.agents.codex_sdk import CodexInvocationError
from nexus.agents.committee import SdkFailureFacts
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.agents.portfolio_synthesis_worker import run_synthesis_job_once
from nexus.agents.responses_budget import UsageSnapshot
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.runtime import Job
from sqlalchemy import select


@pytest.mark.asyncio
@pytest.mark.parametrize("known_usage", [False, True])
@pytest.mark.parametrize("native_sdk", [False, True])
async def test_worker_failure_facts_survive_terminal_and_normal_readback(
    migrated_database_url, known_usage, native_sdk, monkeypatch, caplog
):
    if native_sdk:
        from unittest.mock import AsyncMock

        from nexus.agents.codex_sdk import invoke_codex_portfolio
        from openai_codex._run import _collect_async_turn_result
        from openai_codex.errors import TransportClosedError
        from openai_codex.generated.v2_all import ThreadTokenUsageUpdatedNotification
        from openai_codex.models import Notification
        from unit.agents.test_codex_sdk import _sdk
        from unit.agents.test_sdk_error_details import terminal

        client, thread, _, _ = _sdk(monkeypatch)
        notifications = []
        if known_usage:
            usage = dict(
                input_tokens=10,
                output_tokens=20,
                total_tokens=30,
                cached_input_tokens=0,
                reasoning_output_tokens=0,
            )
            notifications.append(
                Notification(
                    "thread/tokenUsage/updated",
                    ThreadTokenUsageUpdatedNotification.model_validate(
                        {
                            "thread_id": "thread",
                            "turn_id": "turn",
                            "token_usage": {"last": usage, "total": usage},
                        }
                    ),
                )
            )
        notifications.append(
            terminal(
                '{"error":{"code":"rate_limit_exceeded","message":"PRIVATE_DIAGNOSTIC_CANARY"}}',
                {"httpConnectionFailed": {"httpStatusCode": 429}},
            )
        )
        client._client.next_turn_notification = AsyncMock(side_effect=notifications)
        client.close.side_effect = TransportClosedError("PRIVATE_DIAGNOSTIC_CANARY")

        async def fail_turn(*args, **kwargs):
            async def events():
                for _ in notifications:
                    yield await client._client.next_turn_notification("turn")

            return await _collect_async_turn_result(events(), turn_id="turn")

        thread.run.side_effect = fail_turn
    engine, sessions, options = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            run_id, input_hash = run.id, run.input_hash
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run_id))
            )
        calls = []

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                calls.append(1)
                if native_sdk:
                    return await invoke_codex_portfolio({})
                error = CodexInvocationError(
                    "CODEX_UNEXPECTED_EXCEPTION",
                    phase="turn_run",
                    usage=UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1)
                    if known_usage
                    else None,
                )
                error.failure_facts = SdkFailureFacts(
                    exception_kind="rpc_error",
                    cleanup_kind="transport_closed",
                    terminal_status="failed",
                    provider_reason="serverOverloaded",
                )
                raise error from RuntimeError("PRIVATE_DIAGNOSTIC_CANARY")

        arguments = dict(
            transport=Transport(),
            worker_id="failure-facts-test",
            clock=lambda: options["now"] + timedelta(seconds=1),
            job_ids=(job_id,),
        )
        assert await run_synthesis_job_once(sessions, **arguments)
        assert not await run_synthesis_job_once(sessions, **arguments)
        assert calls == [1]
        async with sessions() as session:
            output = await session.scalar(
                select(AgentPortfolioSynthesisOutputRecord).where(
                    AgentPortfolioSynthesisOutputRecord.run_id == run_id
                )
            )
            assert getattr(output, "failure_diagnostic", None) is not None
            from nexus.agents.portfolio_synthesis_diagnostics import read_synthesis_failure

            first = await read_synthesis_failure(session, run_id=run_id)
            second = await read_synthesis_failure(session, run_id=run_id)
            assert first == second
            assert first.state == "valid"
            diag = first.diagnostic
            assert diag.run_id == run_id and diag.job_id == job_id
            assert diag.input_hash == input_hash and diag.output_id == output.id
            assert diag.binding_hash == output.binding_hash
            assert diag.observation.failure_facts.cleanup_kind == "transport_closed"
            assert diag.observation.failure_facts.exception_kind == (
                "runtime_error" if native_sdk else "rpc_error"
            )
            if native_sdk:
                assert diag.observation.failure_facts.http_status_code == 429
                assert diag.observation.failure_facts.provider_error_code == "rate_limit_exceeded"
            assert diag.observation.invocation_phase == "turn_run"
            assert output.intent is None and output.status == "failed"
            stored = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            reserve = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run_id
                )
            )
            assert stored.input_tokens == (10 if known_usage else None)
            assert reserve.state == ("settled" if known_usage else "reserved")
            assert "PRIVATE_DIAGNOSTIC_CANARY" not in json.dumps(output.failure_diagnostic)
            assert "PRIVATE_DIAGNOSTIC_CANARY" not in first.model_dump_json()
            assert not session.dirty and not session.new
        from nexus.ops.research_workload_inspection import inspect_portfolio_synthesis_run
        from nexus.settings import EngineSettings

        visible = await inspect_portfolio_synthesis_run(
            EngineSettings(database_url=migrated_database_url), run_id=run_id
        )
        assert visible["failure_diagnostic"]["state"] == "valid"
        assert visible["run_status"] == "failed" and visible["model_calls"] == 0
        assert "PRIVATE_DIAGNOSTIC_CANARY" not in caplog.text
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["plain", "malformed", "pre_dispatch", "no_facts"])
async def test_failure_projection_and_original_usage_rules(migrated_database_url, fault, caplog):
    from nexus.agents.portfolio_synthesis_diagnostics import read_synthesis_failure

    engine, sessions, options = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                if fault == "plain":
                    raise RuntimeError("PRIVATE_DIAGNOSTIC_CANARY")
                error = CodexInvocationError(
                    "CODEX_TIMEOUT",
                    phase="turn_run",
                    provider_call_started=False if fault == "pre_dispatch" else None,
                )
                if fault == "malformed":
                    error.failure_facts = {"exception_kind": "PRIVATE_DIAGNOSTIC_CANARY"}
                raise error

        assert await run_synthesis_job_once(
            sessions,
            transport=Transport(),
            worker_id="test",
            clock=lambda: options["now"] + timedelta(seconds=1),
            job_ids=(job_id,),
        )
        async with sessions() as session:
            result = await read_synthesis_failure(session, run_id=run.id)
            assert result.state == "valid"
            observation = result.diagnostic.observation
            assert (
                observation.local
                == {
                    "plain": "exception",
                    "malformed": "diagnostic_unavailable",
                    "pre_dispatch": None,
                    "no_facts": None,
                }[fault]
            )
            assert observation.failure_facts is None
            if fault in {"pre_dispatch", "no_facts"}:
                assert observation.timeout_source == "unknown"
            reserve = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
                )
            )
            assert reserve.state == ("released" if fault == "pre_dispatch" else "reserved")
            output = await session.scalar(
                select(AgentPortfolioSynthesisOutputRecord).where(
                    AgentPortfolioSynthesisOutputRecord.run_id == run.id
                )
            )
            assert "PRIVATE_DIAGNOSTIC_CANARY" not in json.dumps(output.failure_diagnostic)
            assert "PRIVATE_DIAGNOSTIC_CANARY" not in result.model_dump_json()
        assert "PRIVATE_DIAGNOSTIC_CANARY" not in caplog.text
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_terminal_idempotency_conflict_and_no_backfill(migrated_database_url, legacy):
    from nexus.agents.portfolio_synthesis_diagnostics import (
        FailureObservation,
        SynthesisDiagnosticConflict,
        read_synthesis_failure,
    )
    from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal

    engine, sessions, options = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
        args = dict(
            run_id=run.id,
            input_hash=run.input_hash,
            status="failed",
            intent=None,
            input_tokens=None,
            output_tokens=None,
            now=options["now"],
        )
        diagnostic = dict(
            failure_observation=FailureObservation(local="orphan_prepared"),
            diagnostic_job_id=job_id,
        )
        async with sessions.begin() as session:
            first = await persist_portfolio_synthesis_terminal(
                session, **args, **({} if legacy else diagnostic)
            )
            first_id, first_hash, first_binding = first.id, first.content_hash, first.binding_hash
        for supplied in ({}, diagnostic):
            async with sessions.begin() as session:
                same = await persist_portfolio_synthesis_terminal(session, **args, **supplied)
                assert (same.id, same.content_hash, same.binding_hash) == (
                    first_id,
                    first_hash,
                    first_binding,
                )
                assert not session.dirty and not session.new
        different = diagnostic | {"failure_observation": FailureObservation(local="exception")}
        async with sessions.begin() as session:
            if legacy:
                await persist_portfolio_synthesis_terminal(session, **args, **different)
            else:
                with pytest.raises(SynthesisDiagnosticConflict, match="DIAGNOSTIC_CONFLICT"):
                    await persist_portfolio_synthesis_terminal(session, **args, **different)
            assert not session.dirty and not session.new
            result = await read_synthesis_failure(session, run_id=run.id)
            assert result.state == ("missing" if legacy else "valid")
            if not legacy:
                assert result.diagnostic.observation.local == "orphan_prepared"
            with pytest.raises(ValueError, match="TERMINAL_CONFLICT"):
                await persist_portfolio_synthesis_terminal(
                    session, **(args | {"input_tokens": 1, "output_tokens": 1}), **different
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_point", ["diagnostic_insert", "after_output", "job_finish"])
async def test_diagnostic_and_terminal_rollback_together(
    migrated_database_url, monkeypatch, failure_point
):
    from nexus.agents import portfolio_synthesis_worker as worker
    from sqlalchemy import text

    engine, sessions, options = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
            if failure_point == "diagnostic_insert":
                await session.execute(
                    text("""CREATE FUNCTION reject_test_diagnostic() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN IF NEW.failure_diagnostic IS NOT NULL THEN
                    RAISE EXCEPTION 'TEST_DIAGNOSTIC_WRITE_FAILURE'; END IF; RETURN NEW; END $$""")
                )
                await session.execute(
                    text("""CREATE TRIGGER reject_test_diagnostic BEFORE INSERT ON
                    agent_portfolio_synthesis_outputs FOR EACH ROW
                    EXECUTE FUNCTION reject_test_diagnostic()""")
                )
        name = (
            "persist_portfolio_synthesis_terminal"
            if failure_point == "after_output"
            else "execute_and_finalize_job"
        )
        original = getattr(worker, name)

        async def fail_after(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("TEST_ATOMIC_FAILURE")

        if failure_point != "diagnostic_insert":
            monkeypatch.setattr(worker, name, fail_after)

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                raise CodexInvocationError("CODEX_TIMEOUT")

        with pytest.raises(Exception):
            await run_synthesis_job_once(
                sessions,
                transport=Transport(),
                worker_id="test",
                clock=lambda: options["now"] + timedelta(seconds=1),
                job_ids=(job_id,),
            )
        async with sessions() as session:
            stored = await session.get(AgentPortfolioSynthesisRunRecord, run.id)
            assert stored.status == "running" and stored.completed_at is None
            assert (
                await session.scalar(
                    select(AgentPortfolioSynthesisOutputRecord).where(
                        AgentPortfolioSynthesisOutputRecord.run_id == run.id
                    )
                )
                is None
            )
            reserve = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
                )
            )
            assert reserve.state == "reserved" and reserve.settled_usd is None
            assert (await session.get(Job, job_id)).status == "running"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption,state",
    [
        (
            {
                "diagnostic": {"schema_version": "future", "private": "PRIVATE_DIAGNOSTIC_CANARY"},
                "diagnostic_hash": "0" * 64,
            },
            "unsupported",
        ),
        ({"private": "PRIVATE_DIAGNOSTIC_CANARY"}, "invalid"),
    ],
)
async def test_corrupt_diagnostic_does_not_destroy_normal_business_readback(
    migrated_database_url, corruption, state
):
    from nexus.agents.portfolio_synthesis_diagnostics import read_synthesis_failure
    from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal
    from sqlalchemy import text

    engine, sessions, options = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            await persist_portfolio_synthesis_terminal(
                session,
                run_id=run.id,
                input_hash=run.input_hash,
                status="failed",
                intent=None,
                input_tokens=None,
                output_tokens=None,
                now=options["now"],
            )
            # Deliberate isolated corruption; production cannot UPDATE immutable outputs.
            await session.execute(
                text("ALTER TABLE agent_portfolio_synthesis_outputs DISABLE TRIGGER USER")
            )
            await session.execute(
                text(
                    "UPDATE agent_portfolio_synthesis_outputs "
                    "SET failure_diagnostic=CAST(:value AS jsonb) WHERE run_id=:id"
                ),
                {"value": json.dumps(corruption), "id": run.id},
            )
            await session.execute(
                text("ALTER TABLE agent_portfolio_synthesis_outputs ENABLE TRIGGER USER")
            )
        async with sessions() as session:
            result = await read_synthesis_failure(session, run_id=run.id)
            assert result.state == state and result.diagnostic is None
            assert "PRIVATE_DIAGNOSTIC_CANARY" not in result.model_dump_json()
            assert (await session.get(AgentPortfolioSynthesisRunRecord, run.id)).status == "failed"
            assert not session.new and not session.dirty
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_commit_ack_lost_recovers_original_terminal_without_dispatch(migrated_database_url):
    from contextlib import asynccontextmanager

    from nexus.agents.portfolio_synthesis_diagnostics import read_synthesis_failure

    engine, sessions, options = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
        calls = []

        class LostAckFactory:
            raised = False

            def __call__(self):
                return sessions()

            @asynccontextmanager
            async def begin(self):
                terminal = False
                async with sessions.begin() as session:
                    yield session
                    terminal = (
                        await session.get(AgentPortfolioSynthesisRunRecord, run.id)
                    ).status == "failed"
                if terminal and not self.raised:
                    self.raised = True
                    raise ConnectionError("TEST_COMMIT_ACK_LOST")

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                calls.append(1)
                raise CodexInvocationError("CODEX_TIMEOUT")

        arguments = dict(
            transport=Transport(),
            worker_id="test",
            clock=lambda: options["now"] + timedelta(seconds=1),
            job_ids=(job_id,),
        )
        with pytest.raises(ConnectionError, match="TEST_COMMIT_ACK_LOST"):
            await run_synthesis_job_once(LostAckFactory(), **arguments)
        assert not await run_synthesis_job_once(sessions, **arguments)
        assert calls == [1]
        async with sessions() as session:
            assert (await read_synthesis_failure(session, run_id=run.id)).state == "valid"
            assert (await session.get(Job, job_id)).status == "failed"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_terminal_diagnostics_keep_first_fact(migrated_database_url):
    import asyncio

    from nexus.agents.portfolio_synthesis_diagnostics import (
        FailureObservation,
        SynthesisDiagnosticConflict,
    )
    from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal
    from sqlalchemy import func

    engine, sessions, options = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )

        async def terminal(local):
            try:
                async with sessions.begin() as session:
                    await persist_portfolio_synthesis_terminal(
                        session,
                        run_id=run.id,
                        input_hash=run.input_hash,
                        status="failed",
                        intent=None,
                        input_tokens=None,
                        output_tokens=None,
                        now=options["now"],
                        diagnostic_job_id=job_id,
                        failure_observation=FailureObservation(local=local),
                    )
                return "stored"
            except SynthesisDiagnosticConflict:
                return "conflict"

        assert sorted(await asyncio.gather(terminal("orphan_prepared"), terminal("exception"))) == [
            "conflict",
            "stored",
        ]
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AgentPortfolioSynthesisOutputRecord)
                    .where(AgentPortfolioSynthesisOutputRecord.run_id == run.id)
                )
                == 1
            )
    finally:
        await engine.dispose()


def test_migration_old_rows_and_retained_diagnostic_rollback(migrated_database_url, monkeypatch):
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from nexus.agents.portfolio_synthesis_diagnostics import (
        read_synthesis_failure,
    )
    from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    async def seed():
        engine, sessions, options = await seed_synthesis(migrated_database_url)
        try:
            async with sessions.begin() as session:
                run = (await admit_portfolio_synthesis(session, **options)).run
                output = await persist_portfolio_synthesis_terminal(
                    session,
                    run_id=run.id,
                    input_hash=run.input_hash,
                    status="failed",
                    intent=None,
                    input_tokens=None,
                    output_tokens=None,
                    now=options["now"],
                )
                return run.id, output.content_hash, output.binding_hash
        finally:
            await engine.dispose()

    run_id, content_hash, binding_hash = asyncio.run(seed())
    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    command.downgrade(config, "0081_historical_sources")
    command.upgrade(config, "head")

    async def check_old_and_insert_diagnostic():
        engine = create_async_engine(migrated_database_url)
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with sessions.begin() as session:
                # Old consumer projection still reads identical terminal and hash columns.
                row = (
                    await session.execute(
                        text(
                            "SELECT status,intent,content_hash,binding_hash "
                            "FROM agent_portfolio_synthesis_outputs WHERE run_id=:id"
                        ),
                        {"id": run_id},
                    )
                ).one()
                assert tuple(row) == ("failed", None, content_hash, binding_hash)
                assert (await read_synthesis_failure(session, run_id=run_id)).state == "missing"
                # Isolated synthetic storage corruption only to exercise downgrade data protection.
                await session.execute(
                    text("ALTER TABLE agent_portfolio_synthesis_outputs DISABLE TRIGGER USER")
                )
                await session.execute(
                    text(
                        "UPDATE agent_portfolio_synthesis_outputs "
                        "SET failure_diagnostic='{}'::jsonb"
                    )
                )
                await session.execute(
                    text("ALTER TABLE agent_portfolio_synthesis_outputs ENABLE TRIGGER USER")
                )
        finally:
            await engine.dispose()

    asyncio.run(check_old_and_insert_diagnostic())
    with pytest.raises(Exception, match="SYNTHESIS_DIAGNOSTIC_FACTS_EXIST"):
        command.downgrade(config, "0081_historical_sources")

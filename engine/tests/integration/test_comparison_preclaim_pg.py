"""Native closure of a rejected, never-claimed comparison; no provider involved."""

import asyncio
import copy
import json
from argparse import Namespace
from decimal import Decimal
from uuid import uuid4

import pytest
from integration import test_comparison_producer_pg as fixtures
from nexus.agents import comparison_preparation
from nexus.agents.committee import INDEPENDENT_MONITOR_PROMPT_VERSION
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.ops.research_comparison import native_command
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


async def legacy_unclaimed(factory, monkeypatch, tmp_path, *, oversized=True):
    original = fixtures.archive_source

    def wide_archive():
        archive = original()
        if not oversized:
            return archive
        member = archive["members"][0]
        for prompt in member["prompts"].values():
            prompt["version"] = INDEPENDENT_MONITOR_PROMPT_VERSION
        member["inputs"]["filing_disclosures"]["items"] = [{"excerpt": "x" * 50000}]
        return archive

    with monkeypatch.context() as legacy:
        legacy.setattr(fixtures, "archive_source", wide_archive)
        legacy.setattr(comparison_preparation, "validate_comparison_capacity", lambda _: None)
        source, prepared, manifest, model_id, old_id = await fixtures.seed(factory)
        async with factory() as session, session.begin():
            admission = await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=1,
                model_config_id=model_id,
                daily_limit_usd=Decimal("100"),
            )
    source_path, prepared_path, manifest_path = (
        tmp_path / name for name in ("source.json", "prepared.json", "manifest.json")
    )
    source_path.write_bytes(source)
    prepared_path.write_text(json.dumps(prepared))
    manifest_path.write_text(manifest.model_dump_json())
    args = Namespace(
        command="inspect-preclaim",
        repo_root=tmp_path,
        source=source_path,
        prepared=prepared_path,
        manifest=manifest_path,
    )
    return args, admission, manifest, old_id


@pytest.mark.asyncio
async def test_normal_preclaim_close_preserves_unknown_and_never_fabricates_a_stage(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.ops import openai_local

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    args, admission, manifest, old_id = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    async with factory() as session:
        old = await session.scalar(
            text("SELECT to_jsonb(b)::text FROM agent_daily_budget_reservations b WHERE id=:id"),
            {"id": old_id},
        )
    eligible = await native_command(args)
    assert eligible["status"] == "preclaim_close_eligible"
    assert eligible["dispatch_authority"] is False
    close = copy.copy(args)
    close.command = "close-preclaim"
    receipt = await native_command(close)
    assert receipt["status"] == "preclaim_closed"
    assert await native_command(args) == receipt
    assert await native_command(close) == receipt
    from nexus.agents.comparison_concurrency import require_comparison_exclusive

    async with factory.begin() as session:
        await require_comparison_exclusive(session, uuid4())
    from nexus.audit.service import append_audit_event

    async with factory() as session, session.begin():
        await append_audit_event(
            session,
            actor_id="unrelated-test",
            action="test.unrelated",
            schema_version="v1",
            payload={"unrelated": True},
            occurred_at=await session.scalar(text("SELECT clock_timestamp()")),
        )
    assert await native_command(args) == receipt
    position = await native_command(
        Namespace(
            command="inspect-position",
            repo_root=tmp_path,
            experiment_id=manifest.experiment_id,
            position=1,
        )
    )
    assert position["job_status"] == "failed" and position["stage_scope_stopped"] is True
    async with factory() as session:
        from nexus.agents.research_stages.proof import read_terminal_proof
        from nexus.agents.research_stages.protocol import ProtocolRejected

        with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
            await read_terminal_proof(
                session,
                run_id=admission.run_id,
                now=await session.scalar(text("SELECT clock_timestamp()")),
            )
        row = (
            (
                await session.execute(
                    text("""SELECT j.status job_status,j.attempt_count,
          j.started_at,j.finished_at,r.status run_status,r.input_tokens,r.output_tokens,
          r.settled_cost_usd,b.state,b.settled_usd,b.reserved_usd,s.stopped
          FROM jobs j JOIN agent_research_runs r ON r.id=:run
          JOIN agent_daily_budget_reservations b ON b.run_id=r.id
          JOIN research_stage_scope s ON s.id=:scope WHERE j.id=:job"""),
                    {
                        "run": admission.run_id,
                        "scope": manifest.experiment_id,
                        "job": admission.job_id,
                    },
                )
            )
            .mappings()
            .one()
        )
        assert row["job_status"] == row["run_status"] == "failed"
        assert row["attempt_count"] == 0 and row["started_at"] is None
        assert row["finished_at"] is not None and row["stopped"] is True
        assert row["state"] == "released" and row["reserved_usd"] > 0
        assert all(
            row[k] is None
            for k in ("input_tokens", "output_tokens", "settled_cost_usd", "settled_usd")
        )
        assert (
            await session.scalar(
                text(
                    "SELECT to_jsonb(b)::text FROM agent_daily_budget_reservations b WHERE id=:id"
                ),
                {"id": old_id},
            )
            == old
        )
        for table in (
            "research_stage_authorization",
            "research_stage_observation",
            "research_stage_cleanup",
            "research_stage_terminal",
        ):
            assert (
                await session.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE run_id=:id"), {"id": admission.run_id}
                )
                == 0
            )
        for table in ("job_attempts", "job_leases"):
            assert (
                await session.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE job_id=:id"), {"id": admission.job_id}
                )
                == 0
            )
    await factory.kw["bind"].dispose()

    from alembic import command
    from alembic.config import Config

    with pytest.raises(RuntimeError, match="COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED"):
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0104_comparison_chain_stop"
        )
    assert await native_command(args) == receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_first", [False, True])
async def test_close_and_native_claim_share_the_same_scope_fence(
    migrated_database_url, monkeypatch, tmp_path, claim_first
):
    from nexus.agents.comparison_claim import claim_comparison_position
    from nexus.agents.comparison_preclaim import capacity_refusal, preclaim_close

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    args, admission, manifest, _ = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    refusal = capacity_refusal(
        args.source.read_bytes(), json.loads(args.prepared.read_text()), manifest
    )
    from sqlalchemy.ext.asyncio import AsyncSession

    pids = {}
    entered = {kind: asyncio.Event() for kind in ("claim", "close")}
    release_claim = asyncio.Event()
    execute = AsyncSession.execute

    async def tracked(session, statement, *arguments, **kwargs):
        sql = str(statement)
        kind = (
            "claim"
            if sql.startswith("SELECT comparison_id,stopped")
            else "close"
            if sql.startswith("SELECT stopped,comparison_id")
            else None
        )
        if kind is not None:
            pids[kind] = (await execute(session, text("SELECT pg_backend_pid()"))).scalar_one()
            if not (kind == "claim" and claim_first):
                entered[kind].set()
        result = await execute(session, statement, *arguments, **kwargs)
        if kind == "claim" and claim_first:
            entered[kind].set()
            await release_claim.wait()  # Hold the real scope row lock before claim commits.
        return result

    monkeypatch.setattr(AsyncSession, "execute", tracked)

    async def blocked_by(kind, owner):
        async with asyncio.timeout(10):
            await entered[kind].wait()
            while True:
                async with factory() as observer:
                    blocked = await observer.scalar(
                        text("SELECT :owner=ANY(pg_blocking_pids(:pid))"),
                        {"owner": owner, "pid": pids[kind]},
                    )
                if blocked:
                    return
                await asyncio.sleep(0)

    async def close_position():
        async with factory() as session, session.begin():
            return await preclaim_close(session, manifest=manifest, refusal=refusal, close=True)

    tasks = []
    try:
        if claim_first:
            # Fault injection at native claim, not a capacity-qualified provider send.
            claiming = asyncio.create_task(
                claim_comparison_position(
                    factory,
                    experiment_id=manifest.experiment_id,
                    ordinal=1,
                    worker_id="preclaim-race",
                )
            )
            tasks.append(claiming)
            async with asyncio.timeout(10):
                await entered["claim"].wait()
            closing = asyncio.create_task(close_position())
            tasks.append(closing)
            await blocked_by("close", pids["claim"])
            release_claim.set()
            assert (await claiming).job_id == admission.job_id
            with pytest.raises(ValueError, match="COMPARISON_PRECLAIM_NOT_ELIGIBLE"):
                await closing
        else:
            async with factory() as session, session.begin():
                await preclaim_close(session, manifest=manifest, refusal=refusal, close=True)
                waiting = asyncio.create_task(
                    claim_comparison_position(
                        factory,
                        experiment_id=manifest.experiment_id,
                        ordinal=1,
                        worker_id="late-claim",
                    )
                )
                tasks.append(waiting)
                await blocked_by("claim", pids["close"])
            with pytest.raises(ValueError, match="COMPARISON_STOPPED"):
                await waiting
    finally:
        release_claim.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    async with factory() as session:
        assert await session.scalar(
            text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"),
            {"id": admission.reservation_id},
        ) == ("reserved" if claim_first else "released")
        assert (
            await session.scalar(
                text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:id"),
                {"id": admission.run_id},
            )
            == 0
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_stopped_comparison_rejects_registration_and_reopening(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.agents.comparison_preclaim import capacity_refusal, preclaim_close
    from nexus.agents.comparison_registration import register_comparison
    from sqlalchemy.exc import DBAPIError

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    args, admission, manifest, _ = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    refusal = capacity_refusal(
        args.source.read_bytes(), json.loads(args.prepared.read_text()), manifest
    )
    async with factory() as session, session.begin():
        await preclaim_close(session, manifest=manifest, refusal=refusal, close=True)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="COMPARISON_STOPPED"):
            await register_comparison(session, manifest)
    async with factory() as session:
        with pytest.raises(DBAPIError, match="COMPARISON_PRECLOSED"):
            await session.execute(
                text("UPDATE jobs SET status='queued',finished_at=NULL WHERE id=:id"),
                {"id": admission.job_id},
            )
        await session.rollback()
        with pytest.raises(DBAPIError, match="query returned no rows"):
            await session.execute(
                text("""INSERT INTO research_stage_authorization
              (run_id,stage,job_id,attempt_no,worker_id,lease_token,fence_token,
               reserved_id,wire_hash,schema_hash,prior_hash,authorized_at)
              SELECT :run,1,:job,1,'late-test',:job,1,:budget,repeat('a',64),
                schema_hash,NULL,clock_timestamp() FROM agent_research_runs WHERE id=:run"""),
                {
                    "run": admission.run_id,
                    "job": admission.job_id,
                    "budget": admission.reservation_id,
                },
            )
        await session.rollback()
        with pytest.raises(DBAPIError, match="foreign key constraint"):
            await session.execute(
                text("""INSERT INTO research_stage_observation
              VALUES(:run,1,repeat('a',64),1,1,true,clock_timestamp(),true)"""),
                {"run": admission.run_id},
            )
        await session.rollback()
        with pytest.raises(DBAPIError, match="COMPARISON_STOPPED"):
            await session.execute(
                text("""INSERT INTO research_comparison_chain
              SELECT experiment_id,2,run_id,job_id,reserved_id,input_hash,created_at
              FROM research_comparison_chain WHERE experiment_id=:id"""),
                {"id": manifest.experiment_id},
            )
        await session.rollback()
        with pytest.raises(DBAPIError, match="COMPARISON_STOPPED"):
            await session.execute(
                text("""INSERT INTO research_stage_member
              VALUES(:run,:job,:scope,clock_timestamp()+interval '1 minute',
                clock_timestamp()+interval '2 minutes')"""),
                {"run": admission.run_id, "job": admission.job_id, "scope": manifest.experiment_id},
            )
        await session.rollback()
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_financial_busy_rolls_back_entire_close(migrated_database_url, monkeypatch, tmp_path):
    from nexus.agents.budget import ResearchAccountingBusy
    from nexus.ops import openai_local

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    args, admission, manifest, _ = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    args.command = "close-preclaim"
    async with factory() as blocker, blocker.begin():
        day = await blocker.scalar(
            text("SELECT business_date FROM agent_daily_budget_reservations WHERE id=:id"),
            {"id": admission.reservation_id},
        )
        await blocker.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"agent-budget:{day}"}
        )
        with pytest.raises(ResearchAccountingBusy):
            await native_command(args)
        async with factory() as check:
            assert (
                await check.scalar(
                    text("SELECT stopped FROM research_stage_scope WHERE id=:id"),
                    {"id": manifest.experiment_id},
                )
                is False
            )
            assert (
                await check.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"),
                    {"id": admission.reservation_id},
                )
                == "reserved"
            )
            assert (
                await check.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='research.comparison.preclaim_closed'"
                    )
                )
                == 0
            )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_qualified_capacity_cannot_use_unclaimed_close(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.ops import openai_local

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    args, admission, manifest, _ = await legacy_unclaimed(
        factory, monkeypatch, tmp_path, oversized=False
    )
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    args.command = "close-preclaim"
    with pytest.raises(ValueError, match="COMPARISON_CAPACITY_REFUSAL_REQUIRED"):
        await native_command(args)
    async with factory() as session:
        assert (
            await session.scalar(
                text("SELECT stopped FROM research_stage_scope WHERE id=:id"),
                {"id": manifest.experiment_id},
            )
            is False
        )
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"),
                {"id": admission.reservation_id},
            )
            == "reserved"
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_empty_preclaim_migration_roundtrip(migrated_database_url):
    from alembic import command
    from alembic.config import Config

    config = Config("engine/alembic.ini")
    await asyncio.to_thread(command.downgrade, config, "0104_comparison_chain_stop")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0104_comparison_chain_stop"
        )
        assert (
            await session.scalar(
                text(
                    "SELECT to_regprocedure('research_comparison_preclaim_bound(uuid,timestamptz)')"
                )
            )
            is None
        )
    # Preserve the original 0104 -> 0105 empty migration contract.
    await asyncio.to_thread(command.upgrade, config, "0105_comparison_preclaim")
    async with factory() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0105_comparison_preclaim"
        )
        assert (
            await session.scalar(
                text(
                    "SELECT to_regprocedure('research_comparison_preclaim_bound(uuid,timestamptz)')"
                )
            )
            is not None
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_downgrade_refuses_member_reader_before_any_ddl(migrated_database_url, monkeypatch):
    from alembic import command, op
    from alembic.config import Config
    from sqlalchemy.exc import DBAPIError

    # Keep this lock-order test on the historical 0105 migration it exercises;
    # 0106 downgrade fences have separate current-schema regression coverage.
    await asyncio.to_thread(
        command.downgrade, Config("engine/alembic.ini"), "0105_comparison_preclaim"
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    statements = []
    execute = op.execute

    def bounded_execute(statement, *args, **kwargs):
        sql = str(statement)
        if sql.startswith("LOCK TABLE audit_events"):
            # Bounds the old erroneous later-DDL wait; no duration assertion.
            execute("SET LOCAL statement_timeout='3s'")
        statements.append(sql)
        return execute(statement, *args, **kwargs)

    monkeypatch.setattr(op, "execute", bounded_execute)

    async def definition(session):
        return (
            await session.scalar(text("SELECT version_num FROM alembic_version")),
            list(
                await session.scalars(
                    text(
                        "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname IN "
                        "('research_comparison_admission_fence',"
                        "'research_comparison_install_fence') "
                        "ORDER BY tgname"
                    )
                )
            ),
            await session.scalar(
                text(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conrelid='jobs'::regclass AND conname='ck_jobs_jobs_time_shape'"
                )
            ),
            await session.scalar(text("SELECT count(*) FROM audit_events")),
        )

    async with factory() as holder, holder.begin():
        before = await definition(holder)
        assert before[0] == "0105_comparison_preclaim" and len(before[1]) == 2
        # Retain only the member table lock; definitions use pg_catalog, not jobs.
        # Read audit count outside the holding transaction below.
    async with factory() as holder, holder.begin():
        await holder.execute(text("SELECT * FROM research_stage_member"))
        with pytest.raises(DBAPIError) as refused:
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0104_comparison_chain_stop"
            )
        assert getattr(refused.value.orig, "sqlstate", None) == "55P03"
        assert str(refused.value.statement).startswith("LOCK TABLE audit_events")
        assert not any(sql.startswith("DROP ") for sql in statements)
        await holder.execute(text("SET LOCAL lock_timeout='1s'"))
        assert await holder.scalar(text("SELECT count(*) FROM jobs")) == 0
        assert await definition(holder) == before
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_preclaim_commit_acknowledgement_loss_requires_readback(
    migrated_database_url, monkeypatch, tmp_path, committed
):
    from nexus.ops import openai_local
    from nexus.persistence import session as session_module

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    args, admission, _, _ = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    dialect = factory.kw["bind"].sync_engine.dialect
    commit, rollback = dialect.do_commit, dialect.do_rollback
    injected = False

    def lost_ack(connection):
        nonlocal injected
        if injected:
            return commit(connection)
        injected = True
        if committed:
            commit(connection)
        else:
            rollback(connection)
        raise OSError("private-ack-loss")

    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    monkeypatch.setattr(session_module, "session_factory", lambda _: factory)
    monkeypatch.setattr(dialect, "do_commit", lost_ack)
    close = copy.copy(args)
    close.command = "close-preclaim"
    with pytest.raises(RuntimeError, match="COMPARISON_PRECLOSE_RESULT_UNKNOWN"):
        await native_command(close)
    assert injected
    assert (await native_command(args))["status"] == (
        "preclaim_closed" if committed else "preclaim_close_eligible"
    )
    async with factory() as session:
        assert await session.scalar(
            text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"),
            {"id": admission.reservation_id},
        ) == ("released" if committed else "reserved")
        assert await session.scalar(
            text(
                "SELECT count(*) FROM audit_events "
                "WHERE action='research.comparison.preclaim_closed'"
            )
        ) == int(committed)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_close_has_one_audit_and_one_release(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.ops import openai_local

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    args, _, _, _ = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    args.command = "close-preclaim"
    first, second = await asyncio.gather(native_command(args), native_command(args))
    assert first == second and first["status"] == "preclaim_closed"
    async with factory() as session:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM audit_events "
                    "WHERE action='research.comparison.preclaim_closed'"
                )
            )
            == 1
        )
    await factory.kw["bind"].dispose()

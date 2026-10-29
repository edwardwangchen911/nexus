"""Formal stage storage preserves old identities and refuses evidence-dropping rollback."""

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


def migrate(revision, *, downgrade=False):
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    (command.downgrade if downgrade else command.upgrade)(config, revision)


@pytest.mark.asyncio
async def test_formal_stage_schema_roundtrip_without_runtime_ddl(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT to_regclass('research_stage_route')")) is not None
            )
        from uuid import uuid4

        from nexus.agents.research_stages.protocol import ProtocolRejected, ensure_schema
        from nexus.agents.research_stages.routes import selected_route

        await asyncio.to_thread(migrate, "0098_independent_monitor", downgrade=True)
        async with factory() as session:
            assert await selected_route(session, uuid4()) is None
            with pytest.raises(ProtocolRejected, match="RESEARCH_STAGE_SCHEMA_REQUIRED"):
                await ensure_schema(session)
            assert await session.scalar(text("SELECT to_regclass('research_stage_scope')")) is None
        await asyncio.to_thread(migrate, "head")
        async with factory() as session:
            await ensure_schema(session)
            assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 0
    finally:
        await factory.kw["bind"].dispose()


class PreparationSaved(BaseException):
    def __init__(self, captured):
        self.captured = captured


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
@pytest.mark.parametrize("selected, known", [(False, False), (True, False), (True, True)])
async def test_formal_route_birth_legacy_isolation_and_unknown_rollback_protection(
    migrated_database_url, monkeypatch, tmp_path, independent, selected, known
):
    from helpers import staged_routes as original_fixture
    from integration.test_staged_roots_pg import (
        test_normal_three_stock_roots_use_one_staged_protocol as prepare,
    )
    from nexus.agents.codex_formal_stage import formal_stage_job_qualified
    from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    from nexus.agents.research_stages import routes
    from nexus.agents.research_stages.protocol import StagedProtocol
    from nexus.jobs.models import QueueName
    from nexus.jobs.repository import claim_next_job

    monkeypatch.setattr(original_fixture, "ensure_routes", routes.ensure_routes)
    if selected:
        monkeypatch.setattr(
            original_fixture, "register_prepared_runs", routes.register_prepared_runs
        )
    else:

        async def leave_legacy(session, run_ids):
            return None

        monkeypatch.setattr(original_fixture, "register_prepared_runs", leave_legacy)
        # These identities deliberately model Jobs born before formal selection.
        # Normal production births are covered by test_research_stages_runtime_pg.
        monkeypatch.setattr(routes, "register_new_prepared_runs", leave_legacy)

    async def stop_before_any_worker(captured, settings, options):
        raise PreparationSaved(captured)

    with pytest.raises(PreparationSaved) as saved:
        await prepare(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            independent,
            False,
            before_worker=stop_before_any_worker,
        )
    captured = saved.value.captured
    factory = captured["fixture"][0]
    now = captured["clock"][0]
    try:
        async with factory() as session:
            jobs = (
                await session.execute(
                    text(
                        "SELECT id,payload FROM jobs "
                        "WHERE job_type='agent.paper.research' ORDER BY id"
                    )
                )
            ).all()
            assert len(jobs) == 3
            job_id, payload = jobs[0]
            from uuid import UUID

            run_id = UUID(payload["run_id"])
            profile = await session.scalar(
                text("SELECT pricing_version FROM agent_research_runs WHERE id=:run"),
                {"run": run_id},
            )
            claim_options = {
                "research_budget_profile": profile,
                "locked_job_guard": formal_stage_job_qualified,
                "lease_seconds": 330 if profile == BOUNDED_STAGE_BUDGET_CONFIG_VERSION else 210,
            }
            route = await routes.selected_route(session, job_id)
            assert bool(route) == selected
            assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='reserved'"
                    )
                )
                == 3
            )
        if not selected:
            with pytest.raises(DBAPIError, match="preparation route binding invalid"):
                async with factory.begin() as session:
                    await routes.register_prepared_runs(session, (run_id,))
            async with factory.begin() as session:
                claim = await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id="legacy-owner",
                    now=now,
                    job_ids=(job_id,),
                    **claim_options,
                )
                assert claim is not None
            await asyncio.to_thread(migrate, "0098_independent_monitor", downgrade=True)
            async with factory() as session:
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM jobs WHERE id=:id"), {"id": job_id}
                    )
                    == 1
                )
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM agent_daily_budget_reservations "
                            "WHERE state='reserved'"
                        )
                    )
                    == 3
                )
            await asyncio.to_thread(migrate, "head")
            return
        assert route["version"] == routes.VERSION
        assert route["kind"] == ("independent" if independent else "formal")
        async with factory.begin() as session:
            await routes.register_prepared_runs(session, (run_id,))
            assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 3
        with pytest.raises(DBAPIError, match="staged worker required"):
            async with factory.begin() as session:
                await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id="old-worker",
                    now=now,
                    job_ids=(job_id,),
                    **claim_options,
                )
        with pytest.raises(Exception, match="RESEARCH_STAGE_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(migrate, "0098_independent_monitor", downgrade=True)
        async with factory.begin() as session:
            await session.execute(
                text("SELECT set_config('nexus.stage_protocol',:version,true)"),
                {"version": routes.VERSION},
            )
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="formal-stage-owner",
                now=now,
                job_ids=(job_id,),
                **claim_options,
            )
            assert claim is not None
        protocol = StagedProtocol(factory)
        await protocol.install(
            scope_id=route["scope_id"],
            claims=(claim,),
            now=now,
            work_deadline=now + timedelta(seconds=100),
            cleanup_deadline=now + timedelta(seconds=110),
            independent=independent,
        )
        ticket = await protocol.authorize(claim, stage=1, now=now, wire=b"synthetic formal wire")
        await protocol.observe(
            ticket,
            now=now + timedelta(seconds=1),
            payload=b"unknown result",
            tokens=(10, 5) if known else None,
            valid=known,
        )
        if known:
            import hashlib

            assert await protocol.verify(
                ticket,
                now=now + timedelta(seconds=1),
                wire_hash=hashlib.sha256(b"synthetic formal wire").hexdigest(),
                payload=b"unknown result",
                tokens=(10, 5),
            )
        await protocol.cleanup(ticket, now=now + timedelta(seconds=2), confirmed=True)
        assert await protocol.close(claim, now=now + timedelta(seconds=3)) is known
        with pytest.raises(Exception, match="RESEARCH_STAGE_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(migrate, "0098_independent_monitor", downgrade=True)
        async with factory() as session:
            terminal = (
                (
                    await session.execute(
                        text("SELECT * FROM research_stage_terminal WHERE run_id=:run"),
                        {"run": run_id},
                    )
                )
                .mappings()
                .one()
            )
            from nexus.agents.research_stages.proof import read_terminal_proof

            proof = await read_terminal_proof(
                session, run_id=run_id, now=now + timedelta(seconds=4)
            )
            assert proof is not None and proof.known is known and proof.cleanup_confirmed
            assert proof.evidence_hash == terminal["chain_hash"]
            assert terminal["known"] is known
            assert await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                {"run": run_id},
            ) == ("settled" if known else "reserved")
            assert await routes.selected_route(session, job_id) == route
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("preparation_commits_scope", [False, True])
async def test_empty_downgrade_waits_for_jobs_before_locking_stage_storage(
    migrated_database_url, preparation_commits_scope
):
    """An existing preparation can finish in jobs→scope order during rollback."""
    import importlib.util

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    file = Path(__file__).resolve().parents[2] / "alembic/versions/0099_research_stages.py"
    spec = importlib.util.spec_from_file_location("stage_migration_test", file)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    engine = factory.kw["bind"]
    cycle_id = None
    if preparation_commits_scope:
        from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis

        seed_engine, _, _ = await seed_synthesis(migrated_database_url, complete_research=False)
        await seed_engine.dispose()
        async with factory() as session:
            cycle_id = await session.scalar(text("SELECT id FROM agent_paper_cycles"))
            budgets_before = (
                await session.execute(
                    text(
                        "SELECT id,state,reserved_usd,settled_usd "
                        "FROM agent_daily_budget_reservations ORDER BY id"
                    )
                )
            ).all()
    started = asyncio.get_running_loop().create_future()

    async def downgrade():
        async with engine.begin() as connection:
            started.set_result(await connection.scalar(text("SELECT pg_backend_pid()")))

            def execute(sync):
                migration.op = Operations(MigrationContext.configure(sync))
                migration.downgrade()

            await connection.run_sync(execute)

    task = None
    try:
        async with factory.begin() as preparation:
            await preparation.execute(text("LOCK TABLE jobs IN ROW EXCLUSIVE MODE"))
            owner_pid = await preparation.scalar(text("SELECT pg_backend_pid()"))
            task = asyncio.create_task(downgrade())
            pid = await started
            async with asyncio.timeout(10):
                while True:
                    async with factory() as observer:
                        blocked = await observer.scalar(
                            text("SELECT :owner=ANY(pg_blocking_pids(:pid))"),
                            {"owner": owner_pid, "pid": pid},
                        )
                    if blocked:
                        break
                    await asyncio.sleep(0)
            # Correct lock order leaves scope available while downgrade waits
            # for this existing jobs writer. NOWAIT proves it without timing.
            await preparation.execute(
                text("LOCK TABLE research_stage_scope IN ROW EXCLUSIVE MODE NOWAIT")
            )
            if preparation_commits_scope:
                await preparation.execute(
                    text("INSERT INTO research_stage_scope(id,cycle_id) VALUES(:cycle,:cycle)"),
                    {"cycle": cycle_id},
                )
        if preparation_commits_scope:
            with pytest.raises(DBAPIError, match="RESEARCH_STAGE_DOWNGRADE_FORBIDDEN"):
                await task
            async with factory() as observer:
                assert (
                    await observer.scalar(
                        text("SELECT cycle_id FROM research_stage_scope WHERE id=:cycle"),
                        {"cycle": cycle_id},
                    )
                    == cycle_id
                )
                assert (
                    await observer.scalar(text("SELECT version_num FROM alembic_version"))
                    == "0099_research_stages"
                )
                assert (
                    await observer.execute(
                        text(
                            "SELECT id,state,reserved_usd,settled_usd "
                            "FROM agent_daily_budget_reservations ORDER BY id"
                        )
                    )
                ).all() == budgets_before
        else:
            await task
            async with factory() as observer:
                assert (
                    await observer.scalar(text("SELECT to_regclass('research_stage_scope')"))
                    is None
                )
    finally:
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("independent_parent", [False, True])
async def test_formal_increment_route_uses_its_own_birth_and_preserves_parent(
    migrated_database_url, monkeypatch, tmp_path, independent_parent
):
    from helpers import staged_routes as fixture_routes
    from integration import test_staged_worker_pg as source
    from nexus.agents.research_stages import routes
    from nexus.monitoring import admission, mixed_admission
    from nexus.persistence.models.monitoring import MonitorIncrement
    from sqlalchemy import select

    async def keep_historical_parents(session, run_ids):
        # This compatibility scenario selects only the new child explicitly
        # below; completed parents intentionally retain their legacy identity.
        return None

    monkeypatch.setattr(routes, "register_new_prepared_runs", keep_historical_parents)

    def prepare_selected(patches):
        def wrap(native):
            async def prepare(session, **kwargs):
                async with session.begin_nested():
                    member = await native(session, **kwargs)
                    await routes.register_prepared_runs(session, (member.run_id,))
                    return member

            return prepare

        patches.setattr(mixed_admission, "admit_batch", wrap(mixed_admission.admit_batch))
        patches.setattr(admission, "admit_trigger", wrap(admission.admit_trigger))

    async def stop_before_worker(settings, *, fixture, **kwargs):
        raise PreparationSaved({"fixture": fixture})

    monkeypatch.setattr(fixture_routes, "install_increment_preparation", prepare_selected)
    monkeypatch.setattr(source, "run_staged_worker", stop_before_worker)
    with pytest.raises(PreparationSaved) as saved:
        await source.exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            independent=independent_parent,
        )
    factory = saved.value.captured["fixture"][0]
    try:
        async with factory() as session:
            member = (await session.scalars(select(MonitorIncrement))).one()
            route = await routes.selected_route(session, member.job_id)
            assert route["run_id"] == member.run_id and route["scope_id"] == member.id
            assert route["kind"] == "increment" and route["version"] == routes.VERSION
            assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 1
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                    {"run": member.run_id},
                )
                == "reserved"
            )
            scope = (
                (
                    await session.execute(
                        text("SELECT * FROM research_stage_scope WHERE id=:scope"),
                        {"scope": member.id},
                    )
                )
                .mappings()
                .one()
            )
            assert scope["cycle_id"] is None and scope["request_id"] is None
        with pytest.raises(DBAPIError, match="stage fact immutable"):
            async with factory.begin() as session:
                await session.execute(
                    text("UPDATE research_stage_route SET version='legacy' WHERE run_id=:run"),
                    {"run": member.run_id},
                )
        async with factory() as session:
            assert await routes.selected_route(session, member.job_id) == route
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_nonempty_downgrade_rejects_without_blocking_stage_to_job_order(
    migrated_database_url,
):
    """Existing stage authorization keeps its scope→Job order during rejected rollback."""
    import importlib.util

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis

    engine, factory, _ = await seed_synthesis(migrated_database_url, complete_research=False)
    async with factory.begin() as session:
        cycle = await session.scalar(text("SELECT id FROM agent_paper_cycles"))
        await session.execute(
            text("INSERT INTO research_stage_scope(id,cycle_id) VALUES(:id,:id)"), {"id": cycle}
        )
    file = Path(__file__).resolve().parents[2] / "alembic/versions/0099_research_stages.py"
    spec = importlib.util.spec_from_file_location("stage_active_migration_test", file)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    started = asyncio.get_running_loop().create_future()

    async def downgrade():
        async with engine.begin() as connection:
            started.set_result(await connection.scalar(text("SELECT pg_backend_pid()")))

            def execute(sync):
                migration.op = Operations(MigrationContext.configure(sync))
                migration.downgrade()

            await connection.run_sync(execute)

    task = None
    try:
        async with factory.begin() as authorization:
            await authorization.execute(
                text("SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE"), {"id": cycle}
            )
            owner = await authorization.scalar(text("SELECT pg_backend_pid()"))
            task = asyncio.create_task(downgrade())
            pid = await started
            async with asyncio.timeout(10):
                while not task.done():
                    async with factory() as observer:
                        blocked = await observer.scalar(
                            text("SELECT :owner=ANY(pg_blocking_pids(:pid))"),
                            {"owner": owner, "pid": pid},
                        )
                    if blocked:
                        break
                    await asyncio.sleep(0)
            await authorization.execute(text("LOCK TABLE jobs IN ROW EXCLUSIVE MODE NOWAIT"))
        with pytest.raises(DBAPIError, match="RESEARCH_STAGE_DOWNGRADE_FORBIDDEN"):
            await task
        async with factory() as session:
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_stage_scope WHERE id=:id"), {"id": cycle}
                )
                == 1
            )
    finally:
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()

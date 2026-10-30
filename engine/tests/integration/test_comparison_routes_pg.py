"""Experimental owners use the real migrated protocol without granting dispatch."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4, uuid5

import pytest
from alembic import command
from alembic.config import Config
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.agents.research_stages.routes import register_prepared_runs, selected_route
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import claim_next_job, enqueue_job, reclaim_expired_jobs
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from test_comparison_producer_pg import seed


async def prepared_position(factory):
    source, prepared, manifest, model_id, old_id = await seed(factory)
    async with factory.begin() as session:
        result = await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=1,
            model_config_id=model_id,
            daily_limit_usd=Decimal(4),
        )
    return result, manifest, old_id


@pytest.mark.asyncio
async def test_normal_producer_selects_exact_native_comparison_route(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, _ = await prepared_position(factory)
    async with factory() as session:
        route = await selected_route(session, result.job_id)
        assert route is not None
        assert dict(route) == {
            "run_id": result.run_id,
            "job_id": result.job_id,
            "scope_id": manifest.experiment_id,
            "kind": "comparison",
            "version": "research-stages-v1",
        }
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
        scope = (await session.execute(text("SELECT * FROM research_stage_scope"))).mappings().one()
        assert scope["comparison_id"] == manifest.experiment_id
        assert (scope["cycle_id"], scope["increment_id"], scope["request_id"]) == (None, None, None)


@pytest.mark.asyncio
async def test_generic_claim_and_recovery_skip_experiment_without_poisoning_queue(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, _, _ = await prepared_position(factory)
    async with factory.begin() as session:
        now = await session.scalar(text("SELECT clock_timestamp()"))
        assert (
            await claim_next_job(
                session, queue=QueueName.STRATEGY_RND, worker_id="ordinary", now=now
            )
            is None
        )
        assert await reclaim_expired_jobs(session, queue=QueueName.STRATEGY_RND, now=now) == 0
        job = await session.get(Job, result.job_id)
        assert (job.status, job.attempt_count, job.manual_retry_count) == ("queued", 0, 0)
        payload = {"fixture": "ordinary-strategy"}
        ordinary = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.STRATEGY_RND,
                scope_kind="global",
                scope_id="fixture-comparison-isolation",
                effective_at=now,
                input_hash=job_input_hash(payload),
            ),
            schema_version="fixture-v1",
            payload=payload,
            queue=QueueName.STRATEGY_RND,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=2,
            available_at=now + timedelta(seconds=1),
        )
        # Give the ordinary Job a later availability; skipping the older
        # experimental candidate must still let the normal queue make progress.
        claim = await claim_next_job(
            session,
            queue=QueueName.STRATEGY_RND,
            worker_id="ordinary",
            now=now + timedelta(seconds=1),
        )
        assert claim is not None and claim.job_id == ordinary.id
        assert (
            await reclaim_expired_jobs(
                session, queue=QueueName.STRATEGY_RND, now=now + timedelta(seconds=32)
            )
            == 1
        )
        await session.refresh(job)
        assert (job.status, job.attempt_count, job.manual_retry_count) == ("queued", 0, 0)


@pytest.mark.asyncio
async def test_comparison_scope_stop_is_permanent_and_blocks_later_positions(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model_id, _ = await seed(factory)
    async with factory.begin() as session:
        result = await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=1,
            model_config_id=model_id,
            daily_limit_usd=Decimal(6),
        )
    await StagedProtocol(factory).stop(manifest.experiment_id)
    with pytest.raises(DBAPIError, match="preparation route binding invalid"):
        async with factory.begin() as session:
            await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=3,
                model_config_id=model_id,
                daily_limit_usd=Decimal(6),
            )
    for sql, params in (
        (
            "UPDATE research_stage_scope SET stopped=false WHERE id=:id",
            {"id": manifest.experiment_id},
        ),
        (
            "UPDATE research_stage_scope SET comparison_id=:other WHERE id=:id",
            {"id": manifest.experiment_id, "other": uuid4()},
        ),
        ("DELETE FROM research_stage_scope WHERE id=:id", {"id": manifest.experiment_id}),
    ):
        with pytest.raises(DBAPIError, match="stop cannot be revoked"):
            async with factory.begin() as session:
                await session.execute(text(sql), params)
    async with factory() as session:
        assert (await selected_route(session, result.job_id))["kind"] == "comparison"
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 1
        assert await session.scalar(
            text("SELECT sum(reserved_usd) FROM agent_daily_budget_reservations")
        ) == Decimal("3.2")


@pytest.mark.asyncio
async def test_registration_cannot_retrofit_a_committed_job(migrated_database_url, monkeypatch):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def omit_route(session, run_ids):
        pass

    with monkeypatch.context() as m:
        m.setattr("nexus.agents.research_stages.routes.register_prepared_runs", omit_route)
        result, _, _ = await prepared_position(factory)
    with pytest.raises(DBAPIError, match="preparation route binding invalid"):
        async with factory.begin() as session:
            await register_prepared_runs(session, (result.run_id,))
    async with factory() as session:
        assert await selected_route(session, result.job_id) is None
        assert await session.scalar(text("SELECT count(*) FROM research_stage_scope")) == 0


@pytest.mark.asyncio
async def test_concurrent_stop_serializes_with_route_registration(
    migrated_database_url, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model_id, _ = await seed(factory)

    async def admit(ordinal):
        async with factory.begin() as session:
            return await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=ordinal,
                model_config_id=model_id,
                daily_limit_usd=Decimal(6),
            )

    await admit(1)
    at_route, stopped, release = (asyncio.Event() for _ in range(3))
    pids = {}
    execute = AsyncSession.execute

    async def hold_before_route(session, statement, params=None, **kwargs):
        if str(statement).startswith("INSERT INTO research_stage_route") and (
            params["run"] == uuid5(manifest.experiment_id, "run:3")
        ):
            pids["producer"] = (
                await execute(session, text("SELECT pg_backend_pid()"))
            ).scalar_one()
            at_route.set()
            await stopped.wait()
        return await execute(session, statement, params, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", hold_before_route)

    async def stop():
        await at_route.wait()
        async with factory.begin() as session:
            pids["stop"] = await session.scalar(text("SELECT pg_backend_pid()"))
            await session.execute(
                text("UPDATE research_stage_scope SET stopped=true WHERE id=:id"),
                {"id": manifest.experiment_id},
            )
            stopped.set()
            await release.wait()

    producer, stopper = asyncio.create_task(admit(3)), asyncio.create_task(stop())
    try:
        async with asyncio.timeout(10):
            await stopped.wait()
            while True:
                assert not producer.done(), "registration must wait for the uncommitted stop"
                async with factory() as observer:
                    blocked = await observer.scalar(
                        text("SELECT :owner=ANY(pg_blocking_pids(:pid))"),
                        {"owner": pids["stop"], "pid": pids["producer"]},
                    )
                if blocked:
                    break
                await asyncio.sleep(0)
        release.set()
        await stopper
        with pytest.raises(DBAPIError, match="preparation route binding invalid"):
            await producer
    finally:
        release.set()
        await asyncio.gather(producer, stopper, return_exceptions=True)
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 1


@pytest.mark.asyncio
async def test_wrong_owner_and_route_mutation_refused_without_dispatch(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, _ = await prepared_position(factory)
    for kind, job in (("formal", result.job_id), ("comparison", uuid4())):
        with pytest.raises(DBAPIError, match="preparation route binding invalid"):
            async with factory.begin() as session:
                await session.execute(
                    text(
                        "INSERT INTO research_stage_route "
                        "VALUES(:run,:job,:scope,:kind,'research-stages-v1')"
                    ),
                    {
                        "run": result.run_id,
                        "job": job,
                        "scope": manifest.experiment_id,
                        "kind": kind,
                    },
                )
    with pytest.raises(DBAPIError, match="stage fact immutable"):
        async with factory.begin() as session:
            await session.execute(
                text("UPDATE research_stage_route SET kind='formal' WHERE run_id=:run"),
                {"run": result.run_id},
            )
    with pytest.raises(DBAPIError, match="COMPARISON_DISPATCH_NOT_ENABLED"):
        async with factory.begin() as session:
            await session.execute(
                text("SELECT set_config('nexus.stage_protocol','research-stages-v1',true)")
            )
            await session.execute(
                text("UPDATE jobs SET status='running',attempt_count=1 WHERE id=:job"),
                {"job": result.job_id},
            )


@pytest.mark.asyncio
async def test_empty_route_migration_roundtrip_preserves_all_old_functions(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    names = (
        "research_stage_route_guard",
        "research_stage_stop_guard",
        "research_stage_route_member_guard",
        "research_stage_root_authorize_guard",
        "research_stage_authorize_guard",
        "research_comparison_claim_guard",
    )

    async def definitions():
        async with factory() as session:
            return {
                name: await session.scalar(
                    text("SELECT pg_get_functiondef(CAST(:name AS regprocedure))"),
                    {"name": name + "()"},
                )
                for name in names
            }

    after = await definitions()
    await asyncio.to_thread(
        command.downgrade, Config("engine/alembic.ini"), "0101_comparison_inputs"
    )
    before = await definitions()
    assert "comparison_id" not in before["research_stage_stop_guard"]
    assert "'comparison'" not in before["research_stage_route_guard"]
    for name in ("research_stage_authorize_guard", "research_comparison_claim_guard"):
        assert before[name] == after[name]
    await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
    assert await definitions() == after

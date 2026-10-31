"""A fixed next position is not a retry of a stopped predecessor."""

import hashlib
from datetime import timedelta
from decimal import Decimal

import pytest
from integration.test_comparison_producer_pg import seed
from nexus.agents.comparison_claim import claim_comparison_position
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "known,clean,orphan",
    [(True, True, False), (False, True, False), (True, False, False), (True, True, True)],
)
async def test_next_exact_position_after_failed_chain_requires_complete_safe_proof(
    migrated_database_url, monkeypatch, known, clean, orphan
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model, old = await seed(factory)
    async with factory.begin() as session:
        first = await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=1,
            model_config_id=model,
            daily_limit_usd=Decimal(5),
        )
        api = await session.scalar(
            select(AgentModelConfigRecord.id).where(
                AgentModelConfigRecord.config_hash == manifest.api_config_hash
            )
        )
        second = await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=2,
            model_config_id=api,
            daily_limit_usd=Decimal(5),
        )
    claim = await claim_comparison_position(
        factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="first"
    )
    now = claim.expires_at - timedelta(seconds=330)
    p = StagedProtocol(factory)
    await p.install(
        scope_id=manifest.experiment_id,
        claims=(claim,),
        now=now,
        work_deadline=now + timedelta(seconds=300),
        cleanup_deadline=now + timedelta(seconds=307),
        comparison=True,
    )
    ticket = await p.authorize(claim, stage=1, now=now, wire=b"first")
    if known:
        await p.observe(ticket, now=now, payload=b"failed", tokens=(20, 9), valid=False)
        assert await p.verify(
            ticket,
            now=now,
            wire_hash=hashlib.sha256(b"first").hexdigest(),
            payload=b"failed",
            tokens=(20, 9),
        )
    await p.cleanup(ticket, now=now, confirmed=clean)
    await p.stop(manifest.experiment_id)
    with pytest.raises(ValueError):
        await p.authorize(claim, stage=2, now=now, wire=b"must not continue")
    with pytest.raises(ValueError, match="COMPARISON_(PREDECESSOR_UNCONFIRMED|STOPPED)"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=2, worker_id="premature"
        )
    closed_at = claim.expires_at + timedelta(seconds=1) if orphan else now
    assert await p.close(claim, now=closed_at, orphan=orphan) is known
    safe = known and clean and not orphan
    from argparse import Namespace
    from pathlib import Path

    from nexus.ops import openai_local
    from nexus.ops.research_comparison import native_command

    monkeypatch.setattr(
        openai_local,
        "local_settings",
        lambda *args, **kwargs: EngineSettings(database_url=migrated_database_url),
    )
    args = Namespace(
        command="inspect-position",
        repo_root=Path.cwd(),
        experiment_id=manifest.experiment_id,
        position=1,
    )
    if orphan:
        from nexus.agents import comparison_closeout

        async def before_close(_session):
            return now

        monkeypatch.setattr(comparison_closeout, "database_now", before_close)
        with pytest.raises(ValueError, match="COMPARISON_TERMINAL_PROOF_INVALID"):
            await native_command(args)

        async def after_close(_session):
            return closed_at

        # The public proof reader must use the same controlled time as this
        # expired-lease fixture; future evidence is not valid at the earlier time.
        monkeypatch.setattr(comparison_closeout, "database_now", after_close)
    receipt = await native_command(args)
    assert receipt["stage_chain_stopped"] is True
    assert receipt["stage_scope_stopped"] is (not safe)
    assert receipt["dispatch_authority"] is False
    assert receipt["budget_state"] == ("settled" if known else "reserved")
    async with factory() as session:
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is (not safe)
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"), {"id": old}
            )
            == "reserved"
        )
    if safe:
        successor = await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=2, worker_id="second"
        )
        assert successor.job_id == second.job_id and successor.attempt_no == 1
        next_protocol = StagedProtocol(factory)
        next_now = successor.expires_at - timedelta(seconds=330)
        await next_protocol.install(
            scope_id=manifest.experiment_id,
            claims=(successor,),
            now=next_now,
            work_deadline=next_now + timedelta(seconds=300),
            cleanup_deadline=next_now + timedelta(seconds=307),
            comparison=True,
        )
        next_ticket = await next_protocol.authorize(
            successor, stage=1, now=next_now, wire=b"new own chain"
        )
        assert next_ticket.run_id == second.run_id and next_ticket.run_id != first.run_id
    else:
        with pytest.raises(ValueError, match="COMPARISON_STOPPED"):
            await claim_comparison_position(
                factory, experiment_id=manifest.experiment_id, ordinal=2, worker_id="second"
            )
    with pytest.raises(ValueError, match="COMPARISON_(POSITION_CONSUMED|STOPPED)"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="replay"
        )


async def first_stage(factory, prepared=None):
    from integration.test_comparison_routes_pg import prepared_position

    result, manifest, old = prepared if prepared is not None else await prepared_position(factory)
    claim = await claim_comparison_position(
        factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="first"
    )
    now = claim.expires_at - timedelta(seconds=330)
    p = StagedProtocol(factory)
    await p.install(
        scope_id=manifest.experiment_id,
        claims=(claim,),
        now=now,
        work_deadline=now + timedelta(seconds=300),
        cleanup_deadline=now + timedelta(seconds=307),
        comparison=True,
    )
    return result, manifest, old, claim, now, p


@pytest.mark.asyncio
async def test_chain_stop_database_fence_is_immutable_and_global_stop_never_cleared(
    migrated_database_url,
):
    from sqlalchemy.exc import DBAPIError

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, p = await first_stage(factory)
    ticket = await p.authorize(claim, stage=1, now=now, wire=b"one")
    await p.observe(ticket, now=now, payload=b"valid", tokens=(20, 9))
    assert await p.verify(
        ticket,
        now=now,
        wire_hash=hashlib.sha256(b"one").hexdigest(),
        payload=b"valid",
        tokens=(20, 9),
    )
    await p.cleanup(ticket, now=now, confirmed=True)
    await p.stop(manifest.experiment_id)
    with pytest.raises(DBAPIError, match="COMPARISON_CHAIN_STOPPED"):
        async with factory.begin() as session:
            await session.execute(
                text("""INSERT INTO research_stage_authorization
              SELECT (jsonb_populate_record(NULL::research_stage_authorization,
                to_jsonb(a)||jsonb_build_object('stage',2,'prior_hash',CAST(:prior AS text)))).*
              FROM research_stage_authorization a WHERE run_id=:run"""),
                {"run": result.run_id, "prior": hashlib.sha256(b"valid").hexdigest()},
            )
    for sql in (
        "DELETE FROM research_comparison_chain_stop",
        "UPDATE research_comparison_chain_stop SET stopped_at=clock_timestamp()",
    ):
        with pytest.raises(DBAPIError, match="stage fact immutable"):
            async with factory.begin() as session:
                await session.execute(text(sql))
    # An explicit/global stop has no installed per-chain context.
    await StagedProtocol(factory).stop(manifest.experiment_id)
    assert await p.close(claim, now=now)
    async with factory() as session:
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is True
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
        assert (
            await session.scalar(text("SELECT count(*) FROM research_comparison_chain_stop")) == 1
        )
    with pytest.raises(ValueError, match="COMPARISON_STOPPED"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=2, worker_id="not-global-resume"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_authorization_commit_ambiguity_still_stops_whole_experiment(
    migrated_database_url, monkeypatch, committed
):
    from sqlalchemy.ext.asyncio import AsyncSession

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, old, claim, now, p = await first_stage(factory)
    commit = AsyncSession.commit
    injected = False

    async def uncertain(session):
        nonlocal injected
        if not injected:
            injected = True
            if committed:
                await commit(session)
            raise OSError("CONTROLLED_AUTHORIZATION_COMMIT_ACK_LOSS")
        return await commit(session)

    monkeypatch.setattr(AsyncSession, "commit", uncertain)
    with pytest.raises(OSError, match="CONTROLLED_AUTHORIZATION_COMMIT_ACK_LOSS"):
        await p.authorize(claim, stage=1, now=now, wire=b"never sent")
    assert injected
    async with factory() as session:
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is True
        assert await session.scalar(
            text("SELECT count(*) FROM research_stage_authorization")
        ) == int(committed)
        assert await session.scalar(text("SELECT count(*) FROM research_stage_observation")) == 0
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"), {"id": old}
            )
            == "reserved"
        )
    with pytest.raises(ValueError, match="COMPARISON_STOPPED"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=2, worker_id="no-more"
        )


@pytest.mark.asyncio
async def test_chain_stop_migration_round_trip_and_populated_rollback_fence(
    migrated_database_url, monkeypatch
):
    import asyncio
    from argparse import Namespace
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from integration.test_comparison_routes_pg import prepared_position
    from nexus.ops import openai_local
    from nexus.ops.research_comparison import native_command

    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config("engine/alembic.ini")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prepared = await prepared_position(factory)
    _, manifest, _ = prepared
    await asyncio.to_thread(command.downgrade, config, "0103_comparison_claim")
    monkeypatch.setattr(
        openai_local,
        "local_settings",
        lambda *args, **kwargs: EngineSettings(database_url=migrated_database_url),
    )
    receipt = await native_command(
        Namespace(
            command="inspect-position",
            repo_root=Path.cwd(),
            experiment_id=manifest.experiment_id,
            position=1,
        )
    )
    assert receipt["stage_chain_stopped"] is None
    assert receipt["stage_scope_stopped"] is False
    assert receipt["dispatch_authority"] is False and receipt["attempt_count"] == 0
    with pytest.raises(ValueError, match="COMPARISON_CHAIN_STOP_SCHEMA_REQUIRED"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="old-schema"
        )
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
    # This is the historical 0103 -> 0105 rollback fence; Platform 0106 has
    # separate populated/empty downgrade coverage and must not redefine it.
    await asyncio.to_thread(command.upgrade, config, "0105_comparison_preclaim")
    _, manifest, _, _, _, p = await first_stage(factory, prepared)
    await p.stop(manifest.experiment_id)
    with pytest.raises(RuntimeError, match="COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED"):
        await asyncio.to_thread(command.downgrade, config, "0103_comparison_claim")
    async with factory() as session:
        assert (
            await session.scalar(text("SELECT version_num FROM alembic_version"))
            == "0105_comparison_preclaim"
        )
        assert (
            await session.scalar(text("SELECT count(*) FROM research_comparison_chain_stop")) == 1
        )


@pytest.mark.asyncio
async def test_concurrent_chain_stop_serializes_with_next_stage_authorization(
    migrated_database_url, monkeypatch
):
    import asyncio

    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.ext.asyncio import AsyncSession

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, _, claim, now, p = await first_stage(factory)
    ticket = await p.authorize(claim, stage=1, now=now, wire=b"first")
    await p.observe(ticket, now=now, payload=b"good", tokens=(20, 9))
    assert await p.verify(
        ticket,
        now=now,
        wire_hash=hashlib.sha256(b"first").hexdigest(),
        payload=b"good",
        tokens=(20, 9),
    )
    await p.cleanup(ticket, now=now, confirmed=True)
    pids = {}
    entered = asyncio.Event()
    execute = AsyncSession.execute

    async def tracked(session, statement, *args, **kwargs):
        if "SELECT m.*,s.stopped" in str(statement):
            pids["authorize"] = (
                await execute(session, text("SELECT pg_backend_pid()"))
            ).scalar_one()
            entered.set()
        return await execute(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", tracked)
    async with factory() as blocker:
        await blocker.begin()
        owner = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await p._stop_comparison_chain(
            blocker, scope_id=manifest.experiment_id, run_id=result.run_id
        )
        task = asyncio.create_task(p.authorize(claim, stage=2, now=now, wire=b"forbidden"))
        try:
            async with asyncio.timeout(5):
                await entered.wait()
                while True:
                    async with factory() as observer:
                        blocked = await observer.scalar(
                            text("SELECT :owner=ANY(pg_blocking_pids(:pid))"),
                            {"owner": owner, "pid": pids["authorize"]},
                        )
                    if blocked:
                        break
                    await asyncio.sleep(0)
            await blocker.commit()
            with pytest.raises(DBAPIError, match="COMPARISON_CHAIN_STOPPED"):
                await task
        finally:
            await blocker.rollback()
            await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is False

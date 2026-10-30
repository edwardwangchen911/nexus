"""Exact native experimental claims and common terminal proof; no model calls."""

import asyncio
from datetime import timedelta
from decimal import Decimal

import pytest
from alembic import command
from alembic.config import Config
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from test_comparison_producer_pg import seed
from test_comparison_routes_pg import prepared_position


@pytest.mark.asyncio
@pytest.mark.parametrize("known", [False, True])
@pytest.mark.parametrize("orphan", [False, True])
async def test_exact_claim_common_authorization_cleanup_and_terminal(
    migrated_database_url, known, orphan
):
    from nexus.agents.comparison_claim import claim_comparison_position

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old = await prepared_position(factory)
    claim = await claim_comparison_position(
        factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="comparison-fixture"
    )
    assert claim.job_id == result.job_id and claim.attempt_no == 1
    now = claim.expires_at - timedelta(seconds=330)
    protocol = StagedProtocol(factory)
    await protocol.install(
        scope_id=manifest.experiment_id,
        claims=(claim,),
        now=now,
        work_deadline=now + timedelta(seconds=300),
        cleanup_deadline=now + timedelta(seconds=307),
        comparison=True,
    )
    ticket = await protocol.authorize(claim, stage=1, now=now, wire=b"frozen-wire")
    if known:
        import hashlib

        await protocol.observe(
            ticket, now=now, payload=b"failed-known", tokens=(10, 5), valid=False
        )
        assert await protocol.verify(
            ticket,
            now=now,
            wire_hash=hashlib.sha256(b"frozen-wire").hexdigest(),
            payload=b"failed-known",
            tokens=(10, 5),
        )
    await protocol.cleanup(ticket, now=now, confirmed=True)
    closed_at = claim.expires_at + timedelta(seconds=1) if orphan else now
    assert await protocol.close(claim, now=closed_at, orphan=orphan) is known
    async with factory() as session:
        proof = await read_terminal_proof(session, run_id=result.run_id, now=closed_at)
        assert proof.known is known and proof.cleanup_confirmed and proof.orphan is orphan
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"), {"id": old}
            )
            == "reserved"
        )
        assert await session.scalar(
            text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:id"),
            {"id": result.run_id},
        ) == ("settled" if known else "reserved")
    if not known:
        await protocol.observe(
            ticket, now=closed_at + timedelta(seconds=1), payload=b"late-diagnostic", tokens=(20, 9)
        )
        async with factory() as session:
            late = await read_terminal_proof(
                session, run_id=result.run_id, now=closed_at + timedelta(seconds=1)
            )
            assert not late.known and late.evidence_hash == proof.evidence_hash
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                    {"run": result.run_id},
                )
                == "reserved"
            )
    with pytest.raises(ValueError, match="COMPARISON_(STOPPED|POSITION_CONSUMED)"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="replacement"
        )


@pytest.mark.asyncio
async def test_competing_claims_have_one_native_attempt(migrated_database_url):
    from nexus.agents.comparison_claim import claim_comparison_position

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, _ = await prepared_position(factory)
    barrier = asyncio.Barrier(2)

    async def claim(worker):
        await barrier.wait()
        return await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id=worker
        )

    results = await asyncio.gather(claim("first"), claim("second"), return_exceptions=True)
    assert sum(not isinstance(r, BaseException) for r in results) == 1
    errors = [r for r in results if isinstance(r, BaseException)]
    assert len(errors) == 1 and str(errors[0]) == "COMPARISON_POSITION_CONSUMED"
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 1
        assert await session.scalar(text("SELECT count(*) FROM job_leases")) == 1
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
        assert (await session.get(Job, result.job_id)).attempt_count == 1
    async with factory.begin() as session:
        # A consumed position is rejected before the generic queued-job branch.
        with pytest.raises(DBAPIError, match="COMPARISON_POSITION_CONSUMED"):
            async with session.begin_nested():
                await session.execute(
                    text("""UPDATE jobs SET status='queued',attempt_count=0,started_at=NULL,
                    version=version+1
                    WHERE id=:id"""),
                    {"id": result.job_id},
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_claim_commit_uncertainty_stops_without_restoring_send_rights(
    migrated_database_url, monkeypatch, committed
):
    from nexus.agents.comparison_claim import claim_comparison_position

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, _ = await prepared_position(factory)
    commit = AsyncSession.commit
    injected = False

    async def uncertain(session):
        nonlocal injected
        target = not injected and any(
            isinstance(row, Job) and row.id == result.job_id and row.status == "running"
            for row in session.identity_map.values()
        )
        if target:
            injected = True
            if committed:
                await commit(session)
            raise OSError("CONTROLLED_COMMIT_ACK_LOSS")
        return await commit(session)

    monkeypatch.setattr(AsyncSession, "commit", uncertain)
    with pytest.raises(OSError, match="CONTROLLED_COMMIT_ACK_LOSS"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="original"
        )
    assert injected
    async with factory() as session:
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is True
        job = await session.get(Job, result.job_id)
        assert (job.status, job.attempt_count) == (("running", 1) if committed else ("queued", 0))
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
        assert await session.scalar(
            text(
                "SELECT sum(reserved_usd) FROM agent_daily_budget_reservations "
                "WHERE state='reserved'"
            )
        ) == Decimal("3.2")
    with pytest.raises(ValueError, match="COMPARISON_STOPPED"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="replacement"
        )


@pytest.mark.asyncio
async def test_missing_prior_position_cannot_be_skipped(migrated_database_url):
    from nexus.agents.comparison_claim import claim_comparison_position

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model, _ = await seed(factory)
    async with factory.begin() as session:
        await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=3,
            model_config_id=model,
            daily_limit_usd=Decimal(4),
        )
    with pytest.raises(ValueError, match="COMPARISON_PREDECESSOR_UNCONFIRMED"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=3, worker_id="skip"
        )
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work,cleanup", [(301, 308), (300, 316)])
async def test_comparison_install_cannot_extend_original_envelope(
    migrated_database_url, work, cleanup
):
    from nexus.agents.comparison_claim import claim_comparison_position

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, _ = await prepared_position(factory)
    claim = await claim_comparison_position(
        factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="original"
    )
    start = claim.expires_at - timedelta(seconds=330)
    with pytest.raises(ValueError, match="COMPARISON_DEADLINE_INVALID"):
        await StagedProtocol(factory).install(
            scope_id=manifest.experiment_id,
            claims=(claim,),
            now=start,
            comparison=True,
            work_deadline=start + timedelta(seconds=work),
            cleanup_deadline=start + timedelta(seconds=cleanup),
        )
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_stage_member")) == 0


@pytest.mark.asyncio
async def test_claimed_position_requires_compatible_reader_rollback(migrated_database_url):
    from nexus.agents.comparison_claim import claim_comparison_position

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, _ = await prepared_position(factory)
    await claim_comparison_position(
        factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="original"
    )
    with pytest.raises(RuntimeError, match="COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED"):
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0102_comparison_routes"
        )
    async with factory() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0103_comparison_claim"
        )
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 1
        assert await session.scalar(text("SELECT count(*) FROM job_leases")) == 1

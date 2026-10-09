"""Candidate cycle authority is explicit and cannot change through resume."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_agent_execution_pg import _INSTRUMENT_ID, _NOW, _SNAPSHOT_ID
from integration.test_candidate_research_authority import prepare
from nexus.jobs.daily_research import admit_agent_paper_research
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from sqlalchemy import select


@pytest.mark.asyncio
async def test_native_paper_admission_binds_explicit_candidate_and_replays_exactly(
    migrated_database_url,
):
    factory, model_id = await prepare(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    )
    try:
        args = dict(
            cycle_id=uuid4(),
            instrument_id=_INSTRUMENT_ID,
            research_snapshot_id=_SNAPSHOT_ID,
            research_as_of=_NOW,
            now=datetime.now(UTC),
            daily_limit_usd=Decimal("3"),
            model_config_id=model_id,
            candidate_stage_capacity=True,
        )
        async with factory.begin() as session:
            admitted = await admit_agent_paper_research(session, **args)
        async with factory.begin() as session:
            replay = await admit_agent_paper_research(session, **args)
            assert replay.run_id == admitted.run_id
            assert replay.run.model_config_id == model_id
            jobs = tuple(await session.scalars(select(Job)))
            assert len(jobs) == 1 and jobs[0].job_type == "agent.paper.research"
            assert jobs[0].attempt_count == 0
            with pytest.raises(ValueError, match="authority|CANDIDATE"):
                await admit_agent_paper_research(
                    session, **(args | {"candidate_stage_capacity": False})
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["start", "advance", "resume"])
async def test_candidate_cycle_refuses_actual_nonisolated_database_before_reads(
    temporary_database_factory, entry
):
    from nexus.paper.agent_cycle import (
        advance_agent_paper_cycle,
        resume_schwab_agent_paper_cycle,
        start_schwab_agent_paper_cycle,
    )
    from nexus.persistence.session import session_factory

    factory = session_factory(
        EngineSettings(
            database_url=temporary_database_factory.admin_url.render_as_string(hide_password=False)
        )
    )
    try:
        async with factory() as session:
            with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
                if entry == "start":
                    await start_schwab_agent_paper_cycle(
                        session,
                        authority=None,
                        decision_slot_at=_NOW,
                        idempotency_key="isolated",
                        requested_instrument_ids=(_INSTRUMENT_ID,),
                        candidate_stage_capacity=True,
                        candidate_model_config_id=uuid4(),
                    )
                elif entry == "advance":
                    await advance_agent_paper_cycle(
                        session,
                        account_id=uuid4(),
                        decision_time=_NOW,
                        idempotency_key="isolated",
                        candidate_stage_capacity=True,
                        candidate_model_config_id=uuid4(),
                    )
                else:
                    await resume_schwab_agent_paper_cycle(
                        session,
                        cycle_id=uuid4(),
                        idempotency_key="isolated",
                        candidate_stage_capacity=True,
                    )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_formal_cycle_cannot_resume_as_candidate_before_claim(
    migrated_database_url, monkeypatch
):
    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
    from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
    from nexus.persistence.models.runtime import IdempotencyRecord
    from sqlalchemy import func

    engine, sessions, args = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
            with pytest.raises(ValueError, match="CODEX_CANDIDATE_CYCLE_PROFILE_INVALID"):
                await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=args["cycle_id"],
                    idempotency_key="cannot-switch-profile",
                    now=args["now"],
                    candidate_stage_capacity=True,
                )
            assert (
                await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_advance_locks_account_before_any_slot_profile_read_or_claim(
    migrated_database_url, monkeypatch
):
    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
    from nexus.paper.agent_cycle import advance_agent_paper_cycle
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.persistence.models.runtime import IdempotencyRecord
    from sqlalchemy import event, func

    engine, sessions, args = await _seed_normal_synthesis(migrated_database_url, monkeypatch)

    class ObservedLockedRead(Exception):
        pass

    def check_lock(conn, cursor, statement, parameters, context, executemany):
        if "from agent_paper_cycles" not in statement.lower():
            return
        held = conn.exec_driver_sql(
            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()"
        ).scalar_one()
        assert held > 0, "slot profile was read outside the account serialization lock"
        raise ObservedLockedRead

    try:
        async with sessions() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            account_id, decision_time, batch_id = (
                cycle.account_id,
                cycle.decision_time,
                cycle.candidate_batch_id,
            )
            before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
        event.listen(engine.sync_engine, "before_cursor_execute", check_lock)
        try:
            async with sessions.begin() as session:
                with pytest.raises(ObservedLockedRead):
                    await advance_agent_paper_cycle(
                        session,
                        account_id=account_id,
                        decision_time=decision_time,
                        candidate_batch_id=batch_id,
                        idempotency_key="serialized-mode-check",
                    )
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", check_lock)
        async with sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
            )
    finally:
        await engine.dispose()

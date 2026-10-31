"""Public closeout uses original native facts in isolated PostgreSQL only."""

import asyncio
import hashlib
import json
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from helpers.commit_ack_fault import CommitAcknowledgementLost, lose_commit_ack
from integration.test_comparison_sequence_pg import first_stage
from integration.test_platform_stage_accounting_pg import native_platform
from nexus.agents import comparison_closeout
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from unit.agents.test_platform_usage import usage


async def arrange(factory, monkeypatch, tmp_path, *, platform=True, known=True, authorize=True):
    if platform:
        result, manifest, claim, protocol, now, compiled, old, _ = await native_platform(
            factory, monkeypatch, tmp_path
        )
        wire, extra = compiled.wire, {"platform_wire": compiled}
        raw = {
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "status": "completed",
            "usage": usage(),
        }
        payload, tokens = json.dumps(raw).encode(), (12000, 1000)
    else:
        result, manifest, old, claim, now, protocol = await first_stage(factory)
        wire, extra, payload, tokens = b"original", {}, b"original-result", (12, 5)
    ticket = None
    if authorize:
        ticket = await protocol.authorize(claim, stage=1, now=now, wire=wire, **extra)
        if known:
            await protocol.observe(ticket, now=now, payload=payload, tokens=tokens, valid=False)
            assert await protocol.verify(
                ticket,
                now=now,
                wire_hash=hashlib.sha256(wire).hexdigest(),
                payload=payload,
                tokens=tokens,
            )
        await protocol.cleanup(ticket, now=now, confirmed=True)

    async def expired(_session):
        return claim.expires_at + timedelta(seconds=1)

    monkeypatch.setattr(comparison_closeout, "database_now", expired)
    return result, manifest, claim, protocol, ticket, old


async def facts(factory):
    async with factory() as session:
        return {
            table: (await session.execute(text(f"SELECT to_jsonb(t) FROM {table} t")))
            .scalars()
            .all()
            for table in (
                "jobs",
                "job_attempts",
                "job_leases",
                "agent_research_runs",
                "agent_daily_budget_reservations",
                "research_stage_terminal",
                "research_stage_authorization",
                "research_stage_observation",
                "research_stage_verification",
                "research_stage_cleanup",
                "research_comparison_chain_stop",
                "research_stage_scope",
            )
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", [False, True])
@pytest.mark.parametrize("known", [False, True])
async def test_public_expired_closeout_and_repeat_read_preserve_original(
    migrated_database_url, monkeypatch, tmp_path, platform, known
):
    from nexus.agents import research_maintenance

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, claim, protocol, ticket, old = await arrange(
        factory, monkeypatch, tmp_path, platform=platform, known=known
    )
    # The compatible rollback keeps original read/closeout/late-evidence paths,
    # while its build-fixed guard prevents any new comparison admission.
    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    before = await facts(factory)
    async with factory() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        pending = await comparison_closeout.inspect_comparison_terminal(
            session, manifest.experiment_id, 1
        )
    assert pending["terminal_proof"] is None and not pending["replay_allowed"]
    assert await facts(factory) == before
    receipt = await comparison_closeout.close_expired_comparison_position(
        factory, manifest.experiment_id, 1
    )
    assert receipt["status"] == "position_registered"
    assert receipt["terminal_proof"]["known"] is known
    assert receipt["terminal_proof"]["budget_settled"] is known
    assert receipt["terminal_proof"]["orphan"] is True
    assert receipt["budget_state"] == ("settled" if known else "reserved")
    assert receipt["stage_scope_stopped"] and receipt["stage_chain_stopped"]
    assert not receipt["dispatch_authority"] and not receipt["replay_allowed"]
    after = await facts(factory)
    assert len(after["job_attempts"]) == 1
    assert after["job_leases"] == before["job_leases"]
    assert after["research_stage_authorization"] == before["research_stage_authorization"]
    assert after["research_stage_observation"] == before["research_stage_observation"]
    async with factory() as session:
        if known and platform:
            assert await session.scalar(
                text("SELECT settled_cost_usd FROM agent_research_runs WHERE id=:id"),
                {"id": result.run_id},
            ) == Decimal("0.0323")
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"), {"id": old}
            )
            == "reserved"
        )
        assert (
            await comparison_closeout.inspect_comparison_terminal(
                session, manifest.experiment_id, 1
            )
            == receipt
        )
    assert (
        await comparison_closeout.close_expired_comparison_position(
            factory, manifest.experiment_id, 1
        )
        == receipt
    )
    assert await facts(factory) == after
    if not known:
        await protocol.observe(
            ticket,
            now=claim.expires_at + timedelta(seconds=2),
            payload=b"late raw diagnostic",
            tokens=(20, 9),
        )
        async with factory() as session:
            late = await comparison_closeout.inspect_comparison_terminal(
                session, manifest.experiment_id, 1
            )
        assert late == receipt
        assert (await facts(factory))["research_stage_terminal"] == after["research_stage_terminal"]


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["active", "no_authorization", "position"])
async def test_refusal_does_not_write_or_create_zero_call_proof(
    migrated_database_url, monkeypatch, tmp_path, reason
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, claim, _, _, _ = await arrange(
        factory, monkeypatch, tmp_path, authorize=reason != "no_authorization"
    )
    if reason == "active":

        async def active(_session):
            return claim.expires_at - timedelta(seconds=1)

        monkeypatch.setattr(comparison_closeout, "database_now", active)
    before = await facts(factory)
    expected = {
        "active": "COMPARISON_LEASE_ACTIVE",
        "no_authorization": "COMPARISON_STAGE_AUTHORIZATION_REQUIRED",
        "position": "COMPARISON_POSITION_INVALID",
    }[reason]
    with pytest.raises(ValueError, match=expected):
        await comparison_closeout.close_expired_comparison_position(
            factory, manifest.experiment_id, 4 if reason == "position" else 1
        )
    assert await facts(factory) == before


@pytest.mark.asyncio
async def test_concurrent_closeout_one_terminal_and_one_original_attempt(
    migrated_database_url, monkeypatch, tmp_path
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, _, _, _, _ = await arrange(factory, monkeypatch, tmp_path)
    barrier = asyncio.Barrier(2)

    async def close():
        await barrier.wait()
        return await comparison_closeout.close_expired_comparison_position(
            factory, manifest.experiment_id, 1
        )

    first, second = await asyncio.gather(close(), close())
    assert first == second
    after = await facts(factory)
    assert len(after["research_stage_terminal"]) == len(after["job_attempts"]) == 1
    assert len(after["research_comparison_chain_stop"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True], ids=["rolled-back", "committed"])
@pytest.mark.parametrize("known", [False, True], ids=["unknown-usage", "known-fee"])
async def test_commit_ack_unknown_is_fixed_and_never_retried(
    migrated_database_url, monkeypatch, tmp_path, server_commits, known
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    observer_factory = session_factory(EngineSettings(database_url=migrated_database_url))
    engine, observer_engine = factory.kw["bind"], observer_factory.kw["bind"]
    assert engine is not observer_engine
    try:
        result, manifest, claim, _, _, old = await arrange(
            factory, monkeypatch, tmp_path, known=known
        )
        before = await facts(observer_factory)
        commit, calls, acknowledged = AsyncSession.commit, [], []

        async def record_commit(session):
            calls.append(1)
            await commit(session)
            acknowledged.append(1)

        # This wrapper only observes Session acknowledgement. The actual fault is
        # inside the dedicated engine's DBAPI commit, after server COMMIT/ROLLBACK.
        monkeypatch.setattr(AsyncSession, "commit", record_commit)
        with lose_commit_ack(engine, server_commits=server_commits) as fault:
            with pytest.raises(
                comparison_closeout.ComparisonCloseoutUnknown,
                match="^COMPARISON_CLOSEOUT_RESULT_UNKNOWN$",
            ) as error:
                await comparison_closeout.close_expired_comparison_position(
                    factory, manifest.experiment_id, 1
                )
        assert error.value.__suppress_context__
        assert isinstance(error.value.__context__, CommitAcknowledgementLost)
        assert str(error.value.__context__) == "COMMIT_OUTCOME_UNCONFIRMED"
        assert fault.boundary_calls == 1 and fault.server_commit_completed is server_commits
        assert calls == [1] and acknowledged == []

        # Read only through a separately pooled engine, never the writer's Session
        # state or its exception. Do not repeat the public close operation.
        after = await facts(observer_factory)
        assert len(after["research_stage_terminal"]) == int(server_commits)
        assert len(after["research_comparison_chain_stop"]) == int(server_commits)
        assert len(after["job_attempts"]) == 1
        attempt, original = after["job_attempts"][0], before["job_attempts"][0]
        for key in ("id", "job_id", "attempt_no", "worker_id", "lease_token", "fence_token"):
            assert attempt[key] == original[key]
        assert attempt["job_id"] == str(claim.job_id)
        assert attempt["outcome"] == ("lost" if server_commits else "running")
        for table in (
            "job_leases",
            "research_stage_authorization",
            "research_stage_observation",
            "research_stage_verification",
            "research_stage_cleanup",
        ):
            assert after[table] == before[table]
        budgets = {row["id"]: row for row in after["agent_daily_budget_reservations"]}
        for original_budget in before["agent_daily_budget_reservations"]:
            if original_budget["run_id"] != str(result.run_id):
                assert budgets[original_budget["id"]] == original_budget
        assert budgets[str(old)]["state"] == "reserved"
        current = next(row for row in budgets.values() if row["run_id"] == str(result.run_id))
        assert current["state"] == ("settled" if server_commits and known else "reserved")
        if server_commits and known:
            assert Decimal(str(current["settled_usd"])) == Decimal("0.0323")
        else:
            assert current["settled_usd"] is None
        if not server_commits:
            assert after == before
        async with observer_factory() as observer:
            receipt = await comparison_closeout.inspect_comparison_terminal(
                observer, manifest.experiment_id, 1
            )
        assert (receipt["terminal_proof"] is not None) is server_commits
        if server_commits:
            assert receipt["terminal_proof"]["known"] is known
            assert receipt["terminal_proof"]["budget_settled"] is known
        assert await facts(observer_factory) == after
        assert calls == [1] and acknowledged == []
    finally:
        await engine.dispose()
        await observer_engine.dispose()


@pytest.mark.asyncio
async def test_known_counts_with_unknown_cache_cost_keep_original_reservation(
    migrated_database_url, monkeypatch, tmp_path
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, claim, protocol, ticket, _ = await arrange(
        factory, monkeypatch, tmp_path, known=False
    )
    raw = {
        "model": "gpt-5.6-terra",
        "service_tier": "default",
        "status": "completed",
        "usage": usage(),
    }
    del raw["usage"]["input_tokens_details"]["cache_write_tokens"]
    payload = json.dumps(raw).encode()
    now = claim.expires_at - timedelta(seconds=329)
    await protocol.observe(ticket, now=now, payload=payload, tokens=(12000, 1000))
    async with factory() as session:
        wire_hash = await session.scalar(
            text("SELECT wire_hash FROM research_stage_authorization WHERE run_id=:id"),
            {"id": result.run_id},
        )
    assert await protocol.verify(
        ticket, now=now, wire_hash=wire_hash, payload=payload, tokens=(12000, 1000)
    )
    receipt = await comparison_closeout.close_expired_comparison_position(
        factory, manifest.experiment_id, 1
    )
    assert not receipt["terminal_proof"]["known"] and receipt["budget_state"] == "reserved"
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT input_tokens,output_tokens,settled_cost_usd "
                    "FROM agent_research_runs WHERE id=:id"
                ),
                {"id": result.run_id},
            )
        ).one()
        assert tuple(row) == (12000, 1000, None)


@pytest.mark.asyncio
async def test_normal_native_terminal_is_read_without_reclassifying_as_orphan(
    migrated_database_url, monkeypatch, tmp_path
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, claim, protocol, _, _ = await arrange(factory, monkeypatch, tmp_path)
    assert await protocol.close(claim, now=claim.expires_at - timedelta(seconds=329))
    before = await facts(factory)
    receipt = await comparison_closeout.close_expired_comparison_position(
        factory, manifest.experiment_id, 1
    )
    assert receipt["terminal_proof"]["orphan"] is False
    assert receipt["terminal_proof"]["known"] and receipt["terminal_proof"]["budget_settled"]
    assert await facts(factory) == before


@pytest.mark.asyncio
async def test_unadmitted_unclaimed_and_legacy_preclaim_terminal_stay_read_only(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_comparison_preclaim_pg import legacy_unclaimed
    from nexus.agents.comparison_preclaim import capacity_refusal, preclaim_close

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        absent = await comparison_closeout.inspect_comparison_terminal(session, uuid4(), 1)
    assert absent["status"] == "position_not_admitted" and absent["terminal_proof"] is None
    args, _, manifest, _ = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    before = await facts(factory)
    async with factory() as session:
        queued = await comparison_closeout.inspect_comparison_terminal(
            session, manifest.experiment_id, 1
        )
    assert queued["attempt_count"] == 0 and queued["terminal_proof"] is None
    assert await facts(factory) == before

    async def no_route(_session, _job_id):
        return None

    with monkeypatch.context() as historical:
        historical.setattr(comparison_closeout, "selected_route", no_route)
        async with factory() as session:
            unselected = await comparison_closeout.inspect_comparison_terminal(
                session, manifest.experiment_id, 1
            )
        assert unselected["stage_route"] is None and unselected["stage_scope_stopped"] is None
        assert unselected["terminal_proof"] is None and unselected["attempt_count"] == 0
    assert await facts(factory) == before
    with pytest.raises(ValueError, match="COMPARISON_ORPHAN_NOT_QUALIFIED"):
        await comparison_closeout.close_expired_comparison_position(
            factory, manifest.experiment_id, 1
        )
    assert await facts(factory) == before
    refusal = capacity_refusal(
        args.source.read_bytes(), json.loads(args.prepared.read_text()), manifest
    )
    async with factory.begin() as session:
        assert (await preclaim_close(session, manifest=manifest, refusal=refusal, close=True))[
            "status"
        ] == "preclaim_closed"
    before = await facts(factory)
    async with factory() as session:
        closed = await comparison_closeout.inspect_comparison_terminal(
            session, manifest.experiment_id, 1
        )
    assert closed["terminal_proof"] is None and closed["attempt_count"] == 0
    assert closed["budget_state"] == "released" and closed["status"] == "position_registered"
    assert await facts(factory) == before
    with pytest.raises(ValueError, match="COMPARISON_ORPHAN_NOT_QUALIFIED"):
        await comparison_closeout.close_expired_comparison_position(
            factory, manifest.experiment_id, 1
        )
    assert await facts(factory) == before


@pytest.mark.asyncio
async def test_concurrent_late_observation_cannot_change_terminal_membership(
    migrated_database_url, monkeypatch, tmp_path
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, claim, protocol, ticket, _ = await arrange(
        factory, monkeypatch, tmp_path, known=False
    )
    barrier = asyncio.Barrier(2)

    async def close():
        await barrier.wait()
        return await comparison_closeout.close_expired_comparison_position(
            factory, manifest.experiment_id, 1
        )

    async def observe():
        await barrier.wait()
        await protocol.observe(
            ticket,
            now=claim.expires_at + timedelta(seconds=2),
            payload=b"late diagnostic",
            tokens=(20, 9),
        )

    receipt, _ = await asyncio.gather(close(), observe())
    assert not receipt["terminal_proof"]["known"] and receipt["budget_state"] == "reserved"
    before = await facts(factory)
    async with factory() as session:
        assert (
            await comparison_closeout.inspect_comparison_terminal(
                session, manifest.experiment_id, 1
            )
            == receipt
        )
    assert await facts(factory) == before


@pytest.mark.asyncio
async def test_closeout_scope_lock_is_bounded_and_preserves_original_facts(
    migrated_database_url, monkeypatch, tmp_path
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, manifest, _, _, _, _ = await arrange(factory, monkeypatch, tmp_path)
    before = await facts(factory)
    async with factory.begin() as blocker:
        await blocker.execute(
            text("SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE"),
            {"id": manifest.experiment_id},
        )
        with pytest.raises(ValueError, match="^COMPARISON_CLOSEOUT_BUSY$"):
            await asyncio.wait_for(
                comparison_closeout.close_expired_comparison_position(
                    factory, manifest.experiment_id, 1
                ),
                timeout=10,
            )
    assert await facts(factory) == before


@pytest.mark.asyncio
async def test_pre_chain_stop_schema_inspection_preserves_unknown_stop_field(
    migrated_database_url, monkeypatch, tmp_path
):
    from alembic import command
    from alembic.config import Config
    from integration.test_comparison_preclaim_pg import legacy_unclaimed

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    _, _, manifest, _ = await legacy_unclaimed(factory, monkeypatch, tmp_path)
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    await asyncio.to_thread(
        command.downgrade, Config("engine/alembic.ini"), "0103_comparison_claim"
    )
    async with factory() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        receipt = await comparison_closeout.inspect_comparison_terminal(
            session, manifest.experiment_id, 1
        )
    assert receipt["stage_chain_stopped"] is None
    assert receipt["status"] == "position_registered" and receipt["terminal_proof"] is None
    assert receipt["attempt_count"] == 0 and not receipt["dispatch_authority"]

"""Read-only recovery uses real account history without initiating business work."""

import importlib.util
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops.openai_local import prepare_database
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_request import admit_manual_request
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.runtime import IdempotencyRecord, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import event, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape, expected_code",
    [
        ("ready", None),
        ("comparison_settled", None),
        ("comparison_held", "RESEARCH_ACCOUNTING_UNSETTLED"),
        ("comparison_held_zero", "RESEARCH_ACCOUNTING_UNSETTLED"),
        ("old_unknown_claim", "MANUAL_PREPARATION_ACK_BLOCKED"),
        ("fresh_host", "AGENT_PAPER_RECOVERY_HOST_PRESENT"),
        ("bad_schema", "AGENT_PAPER_RECOVERY_SCHEMA_MISMATCH"),
        ("queued", "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"),
        ("late_queued", "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"),
        ("wrong_scope", "AGENT_PAPER_RECOVERY_SCOPE_INVALID"),
        ("occupied", "INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN"),
    ],
)
async def test_recovery_reads_actual_schema_host_and_cross_day_claims(
    migrated_database_url, monkeypatch, shape, expected_code
):
    # Business setup needs a valid market session, independently of when CI runs.
    at = AT
    assert importlib.util.find_spec("nexus.ops.agent_paper_recovery") is not None
    from nexus.ops.agent_paper_recovery import inspect_recovery_readiness

    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **OPTIONS,
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=at, occupied=shape == "occupied"
    )
    if shape.startswith("comparison_"):
        # Native comparison claims bind the real database budget day. Recovery is
        # cross-session and must inspect that clock without reseeding business work.
        async with sessions() as session:
            at = await session.scalar(text("SELECT clock_timestamp()"))
    original_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return at
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    try:
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="recovery-batch",
                created_at=at,
            )
            if shape == "old_unknown_claim":
                session.add(
                    IdempotencyRecord(
                        actor_id=f"local-agent-paper-schwab:{authority.account_id}",
                        route="agent-paper-schwab-trigger-v2",
                        idempotency_key=(at - timedelta(days=1)).isoformat(),
                        request_hash="a" * 64,
                        schema_version="agent_paper_schwab_trigger_v2",
                        response_payload={},
                        created_at=at - timedelta(days=1),
                    )
                )
            if shape == "fresh_host":
                session.add(
                    WorkerHeartbeat(
                        queue="agent_paper",
                        worker_id="other-host",
                        started_at=at,
                        seen_at=at,
                        process_version="other-version",
                        migration_head="other-schema",
                    )
                )
            if shape == "bad_schema":
                await session.execute(text("UPDATE alembic_version SET version_num='old'"))
            if shape == "queued":
                await admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_only",
                    idempotency_key="old-queued-request",
                )
        settings = EngineSettings(
            database_url=migrated_database_url,
            agent_runtime_mode="codex_sdk",
            agent_paper_cycle_enabled=True,
            agent_paper_account_id=UUID(int=999)
            if shape == "wrong_scope"
            else authority.account_id,
            agent_paper_candidate_batch_id=batch.batch_id,
            agent_paper_formal_model_config_id=model.id,
            release_revision="a" * 40,
        )
        if shape == "late_queued":
            from nexus.ops import agent_paper_loop as loop

            assert (await inspect_recovery_readiness(settings))["status"] == "ready"
            async with sessions.begin() as session:
                await admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_only",
                    idempotency_key="late-queued-request",
                )

            async def guard(effective):
                current = await inspect_recovery_readiness(effective)
                assert current["blocking_codes"] == ["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"]
                raise ValueError("AGENT_PAPER_RECOVERY_OCCUPIED")

            async def forbidden(*args, **kwargs):
                pytest.fail("late account occupation passed the pre-heartbeat activation guard")

            monkeypatch.setattr(loop, "acquire_agent_paper_lock", nullcontext)
            monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", forbidden)
            monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", forbidden)
            with pytest.raises(ValueError, match="^AGENT_PAPER_RECOVERY_OCCUPIED$"):
                await loop.run_local_agent_paper_loop(
                    settings,
                    account_id=authority.account_id,
                    repo_root=Path.cwd(),
                    recovery_guard=guard,
                )
        if shape.startswith("comparison_"):
            from integration.test_research_occupancy_pg import observed, verify
            from nexus.agents import execution
            from nexus.agents.budget import BudgetExhausted

            tokens = (20_000_000, 0) if shape == "comparison_held" else (0, 0)
            _, _, old, claim, now, protocol, ticket = await observed(
                sessions, tokens=tokens, protect_unknown=False
            )
            assert old is None
            assert await verify(protocol, ticket, now, tokens=tokens)
            await protocol.cleanup(ticket, now=now, confirmed=True)
            if shape == "comparison_held_zero":

                async def refusal(*args, **kwargs):
                    raise BudgetExhausted("synthetic deterministic refusal")

                monkeypatch.setattr(execution, "settle_daily_budget", refusal)
            assert await protocol.close(claim, now=now)
            async with sessions() as session:
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM agent_daily_budget_reservations")
                    )
                    == 1
                )
                assert await session.scalar(text("SELECT count(*) FROM manual_paper_requests")) == 0
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM research_stage_terminal WHERE known")
                    )
                    == 1
                )
            at = max(at, now + timedelta(seconds=1))
        writes = []

        def observe(_conn, _cursor, statement, _parameters, _context, _executemany):
            if statement.lstrip().split(None, 1)[0].upper() in {"INSERT", "UPDATE", "DELETE"}:
                writes.append(statement)

        event.listen(Engine, "before_cursor_execute", observe)
        try:
            result = await inspect_recovery_readiness(settings)
        finally:
            event.remove(Engine, "before_cursor_execute", observe)
        assert writes == []
        assert result["checked_at"] == at.isoformat()
        assert result["execution_locked"] is True and result["external_requests"] == 0
        assert result["status"] == ("ready" if expected_code is None else "blocked")
        assert result["blocking_codes"] == ([] if expected_code is None else [expected_code])
        async with sessions() as session:
            claims = list(
                await session.scalars(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.actor_id
                        == f"local-agent-paper-schwab:{authority.account_id}"
                    )
                )
            )
            assert len(claims) == (1 if shape == "old_unknown_claim" else 0)
            if claims:
                assert claims[0].response_payload == {}
                assert claims[0].idempotency_key == (at - timedelta(days=1)).isoformat()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape, expected_code",
    [
        ("comparison_settled", None),
        ("comparison_held", "RESEARCH_ACCOUNTING_UNSETTLED"),
        ("comparison_held_zero", "RESEARCH_ACCOUNTING_UNSETTLED"),
    ],
)
async def test_comparison_recovery_fixture_is_independent_of_closed_wall_clock(
    migrated_database_url, monkeypatch, shape, expected_code
):
    class ClosedWallClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 31, 23, tzinfo=UTC).astimezone(tz)

    # Freeze only this test module's wall clock. Database time and production
    # market/budget gates remain real; recovery must reach the accounting check.
    monkeypatch.setattr(f"{__name__}.datetime", ClosedWallClock)
    await test_recovery_reads_actual_schema_host_and_cross_day_claims(
        migrated_database_url, monkeypatch, shape, expected_code
    )

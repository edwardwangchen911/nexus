"""Normal identity producer, native ledger/projection and baseline approval."""

import asyncio
import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from helpers.paper_api import settings
from nexus.paper.economic_version_binding import bind_versions
from nexus.paper.portfolio_economic_protocol import register_protocol
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.economic import EconomicEvaluationRecord, EconomicPairSlotRecord
from nexus.persistence.models.paper import PaperInitialTargetApprovalRecord
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text

from engine.tests.integration.test_economic_authorities_pg import seed_pair
from engine.tests.integration.test_portfolio_economic_protocol_pg import protocol_request


async def prepare(url):
    factory = session_factory(settings(url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        await seed_pair(session, False)
        protocol = await register_protocol(
            session,
            protocol_request().model_copy(
                update={"research_cost_usd": Decimal(0), "synthesis_cost_usd": Decimal(0)}
            ),
            expected_database=actual,
        )
        binding = await bind_versions(
            session,
            protocol_hash=protocol.payload["content_hash"],
            proposal_hash="b" * 64,
            expected_database=actual,
        )
    return factory, dict(
        protocol_hash=protocol.payload["content_hash"],
        version_binding_hash=binding.payload["content_hash"],
        expected_database=actual,
    )


@pytest.mark.asyncio
async def test_normal_registration_produces_two_equal_native_baselines(migrated_database_url):
    from nexus.paper.economic_pair import register_economic_identity

    factory, kwargs = await prepare(migrated_database_url)
    try:
        async with factory.begin() as session:
            identity = await register_economic_identity(session, **kwargs)
            ids = (identity.incumbent_account_id, identity.challenger_account_id)
            assert ids[0] != ids[1]
            slots = tuple(
                await session.scalars(
                    select(EconomicPairSlotRecord)
                    .where(EconomicPairSlotRecord.evaluation_id == identity.id)
                    .order_by(EconomicPairSlotRecord.ordinal)
                )
            )
            assert len(slots) == 24 and [slot.ordinal for slot in slots] == list(range(1, 25))
            assert all(
                slot.execution_locked and slot.cutoff_at == slot.decision_at for slot in slots
            )
            for account_id in ids:
                projection = await load_paper_projection(
                    session, account_id=account_id, as_of=identity.created_at
                )
                assert projection.available_cash == Decimal(10000)
                assert not projection.positions and not projection.open_orders
                approval = await session.scalar(
                    select(PaperInitialTargetApprovalRecord).where(
                        PaperInitialTargetApprovalRecord.account_id == account_id
                    )
                )
                assert approval.projection_hash == projection.projection_hash
            for table in ("jobs", "agent_paper_cycles", "paper_orders", "paper_target_versions"):
                assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 0
        async with factory.begin() as session:
            again = await register_economic_identity(session, **kwargs)
            assert again.id == identity.id
            assert (
                await session.scalar(select(func.count()).select_from(EconomicEvaluationRecord))
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(EconomicPairSlotRecord)) == 24
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_registration_and_conflict_never_duplicate_funds(migrated_database_url):
    from nexus.paper.economic_pair import register_economic_identity

    factory, kwargs = await prepare(migrated_database_url)
    try:

        async def register():
            async with factory.begin() as session:
                return (await register_economic_identity(session, **kwargs)).id

        results = await asyncio.gather(register(), register())
        assert results[0] == results[1]
        async with factory.begin() as session:
            before = await session.scalar(text("SELECT count(*) FROM paper_ledger_transactions"))
            with pytest.raises(ValueError, match="ECONOMIC_PAIR_IDEMPOTENCY_CONFLICT"):
                await register_economic_identity(
                    session, **{**kwargs, "version_binding_hash": "c" * 64}
                )
            assert (
                await session.scalar(text("SELECT count(*) FROM paper_ledger_transactions"))
                == before
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_misnamed_database_rejected_before_any_write(migrated_database_url):
    from nexus.paper.economic_pair import register_economic_identity

    factory, kwargs = await prepare(migrated_database_url)
    try:
        async with factory.begin() as session:
            before = await session.scalar(text("SELECT count(*) FROM accounts"))
            with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_DATABASE_MISMATCH"):
                await register_economic_identity(
                    session, **{**kwargs, "expected_database": "nexus_test_" + "0" * 32}
                )
            assert await session.scalar(text("SELECT count(*) FROM accounts")) == before
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_second_lane_failure_rolls_back_first_lane_funding(
    migrated_database_url, monkeypatch
):
    import nexus.paper.economic_pair as pair

    factory, kwargs = await prepare(migrated_database_url)
    original = pair.approve_initial_target_projection
    calls = 0

    async def fail_second(*args, **parameters):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError("TEST_SECOND_LANE_FAILURE")
        return await original(*args, **parameters)

    monkeypatch.setattr(pair, "approve_initial_target_projection", fail_second)
    try:
        async with factory.begin() as session:
            tables = (
                "accounts",
                "portfolios",
                "paper_accounts",
                "paper_ledger_accounts",
                "paper_ledger_transactions",
                "paper_ledger_entries",
                "paper_drawdown_lock_events",
                "paper_initial_target_approvals",
                "economic_evaluations",
                "economic_pair_slots",
                "audit_events",
                "idempotency_records",
            )
            before = {
                table: await session.scalar(text(f"SELECT count(*) FROM {table}"))
                for table in tables
            }
        with pytest.raises(ValueError, match="TEST_SECOND_LANE_FAILURE"):
            async with factory.begin() as session:
                await pair.register_economic_identity(session, **kwargs)
        async with factory.begin() as session:
            for table in tables:
                assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == before[table]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_real_cli_registration_and_replay(migrated_database_url, tmp_path):
    factory, kwargs = await prepare(migrated_database_url)
    config = tmp_path / "database-url"
    config.write_text(migrated_database_url)
    config.chmod(0o600)
    try:

        async def run(binding_hash):
            process = await asyncio.create_subprocess_exec(
                str(Path(sys.executable).with_name("nexus-engine-economic-cycle")),
                "register",
                "--database-url-file",
                str(config),
                "--expected-database",
                kwargs["expected_database"],
                "--protocol-hash",
                kwargs["protocol_hash"],
                "--version-binding-hash",
                binding_hash,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
            assert not stderr
            return process.returncode, json.loads(stdout)

        first_status, first = await run(kwargs["version_binding_hash"])
        second_status, second = await run(kwargs["version_binding_hash"])
        assert first_status == second_status == 0
        assert first == second and first["status"] == "registered_not_started"
        assert first["planned_slots"] == 24 and first["lane_count"] == 2
        assert first["execution_locked"] is True and first["economic_status"] == "not_evaluated"
        assert "account_id" not in first and "cash" not in first
        failure_status, failure = await run("c" * 64)
        assert failure_status == 1 and failure["code"] == "ECONOMIC_PAIR_IDEMPOTENCY_CONFLICT"
        config.chmod(0o644)
        unsafe_status, unsafe = await run(kwargs["version_binding_hash"])
        assert unsafe_status == 1 and unsafe["code"] == "ECONOMIC_PAIR_CONFIG_UNSAFE"
        async with factory.begin() as session:
            assert (
                await session.scalar(select(func.count()).select_from(EconomicEvaluationRecord))
                == 1
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("already_registered", [False, True])
async def test_fresh_registration_requires_empty_business_but_replay_is_read_only(
    migrated_database_url, already_registered
):
    from nexus.paper.economic_pair import register_economic_identity
    from nexus.persistence.models.runtime import Job

    factory, kwargs = await prepare(migrated_database_url)
    try:
        async with factory.begin() as session:
            identity = (
                await register_economic_identity(session, **kwargs) if already_registered else None
            )
            now = await session.scalar(select(func.clock_timestamp()))
            session.add(
                Job(
                    job_key="registration-guard-case",
                    input_hash="a" * 64,
                    status="queued",
                    schema_version="test_v1",
                    payload={},
                    job_type="test.registration",
                    queue="general",
                    scope_kind="global",
                    scope_id="global",
                    effective_at=now,
                    priority=1,
                    retry_policy="never",
                    max_attempts=1,
                    available_at=now,
                    started_at=None,
                    finished_at=None,
                )
            )
        async with factory.begin() as session:
            before = await session.scalar(text("SELECT count(*) FROM paper_ledger_transactions"))
            if already_registered:
                assert (await register_economic_identity(session, **kwargs)).id == identity.id
            else:
                with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"):
                    await register_economic_identity(session, **kwargs)
            assert (
                await session.scalar(text("SELECT count(*) FROM paper_ledger_transactions"))
                == before
            )
    finally:
        await factory.kw["bind"].dispose()

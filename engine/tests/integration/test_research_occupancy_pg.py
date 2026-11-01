"""Budget consumers see committed stage costs before finalization can succeed."""

import asyncio
import hashlib
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_comparison_claim_pg import prepared_position
from nexus.agents.comparison_claim import claim_comparison_position
from nexus.agents.persistence import (
    _lock_budget_day,
    _shared_daily_budget_used,
    release_daily_budget_if_reserved,
)
from nexus.agents.research_stages.protocol import ProtocolRejected, StagedProtocol
from nexus.paper.formal_budget_inventory import inspect_inventory
from nexus.paper.formal_budget_protection import _native_usage
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text


async def observed(factory, *, valid=False, tokens=(20_000_000, 0), protect_unknown=True):
    result, manifest, old = await prepared_position(factory, protect_unknown=protect_unknown)
    claim = await claim_comparison_position(
        factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="occupancy-offline"
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
    ticket = await p.authorize(claim, stage=1, now=now, wire=b"own")
    await p.observe(ticket, now=now, payload=b"failed-known", tokens=tokens, valid=valid)
    return result, manifest, old, claim, now, p, ticket


async def verify(p, ticket, now, *, tokens=(20_000_000, 0)):
    return await p.verify(
        ticket,
        now=now,
        wire_hash=hashlib.sha256(b"own").hexdigest(),
        payload=b"failed-known",
        tokens=tokens,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True])
async def test_committed_usage_counts_before_terminal_and_after_closeout_rollback(
    migrated_database_url, orphan, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, p, ticket = await observed(factory)
    async with factory() as s:
        assert await _shared_daily_budget_used(s, business_date=now.date()) == Decimal("3.2")
    assert await verify(p, ticket, now)
    await p.cleanup(ticket, now=now, confirmed=True)
    from nexus.agents.research_stages import protocol as native_protocol

    settle = native_protocol._settle_or_hold_usage

    async def finalization_rollback(*args, **kwargs):
        await settle(*args, **kwargs)
        raise RuntimeError("synthetic finalization rollback")

    monkeypatch.setattr(native_protocol, "_settle_or_hold_usage", finalization_rollback)
    with pytest.raises(RuntimeError, match="synthetic finalization rollback"):
        await p.close(
            claim, now=claim.expires_at + timedelta(seconds=1) if orphan else now, orphan=orphan
        )
    async with factory() as s:
        run = await s.get(AgentResearchRunRecord, result.run_id)
        assert run.input_tokens is None and run.output_tokens is None
        budget = await s.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run.id
            )
        )
        assert (
            budget.state == "reserved"
            and budget.reserved_usd == Decimal("1.2")
            and budget.settled_usd is None
        )
        assert await _shared_daily_budget_used(s, business_date=now.date()) == Decimal(252)
        assert await _shared_daily_budget_used(s, business_date=now.date() + timedelta(days=1)) == 0
        assert await _native_usage(s, budget) == Decimal(250)
        inventory = await inspect_inventory(
            s,
            account_id=uuid4(),
            batch_id=uuid4(),
            enabled=False,
            now=now,
            configuration_hash="a" * 64,
        )
        assert inventory.conservative_native_occupied_usd == Decimal(252)
        assert await s.scalar(text("SELECT stopped FROM research_stage_scope")) is True
        assert await s.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
        assert (await s.get(AgentDailyBudgetReservationRecord, old)).state == "reserved"
    async with factory.begin() as s:
        with pytest.raises(ValueError, match="RESEARCH_USAGE_RELEASE_FORBIDDEN"):
            await release_daily_budget_if_reserved(s, run_id=result.run_id)
    from nexus.agents.persistence import (
        BudgetExhausted,
        reserve_daily_budget,
        reserve_portfolio_decision_budget,
    )
    from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget

    for reserve in (
        reserve_daily_budget,
        reserve_portfolio_decision_budget,
        reserve_portfolio_synthesis_budget,
    ):
        async with factory.begin() as s:
            with pytest.raises(BudgetExhausted):
                await reserve(
                    s,
                    run_id=uuid4(),
                    now=now,
                    daily_limit_usd=Decimal(4),
                    worst_case_usd=Decimal(".1"),
                    pricing_version="synthetic",
                    pricing_fresh=True,
                )


@pytest.mark.asyncio
async def test_usage_publication_waits_for_original_budget_day_then_new_reader_counts_it(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, p, ticket = await observed(factory)
    async with factory() as blocker:
        await blocker.begin()
        owner = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await _lock_budget_day(blocker, now.date().isoformat())
        task = asyncio.create_task(verify(p, ticket, now))

        async def blocked():
            while True:
                async with factory() as probe:
                    count = await probe.scalar(
                        text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE :owner=ANY(pg_blocking_pids(pid))"
                        ),
                        {"owner": owner},
                    )
                if count:
                    return
                await asyncio.sleep(0.02)

        try:
            await asyncio.wait_for(blocked(), 10)
            assert not task.done()
            assert await _shared_daily_budget_used(blocker, business_date=now.date()) == Decimal(
                "3.2"
            )
        finally:
            await blocker.rollback()
        assert await asyncio.wait_for(task, 10)
    async with factory.begin() as s:
        await _lock_budget_day(s, now.date().isoformat())
        assert await _shared_daily_budget_used(s, business_date=now.date()) == Decimal(252)
        assert await s.scalar(text("SELECT stopped FROM research_stage_scope")) is True


@pytest.mark.asyncio
async def test_late_verification_does_not_expand_frozen_financial_membership(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, p, ticket = await observed(factory)
    await p.cleanup(ticket, now=now, confirmed=True)
    assert await p.close(claim, now=now) is False
    async with factory.begin() as s:
        await s.execute(
            text("INSERT INTO research_stage_verification VALUES(:run,1,true)"),
            {"run": result.run_id},
        )
    async with factory() as s:
        assert await _shared_daily_budget_used(s, business_date=now.date()) == Decimal("3.2")
        run = await s.get(AgentResearchRunRecord, result.run_id)
        assert run.input_tokens is None and run.settled_cost_usd is None


@pytest.mark.asyncio
async def test_historical_unknown_suffix_cannot_hide_known_overrun_after_midnight(
    migrated_database_url, monkeypatch
):
    from unittest.mock import AsyncMock

    from nexus.agents.research_stages.proof import read_terminal_proof

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, p, ticket = await observed(factory, valid=True)
    # Construct the earlier protocol's possible state, before its new early
    # overrun fence. Current terminal reading and admission are never mocked.
    with monkeypatch.context() as historical:
        historical.setattr(p, "_fence_exceeded_usage", AsyncMock())
        # This historical producer also predates the shared cross-experiment
        # accounting check. Restore only that old construction path; terminal
        # reading and every admission assertion below use the real new guards.
        historical.setattr(
            "nexus.agents.comparison_concurrency.require_comparison_exclusive", AsyncMock()
        )
        assert await verify(p, ticket, now)
        await p.cleanup(ticket, now=now, confirmed=True)
        second = await p.authorize(claim, stage=2, now=now, wire=b"unknown suffix")
    await p.cleanup(second, now=now, confirmed=True)
    assert await p.close(claim, now=now) is False
    async with factory() as s:
        assert await _shared_daily_budget_used(s, business_date=now.date()) == Decimal(252)
        assert await _shared_daily_budget_used(s, business_date=now.date() + timedelta(days=1)) == 0
        for at in (now, now + timedelta(days=1)):
            with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                await read_terminal_proof(s, run_id=result.run_id, now=at)

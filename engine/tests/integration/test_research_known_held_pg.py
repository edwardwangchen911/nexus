"""Known held accounting is readable, never permission to resume or waive costs."""

from datetime import timedelta
from types import SimpleNamespace

import pytest
from integration.test_research_occupancy_pg import observed, verify
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.public import _finished, _selected_member_settled
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True])
async def test_known_held_close_readback_and_all_admission_modes(migrated_database_url, orphan):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, protocol, ticket = await observed(factory)
    assert await verify(protocol, ticket, now)
    await protocol.cleanup(ticket, now=now, confirmed=True)
    closed_at = claim.expires_at + timedelta(seconds=1) if orphan else now
    assert await protocol.close(claim, now=closed_at, orphan=orphan)
    from dataclasses import replace

    from nexus.agents.research_stages.protocol import ProtocolRejected

    with pytest.raises(ProtocolRejected, match="TERMINAL_READBACK_BINDING_INVALID"):
        await protocol.close(replace(claim, worker_id="wrong-worker"), now=closed_at, orphan=orphan)
    # Repeat is original-identity readback, never a second settlement attempt.
    assert await protocol.close(claim, now=closed_at + timedelta(seconds=1), orphan=orphan)
    async with factory() as session:
        proof = await read_terminal_proof(session, run_id=result.run_id, now=closed_at)
        assert proof.known and not proof.budget_settled and proof.cleanup_confirmed
        assert proof.orphan is orphan
        run = await session.get(AgentResearchRunRecord, result.run_id)
        assert run.status == "failed" and run.input_tokens == 20_000_000
        assert run.settled_cost_usd is None
        assert await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 1
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is True
        assert (
            await session.scalar(
                text("SELECT payload->>'reason' FROM agent_research_outputs WHERE run_id=:run"),
                {"run": run.id},
            )
            == "usage_accounting_failed"
        )
        assert not await _finished(session, run=run, now=closed_at)
        for at in (closed_at, closed_at + timedelta(days=1)):
            for allow_unknown in (False, True):
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await _selected_member_settled(
                        session,
                        owner_id=manifest.experiment_id,
                        kind="comparison",
                        member=SimpleNamespace(run_id=run.id, job_id=claim.job_id),
                        now=at,
                        allow_unknown_usage=allow_unknown,
                    )
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"), {"id": old}
            )
            == "reserved"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True])
@pytest.mark.parametrize("error_type", [RuntimeError, ValueError])
@pytest.mark.parametrize("tokens", [(20_000_000, 0), (10, 5)])
async def test_storage_failure_is_not_reported_as_a_known_closed_terminal(
    migrated_database_url, monkeypatch, orphan, error_type, tokens
):
    from nexus.agents import execution

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, protocol, ticket = await observed(factory, tokens=tokens)
    assert await verify(protocol, ticket, now, tokens=tokens)
    await protocol.cleanup(ticket, now=now, confirmed=True)

    async def broken_cost(*args, **kwargs):
        raise error_type("original storage failure")

    # Keep both the original overrun regression and a legal, unstopped scope.
    if tokens[0] == 20_000_000:
        monkeypatch.setattr(execution, "usage_cost", lambda *args: 0)
    async with factory() as session:
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is (
            tokens[0] == 20_000_000
        )
    monkeypatch.setattr(execution, "settle_daily_budget", broken_cost)
    with pytest.raises(error_type, match="original storage failure"):
        await protocol.close(
            claim, now=claim.expires_at + timedelta(seconds=1) if orphan else now, orphan=orphan
        )
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
        run = await session.get(AgentResearchRunRecord, result.run_id)
        assert run.settled_cost_usd is None and run.input_tokens is None
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is (
            tokens[0] == 20_000_000
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True])
@pytest.mark.parametrize("refused", [False, True])
async def test_zero_reported_usage_is_settled_or_explicitly_held_never_released(
    migrated_database_url, monkeypatch, orphan, refused
):
    from nexus.agents import execution
    from nexus.agents.budget import BudgetExhausted

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, protocol, ticket = await observed(factory, tokens=(0, 0))
    assert await verify(protocol, ticket, now, tokens=(0, 0))
    await protocol.cleanup(ticket, now=now, confirmed=True)
    async with factory() as session:
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is False

    async def refusal(*args, **kwargs):
        raise BudgetExhausted("deterministic accounting refusal")

    if refused:
        monkeypatch.setattr(execution, "settle_daily_budget", refusal)
    closed_at = claim.expires_at + timedelta(seconds=1) if orphan else now
    assert await protocol.close(claim, now=closed_at, orphan=orphan)
    async with factory() as session:
        proof = await read_terminal_proof(session, run_id=result.run_id, now=closed_at)
        assert proof.known and proof.budget_settled is (not refused)
        assert proof.cleanup_confirmed
        run = await session.get(AgentResearchRunRecord, result.run_id)
        assert (run.input_tokens, run.output_tokens) == (0, 0)
        assert await session.scalar(
            text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
            {"run": run.id},
        ) == ("reserved" if refused else "settled")
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is (
            refused or orphan
        )
        if refused:
            assert protocol.local_stopped


@pytest.mark.asyncio
async def test_known_held_accounting_blocks_all_new_budget_kinds_after_midnight(
    migrated_database_url,
):
    from decimal import Decimal
    from uuid import uuid4

    from nexus.agents.persistence import (
        BudgetExhausted,
        _shared_daily_budget_used,
        reserve_daily_budget,
        reserve_portfolio_decision_budget,
    )
    from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, protocol, ticket = await observed(factory)
    assert await verify(protocol, ticket, now)
    await protocol.cleanup(ticket, now=now, confirmed=True)
    assert await protocol.close(claim, now=now)
    tomorrow = now + timedelta(days=1)
    async with factory() as session:
        # Costs remain on their original day; historical eligibility is separate.
        assert await _shared_daily_budget_used(session, business_date=tomorrow.date()) == 0
    for reserve in (
        reserve_daily_budget,
        reserve_portfolio_decision_budget,
        reserve_portfolio_synthesis_budget,
    ):
        async with factory.begin() as session:
            with pytest.raises(BudgetExhausted, match="RESEARCH_ACCOUNTING_UNSETTLED"):
                await reserve(
                    session,
                    run_id=uuid4(),
                    now=tomorrow,
                    daily_limit_usd=Decimal(1000),
                    worst_case_usd=Decimal(".1"),
                    pricing_version="synthetic",
                    pricing_fresh=True,
                )

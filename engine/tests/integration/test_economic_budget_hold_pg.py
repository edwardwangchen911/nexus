"""Atomic whole-pair holds share the native budget, without model or order jobs."""

import asyncio
from decimal import Decimal

import pytest
from helpers.paper_api import settings
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import (
    _shared_daily_budget_used,
    reserve_daily_budget,
    reserve_portfolio_decision_budget,
)
from nexus.paper.economic_budget_hold import HoldRequest, hold_pair_budget
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text


@pytest.fixture
def budget_ledger_only_authority(monkeypatch):
    """Isolate ledger algebra; authority rejection is tested without this fixture."""
    from nexus.paper import economic_budget_hold

    async def already_qualified(*args, **kwargs):
        return None

    monkeypatch.setattr(economic_budget_hold, "_require_new_transfer_authority", already_qualified)


def request(key="diagnostic-pair", **changes):
    return HoldRequest.model_validate(
        dict(
            evaluation_key=key,
            protocol_hash="a" * 64,
            slot_ordinal=1,
            research_cost_usd="1",
            synthesis_cost_usd="1",
            global_limit_usd="10",
            uncovered_formal_usd="3",
        )
        | changes
    )


@pytest.mark.asyncio
async def test_whole_pair_hold_is_native_shared_usage_and_idempotent(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        first = await hold_pair_budget(session, request(), expected_database=actual)
    async with factory.begin() as session:
        second = await hold_pair_budget(session, request(), expected_database=actual)
        assert first.id == second.id
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
        assert await _shared_daily_budget_used(session, business_date=first.occurred_at.date()) == 6
        with pytest.raises(ValueError, match="ECONOMIC_HOLD_CONFLICT"):
            await hold_pair_budget(
                session, request(research_cost_usd="0.5"), expected_database=actual
            )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_transfer_uses_own_hold_with_multiple_priced_groups(
    migrated_database_url, budget_ledger_only_authority
):
    from uuid import uuid4

    from nexus.paper.economic_budget_hold import AllocationRequest
    from test_portfolio_decision_budget_pg import _seed_runs

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        first = await hold_pair_budget(
            session,
            request("first", global_limit_usd="30", uncovered_formal_usd="0"),
            expected_database=actual,
        )
        await hold_pair_budget(
            session,
            request(
                "second",
                global_limit_usd="30",
                uncovered_formal_usd="0",
                research_cost_usd="2",
                synthesis_cost_usd="2",
            ),
            expected_database=actual,
        )
    run_id = uuid4()
    await _seed_runs(factory, research_run_ids=(run_id,), decision_run_ids=())
    async with factory.begin() as session:
        await reserve_daily_budget(
            session,
            run_id=run_id,
            now=first.occurred_at,
            daily_limit_usd=Decimal(30),
            worst_case_usd=Decimal(1),
            pricing_version="pricing-v1",
            pricing_fresh=True,
            economic_allocation=AllocationRequest(
                hold_event_id=first.id, lane="challenger", action="research_2"
            ),
        )
        assert (
            await _shared_daily_budget_used(session, business_date=first.occurred_at.date()) == 18
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_transfer_failure_rolls_back_child_and_credit(
    migrated_database_url, budget_ledger_only_authority, monkeypatch
):
    from uuid import uuid4

    from nexus.paper.economic_budget_hold import TRANSFER_ACTION, AllocationRequest
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from test_portfolio_decision_budget_pg import _seed_runs

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        hold = await hold_pair_budget(
            session,
            request(global_limit_usd="6", uncovered_formal_usd="0"),
            expected_database=actual,
        )
    run_id = uuid4()
    await _seed_runs(factory, research_run_ids=(run_id,), decision_run_ids=())

    async def reject(*args, **kwargs):
        raise BudgetExhausted("injected post-write failure")

    with monkeypatch.context() as patch:
        patch.setattr("nexus.agents.persistence._shared_daily_budget_used", reject)
        async with factory.begin() as session:
            with pytest.raises(BudgetExhausted):
                await reserve_daily_budget(
                    session,
                    run_id=run_id,
                    now=hold.occurred_at,
                    daily_limit_usd=Decimal(6),
                    worst_case_usd=Decimal(1),
                    pricing_version="pricing-v1",
                    pricing_fresh=True,
                    economic_allocation=AllocationRequest(
                        hold_event_id=hold.id, lane="incumbent", action="research_1"
                    ),
                )
    async with factory.begin() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == TRANSFER_ACTION)
            )
            == 0
        )
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_native_research_transfer_never_double_counts_and_unknown_stays_held(
    migrated_database_url,
    budget_ledger_only_authority,
):
    from uuid import uuid4

    from nexus.paper.economic_budget_hold import AllocationRequest
    from test_portfolio_decision_budget_pg import _seed_runs

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        hold = await hold_pair_budget(
            session,
            request(global_limit_usd="6", uncovered_formal_usd="0"),
            expected_database=actual,
        )
    run_id = uuid4()
    await _seed_runs(factory, research_run_ids=(run_id,), decision_run_ids=())
    allocation = AllocationRequest(hold_event_id=hold.id, lane="incumbent", action="research_1")

    async def reserve_one():
        async with factory.begin() as session:
            return await reserve_daily_budget(
                session,
                run_id=run_id,
                now=hold.occurred_at,
                daily_limit_usd=Decimal(6),
                worst_case_usd=Decimal(1),
                pricing_version="pricing-v1",
                pricing_fresh=True,
                economic_allocation=allocation,
            )

    child, concurrent = await asyncio.gather(reserve_one(), reserve_one())
    assert child.id == concurrent.id
    async with factory.begin() as session:
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
    async with factory.begin() as session:
        repeated = await reserve_daily_budget(
            session,
            run_id=run_id,
            now=hold.occurred_at,
            daily_limit_usd=Decimal(6),
            worst_case_usd=Decimal(1),
            pricing_version="pricing-v1",
            pricing_fresh=True,
            economic_allocation=allocation,
        )
        assert repeated.id == child.id
        parent = await hold_pair_budget(
            session,
            request(global_limit_usd="6", uncovered_formal_usd="0"),
            expected_database=actual,
        )
        assert parent.id == hold.id
        with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_CONFLICT"):
            await reserve_daily_budget(
                session,
                run_id=uuid4(),
                now=hold.occurred_at,
                daily_limit_usd=Decimal(6),
                worst_case_usd=Decimal(1),
                pricing_version="pricing-v1",
                pricing_fresh=True,
                economic_allocation=allocation,
            )
    async with factory.begin() as session:
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError, match="settled Agent budget requires usage"):
            async with session.begin_nested():
                await session.execute(
                    text(
                        "UPDATE agent_daily_budget_reservations "
                        "SET state='settled',settled_usd=NULL WHERE id=:id"
                    ),
                    {"id": child.id},
                )
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_one_lane_capacity_cannot_create_half_hold(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        with pytest.raises(BudgetExhausted):
            await hold_pair_budget(session, request(global_limit_usd="8"), expected_database=actual)
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_pairs_cannot_overcommit_shared_limit(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))

    async def hold(key):
        async with factory.begin() as session:
            actual = await session.scalar(text("SELECT current_database()"))
            return await hold_pair_budget(
                session, request(key, uncovered_formal_usd="0"), expected_database=actual
            )

    results = await asyncio.gather(hold("pair-a"), hold("pair-b"), return_exceptions=True)
    assert sum(isinstance(result, BudgetExhausted) for result in results) == 1
    async with factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        assert await _shared_daily_budget_used(session, business_date=now.date()) == Decimal(6)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_wrong_actual_database_prevents_hold(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_DATABASE_MISMATCH"):
            await hold_pair_budget(
                session, request(), expected_database="nexus_diag_economic_protocol_wrong"
            )
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("reserve", [reserve_daily_budget, reserve_portfolio_decision_budget])
async def test_native_entry_cannot_spend_held_pair_capacity(migrated_database_url, reserve):
    from uuid import uuid4

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        hold = await hold_pair_budget(
            session,
            request(uncovered_formal_usd="0", global_limit_usd="6"),
            expected_database=actual,
        )
    async with factory.begin() as session:
        with pytest.raises(BudgetExhausted):
            await reserve(
                session,
                run_id=uuid4(),
                now=hold.occurred_at,
                daily_limit_usd=Decimal(6),
                worst_case_usd=Decimal("0.01"),
                pricing_version="test",
                pricing_fresh=True,
            )
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
    await factory.kw["bind"].dispose()


async def seed_synthesis_receiver(factory, *, pricing_version="pricing-v1", model_id=None):
    """Synthetic prepared budget receiver only; no qualified research or Job."""
    from uuid import uuid4

    from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord

    async with factory.begin() as session:
        await session.execute(text("SET LOCAL session_replication_role = replica"))
        now = await session.scalar(select(func.clock_timestamp()))
        run = AgentPortfolioSynthesisRunRecord(
            cycle_id=uuid4(),
            candidate_batch_id=uuid4(),
            research_output_ids=[uuid4()],
            projection_hash="a" * 64,
            prompt_version_id=uuid4(),
            model_config_id=model_id or uuid4(),
            prompt_hash="a" * 64,
            model_hash="a" * 64,
            input_hash="a" * 64,
            frozen_input={},
            idempotency_key=f"diagnostic-synthesis:{uuid4()}",
            status="prepared",
            pricing_version=pricing_version,
            decision_time=now,
        )
        session.add(run)
        await session.flush()
        return run.id


@pytest.mark.asyncio
async def test_synthesis_budget_transfer_is_concurrent_exact_once_and_unknown_stays_held(
    migrated_database_url,
    budget_ledger_only_authority,
):
    from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget
    from nexus.paper.economic_budget_hold import TRANSFER_ACTION, AllocationRequest
    from nexus.persistence.models.agents import AgentPortfolioSynthesisBudgetReservationRecord
    from nexus.persistence.models.runtime import Job

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        hold = await hold_pair_budget(
            session,
            request(global_limit_usd="6", uncovered_formal_usd="0"),
            expected_database=actual,
        )
    run_id = await seed_synthesis_receiver(factory)
    allocation = AllocationRequest(hold_event_id=hold.id, lane="challenger", action="synthesis")

    async def reserve():
        async with factory.begin() as session:
            return await reserve_portfolio_synthesis_budget(
                session,
                run_id=run_id,
                now=hold.occurred_at,
                daily_limit_usd=Decimal(6),
                worst_case_usd=Decimal(1),
                pricing_version="pricing-v1",
                pricing_fresh=True,
                economic_allocation=allocation,
            )

    async with factory.begin() as session:
        with pytest.raises(BudgetExhausted, match="stale pricing"):
            await reserve_portfolio_synthesis_budget(
                session,
                run_id=run_id,
                now=hold.occurred_at,
                daily_limit_usd=Decimal(6),
                worst_case_usd=Decimal(1),
                pricing_version="pricing-v1",
                pricing_fresh=False,
                economic_allocation=allocation,
            )
        with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RUN_INVALID"):
            await reserve_portfolio_synthesis_budget(
                session,
                run_id=run_id,
                now=hold.occurred_at,
                daily_limit_usd=Decimal(6),
                worst_case_usd=Decimal(1),
                pricing_version="pricing-v1",
                pricing_fresh=True,
                economic_allocation=allocation.model_copy(update={"action": "research_1"}),
            )
    first, second = await asyncio.gather(reserve(), reserve())
    assert first.id == second.id
    async with factory.begin() as session:
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == TRANSFER_ACTION)
            )
            == 1
        )
        child = await session.get(AgentPortfolioSynthesisBudgetReservationRecord, first.id)
        # Synthesis unknown usage remains reserved; its DB contract has no usage_unknown state.
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError, match="invalid synthesis budget transition"):
            async with session.begin_nested():
                child.state = "usage_unknown"
                await session.flush()
        await session.refresh(child)
        assert child.state == "reserved"
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
        with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_AMOUNT_MISMATCH"):
            await reserve_portfolio_synthesis_budget(
                session,
                run_id=run_id,
                now=hold.occurred_at,
                daily_limit_usd=Decimal(6),
                worst_case_usd=Decimal(2),
                pricing_version="pricing-v1",
                pricing_fresh=True,
                economic_allocation=allocation,
            )
        child.state = "settled"
        child.settled_usd = Decimal("0.4")
        await session.flush()
        # Parent residual 5 plus known native usage 0.4; transferred ceiling is not re-counted.
        assert await _shared_daily_budget_used(
            session, business_date=hold.occurred_at.date()
        ) == Decimal("5.4")
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_synthesis_transfer_failure_rolls_back_child_and_credit(
    migrated_database_url,
    budget_ledger_only_authority,
    monkeypatch,
):
    from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget
    from nexus.paper.economic_budget_hold import TRANSFER_ACTION, AllocationRequest
    from nexus.persistence.models.agents import AgentPortfolioSynthesisBudgetReservationRecord

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        hold = await hold_pair_budget(
            session,
            request(global_limit_usd="6", uncovered_formal_usd="0"),
            expected_database=actual,
        )
    run_id = await seed_synthesis_receiver(factory)

    async def reject(*args, **kwargs):
        raise BudgetExhausted("injected post-write failure")

    with monkeypatch.context() as patch:
        patch.setattr("nexus.agents.persistence._shared_daily_budget_used", reject)
        async with factory.begin() as session:
            with pytest.raises(BudgetExhausted):
                await reserve_portfolio_synthesis_budget(
                    session,
                    run_id=run_id,
                    now=hold.occurred_at,
                    daily_limit_usd=Decimal(6),
                    worst_case_usd=Decimal(1),
                    pricing_version="pricing-v1",
                    pricing_fresh=True,
                    economic_allocation=AllocationRequest(
                        hold_event_id=hold.id, lane="incumbent", action="synthesis"
                    ),
                )
    async with factory.begin() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentPortfolioSynthesisBudgetReservationRecord)
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == TRANSFER_ACTION)
            )
            == 0
        )
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_transfer_entry_rejects_future_protocol_slot_without_budget_writes(
    migrated_database_url, tmp_path, monkeypatch
):
    from types import SimpleNamespace
    from uuid import uuid4

    from nexus.ops import economic_budget_transfer as entry
    from nexus.paper.portfolio_economic_protocol import decode_protocol, register_protocol
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from test_portfolio_economic_protocol_pg import protocol_request

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        protocol_event = await register_protocol(
            session, protocol_request(), expected_database=actual
        )
        protocol = decode_protocol(protocol_event)
        hold = await hold_pair_budget(
            session,
            request(
                key=protocol.request.evaluation_key,
                protocol_hash=protocol.content_hash,
                synthesis_cost_usd="2",
                global_limit_usd="20",
                uncovered_formal_usd="0",
            ),
            expected_database=actual,
        )
        before = await session.scalar(select(func.count()).select_from(AuditEvent))
    import json

    input_path = tmp_path / "transfer.json"
    input_path.write_text(
        json.dumps(
            {
                "allocation": {
                    "hold_event_id": str(hold.id),
                    "lane": "incumbent",
                    "action": "research_1",
                },
                "run_id": str(uuid4()),
            }
        )
    )
    # The owned pytest database is supplied explicitly; no production config or connection.
    monkeypatch.setattr(entry, "read_database_url", lambda _: migrated_database_url)
    monkeypatch.setattr(
        entry, "local_settings", lambda *a, **k: SimpleNamespace(agent_daily_budget_usd=Decimal(20))
    )
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_SLOT_DAY_MISMATCH"):
        await entry.execute(
            repo_root=tmp_path,
            input_path=input_path,
            database_url_file=tmp_path / "isolated-url",
            expected_database=actual,
        )
    async with factory.begin() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == before
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 0
        )
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 8
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["research", "synthesis"])
@pytest.mark.parametrize("with_future_protocol", [False, True])
async def test_direct_native_transfer_cannot_bypass_protocol_slot_authority(
    migrated_database_url, kind, with_future_protocol
):
    from uuid import uuid4

    from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget
    from nexus.paper.economic_budget_hold import TRANSFER_ACTION, AllocationRequest
    from nexus.paper.portfolio_economic_protocol import decode_protocol, register_protocol
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentPortfolioSynthesisBudgetReservationRecord,
    )
    from test_portfolio_decision_budget_pg import _seed_runs
    from test_portfolio_economic_protocol_pg import protocol_request

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("select current_database()"))
        changes = {"global_limit_usd": "20", "uncovered_formal_usd": "0"}
        if with_future_protocol:
            protocol = decode_protocol(
                await register_protocol(session, protocol_request(), expected_database=actual)
            )
            changes.update(
                evaluation_key=protocol.request.evaluation_key,
                protocol_hash=protocol.content_hash,
                synthesis_cost_usd="2",
            )
        hold = await hold_pair_budget(session, request(**changes), expected_database=actual)
    if kind == "synthesis":
        run_id = await seed_synthesis_receiver(factory)
    else:
        run_id = uuid4()
        await _seed_runs(factory, research_run_ids=(run_id,), decision_run_ids=())
    reserve = reserve_portfolio_synthesis_budget if kind == "synthesis" else reserve_daily_budget
    code = (
        "ECONOMIC_TRANSFER_SLOT_DAY_MISMATCH"
        if with_future_protocol
        else "ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID"
    )
    async with factory.begin() as session:
        before = await _shared_daily_budget_used(session, business_date=hold.occurred_at.date())
        with pytest.raises(ValueError, match=code):
            await reserve(
                session,
                run_id=run_id,
                now=hold.occurred_at,
                daily_limit_usd=Decimal(20),
                worst_case_usd=Decimal(2)
                if kind == "synthesis" and with_future_protocol
                else Decimal(1),
                pricing_version="pricing-v1",
                pricing_fresh=True,
                economic_allocation=AllocationRequest(
                    hold_event_id=hold.id,
                    lane="challenger",
                    action="synthesis" if kind == "synthesis" else "research_1",
                ),
            )
        assert (
            await _shared_daily_budget_used(session, business_date=hold.occurred_at.date())
            == before
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == TRANSFER_ACTION)
            )
            == 0
        )
        for model in (
            AgentDailyBudgetReservationRecord,
            AgentPortfolioSynthesisBudgetReservationRecord,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["research", "synthesis"])
async def test_existing_native_receipt_readback_never_requalifies_or_releases(
    migrated_database_url, budget_ledger_only_authority, monkeypatch, kind
):
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget
    from nexus.paper import economic_budget_hold
    from nexus.paper.economic_budget_hold import AllocationRequest
    from test_portfolio_decision_budget_pg import _seed_runs

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("select current_database()"))
        hold = await hold_pair_budget(
            session,
            request(global_limit_usd="6", uncovered_formal_usd="0"),
            expected_database=actual,
        )
    if kind == "synthesis":
        run_id = await seed_synthesis_receiver(factory)
    else:
        run_id = uuid4()
        await _seed_runs(factory, research_run_ids=(run_id,), decision_run_ids=())
    reserve = reserve_portfolio_synthesis_budget if kind == "synthesis" else reserve_daily_budget
    kwargs = dict(
        run_id=run_id,
        now=hold.occurred_at,
        daily_limit_usd=Decimal(6),
        worst_case_usd=Decimal(1),
        pricing_version="pricing-v1",
        pricing_fresh=True,
        economic_allocation=AllocationRequest(
            hold_event_id=hold.id,
            lane="incumbent",
            action="synthesis" if kind == "synthesis" else "research_1",
        ),
    )
    async with factory.begin() as session:
        first = await reserve(session, **kwargs)
        before = await _shared_daily_budget_used(session, business_date=hold.occurred_at.date())
        count = await session.scalar(select(func.count()).select_from(AuditEvent))
    reject = AsyncMock(side_effect=AssertionError("new authority must not reinterpret old receipt"))
    monkeypatch.setattr(economic_budget_hold, "_require_new_transfer_authority", reject)
    async with factory.begin() as session:
        second = await reserve(session, **kwargs)
        assert first.id == second.id
        assert (
            await _shared_daily_budget_used(session, business_date=hold.occurred_at.date())
            == before
        )
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == count
    reject.assert_not_awaited()
    await factory.kw["bind"].dispose()

"""A manual root's snapshot must not drift to the later portfolio snapshot."""

from datetime import timedelta
from uuid import UUID

import pytest
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


async def _snapshot(session, portfolio, *, offset=0, held=True, status="READY"):
    at = NOW + timedelta(seconds=offset)
    sync = BrokerSyncRun(
        account_id=portfolio.account_id,
        portfolio_id=portfolio.id,
        trigger_kind="MANUAL",
        actor_id="synthetic",
        route="explicit-context",
        idempotency_key=str(offset),
        request_hash="c" * 64,
        status=status,
        accepted_at=at,
        completed_at=at,
        attempt_count=1,
        cash_flow_quality="PASS" if status == "READY" else "NOT_RUN",
        created_at=at,
    )
    session.add(sync)
    await session.flush()
    snapshot = PortfolioSnapshotRecord(
        portfolio_id=portfolio.id,
        sync_run_id=sync.id,
        status=status,
        as_of=at,
        position_hash_version="broker_position_hash_v1",
        position_hash="d" * 64,
        order_hash_version="broker_order_hash_v1",
        order_hash="e" * 64,
        source_request_ids=[],
        source_observations=[],
        created_at=at,
    )
    session.add(snapshot)
    await session.flush()
    session.add(
        CashSnapshot(
            portfolio_snapshot_id=snapshot.id,
            currency="USD",
            cash_balance=100,
            available_cash=100,
            provider_as_of=at,
            created_at=at,
        )
    )
    if held:
        session.add(
            PositionSnapshot(
                portfolio_snapshot_id=snapshot.id,
                instrument_id=INSTRUMENT,
                currency="USD",
                quantity=10,
                available_quantity=10,
                frozen_quantity=0,
                avg_cost=10,
                diluted_cost=10,
                current_price=10,
                provider_as_of=at,
                created_at=at,
            )
        )
    await session.flush()
    return snapshot


async def _seed(session):
    await seed_filing(session)
    account = Account(provider="fosun", status="ACTIVE", created_at=NOW)
    session.add(account)
    await session.flush()
    portfolio = Portfolio(account_id=account.id, name="synthetic", created_at=NOW)
    session.add(portfolio)
    await session.flush()
    return portfolio, await _snapshot(session, portfolio)


@pytest.mark.asyncio
async def test_explicit_binding_preserves_old_hash_and_does_not_choose_later_snapshot(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        portfolio, old = await _seed(session)
        original = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        explicit = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=NOW,
            portfolio_snapshot_id=old.id,
        )
        assert original is not None and explicit is not None
        assert original.id == explicit.id and original.revision == explicit.revision
        original_payload = dict(original.payload)
        latest = await _snapshot(session, portfolio, offset=1)
        later = NOW + timedelta(seconds=2)
        selected = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=later,
            portfolio_snapshot_id=old.id,
        )
        default = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        assert selected is not None and default is not None
        assert selected.payload["portfolio_context"]["portfolio_snapshot_id"] == str(old.id)
        assert default.payload["portfolio_context"]["portfolio_snapshot_id"] == str(latest.id)
        assert selected.revision != default.revision
        await session.refresh(original)
        assert original.payload == original_payload


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["missing", "future", "not_held"])
async def test_explicit_missing_context_does_not_fall_back_to_generic_research(
    migrated_database_url,
    case,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        portfolio, _ = await _seed(session)
        target_id = UUID(int=999)
        if case != "missing":
            target = await _snapshot(session, portfolio, offset=1, held=case != "not_held")
            target_id = target.id
        at = NOW if case == "future" else NOW + timedelta(seconds=2)
        with pytest.raises(ValueError, match="OPENAI_PORTFOLIO_CONTEXT_UNAVAILABLE"):
            await prepare_research_snapshot(
                session,
                INSTRUMENT,
                as_of=at,
                portfolio_snapshot_id=target_id,
            )
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0


@pytest.mark.asyncio
async def test_explicit_partial_snapshot_does_not_fall_back_to_ready(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        portfolio, _ = await _seed(session)
        partial = await _snapshot(session, portfolio, offset=1, status="PARTIAL")
        result = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=NOW + timedelta(seconds=2),
            portfolio_snapshot_id=partial.id,
        )
        assert result is not None
        context = result.payload["portfolio_context"]
        assert context["portfolio_snapshot_id"] == str(partial.id)
        assert context["snapshot_decision_ready"] is False
        assert context["execution_locked"] is True

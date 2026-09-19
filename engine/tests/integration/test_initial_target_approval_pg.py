import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from helpers.paper_batch import seed_paper_batch
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import approve_initial_target_projection
from nexus.persistence.models.paper import (
    PaperFillRecord,
    PaperInitialTargetApprovalRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


async def setup_approval(url):
    seed = await seed_paper_batch(url)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        projection = await load_paper_projection(
            session, account_id=seed.account_id, as_of=seed.decision_time
        )
    args = dict(
        account_id=seed.account_id,
        projection_as_of=projection.as_of,
        expected_projection_hash=projection.projection_hash,
        approved_by="local-user",
        approved_at=projection.as_of + timedelta(seconds=1),
        idempotency_key="approval-1",
    )
    return engine, sessions, args


@pytest.mark.asyncio
async def test_approval_exact_replay_concurrency_and_no_trading_side_effects(migrated_database_url):
    engine, sessions, args = await setup_approval(migrated_database_url)
    try:

        async def approve():
            async with sessions.begin() as session:
                return await approve_initial_target_projection(session, **args)

        results = await asyncio.gather(approve(), approve())
        assert results[0] == results[1]
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperInitialTargetApprovalRecord)
                )
                == 1
            )
            for model in (
                PaperTargetVersionRecord,
                PaperOrderBatchRecord,
                PaperOrderRecord,
                PaperFillRecord,
                PaperReservationRecord,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(PaperLedgerTransactionRecord))
                == 1
            )
        for patch in ({"idempotency_key": "another"}, {"expected_projection_hash": "a" * 64}):
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match="CONFLICT"):
                    await approve_initial_target_projection(session, **(args | patch))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["hash", "future", "unseen", "wrong_account"])
async def test_approval_rejects_wrong_snapshot_authority(migrated_database_url, kind):
    engine, sessions, args = await setup_approval(migrated_database_url)
    patch = {
        "hash": {"expected_projection_hash": "a" * 64},
        "future": {"projection_as_of": args["approved_at"] + timedelta(days=1)},
        "unseen": {"projection_as_of": args["projection_as_of"] - timedelta(days=1000)},
        "wrong_account": {"account_id": uuid4()},
    }[kind]
    try:
        async with sessions.begin() as session:
            with pytest.raises(ValueError):
                await approve_initial_target_projection(session, **(args | patch))
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperInitialTargetApprovalRecord)
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_approval_rejects_new_occupancy_and_replay_retains_old_approval(
    migrated_database_url,
    monkeypatch,
):
    from nexus.paper.service import create_paper_order_batch
    from nexus.persistence.models.paper import StrategyDecisionRecord

    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available", lambda: True
    )

    engine, sessions, args = await setup_approval(migrated_database_url)
    try:
        async with sessions.begin() as session:
            approved = await approve_initial_target_projection(session, **args)
        async with sessions.begin() as session:
            decision = await session.scalar(select(StrategyDecisionRecord))
            await create_paper_order_batch(
                session,
                account_id=args["account_id"],
                strategy_decision_id=decision.id,
                idempotency_key="test-occupancy",
                request_time=args["projection_as_of"] + timedelta(seconds=2),
            )
        async with sessions.begin() as session:
            assert await approve_initial_target_projection(session, **args) == approved
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_new_approval_rejects_current_open_order_not_only_historical_snapshot(
    migrated_database_url,
    monkeypatch,
):
    from nexus.paper.service import create_paper_order_batch
    from nexus.persistence.models.paper import StrategyDecisionRecord

    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available", lambda: True
    )

    engine, sessions, args = await setup_approval(migrated_database_url)
    try:
        async with sessions.begin() as session:
            decision = await session.scalar(select(StrategyDecisionRecord))
            at = args["projection_as_of"] + timedelta(seconds=2)
            await create_paper_order_batch(
                session,
                account_id=args["account_id"],
                strategy_decision_id=decision.id,
                idempotency_key="test-occupancy",
                request_time=at,
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="OCCUPIED|STALE"):
                await approve_initial_target_projection(session, **(args | {"approved_at": at}))
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperInitialTargetApprovalRecord)
                )
                == 0
            )
    finally:
        await engine.dispose()

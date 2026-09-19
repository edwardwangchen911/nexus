import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID, seed_paper_batch
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops import agent_paper_once
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import approve_initial_target_projection
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.paper import (
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleRecord,
    PaperFillRecord,
    PaperInitialTargetApprovalRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
)
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, literal, select
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


def test_normal_cli_parses_dispatches_and_persists_initial_portfolio_chain(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seeded_engine, _, authority, manifest = asyncio.run(
        seed_normal_path(migrated_database_url, monkeypatch)
    )
    monkeypatch.setattr(
        agent_paper_once,
        "load_local_environment",
        lambda *_args, **_kwargs: {
            "ALPACA_API_KEY_ID": "synthetic-test-key",
            "ALPACA_API_SECRET_KEY": "synthetic-test-secret",
            "DATABASE_URL": migrated_database_url,
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        },
    )

    as_of = authority.decision_time.isoformat()
    assert agent_paper_once.main(
        [
            "inspect-projection",
            "--account-id",
            str(authority.account_id),
            "--as-of",
            as_of,
        ]
    ) == 0
    projection = json.loads(capsys.readouterr().out)

    approval_args = [
        "approve-initial-target",
        "--account-id",
        str(authority.account_id),
        "--projection-as-of",
        as_of,
        "--projection-hash",
        projection["projection_hash"],
        "--approved-by",
        "local-user",
        "--approved-at",
        (authority.decision_time + timedelta(seconds=1)).isoformat(),
        "--idempotency-key",
        "cli-initial-target",
    ]
    assert agent_paper_once.main(approval_args) == 0
    approval = json.loads(capsys.readouterr().out)
    assert agent_paper_once.main(approval_args) == 0
    assert json.loads(capsys.readouterr().out) == approval

    candidate_args = [
        "prepare-candidates",
        "--account-id",
        str(authority.account_id),
        "--universe-snapshot-id",
        str(authority.universe_snapshot_id),
        "--instrument-id",
        str(manifest[0].instrument_id),
        "--instrument-id",
        str(manifest[1].instrument_id),
        "--idempotency-key",
        "cli-candidate-batch",
    ]
    monkeypatch.setattr(
        agent_paper_once,
        "func",
        SimpleNamespace(clock_timestamp=lambda: literal(authority.decision_time)),
    )
    assert agent_paper_once.main(candidate_args) == 0
    candidates = json.loads(capsys.readouterr().out)
    assert agent_paper_once.main(candidate_args) == 0
    assert json.loads(capsys.readouterr().out) == candidates

    async def add_next_calendar_session() -> None:
        calendar_engine = create_async_engine(migrated_database_url)
        sessions = async_sessionmaker(calendar_engine, expire_on_commit=False)
        next_day = authority.decision_time + timedelta(days=1)
        try:
            async with sessions.begin() as session:
                session.add(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=CALENDAR_ID,
                        session=next_day.date(),
                        market_open_at=next_day.replace(hour=13, minute=30),
                        market_close_at=next_day.replace(hour=20, minute=0),
                        decision_at=next_day.replace(hour=21, minute=0),
                        created_at=authority.decision_time,
                    )
                )
        finally:
            await calendar_engine.dispose()

    asyncio.run(add_next_calendar_session())

    async def use_seeded_authority(*_args, **_kwargs):
        return authority

    monkeypatch.setattr(agent_paper_once, "_validate_trigger_dependencies", lambda *_args: None)
    monkeypatch.setattr(
        agent_paper_once,
        "S3ProviderEvidenceStore",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        agent_paper_once,
        "prepare_schwab_decision_authority",
        use_seeded_authority,
    )
    trigger_args = [
        "trigger",
        "--account-id",
        str(authority.account_id),
        "--idempotency-key",
        "cli-portfolio-cycle",
        "--candidate-batch-id",
        candidates["batch"]["batch_id"],
    ]
    assert agent_paper_once.main(trigger_args) == 0
    cycle = json.loads(capsys.readouterr().out)
    assert cycle["status"] == "researching"
    assert len(cycle["research_run_ids"]) == 2
    assert agent_paper_once.main(trigger_args) == 0
    replay = json.loads(capsys.readouterr().out)
    assert replay["cycle_id"] == cycle["cycle_id"]
    assert replay["replayed"] is True

    async def persisted_counts() -> tuple[int, int, int, int, int]:
        engine = create_async_engine(migrated_database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return (
                    int(
                        await session.scalar(
                            select(func.count()).select_from(
                                PaperInitialTargetApprovalRecord
                            )
                        )
                        or 0
                    ),
                    int(
                        await session.scalar(
                            select(func.count()).select_from(
                                AgentPaperCandidateBatchRecord
                            )
                        )
                        or 0
                    ),
                    int(
                        await session.scalar(
                            select(func.count()).select_from(AgentPaperCycleRecord)
                        )
                        or 0
                    ),
                    int(
                        await session.scalar(
                            select(func.count()).select_from(AgentResearchRunRecord)
                        )
                        or 0
                    ),
                    int(
                        await session.scalar(select(func.count()).select_from(Job))
                        or 0
                    ),
                )
        finally:
            await engine.dispose()

    assert asyncio.run(persisted_counts()) == (1, 1, 1, 2, 2)
    asyncio.run(seeded_engine.dispose())


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

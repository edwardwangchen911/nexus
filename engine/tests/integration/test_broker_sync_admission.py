import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.persistence.models.portfolio import Account, BrokerSyncRequest, BrokerSyncRun, Portfolio
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.persistence.session import session_factory
from nexus.reconciliation.admission import TriggerKind
from nexus.reconciliation.persistence import (
    BrokerSyncRepository,
    IdempotencyConflict,
    SyncCooldownActive,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
ACCOUNT_ID = UUID("0198d400-1000-7000-8000-000000000001")
PORTFOLIO_ID = UUID("0198d400-1000-7000-8000-000000000002")


@pytest.mark.asyncio
async def test_admission_replays_same_body_and_rejects_conflict(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed(factory)
    async with factory() as session, session.begin():
        first = await _admit(session, "same-key", "a" * 64, "MANUAL", NOW)
    async with factory() as session, session.begin():
        replay = await _admit(session, "same-key", "a" * 64, "MANUAL", NOW)
        with pytest.raises(IdempotencyConflict):
            await _admit(session, "same-key", "b" * 64, "MANUAL", NOW)

    assert replay.replayed is True
    assert replay.run.id == first.run.id


@pytest.mark.asyncio
async def test_concurrent_admission_joins_one_account_run_and_audits_both_requests(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed(factory)

    async def admit(key: str):
        async with factory() as session, session.begin():
            return await _admit(session, key, key[-1] * 64, "AUTOMATIC", NOW)

    left, right = await asyncio.wait_for(
        asyncio.gather(admit("key-a"), admit("key-b")), timeout=5
    )
    async with factory() as session:
        active = await session.scalar(
            select(func.count()).select_from(BrokerSyncRun).where(BrokerSyncRun.status == "ACTIVE")
        )
        requests = await session.scalar(select(func.count()).select_from(BrokerSyncRequest))
        claims = await session.scalar(select(func.count()).select_from(IdempotencyRecord))

    assert left.run.id == right.run.id
    assert {left.joined_active, right.joined_active} == {False, True}
    assert (active, requests, claims) == (1, 2, 2)


@pytest.mark.asyncio
async def test_joined_manual_request_sets_exact_accepted_at_cooldown(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed(factory)
    async with factory() as session, session.begin():
        owner = await _admit(session, "owner", "c" * 64, "AUTOMATIC", NOW)
        joined = await _admit(
            session, "joined", "d" * 64, "MANUAL", NOW + timedelta(seconds=5)
        )
        owner.run.status = "READY"
        owner.run.completed_at = NOW + timedelta(seconds=6)
    assert joined.joined_active is True

    async with factory() as session, session.begin():
        with pytest.raises(SyncCooldownActive):
            await _admit(
                session,
                "too-soon",
                "e" * 64,
                "MANUAL",
                NOW + timedelta(seconds=64, milliseconds=999),
            )
    async with factory() as session, session.begin():
        boundary = await _admit(
            session, "boundary", "f" * 64, "MANUAL", NOW + timedelta(seconds=65)
        )
    assert boundary.run.status == "ACTIVE"


async def _admit(
    session,
    key: str,
    request_hash: str,
    trigger: TriggerKind,
    accepted_at: datetime,
):
    return await BrokerSyncRepository(session).admit(
        account_id=ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        trigger_kind=trigger,
        actor_id="synthetic-user",
        route="broker-sync",
        idempotency_key=key,
        request_hash=request_hash,
        accepted_at=accepted_at,
    )


async def _seed(factory) -> None:
    async with factory() as session:
        session.add(Account(id=ACCOUNT_ID, provider="fosun", status="ACTIVE"))
        session.add(Portfolio(id=PORTFOLIO_ID, account_id=ACCOUNT_ID, name="Broker"))
        await session.commit()

import asyncio

import pytest
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.persistence.session import session_factory
from nexus.reconciliation.admission_claim import claim_or_replay, validate_request_identity
from nexus.reconciliation.persistence_errors import (
    BrokerSyncPersistenceError,
    IdempotencyConflict,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_global_idempotency_claim_serializes_conflict_without_integrity_leak(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    owner_ready = asyncio.Event()
    release_owner = asyncio.Event()

    async def owner() -> None:
        async with factory() as session, session.begin():
            await claim_or_replay(
                session, "synthetic-user", "broker-sync", "shared-key", "a" * 64
            )
            owner_ready.set()
            await release_owner.wait()

    async def conflicting() -> None:
        await owner_ready.wait()
        async with factory() as session, session.begin():
            with pytest.raises(IdempotencyConflict):
                await claim_or_replay(
                    session, "synthetic-user", "broker-sync", "shared-key", "b" * 64
                )

    owner_task = asyncio.create_task(owner())
    conflict_task = asyncio.create_task(conflicting())
    await asyncio.wait_for(owner_ready.wait(), timeout=2)
    await asyncio.sleep(0.05)
    assert conflict_task.done() is False
    release_owner.set()
    await asyncio.wait_for(asyncio.gather(owner_task, conflict_task), timeout=5)

    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
    assert count == 1


@pytest.mark.parametrize(
    "identity",
    (("", "route", "key", "a" * 64), ("actor", "route", "key", "not-sha256")),
)
def test_claim_identity_is_validated_before_database_access(
    identity: tuple[str, str, str, str],
) -> None:
    with pytest.raises(BrokerSyncPersistenceError):
        validate_request_identity(*identity)

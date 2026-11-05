"""External-call checks reject all other client transactions, not server maintenance."""

import pytest
from integration.test_comparison_execution_pg import OPEN_CLIENT_TRANSACTIONS
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,state,expected",
    [
        ("autovacuum worker", None, 0),
        ("client backend", "idle in transaction", 1),
        ("client backend", "active", 1),
        ("client backend", "idle in transaction (aborted)", 1),
    ],
)
async def test_frozen_activity_classification(migrated_database_url, backend, state, expected):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as db:
            # Replay only the isolated observer's shape, never an old business request.
            await db.execute(
                text(
                    "CREATE TEMP TABLE pg_stat_activity(pid integer,datname name,"
                    "xact_start timestamptz,backend_type text,state text) ON COMMIT DROP"
                )
            )
            await db.execute(
                text(
                    "INSERT INTO pg_temp.pg_stat_activity VALUES("
                    "pg_backend_pid()+1,current_database(),clock_timestamp(),:backend,:state)"
                ),
                {"backend": backend, "state": state},
            )
            assert await db.scalar(text(OPEN_CLIENT_TRANSACTIONS)) == expected
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_another_real_client_transaction_still_rejected(migrated_database_url):
    holder = session_factory(EngineSettings(database_url=migrated_database_url))
    probe = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with holder() as held:
            await held.execute(text("SELECT 1"))
            async with probe() as db:
                assert await db.scalar(text(OPEN_CLIENT_TRANSACTIONS)) == 1
            await held.rollback()
            async with probe() as db:
                assert await db.scalar(text(OPEN_CLIENT_TRANSACTIONS)) == 0
    finally:
        await holder.kw["bind"].dispose()
        await probe.kw["bind"].dispose()

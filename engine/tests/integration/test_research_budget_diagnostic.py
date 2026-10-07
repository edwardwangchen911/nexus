import asyncio
from uuid import uuid4

import pytest
from nexus.ops.research_budget_diagnostic import require_isolated_case
from nexus.persistence.models.portfolio import Account
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def diagnostic_database_url(temporary_database_factory, migrated_database_template):
    name = f"nexus_diag_{uuid4().hex}"
    admin_url = temporary_database_factory.admin_url

    async def change_database(create: bool) -> None:
        engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                if create:
                    await connection.execute(
                        text(
                            f'CREATE DATABASE "{name}" TEMPLATE "{migrated_database_template.name}"'
                        )
                    )
                else:
                    await connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        finally:
            await engine.dispose()

    try:
        asyncio.run(change_database(True))
        url = admin_url.set(database=name).render_as_string(hide_password=False)

        async def remove_migration_seed() -> None:
            engine = create_async_engine(url)
            try:
                async with engine.begin() as connection:
                    # Only this owned ephemeral clone: migrations seed an internal Paper account.
                    await connection.execute(text("TRUNCATE accounts CASCADE"))
            finally:
                await engine.dispose()

        asyncio.run(remove_migration_seed())
        yield url
    finally:
        asyncio.run(change_database(False))


@pytest.mark.asyncio
async def test_actual_database_identity_is_checked_before_case_reads(migrated_database_url: str):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            # A local, migrated test database is still not the dedicated diagnostic database.
            with pytest.raises(ValueError, match="DIAGNOSTIC_DATABASE_REQUIRED"):
                await require_isolated_case(session, uuid4())
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_real_account_is_rejected_in_actual_diagnostic_database(diagnostic_database_url):
    factory = session_factory(EngineSettings(database_url=diagnostic_database_url))
    try:
        async with factory() as session, session.begin():
            session.add(Account(provider="fosun", status="ACTIVE"))
        async with factory() as session:
            with pytest.raises(ValueError, match="DIAGNOSTIC_SYNTHETIC_ACCOUNTS_REQUIRED"):
                await require_isolated_case(session, uuid4())
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_synthetic_account_does_not_substitute_for_fresh_case(diagnostic_database_url):
    factory = session_factory(EngineSettings(database_url=diagnostic_database_url))
    try:
        async with factory() as session, session.begin():
            session.add(Account(provider="diagnostic_synthetic", status="ACTIVE"))
        async with factory() as session:
            with pytest.raises(ValueError, match="DIAGNOSTIC_SINGLE_CASE_REQUIRED"):
                await require_isolated_case(session, uuid4())
    finally:
        await factory.kw["bind"].dispose()

"""Ephemeral PostgreSQL databases for destructive migration tests."""

import asyncio
import os
from collections.abc import Generator
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def temporary_database_url() -> Generator[str]:
    """Create and always drop an isolated database using injected admin connection data."""
    configured_url = os.environ.get("DATABASE_URL")
    if configured_url is None:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")

    database_name = f"nexus_test_{uuid4().hex}"
    database_url = make_url(configured_url).set(database=database_name)
    asyncio.run(_create_database(make_url(configured_url).set(database="postgres"), database_name))
    try:
        yield database_url.render_as_string(hide_password=False)
    finally:
        asyncio.run(
            _drop_database(make_url(configured_url).set(database="postgres"), database_name)
        )


@pytest.fixture
def migrated_database_url(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> Generator[str]:
    """Provide an isolated database upgraded to the current Alembic head."""
    engine_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(Config(os.path.join(engine_root, "alembic.ini")), "head")
    yield temporary_database_url


async def _create_database(admin_url: URL, database_name: str) -> None:
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    finally:
        await engine.dispose()


async def _drop_database(admin_url: URL, database_name: str) -> None:
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
    finally:
        await engine.dispose()

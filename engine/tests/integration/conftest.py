"""Ephemeral PostgreSQL databases for destructive migration tests."""

import asyncio
import os
import re
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine

_TEMPORARY_DATABASE_NAME = re.compile(r"^nexus_test_[0-9a-f]{32}$")


@dataclass(frozen=True)
class TemporaryDatabase:
    """Connection details for one isolated test database."""

    name: str
    url: str = field(repr=False)


@dataclass(frozen=True)
class TemporaryDatabaseFactory:
    """Build isolated PostgreSQL databases from process-injected admin connection data."""

    admin_url: URL

    @contextmanager
    def provision(
        self,
        after_create: Callable[[TemporaryDatabase], None] | None = None,
    ) -> Generator[TemporaryDatabase]:
        """Create, expose, and remove an isolated database."""
        database_name = f"nexus_test_{uuid4().hex}"
        database_url = self.admin_url.set(database=database_name)
        temporary_database = TemporaryDatabase(
            name=database_name,
            url=database_url.render_as_string(hide_password=False),
        )
        try:
            asyncio.run(_create_database(self.admin_url, database_name))
            if after_create is not None:
                after_create(temporary_database)
            yield temporary_database
        finally:
            asyncio.run(_drop_database(self.admin_url, database_name))

    def exists(self, database_name: str) -> bool:
        """Read the real PostgreSQL catalog for an isolated database name."""
        return asyncio.run(_database_exists(self.admin_url, database_name))

    def drop(self, temporary_database: TemporaryDatabase) -> None:
        """Explicit test cleanup for failure-path assertions."""
        if not isinstance(temporary_database, TemporaryDatabase):
            raise TypeError("temporary_database must be a TemporaryDatabase")
        asyncio.run(_drop_database(self.admin_url, temporary_database.name))


@pytest.fixture
def temporary_database_factory() -> TemporaryDatabaseFactory:
    """Create an isolated-database factory from the process environment only."""
    configured_url = os.environ.get("DATABASE_URL")
    if configured_url is None:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")
    return TemporaryDatabaseFactory(make_url(configured_url).set(database="postgres"))


@pytest.fixture
def temporary_database_url(temporary_database_factory: TemporaryDatabaseFactory) -> Generator[str]:
    """Create and always drop an isolated database using injected admin connection data."""
    with temporary_database_factory.provision() as temporary_database:
        yield temporary_database.url


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
    _require_temporary_database_name(database_name)
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    finally:
        await engine.dispose()


async def _drop_database(admin_url: URL, database_name: str) -> None:
    _require_temporary_database_name(database_name)
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            )
    finally:
        await engine.dispose()


def _require_temporary_database_name(database_name: str) -> None:
    """Limit destructive test database operations to UUID-derived temporary names."""
    if _TEMPORARY_DATABASE_NAME.fullmatch(database_name) is None:
        raise ValueError("invalid temporary database name")


async def _database_exists(admin_url: URL, database_name: str) -> bool:
    engine = create_async_engine(admin_url)
    try:
        async with engine.connect() as connection:
            return bool(
                await connection.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_database "
                        "WHERE datname = :database_name)"
                    ),
                    {"database_name": database_name},
                )
            )
    finally:
        await engine.dispose()

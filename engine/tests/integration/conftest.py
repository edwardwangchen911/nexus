"""Ephemeral PostgreSQL databases for destructive migration tests."""

import asyncio
import os
import re
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

_TEMPORARY_DATABASE_NAME = re.compile(r"^nexus_test_[0-9a-f]{32}$")
_FORCE_DROP_ATTEMPTS = 20
_FORCE_DROP_RETRY_SECONDS = 0.05
_POST_MIGRATION_TEST_CLOCK = datetime(2026, 1, 1, tzinfo=UTC)


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
        *,
        template: TemporaryDatabase | None = None,
    ) -> Generator[TemporaryDatabase]:
        """Create, expose, and remove an isolated database."""
        if template is not None and not isinstance(template, TemporaryDatabase):
            raise TypeError("template must be a TemporaryDatabase")
        database_name = f"nexus_test_{uuid4().hex}"
        database_url = self.admin_url.set(database=database_name)
        temporary_database = TemporaryDatabase(
            name=database_name,
            url=database_url.render_as_string(hide_password=False),
        )
        try:
            asyncio.run(
                _create_database(
                    self.admin_url,
                    database_name,
                    template.name if template is not None else None,
                )
            )
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


@pytest.fixture(scope="session")
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
    temporary_database_factory: TemporaryDatabaseFactory,
    migrated_database_template: TemporaryDatabase,
) -> Generator[str]:
    """Clone an isolated database already upgraded to the current Alembic head."""
    with temporary_database_factory.provision(
        template=migrated_database_template
    ) as temporary_database:
        monkeypatch.setenv("DATABASE_URL", temporary_database.url)
        yield temporary_database.url


@pytest.fixture(scope="session")
def migrated_database_template(
    temporary_database_factory: TemporaryDatabaseFactory,
) -> Generator[TemporaryDatabase]:
    """Migrate and freeze one template database for fast per-test clones."""

    def prepare_template(temporary_database: TemporaryDatabase) -> None:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv("DATABASE_URL", temporary_database.url)
            engine_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            command.upgrade(Config(os.path.join(engine_root, "alembic.ini")), "head")
        _freeze_post_migration_created_at_defaults(temporary_database.url)

    with temporary_database_factory.provision(
        after_create=prepare_template
    ) as temporary_database:
        yield temporary_database


def _freeze_post_migration_created_at_defaults(database_url: str) -> None:
    """Make migration-seeded and test-created immutable rows available to fixed clocks."""
    asyncio.run(_freeze_post_migration_created_at_defaults_async(database_url))


async def _freeze_post_migration_created_at_defaults_async(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "DO $$ DECLARE table_record record; BEGIN "
                    "FOR table_record IN "
                    "SELECT table_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND column_name = 'created_at' "
                    "LOOP EXECUTE format("
                    "'ALTER TABLE %I ALTER COLUMN created_at SET DEFAULT "
                    f"''{_POST_MIGRATION_TEST_CLOCK.isoformat()}''::timestamptz', "
                    "table_record.table_name); END LOOP; END $$"
                )
            )
            await connection.execute(
                text(
                    "DO $$ DECLARE table_record record; BEGIN "
                    "FOR table_record IN "
                    "SELECT table_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND column_name = 'created_at' "
                    "LOOP EXECUTE format('ALTER TABLE %I DISABLE TRIGGER USER', "
                    "table_record.table_name); EXECUTE format('UPDATE %I SET created_at = "
                    f"''{_POST_MIGRATION_TEST_CLOCK.isoformat()}''::timestamptz', "
                    "table_record.table_name); EXECUTE format("
                    "'ALTER TABLE %I ENABLE TRIGGER USER', "
                    "table_record.table_name); END LOOP; END $$"
                )
            )
    finally:
        await engine.dispose()


async def _create_database(
    admin_url: URL,
    database_name: str,
    template_database_name: str | None = None,
) -> None:
    _require_temporary_database_name(database_name)
    if template_database_name is not None:
        _require_temporary_database_name(template_database_name)
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            statement = f'CREATE DATABASE "{database_name}"'
            if template_database_name is not None:
                statement += f' TEMPLATE "{template_database_name}"'
            await connection.execute(text(statement))
    finally:
        await engine.dispose()


async def _drop_database(admin_url: URL, database_name: str) -> None:
    _require_temporary_database_name(database_name)
    statement = text(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
    for attempt in range(_FORCE_DROP_ATTEMPTS):
        engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                await connection.execute(statement)
            return
        except DBAPIError as error:
            if not _is_transient_protected_backend(error) or attempt == _FORCE_DROP_ATTEMPTS - 1:
                raise
        finally:
            await engine.dispose()
        await asyncio.sleep(_FORCE_DROP_RETRY_SECONDS)


def _is_transient_protected_backend(error: DBAPIError) -> bool:
    """Recognize only FORCE-drop collisions with PostgreSQL protected workers."""
    first_line = str(error.orig).splitlines()[0]
    reason = first_line.rsplit(": ", 1)[-1]
    return (
        getattr(error.orig, "sqlstate", None) == "42501"
        and reason == "permission denied to terminate process"
    )


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
                        "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = :database_name)"
                    ),
                    {"database_name": database_name},
                )
            )
    finally:
        await engine.dispose()

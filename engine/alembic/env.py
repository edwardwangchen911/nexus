"""Alembic environment configured for the engine's async PostgreSQL driver."""

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from nexus.persistence.base import Base, UTCDateTime
from nexus.persistence.models import (  # noqa: F401
    agents,
    data,
    diagnostics,
    market,
    portfolio,
    runtime,
    shadow,
)
from nexus.settings import EngineSettings
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def render_item(type_: str, obj: object, autogen_context: object) -> str | bool:
    """Render the UTC decorator as portable native PostgreSQL timestamp DDL."""
    del autogen_context
    if type_ == "type" and isinstance(obj, UTCDateTime):
        return "sa.DateTime(timezone=True)"
    return False


def database_url() -> str:
    """Use a process-injected test/production URL or safe non-production settings."""
    return os.environ.get("DATABASE_URL") or EngineSettings().database_url or ""


def run_migrations_offline() -> None:
    """Generate SQL without opening a database connection."""
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_item=render_item,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run migrations against a synchronous facade over an async connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_item=render_item,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations with the project's asyncpg database URL."""
    config.set_main_option("sqlalchemy.url", database_url())
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    try:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())

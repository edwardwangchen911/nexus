"""Asynchronous PostgreSQL session construction."""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.settings import EngineSettings


def session_factory(settings: EngineSettings) -> async_sessionmaker[AsyncSession]:
    """Create sessions bound to the configured PostgreSQL database."""
    if settings.database_url is None:
        raise ValueError("database_url is required")
    engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
    )
    return async_sessionmaker(engine, expire_on_commit=False)

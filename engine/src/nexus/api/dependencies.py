"""FastAPI dependencies that keep authentication and business writes transactional."""

from collections.abc import AsyncIterator
from typing import cast

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.api.auth import VerifiedServiceActor, verify_hmac_v1
from nexus.settings import EngineSettings


def get_settings(request: Request) -> EngineSettings:
    return cast(EngineSettings, request.app.state.settings)


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)


async def transactional_session(
    factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> AsyncIterator[AsyncSession]:
    """Yield one outer transaction so authenticated nonces commit with business work."""
    async with factory() as session:
        async with session.begin():
            yield session


async def require_service_actor(
    request: Request,
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
) -> VerifiedServiceActor:
    return await verify_hmac_v1(request, await request.body(), session, settings)

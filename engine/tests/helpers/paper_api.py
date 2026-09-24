"""Authenticated HTTP fixtures for the internal-paper Engine API."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from helpers.paper_performance import MARKET_CLOSE, PaperPerformanceSeed, seed_paper_performance
from nexus.main import create_engine_app
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from uuid6 import uuid7


def settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def app(
    database_url: str,
    *,
    now: datetime = MARKET_CLOSE,
    release_revision: str | None = None,
):
    runtime_settings = settings(database_url).model_copy(
        update={"release_revision": release_revision}
    )
    engine_app = create_engine_app(runtime_settings)
    engine_app.state.utc_clock = lambda: now
    return engine_app


def headers(method: str, path: str, body: bytes = b"") -> dict[str, str]:
    result = build_test_hmac_headers(method, path, body, nonce=str(uuid7()))
    if body:
        result["Content-Type"] = "application/json"
    return result


async def seed_ready_paper_api(database_url: str) -> PaperPerformanceSeed:
    seed = await seed_paper_performance(database_url)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        with patch(
            "nexus.paper.performance.current_paper_performance_price_authority_available",
            return_value=True,
        ):
            await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )
    return seed

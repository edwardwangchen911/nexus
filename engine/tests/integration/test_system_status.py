import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.main import create_engine_app
from nexus.settings import EngineSettings


@pytest.mark.asyncio
async def test_internal_system_status_commits_a_valid_nonce_and_rejects_replay(
    migrated_database_url: str,
) -> None:
    """The authenticated fixture endpoint makes its nonce durable with its response."""
    settings = EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=migrated_database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )
    app = create_engine_app(settings)
    headers = build_test_hmac_headers("GET", "/internal/v1/system-status", b"")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.get("/internal/v1/system-status", headers=headers)
        replay = await client.get("/internal/v1/system-status", headers=headers)

    assert response.status_code == 200
    assert response.json()["data"] == {"status": "ok"}
    assert replay.status_code == 401
    assert replay.json()["error"]["code"] == "UNAUTHENTICATED"

import json

import httpx
import pytest
from helpers.portfolio_api import PORTFOLIOS, app, headers, seed_portfolio_list
from nexus.api.routes.diagnostics import router as diagnostics_router


@pytest.mark.asyncio
async def test_latest_diagnostic_is_redacted_and_returns_persisted_item_shape(
    migrated_database_url: str,
) -> None:
    """Replacing the stored projection with client-side calculation must fail this API contract."""
    await seed_portfolio_list(migrated_database_url)
    assert diagnostics_router is not None
    path = f"/internal/v1/portfolios/{PORTFOLIOS[0]}/diagnostics/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 404
    assert "account_id" not in json.dumps(response.json())

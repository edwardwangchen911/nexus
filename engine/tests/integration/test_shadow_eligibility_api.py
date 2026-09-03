"""Current start eligibility agrees with the immutable price-purpose boundary."""

import httpx
import nexus.api.routes.optimization as optimization_api
import nexus.shadow.start as shadow_start
import pytest
from helpers.portfolio_api import app, headers, settings
from helpers.shadow import seed_shadow_eligible_group
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.shadow import ShadowGroupRecord, ShadowPortfolioRecord
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


def _start_headers(path: str) -> dict[str, str]:
    return headers("POST", path, b'{"version":1}') | {
        "Idempotency-Key": "shadow-eligibility-start",
        "If-Match": '"1"',
    }


@pytest.mark.asyncio
async def test_current_price_purpose_blocks_get_and_post_without_new_shadow(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    group_path = f"/internal/v1/optimization-proposals/{seed.group_id}"
    path = f"{group_path}/shadow"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(group_path, headers=headers("GET", group_path))
        rejected = await client.post(
            path, content=b'{"version":1}', headers=_start_headers(path)
        )
    assert response.status_code == 200
    assert response.json()["data"]["shadow_eligibility"] == {
        "eligible": False,
        "reasons": ["SHADOW_PRICE_AUTHORITY_UNAVAILABLE"],
    }
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "PROPOSAL_NOT_EXECUTABLE"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, seed.group_id)
        assert group is not None and group.status == "compared" and group.version == 1
        assert await session.scalar(select(func.count()).select_from(ShadowGroupRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(ShadowPortfolioRecord)) == 0


@pytest.mark.asyncio
async def test_saved_shadow_and_start_replay_survive_current_purpose_closure(
    migrated_database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    group_path = f"/internal/v1/optimization-proposals/{seed.group_id}"
    path = f"{group_path}/shadow"
    # Historical contract is enabled only inside this isolated synthetic test.
    with monkeypatch.context() as legacy:
        for module in (optimization_api, shadow_start):
            legacy.setattr(
                module, "current_proposal_shadow_price_authority_available", lambda _: True
            )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url)),
            base_url="http://testserver",
        ) as client:
            eligible = await client.get(group_path, headers=headers("GET", group_path))
            started = await client.post(
                path, content=b'{"version":1}', headers=_start_headers(path)
            )
        assert eligible.status_code == started.status_code == 200
        assert eligible.json()["data"]["shadow_eligibility"] == {"eligible": True, "reasons": []}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        current = await client.get(group_path, headers=headers("GET", group_path))
        saved = await client.get(path, headers=headers("GET", path))
        replay = await client.post(
            path, content=b'{"version":1}', headers=_start_headers(path)
        )
    assert current.status_code == saved.status_code == replay.status_code == 200
    assert not current.json()["data"]["shadow_eligibility"]["eligible"]
    assert replay.json()["data"] == started.json()["data"]
    assert saved.json()["data"]["shadow_group_id"] == started.json()["data"]["shadow_group_id"]

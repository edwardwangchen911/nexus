"""Current start eligibility agrees with the immutable price-purpose boundary."""

import asyncio
from datetime import timedelta
from uuid import UUID

import httpx
import nexus.api.routes.optimization as optimization_api
import nexus.shadow.start as shadow_start
import pytest
from helpers.broker_snapshot import _snapshot, _sync_result
from helpers.portfolio_api import ACCOUNTS, PORTFOLIOS, app, headers, settings
from helpers.shadow import seed_shadow_eligible_group
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.portfolio import BrokerSyncRun, PortfolioSnapshotRecord
from nexus.persistence.models.shadow import ShadowGroupRecord, ShadowPortfolioRecord
from nexus.persistence.session import session_factory
from nexus.reconciliation.persistence import BrokerSyncRepository
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


@pytest.mark.asyncio
async def test_newer_ready_portfolio_snapshot_blocks_direct_shadow_start(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calling POST directly must not bypass a superseded proposal baseline."""
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        run = BrokerSyncRun(
            id=UUID("0198d500-1000-7000-8000-000000000071"),
            account_id=ACCOUNTS[0],
            portfolio_id=PORTFOLIOS[0],
            trigger_kind="MANUAL",
            actor_id="newer-shadow-baseline",
            route="broker-sync",
            idempotency_key="newer-shadow-baseline",
            request_hash="8" * 64,
            status="READY",
            accepted_at=seed.database_time,
            completed_at=seed.database_time + timedelta(seconds=1),
            attempt_count=1,
            failure_reason=None,
            cash_flow_quality="PASS",
        )
        snapshot = PortfolioSnapshotRecord(
            id=UUID("0198d500-1000-7000-8000-000000000072"),
            portfolio_id=PORTFOLIOS[0],
            sync_run_id=run.id,
            stale_from_snapshot_id=None,
            status="READY",
            as_of=seed.database_time,
            position_hash_version="broker_position_hash_v1",
            position_hash="8" * 64,
            order_hash_version="broker_order_hash_v1",
            order_hash="9" * 64,
            source_request_ids=[],
            source_observations=[],
        )
        session.add_all((run, snapshot))

    monkeypatch.setattr(
        optimization_api,
        "current_proposal_shadow_price_authority_available",
        lambda _: True,
    )
    monkeypatch.setattr(
        shadow_start,
        "current_proposal_shadow_price_authority_available",
        lambda _: True,
    )
    path = f"/internal/v1/optimization-proposals/{seed.group_id}/shadow"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            path,
            content=b'{"version":1}',
            headers=_start_headers(path),
        )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "PROPOSAL_NOT_EXECUTABLE"
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, seed.group_id)
        shadows = await session.scalar(select(func.count()).select_from(ShadowGroupRecord))
    assert group is not None and group.status == "compared" and group.version == 1
    assert shadows == 0


@pytest.mark.asyncio
async def test_concurrent_ready_snapshot_finalization_blocks_stale_shadow_start(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot finalization and Shadow admission must serialize per portfolio."""
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    run_id = UUID("0198d500-1000-7000-8000-000000000081")
    snapshot_id = UUID("0198d500-1000-7000-8000-000000000082")
    async with factory() as session, session.begin():
        session.add(
            BrokerSyncRun(
                id=run_id,
                account_id=ACCOUNTS[0],
                portfolio_id=PORTFOLIOS[0],
                trigger_kind="MANUAL",
                actor_id="concurrent-shadow-baseline",
                route="broker-sync",
                idempotency_key="concurrent-shadow-baseline",
                request_hash="a" * 64,
                status="ACTIVE",
                accepted_at=seed.database_time,
                completed_at=None,
                attempt_count=0,
                failure_reason=None,
                cash_flow_quality="NOT_RUN",
            )
        )
    writer = factory()
    await writer.begin()
    response_task: asyncio.Task[httpx.Response] | None = None
    try:
        snapshot = _snapshot(
            UUID("0198d500-1000-7000-8000-000000000001"), snapshot_id, "READY"
        ).model_copy(update={"portfolio_id": PORTFOLIOS[0]})
        await BrokerSyncRepository(writer).finalize(
            _sync_result(run_id, snapshot), completed_at=seed.database_time
        )

        monkeypatch.setattr(
            optimization_api,
            "current_proposal_shadow_price_authority_available",
            lambda _: True,
        )
        monkeypatch.setattr(
            shadow_start,
            "current_proposal_shadow_price_authority_available",
            lambda _: True,
        )
        path = f"/internal/v1/optimization-proposals/{seed.group_id}/shadow"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url)),
            base_url="http://testserver",
        ) as client:
            response_task = asyncio.create_task(
                client.post(path, content=b'{"version":1}', headers=_start_headers(path))
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(response_task), timeout=0.25)
            await writer.commit()
            response = await asyncio.wait_for(response_task, timeout=5)
    finally:
        if writer.in_transaction():
            await writer.rollback()
        await writer.close()
        if response_task is not None and not response_task.done():
            response_task.cancel()

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "PROPOSAL_NOT_EXECUTABLE"
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, seed.group_id)
        shadows = await session.scalar(select(func.count()).select_from(ShadowGroupRecord))
    assert group is not None and group.status == "compared" and group.version == 1
    assert shadows == 0

"""The production entry must construct a first scenario without seeded admission flags."""

import json
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import CLOCK, app, headers, settings
from integration.test_initial_proposal_draft import _post, _seed
from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


async def _construct(client, data, *, key="first-construction", **changes):
    path = "/internal/v1/optimization-proposals"
    body = json.dumps(
        {
            "portfolio_snapshot_id": data["baseline_portfolio_snapshot_id"],
            "data_snapshot_id": data["data_snapshot_id"],
            "cost_policy_id": data["cost_policy_id"],
            **changes,
        }
    ).encode()
    signed = headers("POST", path, body)
    signed["Idempotency-Key"] = key
    return await client.post(path, content=body, headers=signed)


@pytest.mark.asyncio
async def test_first_proposal_api_worker_builds_without_seeded_admission(
    migrated_database_url: str,
) -> None:
    baseline, diagnostic = await _seed(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://test",
    ) as client:
        draft = await _post(client, baseline, diagnostic)
        assert draft.status_code == 201
        data = draft.json()["data"]
        response = await _construct(client, data)
        assert response.status_code == 202, response.text
        replay = await _construct(client, data)
        assert replay.json()["data"]["job_id"] == response.json()["data"]["job_id"]

    from nexus.optimizer.jobs import build_optimization_registry

    assert await run_job_worker_once(
        factory,
        registry=build_optimization_registry(),
        queue=QueueName.GENERAL,
        worker_id="proposal-production-test",
        worker_started_at=CLOCK,
        process_version="test",
        migration_head="test",
        clock=lambda: CLOCK,
        claim_registered_only=True,
    )
    async with factory() as session:
        job = await session.get(Job, UUID(response.json()["data"]["job_id"]))
        assert job is not None and job.status == "succeeded"
        assert job.result_code == "ANALYSIS_PROPOSAL_BUILT"
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
        assert (
            await session.scalar(
                select(func.count()).select_from(OptimizationAdmissionAuthorityRecord)
            )
            == 0
        )
        report = await session.get(PortfolioDiagnosticReportRecord, diagnostic)
        assert report is not None and report.optimization_need == "RESEARCH_REQUIRED"
        groups = tuple(await session.scalars(select(OptimizationProposalGroupRecord)))
        assert len(groups) == 2
        assert all(g.portfolio_scope == "real_read_only" and g.partial for g in groups)
        assert all(not g.cost_complete and not g.diagnostic_complete for g in groups)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["snapshot", "data", "cost", "future_index"])
async def test_construction_rejects_mismatched_or_future_inputs_without_job(
    migrated_database_url: str,
    invalid: str,
) -> None:
    baseline, diagnostic = await _seed(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://test",
    ) as client:
        draft = await _post(client, baseline, diagnostic)
        data = draft.json()["data"]
    now = CLOCK - timedelta(seconds=1) if invalid == "future_index" else CLOCK
    changes = (
        {}
        if invalid == "future_index"
        else {
            {
                "snapshot": "portfolio_snapshot_id",
                "data": "data_snapshot_id",
                "cost": "cost_policy_id",
            }[invalid]: str(UUID(int=999999))
        }
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=now)),
        base_url="http://test",
    ) as client:
        response = await _construct(client, data, **changes)
        assert response.status_code == 409
    async with session_factory(settings(migrated_database_url))() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert (
            await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord))
            == 1
        )

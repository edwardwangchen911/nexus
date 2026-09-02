"""Real DB metadata and hashed artifacts feed proposal metrics without external calls."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.portfolio_api import CLOCK, headers, settings
from integration.test_diagnostic_assembly import SPY_ID, _add_series, _seed_spy
from integration.test_initial_proposal_draft import _post, _seed
from integration.test_proposal_construction_path import _construct
from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.main import create_engine_app
from nexus.persistence.models.market import SectorExposure
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.worker import _general_registry


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["", "future", "unreviewed", "future_prices"])
async def test_draft_api_recomputes_risk_from_db_and_rejects_unqualified_sectors(
    migrated_database_url: str,
    invalid: str,
) -> None:
    baseline, diagnostic = await _seed(migrated_database_url)
    payloads: dict[str, bytes] = {}
    async with session_factory(settings(migrated_database_url))() as db, db.begin():
        await _seed_spy(db)
        await _add_series(db, payloads, SPY_ID, "benchmark")
        for index in range(5):
            instrument = UUID(int=801 + index)
            await _add_series(
                db,
                payloads,
                instrument,
                f"asset-{index}",
                available_at=CLOCK if invalid == "future_prices" else None,
            )
            db.add(
                SectorExposure(
                    instrument_id=instrument,
                    sector_code="technology",
                    weight=Decimal(1),
                    as_of=(CLOCK - timedelta(days=1)).date(),
                    source_url="fixture://sector",
                    content_hash="a" * 64,
                    synthetic=False,
                    reviewed=invalid != "unreviewed",
                    created_at=CLOCK if invalid == "future" else CLOCK - timedelta(days=1),
                )
            )
    reader = FixtureArtifactReader(payloads)
    engine_app = create_engine_app(
        settings(migrated_database_url), diagnostic_artifact_reader=reader,
    )
    engine_app.state.utc_clock = lambda: CLOCK
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app),
        base_url="http://test",
    ) as client:
        response = await _post(client, baseline, diagnostic)
        assert response.status_code == 201, response.text
        data = response.json()["data"]
        hold = data["proposals"][0]["comparison"]["metrics"]["dimensions"]
        minimum = data["proposals"][1]["comparison"]["metrics"]["dimensions"]
        if invalid == "future_prices":
            assert hold["volatility"]["status"] == minimum["drawdown"]["status"] == "unknown"
        else:
            assert hold["volatility"]["status"] == "known"
            assert minimum["drawdown"]["status"] == "known"
            assert Decimal(minimum["drawdown"]["value"]) < Decimal(hold["drawdown"]["value"])
        assert minimum["cost"]["status"] == "unknown"
        assert minimum["sector_exposure"]["status"] == (
            "unknown" if invalid in {"future", "unreviewed"} else "known"
        )
        if not invalid:
            assert Decimal(minimum["sector_exposure"]["value"]) < Decimal("1")
        assert not data["shadow_eligibility"]["eligible"]
        assert all(not p["comparison"]["metrics"]["ranking_eligible"] for p in data["proposals"])
        path = f"/internal/v1/optimization-proposals/{data['proposal_group_id']}"
        replay = await client.get(path, headers=headers("GET", path))
        assert replay.json()["data"] == data
        if not invalid:
            admitted = await _construct(client, data)
            assert admitted.status_code == 202
            factory = session_factory(settings(migrated_database_url))
            assert await run_job_worker_once(
                factory,
                registry=_general_registry(
                    settings(migrated_database_url),
                    responses_client=None,
                    tools=None,
                    artifact_reader=reader,
                ),
                queue=QueueName.GENERAL,
                worker_id="evidenced-proposal",
                worker_started_at=CLOCK,
                process_version="test",
                migration_head="test",
                clock=lambda: CLOCK,
                claim_registered_only=True,
            )
            async with factory() as db:
                job = await db.get(Job, UUID(admitted.json()["data"]["job_id"]))
                assert job is not None and job.status == "succeeded"

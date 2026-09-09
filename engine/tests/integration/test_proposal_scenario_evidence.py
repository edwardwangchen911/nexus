"""Real DB metadata and hashed artifacts feed proposal metrics without external calls."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.optimization import complete_comparison_metrics
from helpers.optimization_api import seed_readable_group
from helpers.portfolio_api import CLOCK, headers, seed_portfolio_list, settings
from integration.test_diagnostic_assembly import SPY_ID, _add_series, _seed_spy
from integration.test_initial_proposal_draft import _post, _seed
from integration.test_proposal_construction_path import _construct
from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.main import create_engine_app
from nexus.persistence.models.market import SectorExposure
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.worker import _general_registry
from sqlalchemy import select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "",
        "future",
        "unreviewed",
        "future_prices",
        "short64",
        "raw",
        "non_equity_without_sector",
        "rounded_etf_sector_total",
    ],
)
async def test_draft_api_recomputes_risk_from_db_and_rejects_unqualified_sectors(
    migrated_database_url: str,
    invalid: str,
) -> None:
    baseline, diagnostic = await _seed(migrated_database_url, invalid)
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
                session_count=64 if invalid == "short64" else 253,
                adjustment="raw" if invalid == "raw" else "all",
            )
            if invalid == "non_equity_without_sector" and index == 4:
                continue
            if invalid == "rounded_etf_sector_total" and index == 4:
                for sector, weight in (
                    ("financials", "0.400"),
                    ("technology", "0.599"),
                ):
                    db.add(
                        SectorExposure(
                            instrument_id=instrument,
                            sector_code=sector,
                            weight=Decimal(weight),
                            as_of=(CLOCK - timedelta(days=1)).date(),
                            source_url="https://issuer.example/rounded-etf.csv",
                            content_hash="b" * 64,
                            synthetic=False,
                            reviewed=True,
                            created_at=CLOCK - timedelta(days=1),
                        )
                    )
                continue
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
        settings(migrated_database_url),
        diagnostic_artifact_reader=reader,
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
        if invalid in {"future_prices", "raw"}:
            assert hold["volatility"]["status"] == minimum["drawdown"]["status"] == "unknown"
        elif invalid == "short64":
            assert hold["volatility"]["status"] == minimum["volatility"]["status"] == "known"
            assert hold["drawdown"]["status"] == minimum["drawdown"]["status"] == "unknown"
            assert hold["correlation"]["status"] == minimum["correlation"]["status"] == "unknown"
        else:
            assert hold["volatility"]["status"] == "known"
            assert minimum["drawdown"]["status"] == "known"
            assert Decimal(minimum["drawdown"]["value"]) < Decimal(hold["drawdown"]["value"])
        assert minimum["cost"]["status"] == "unknown"
        assert minimum["sector_exposure"]["status"] == (
            "unknown" if invalid in {"future", "unreviewed"} else "known"
        )
        if invalid == "non_equity_without_sector":
            assert Decimal(hold["sector_exposure"]["value"]) == Decimal("0.9")
        if not invalid:
            assert Decimal(minimum["sector_exposure"]["value"]) < Decimal("1")
        assert not data["shadow_eligibility"]["eligible"]
        assert all(not p["comparison"]["metrics"]["ranking_eligible"] for p in data["proposals"])
        path = f"/internal/v1/optimization-proposals/{data['proposal_group_id']}"
        replay = await client.get(path, headers=headers("GET", path))
        assert replay.json()["data"] == data
        if invalid in {"", "short64"}:
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
                constructed_id = await db.scalar(
                    select(OptimizationProposalGroupRecord.id).where(
                        OptimizationProposalGroupRecord.id != UUID(data["proposal_group_id"])
                    )
                )
            assert constructed_id is not None
            constructed_path = f"/internal/v1/optimization-proposals/{constructed_id}"
            constructed = await client.get(
                constructed_path, headers=headers("GET", constructed_path)
            )
            assert constructed.status_code == 200
            built = constructed.json()["data"]
            built_hold = built["proposals"][0]["comparison"]["metrics"]["dimensions"]
            assert built_hold["volatility"] == hold["volatility"]
            assert built_hold["drawdown"]["status"] == (
                "unknown" if invalid == "short64" else "known"
            )
            assert built_hold["cost"]["status"] == "unknown"
            assert not built["shadow_eligibility"]["eligible"]
            assert built["economic_comparison"]["status"] == "incomparable"


@pytest.mark.asyncio
async def test_old_persisted_risk_dimensions_are_read_without_recalculation(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    # A historical 253-anchor result remains a frozen fact after the algorithm changes.
    old_values = {
        "volatility": "1.509360544191451267546519725",
        "drawdown": "0.25",
        "correlation": "0.765432109876543210",
    }

    def legacy_metrics(**kwargs):
        metrics = complete_comparison_metrics(**kwargs)
        for name, value in old_values.items():
            metrics["dimensions"][name] = {"status": "known", "value": value, "reason": None}
        return metrics

    # Supply old values before INSERT; persisted comparisons remain immutable.
    monkeypatch.setattr("helpers.optimization_api.complete_comparison_metrics", legacy_metrics)
    async with factory() as db, db.begin():
        group_id, _ = await seed_readable_group(db)

    def must_not_recalculate(*args, **kwargs):
        raise AssertionError("historical reads must not recalculate scenario risk")

    monkeypatch.setattr("nexus.optimizer.scenario_risk.scenario_metrics", must_not_recalculate)
    engine_app = create_engine_app(settings(migrated_database_url))
    engine_app.state.utc_clock = lambda: CLOCK
    path = f"/internal/v1/optimization-proposals/{group_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    for proposal in response.json()["data"]["proposals"]:
        dimensions = proposal["comparison"]["metrics"]["dimensions"]
        assert {name: dimensions[name]["value"] for name in old_values} == old_values

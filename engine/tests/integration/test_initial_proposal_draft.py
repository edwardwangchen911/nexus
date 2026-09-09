"""A first analysis draft needs no previous proposal or optimization admission."""

import json
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.initial_draft import seed_initial_draft as _seed
from helpers.portfolio_api import CLOCK, app, headers, settings
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
)
from nexus.persistence.models.portfolio import (
    CashSnapshot,
    PositionSnapshot,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


async def _post(client, baseline: UUID, diagnostic: UUID):
    path = "/internal/v1/optimization-proposal-drafts"
    body = json.dumps(
        {"portfolio_snapshot_id": str(baseline), "diagnostic_report_id": str(diagnostic)}
    ).encode()
    signed = headers("POST", path, body)
    signed["Idempotency-Key"] = "initial-draft"
    return await client.post(path, content=body, headers=signed)


@pytest.mark.asyncio
async def test_first_draft_is_created_readable_and_replayed_without_formal_admission(
    migrated_database_url: str,
) -> None:
    baseline, diagnostic = await _seed(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://test",
    ) as client:
        response = await _post(client, baseline, diagnostic)
        assert response.status_code == 201, response.text
        data = response.json()["data"]
        assert data["status"] == "draft"
        assert data["economic_comparison"] == {
            "status": "incomparable",
            "recommendation": None,
            "reasons": ["COST_EVIDENCE_INCOMPLETE", "COMPARISON_EVIDENCE_INCOMPLETE"],
        }
        assert data["baseline_portfolio_snapshot_id"] == str(baseline)
        assert all(p["comparison"]["non_executable"] for p in data["proposals"])
        assert all(not p["comparison"]["metrics"]["ranking_eligible"] for p in data["proposals"])
        minimum = data["proposals"][1]
        assert any(
            Decimal(p["target_weight"]) < Decimal(p["current_weight"]) for p in minimum["positions"]
        )
        target_weights = [Decimal(p["target_weight"]) for p in minimum["positions"]]
        assert max(target_weights) <= Decimal("0.30") * sum(target_weights)
        replay = await _post(client, baseline, diagnostic)
        assert replay.json()["data"]["proposal_group_id"] == data["proposal_group_id"]
        path = f"/internal/v1/optimization-proposals/{data['proposal_group_id']}"
        read = await client.get(path, headers=headers("GET", path))
        assert read.status_code == 200
        assert read.json()["data"] == data
        shadow_path = path + "/shadow"
        shadow_body = b'{"version":0}'
        signed = headers("POST", shadow_path, shadow_body)
        signed.update({"Idempotency-Key": "draft-shadow-forbidden", "If-Match": '"0"'})
        shadow = await client.post(shadow_path, content=shadow_body, headers=signed)
        assert shadow.status_code == 409
    async with session_factory(settings(migrated_database_url))() as db:
        assert (
            await db.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord)) == 1
        )
        assert (
            await db.scalar(select(func.count()).select_from(OptimizationAdmissionAuthorityRecord))
            == 0
        )
        assert await db.scalar(select(func.count()).select_from(Job)) == 0
        report = await db.get(PortfolioDiagnosticReportRecord, diagnostic)
        assert report.optimization_need == "RESEARCH_REQUIRED"
        group = await db.get(OptimizationProposalGroupRecord, UUID(data["proposal_group_id"]))
        frozen = await db.get(DataSnapshot, group.data_snapshot_id)
        assert frozen.available_at == CLOCK > group.valuation_time
        from nexus.agents.proposal_context import read_comparison_for_freeze

        comparison = await read_comparison_for_freeze(
            db,
            portfolio_snapshot_id=baseline,
            diagnostic_report_id=diagnostic,
            diagnostic_input_hash=report.diagnostic_input_hash,
            valuation_time=group.valuation_time,
            as_of=CLOCK,
        )
        assert comparison is not None and comparison.partial
        assert all(variant.non_executable for variant in comparison.variants)
        assert (
            await read_comparison_for_freeze(
                db,
                portfolio_snapshot_id=baseline,
                diagnostic_report_id=diagnostic,
                diagnostic_input_hash=report.diagnostic_input_hash,
                valuation_time=group.valuation_time,
                as_of=CLOCK - timedelta(seconds=1),
            )
            is None
        )


@pytest.mark.asyncio
async def test_zero_quantity_history_is_retained_but_does_not_block_initial_draft(
    migrated_database_url: str,
) -> None:
    baseline, diagnostic = await _seed(migrated_database_url, "zero_quantity")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://test",
    ) as client:
        response = await _post(client, baseline, diagnostic)
        assert response.status_code == 201, response.text
        proposals = response.json()["data"]["proposals"]
        assert all(len(proposal["positions"]) == 4 for proposal in proposals)
        assert all(
            str(UUID(int=805))
            not in {position["instrument_id"] for position in proposal["positions"]}
            for proposal in proposals
        )

    async with session_factory(settings(migrated_database_url))() as db:
        positions = tuple(
            await db.scalars(
                select(PositionSnapshot).where(PositionSnapshot.portfolio_snapshot_id == baseline)
            )
        )
        assert len(positions) == 5
        assert sum(position.quantity == 0 for position in positions) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "cash_flow",
        "mixed_currency",
        "foreign_cash",
        "insufficient_names",
        "missing_price",
        "missing_liquidity",
        "future_evidence",
        "blocked_report",
        "negative_quantity",
    ],
)
async def test_initial_draft_refuses_unproved_baseline_without_writing(
    migrated_database_url: str,
    invalid: str,
) -> None:
    baseline, diagnostic = await _seed(migrated_database_url, invalid)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://test",
    ) as client:
        response = await _post(client, baseline, diagnostic)
        assert response.status_code == 409
        if invalid == "foreign_cash":
            assert response.json()["error"]["details"]["reason"] == "DRAFT_FX_VALUATION_UNAVAILABLE"
    async with session_factory(settings(migrated_database_url))() as db:
        assert (
            await db.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord)) == 0
        )
        assert await db.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        assert await db.scalar(select(func.count()).select_from(Job)) == 0


@pytest.mark.asyncio
async def test_hkd_cash_uses_received_historical_fx_and_stays_non_executable(
    migrated_database_url: str,
) -> None:
    from hashlib import sha256

    from nexus.research.fx import import_dexhkus_vintage

    class Store:
        async def put_if_absent(self, *, key: str, content: bytes) -> str:
            return f"memory://sha256/{sha256(content).hexdigest()}"

    baseline, diagnostic = await _seed(migrated_database_url, "foreign_cash")
    async with session_factory(settings(migrated_database_url))() as db, db.begin():
        await import_dexhkus_vintage(
            db,
            payload=b"observation_date,DEXHKUS_20260820\n2026-08-20,7.8500\n",
            url="https://alfred.stlouisfed.org/graph/alfredgraph.csv?id=DEXHKUS&cosd=2026-08-20&coed=2026-08-20&vintage_date=2026-08-20",
            received_at=CLOCK,
            store=Store(),
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://test",
    ) as client:
        response = await _post(client, baseline, diagnostic)
        assert response.status_code == 201, response.text
        data = response.json()["data"]
        assert all(p["comparison"]["requires_fx"] for p in data["proposals"])
        assert all(p["comparison"]["non_executable"] for p in data["proposals"])
        hold = data["proposals"][0]
        weights = [Decimal(p["current_weight"]) for p in hold["positions"]]
        expected_security_weight = Decimal(100) / (Decimal(100) + Decimal(1) / Decimal("7.85"))
        assert abs(sum(weights) - expected_security_weight) < Decimal("1e-11")
    async with session_factory(settings(migrated_database_url))() as db:
        cash = await db.scalar(
            select(CashSnapshot).where(CashSnapshot.portfolio_snapshot_id == baseline)
        )
        assert cash.currency == "HKD" and cash.cash_balance == 1
        group = await db.get(OptimizationProposalGroupRecord, UUID(data["proposal_group_id"]))
        assert group.requires_fx
        inputs = await db.get(DataSnapshot, group.data_snapshot_id)
        assert inputs.payload["fx_valuation"]["rate"] == "7.850000000000"
        assert inputs.payload["fx_valuation"]["received_at"] == CLOCK.isoformat()

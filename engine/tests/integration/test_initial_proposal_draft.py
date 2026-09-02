"""A first analysis draft needs no previous proposal or optimization admission."""

import json
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import CLOCK, app, headers, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
)
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


async def _seed(database_url: str, invalid: str = "") -> tuple[UUID, UUID]:
    at = CLOCK - timedelta(minutes=1)
    async with session_factory(settings(database_url))() as db, db.begin():
        account = Account(provider="fosun", status="ACTIVE")
        db.add(account)
        await db.flush()
        portfolio = Portfolio(account_id=account.id, name="private-draft-canary")
        db.add(portfolio)
        await db.flush()
        sync = BrokerSyncRun(
            account_id=account.id,
            portfolio_id=portfolio.id,
            trigger_kind="MANUAL",
            actor_id="test",
            route="test",
            idempotency_key="seed",
            request_hash="a" * 64,
            status="READY",
            accepted_at=at,
            completed_at=at,
            attempt_count=1,
            cash_flow_quality="NOT_RUN" if invalid == "cash_flow" else "PASS",
        )
        db.add(sync)
        await db.flush()
        snapshot = PortfolioSnapshotRecord(
            portfolio_id=portfolio.id,
            sync_run_id=sync.id,
            status="READY",
            as_of=at,
            position_hash_version="broker_position_hash_v1",
            position_hash="b" * 64,
            order_hash_version="broker_order_hash_v1",
            order_hash="c" * 64,
            source_request_ids=[],
            source_observations=[],
        )
        db.add(snapshot)
        await db.flush()
        db.add(
            CashSnapshot(
                portfolio_snapshot_id=snapshot.id,
                currency="HKD" if invalid == "foreign_cash" else "USD",
                cash_balance=1 if invalid == "foreign_cash" else 0,
                available_cash=0,
                provider_as_of=at,
            )
        )
        instrument_ids = tuple(
            UUID(int=801 + index)
            for index in range(2 if invalid == "insufficient_names" else 5)
        )
        for index, instrument_id in enumerate(instrument_ids):
            currency = "HKD" if invalid == "mixed_currency" and index == 1 else "USD"
            quantity = 60 if index == 0 else 40 if invalid == "insufficient_names" else 10
            db.add(
                Instrument(
                    id=instrument_id,
                    asset_type="equity",
                    exchange="XNAS",
                    currency=currency,
                    tick_size=Decimal("0.01"),
                    risk_bucket="us_equity",
                )
            )
            await db.flush()
            db.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=f"TEST{index}",
                    alias_type="ticker",
                    valid_from=at.date(),
                )
            )
            db.add(
                PositionSnapshot(
                    portfolio_snapshot_id=snapshot.id,
                    instrument_id=instrument_id,
                    currency=currency,
                    quantity=quantity,
                    available_quantity=quantity,
                    frozen_quantity=0,
                    current_price=None if invalid == "missing_price" else 1,
                    provider_as_of=at,
                )
            )
        report = PortfolioDiagnosticReportRecord(
            portfolio_id=portfolio.id,
            portfolio_snapshot_id=snapshot.id,
            snapshot_as_of=at,
            trading_day=at.date(),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="BLOCKED" if invalid == "blocked_report" else "RESEARCH_REQUIRED",
            diagnostic_input_hash="d" * 64,
        )
        db.add(report)
        await db.flush()
        metrics = [("CONCENTRATION_TOP1", "0.60", "0.30", "ratio", "critical", "reduce_only")]
        if invalid != "missing_liquidity":
            metrics += [
                (f"LIQUIDITY_DAYS_TO_EXIT:{i}", "1", "2", "trading_days", "info", "monitor")
                for i in instrument_ids
            ]
        for code, value, limit, unit, severity, action in metrics:
            item = PortfolioDiagnosticItemRecord(
                report_id=report.id,
                diagnostic_code=code,
                metric_value=Decimal(value),
                unit=unit,
                reference_limit=Decimal(limit),
                policy_scope="currency:USD",
                severity=severity,
                actionability=action,
            )
            db.add(item)
            await db.flush()
            db.add(
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=item.id,
                    source="fixture",
                    object_id=code,
                    available_at=CLOCK if invalid == "future_evidence" else at,
                    location=f"fixture://{item.id}",
                    content_hash="e" * 64,
                )
            )
        return snapshot.id, report.id


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

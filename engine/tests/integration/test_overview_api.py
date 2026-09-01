"""Read-only overview API contracts backed by persisted diagnostics."""

from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import PORTFOLIOS, app, headers, seed_portfolio_list, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.session import session_factory
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_overview_selects_latest_persisted_report_and_projects_headroom(
    migrated_database_url: str,
) -> None:
    """A browser must receive server-projected threshold headroom, never calculate it."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        older = await _seed_report(
            session,
            portfolio_id=PORTFOLIOS[0],
            snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
            as_of=datetime(2026, 8, 23, 3, 59, tzinfo=UTC),
        )
        latest = await _seed_report(
            session,
            portfolio_id=PORTFOLIOS[2],
            snapshot_id=UUID("0198d500-1000-7000-8000-000000000042"),
            as_of=datetime(2026, 8, 24, 3, 59, tzinfo=UTC),
        )

    path = "/internal/v1/overview"
    request_headers = headers("GET", path)
    engine_app = app(migrated_database_url)
    operation = engine_app.openapi()["paths"][path]["get"]
    assert operation["operationId"] == "getOverview"
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/OverviewEnvelope"
    }
    assert operation["responses"]["404"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/NexusErrorEnvelope"
    }
    assert operation["responses"]["409"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/NexusErrorEnvelope"
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=request_headers)
        filtered_path = f"{path}?portfolio_id={PORTFOLIOS[0]}&session_date=2026-08-23"
        filtered = await client.get(
            filtered_path, headers=headers("GET", filtered_path)
        )
        missing_path = f"{path}?portfolio_id={PORTFOLIOS[1]}"
        missing = await client.get(missing_path, headers=headers("GET", missing_path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["portfolio_id"] == str(PORTFOLIOS[2])
    assert data["portfolio_snapshot_id"] == str(latest.portfolio_snapshot_id)
    assert data["snapshot_as_of"] == "2026-08-24T03:59:00Z"
    assert data["trading_day"] == "2026-08-24"
    assert data["proposal_group"] is None
    assert data["proposal_admission"] is None
    assert data["proposal_admission_reason"] == "OPTIMIZATION_AUTHORITY_MISSING"
    assert data["diagnostic_unavailable_reason"] is None
    items = {item["diagnostic_code"]: item for item in data["diagnostic_report"]["items"]}
    assert items["CONCENTRATION_TOP1"] == {
        "diagnostic_code": "CONCENTRATION_TOP1",
        "metric_value": "0.400000000000",
        "unit": "ratio",
        "reference_limit": "0.300000000000",
        "headroom": "-0.100000000000",
        "headroom_reason": None,
        "policy_scope": "currency:USD",
        "severity": "critical",
        "actionability": "reduce_only",
        "unknown_reason": None,
        "evidence_refs": items["CONCENTRATION_TOP1"]["evidence_refs"],
    }
    assert items["CASH_RATIO"]["headroom"] == "0.100000000000"
    assert items["CASH_RATIO"]["headroom_reason"] is None
    assert items["LIQUIDITY_DAYS_TO_EXIT"]["headroom"] is None
    assert items["LIQUIDITY_DAYS_TO_EXIT"]["headroom_reason"] == "ADV20_MISSING"
    assert filtered.status_code == 200
    assert filtered.json()["data"]["portfolio_snapshot_id"] == str(older.portfolio_snapshot_id)
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_overview_returns_fosun_portfolio_facts_when_no_diagnostic_exists(
    migrated_database_url: str,
) -> None:
    """Removing diagnostics must not hide the latest persisted read-only broker facts."""
    await seed_portfolio_list(migrated_database_url)
    path = f"/internal/v1/overview?portfolio_id={PORTFOLIOS[0]}"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    envelope = response.json()
    data = envelope["data"]
    assert data["overview_mode"] == "PORTFOLIO_FACTS_ONLY"
    assert data["portfolio_id"] == str(PORTFOLIOS[0])
    assert data["portfolio_name"] == "Alpha"
    assert data["provider"] == "fosun"
    assert data["trading_day"] is None
    assert data["diagnostic_report"] is None
    assert data["proposal_group"] is None
    assert data["proposal_admission"] is None
    assert data["proposal_admission_reason"] is None
    assert data["diagnostic_unavailable_reason"] == "DIAGNOSTIC_NOT_AVAILABLE"
    assert data["portfolio_snapshot"] == {
        "portfolio_snapshot_id": "0198d500-1000-7000-8000-000000000041",
        "portfolio_id": str(PORTFOLIOS[0]),
        "coverage_scope": "broker_account",
        "status": "READY",
        "cash": [
            {
                "currency": "USD",
                "cash_balance": "1000.000000000000",
                "available_cash": "900.000000000000",
                "provider_as_of": "2026-08-24T03:59:00Z",
            }
        ],
        "positions": [
            {
                "instrument_id": "0198d500-1000-7000-8000-000000000001",
                "symbol": "00656.HK",
                "exchange": "XHKG",
                "currency": "HKD",
                "quantity": "90000.000000000000",
                "available_quantity": "90000.000000000000",
                "frozen_quantity": "0.000000000000",
                "avg_cost": "5.250000000000",
                "diluted_cost": "4.950000000000",
                "current_price": "6.125000000000",
                "provider_as_of": "2026-08-24T03:59:00Z",
                "read_only": True,
            }
        ],
        "open_orders": [
            {
                "broker_order_snapshot_id": "0198d500-1000-7000-8000-000000000002",
                "instrument_id": "0198d500-1000-7000-8000-000000000003",
                "symbol": "00700.HK",
                "currency": "HKD",
                "side": "sell",
                "quantity": "1000.000000000000",
                "filled_quantity": "0.000000000000",
                "price": "6.500000000000",
                "status": "submitted",
                "updated_at": None,
                "read_only": True,
            }
        ],
        "converted_view": None,
        "as_of": "2026-08-24T03:59:00Z",
    }
    assert data["portfolio_snapshot_id"] == data["portfolio_snapshot"]["portfolio_snapshot_id"]
    assert data["snapshot_as_of"] == data["portfolio_snapshot"]["as_of"]
    assert envelope["meta"]["as_of"] == data["snapshot_as_of"]
    assert "provider_order_id" not in response.text
    assert "source_observations" not in response.text


@pytest.mark.asyncio
async def test_overview_names_snapshot_admission_as_the_only_missing_condition(
    migrated_database_url: str,
) -> None:
    """A non-ready frozen snapshot must stay facts-only with one closed reason."""
    await seed_portfolio_list(migrated_database_url)
    path = f"/internal/v1/overview?portfolio_id={PORTFOLIOS[2]}"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["overview_mode"] == "PORTFOLIO_FACTS_ONLY"
    assert data["portfolio_snapshot"]["status"] == "PARTIAL"
    assert data["diagnostic_unavailable_reason"] == "SNAPSHOT_NOT_READY"
    assert data["diagnostic_report"] is None
    assert data["proposal_group"] is None
    assert data["proposal_admission"] is None


@pytest.mark.asyncio
async def test_overview_reports_persisted_policy_conflict_as_409(
    migrated_database_url: str,
) -> None:
    """A stale diagnostic policy is a stable state conflict, not malformed input."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_report(
            session,
            portfolio_id=PORTFOLIOS[0],
            snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
            as_of=datetime(2026, 8, 24, 3, 59, tzinfo=UTC),
            policy_version="obsolete_policy",
        )
    path = "/internal/v1/overview"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"


async def _seed_report(
    session: AsyncSession,
    *,
    portfolio_id: UUID,
    snapshot_id: UUID,
    as_of: datetime,
    policy_version: str = "real_advisory_policy_v1",
) -> PortfolioDiagnosticReportRecord:
    report = PortfolioDiagnosticReportRecord(
        portfolio_id=portfolio_id,
        portfolio_snapshot_id=snapshot_id,
        snapshot_as_of=as_of,
        trading_day=date(2026, 8, as_of.day),
        policy_id=REAL_ADVISORY_POLICY_ID,
        policy_version=policy_version,
        optimization_need="OPTIMIZATION_CANDIDATE",
        diagnostic_input_hash=("a" if as_of.day == 23 else "b") * 64,
    )
    session.add(report)
    await session.flush()
    for code, value, limit, unit, severity, actionability, reason in (
        ("CONCENTRATION_TOP1", "0.4", "0.3", "ratio", "critical", "reduce_only", None),
        ("CASH_RATIO", "0.2", "0.1", "ratio", "info", "monitor", None),
        ("LIQUIDITY_DAYS_TO_EXIT", None, None, "trading_days", "info", "research", "ADV20_MISSING"),
    ):
        item = PortfolioDiagnosticItemRecord(
            report_id=report.id,
            diagnostic_code=code,
            metric_value=value,
            unit=unit,
            reference_limit=limit,
            policy_scope="currency:USD",
            severity=severity,
            actionability=actionability,
            unknown_reason=reason,
        )
        session.add(item)
        await session.flush()
        session.add(
            PortfolioDiagnosticEvidenceRefRecord(
                item_id=item.id,
                source="fixture",
                object_id=f"item-{code}-{as_of.day}",
                available_at=as_of,
                location="fixture://diagnostic",
                content_hash="a" * 64,
            )
        )
    return report

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


async def _seed_report(
    session: AsyncSession, *, portfolio_id: UUID, snapshot_id: UUID, as_of: datetime
) -> PortfolioDiagnosticReportRecord:
    report = PortfolioDiagnosticReportRecord(
        portfolio_id=portfolio_id,
        portfolio_snapshot_id=snapshot_id,
        snapshot_as_of=as_of,
        trading_day=date(2026, 8, as_of.day),
        policy_id=REAL_ADVISORY_POLICY_ID,
        policy_version="real_advisory_policy_v1",
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

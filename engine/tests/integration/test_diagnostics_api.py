import json
from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import PORTFOLIOS, app, headers, seed_portfolio_list, settings
from nexus.api.routes.diagnostics import router as diagnostics_router
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.diagnostics.policy import load_real_advisory_policy_v1
from nexus.diagnostics.repository import persist_report
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.session import session_factory


@pytest.mark.asyncio
async def test_latest_diagnostic_is_redacted_and_returns_persisted_item_shape(
    migrated_database_url: str,
) -> None:
    """Replacing the stored projection with client-side calculation must fail this API contract."""
    await seed_portfolio_list(migrated_database_url)
    assert diagnostics_router is not None
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        policy = await load_real_advisory_policy_v1(session)
        await persist_report(
            session,
            portfolio_id=PORTFOLIOS[0],
            report=PortfolioDiagnosticReport(
                portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
                policy_version=policy.version,
                optimization_need="RESEARCH_REQUIRED",
                items=(
                    DiagnosticItem(
                        diagnostic_code="LIQUIDITY_DAYS_TO_EXIT",
                        metric_value=None,
                        unit="trading_days",
                        reference_limit="2",
                        policy_scope="currency:HKD",
                        severity="info",
                        actionability="research",
                        unknown_reason="ADV20_MISSING",
                        evidence_refs=(
                            EvidenceRef(
                                source="fixture",
                                object_id="market-1",
                                available_at=datetime(2026, 8, 24, tzinfo=UTC),
                                location="fixture://market-1",
                                content_hash="a" * 64,
                            ),
                        ),
                    ),
                ),
            ),
        )
    path = f"/internal/v1/portfolios/{PORTFOLIOS[0]}/diagnostics/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 200
    item = response.json()["data"]["items"][0]
    assert item["metric_value"] is None
    assert item["reference_limit"] == "2.000000000000"
    assert item["policy_scope"] == "currency:HKD"
    assert item["evidence_refs"][0]["content_hash"] == "a" * 64
    assert response.json()["data"]["policy_version"] == "real_advisory_policy_v1"
    assert "account_id" not in json.dumps(response.json())

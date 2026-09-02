import json
from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import (
    ACCOUNTS,
    CLOCK,
    PORTFOLIOS,
    app,
    headers,
    seed_portfolio_list,
    settings,
    snapshot_record,
)
from nexus.api.routes.diagnostics import router as diagnostics_router
from nexus.diagnostics.assembler import DiagnosticEvidenceCapabilityUnavailable
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.diagnostics.repository import (
    load_frozen_snapshot,
    load_instrument_identities,
)
from nexus.diagnostics.service import diagnose_and_persist
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.portfolio import BrokerSyncRun
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

INPUT_HASH = "e" * 64


@pytest.mark.asyncio
async def test_latest_diagnostic_never_reuses_an_old_formal_report_for_a_new_snapshot(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    run_id = UUID("0198d500-1000-7000-8000-000000000091")
    snapshot_id = UUID("0198d500-1000-7000-8000-000000000092")
    async with factory() as session, session.begin():
        await _seed_diagnostic_graph(session)
        session.add(
            BrokerSyncRun(
                id=run_id,
                account_id=ACCOUNTS[0],
                portfolio_id=PORTFOLIOS[0],
                trigger_kind="AUTOMATIC",
                actor_id="preview-test",
                route="broker-sync",
                idempotency_key="preview-test",
                request_hash="9" * 64,
                status="READY",
                accepted_at=CLOCK,
                completed_at=CLOCK,
                attempt_count=1,
                cash_flow_quality="NOT_RUN",
            )
        )
        latest = snapshot_record(snapshot_id, run_id, "READY", CLOCK, None)
        latest.portfolio_id = PORTFOLIOS[0]
        session.add(latest)
    path = f"/internal/v1/portfolios/{PORTFOLIOS[0]}/diagnostics/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["portfolio_snapshot_id"] == str(snapshot_id)
    assert any(item["diagnostic_code"] == "PRELIMINARY_DIAGNOSTIC" for item in data["items"])
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
            == 1
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("cash_flow_quality", ("PARTIAL", "NOT_RUN"))
async def test_unreconciled_cash_flows_allow_read_only_preview_without_formal_report(
    migrated_database_url: str,
    cash_flow_quality: str,
) -> None:
    """Missing cash-flow evidence must not hide independently verified snapshot metrics."""
    await seed_portfolio_list(migrated_database_url, ready_cash_flow_quality=cash_flow_quality)
    factory = session_factory(settings(migrated_database_url))
    path = f"/internal/v1/portfolios/{PORTFOLIOS[0]}/diagnostics/latest"
    snapshot_path = f"/internal/v1/portfolios/{PORTFOLIOS[0]}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        snapshot_response = await client.get(snapshot_path, headers=headers("GET", snapshot_path))
    assert response.status_code == 200
    report = response.json()["data"]
    assert report["optimization_need"] == "RESEARCH_REQUIRED"
    assert (
        report["portfolio_snapshot_id"] == snapshot_response.json()["data"]["portfolio_snapshot_id"]
    )
    items = report["items"]
    assert any(item["diagnostic_code"] == "PRELIMINARY_DIAGNOSTIC" for item in items)
    assert any(item["unknown_reason"] == "CASH_FLOW_RECONCILIATION_PENDING" for item in items)
    top1 = next(item for item in items if item["diagnostic_code"] == "CONCENTRATION_TOP1")
    assert top1["metric_value"] == "1"
    assert top1["evidence_refs"]
    cash = next(
        item
        for item in items
        if item["diagnostic_code"] == "CASH_RATIO" and item["policy_scope"] == "currency:USD"
    )
    assert cash["metric_value"] == "1"
    assert all(item["actionability"] in {"research", "monitor", "blocked"} for item in items)
    assert not any(item["diagnostic_code"].startswith(("VOLATILITY", "DRAWDOWN")) for item in items)
    assert snapshot_response.json()["data"]["decision_readiness"] == "RECONCILIATION_PENDING"
    assert snapshot_response.json()["data"]["execution_locked"] is True
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
            == 0
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_latest_diagnostic_is_redacted_and_returns_persisted_item_shape(
    migrated_database_url: str,
) -> None:
    """Replacing the stored projection with client-side calculation must fail this API contract."""
    await seed_portfolio_list(migrated_database_url)
    assert diagnostics_router is not None
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_diagnostic_graph(session)
    path = f"/internal/v1/portfolios/{PORTFOLIOS[0]}/diagnostics/latest"
    engine_app = app(migrated_database_url)
    operation = engine_app.openapi()["paths"][
        "/internal/v1/portfolios/{portfolio_id}/diagnostics/latest"
    ]["get"]
    assert operation["operationId"] == "getLatestPortfolioDiagnostic"
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/PortfolioDiagnosticEnvelope"
    }
    for status_code in ("404", "422"):
        assert operation["responses"][status_code]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/NexusErrorEnvelope"
        }
    request_headers = headers("GET", path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=request_headers)
        missing_path = f"/internal/v1/portfolios/{PORTFOLIOS[1]}/diagnostics/latest"
        missing = await client.get(missing_path, headers=headers("GET", missing_path))
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "NOT_FOUND"
        partial_path = f"/internal/v1/portfolios/{PORTFOLIOS[2]}/diagnostics/latest"
        partial = await client.get(partial_path, headers=headers("GET", partial_path))
        assert partial.status_code == 200
        assert partial.json()["data"]["optimization_need"] == "BLOCKED"
        assert all(item["metric_value"] is None for item in partial.json()["data"]["items"])

    assert response.status_code == 200
    item = response.json()["data"]["items"][0]
    assert item["metric_value"] is None
    assert item["reference_limit"] == "2.000000000000"
    assert item["policy_scope"] == "currency:HKD"
    assert item["evidence_refs"][0]["content_hash"] == "a" * 64
    assert item["evidence_refs"][0]["available_at"] == "2026-08-24T00:00:00Z"
    assert response.json()["data"]["policy_version"] == "real_advisory_policy_v1"
    assert response.json()["data"]["snapshot_as_of"] == "2026-08-24T03:59:00Z"
    assert response.json()["meta"] == {
        "request_id": request_headers["X-Request-Id"],
        "schema_version": "1",
        "as_of": "2026-08-24T03:59:00Z",
    }
    assert "account_id" not in json.dumps(response.json())


@pytest.mark.asyncio
async def test_database_triggers_reject_update_and_delete_for_every_diagnostic_record(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        report_id, item_id, evidence_id = await _seed_diagnostic_graph(session)
    records = (
        ("diagnostic_policies", REAL_ADVISORY_POLICY_ID),
        ("portfolio_diagnostic_reports", report_id),
        ("portfolio_diagnostic_items", item_id),
        ("portfolio_diagnostic_evidence_refs", evidence_id),
    )
    for table_name, record_id in records:
        for operation in (
            f"UPDATE {table_name} SET created_at = created_at",
            f"DELETE FROM {table_name}",
        ):
            async with factory() as session:
                with pytest.raises(IntegrityError, match="diagnostic record is immutable"):
                    async with session.begin():
                        await session.execute(
                            text(f"{operation} WHERE id = :id"), {"id": record_id}
                        )


@pytest.mark.asyncio
async def test_production_diagnosis_requires_artifact_transport_capability(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        with pytest.raises(DiagnosticEvidenceCapabilityUnavailable):
            await diagnose_and_persist(
                session,
                portfolio_id=PORTFOLIOS[0],
                snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
            )


@pytest.mark.asyncio
async def test_latest_diagnostic_reports_policy_conflict_as_409(
    migrated_database_url: str,
) -> None:
    """An invalid persisted policy is a stable authority conflict, not a 422 input error."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_diagnostic_graph(session, policy_version="obsolete_policy")
    path = f"/internal/v1/portfolios/{PORTFOLIOS[0]}/diagnostics/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"


@pytest.mark.asyncio
async def test_production_identity_is_minted_from_point_in_time_instrument_master(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        snapshot = await load_frozen_snapshot(
            session,
            portfolio_id=PORTFOLIOS[0],
            snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
        )
        identities = await load_instrument_identities(session, snapshot=snapshot)

    identity = identities[snapshot.positions[0].instrument_id]
    assert identity.canonical_ticker == "00656.HK"
    assert identity.exchange == "XHKG"
    assert identity.restriction == "FOSUN_READ_ONLY"
    assert identity.minted_by == "instrument_repository"


async def _seed_diagnostic_graph(
    session: AsyncSession, *, policy_version: str = "real_advisory_policy_v1"
) -> tuple[UUID, UUID, UUID]:
    report = PortfolioDiagnosticReportRecord(
        portfolio_id=PORTFOLIOS[0],
        portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
        snapshot_as_of=datetime(2026, 8, 24, 3, 59, tzinfo=UTC),
        trading_day=date(2026, 8, 24),
        policy_id=REAL_ADVISORY_POLICY_ID,
        policy_version=policy_version,
        optimization_need="RESEARCH_REQUIRED",
        diagnostic_input_hash=INPUT_HASH,
    )
    session.add(report)
    await session.flush()
    item = PortfolioDiagnosticItemRecord(
        report_id=report.id,
        diagnostic_code="LIQUIDITY_DAYS_TO_EXIT",
        metric_value=None,
        unit="trading_days",
        reference_limit="2",
        policy_scope="currency:HKD",
        severity="info",
        actionability="research",
        unknown_reason="ADV20_MISSING",
    )
    session.add(item)
    await session.flush()
    evidence = PortfolioDiagnosticEvidenceRefRecord(
        item_id=item.id,
        source="fixture",
        object_id="market-1",
        available_at=datetime(2026, 8, 24, tzinfo=UTC),
        location="fixture://market-1",
        content_hash="a" * 64,
    )
    session.add(evidence)
    await session.flush()
    return report.id, item.id, evidence.id

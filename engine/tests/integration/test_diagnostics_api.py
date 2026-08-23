import json
from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import PORTFOLIOS, app, headers, seed_portfolio_list, settings
from nexus.api.routes.diagnostics import router as diagnostics_router
from nexus.diagnostics.assembler import DiagnosticEvidenceCapabilityUnavailable
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, load_real_advisory_policy_v1
from nexus.diagnostics.repository import (
    _persist_report,
    load_frozen_snapshot,
    load_instrument_identities,
)
from nexus.diagnostics.service import diagnose_and_persist
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
)
from nexus.persistence.session import session_factory
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

INPUT_HASH = "e" * 64


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
        await _persist_report(
            session,
            portfolio_id=PORTFOLIOS[0],
            report=PortfolioDiagnosticReport(
                portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
                trading_day=date(2026, 8, 24),
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
            diagnostic_input_hash=INPUT_HASH,
        )
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
        assert operation["responses"][status_code]["content"]["application/json"][
            "schema"
        ] == {"$ref": "#/components/schemas/NexusErrorEnvelope"}
    request_headers = headers("GET", path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=request_headers)
        for portfolio_id in PORTFOLIOS[1:]:
            missing_path = f"/internal/v1/portfolios/{portfolio_id}/diagnostics/latest"
            missing = await client.get(missing_path, headers=headers("GET", missing_path))
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "NOT_FOUND"

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
        policy = await load_real_advisory_policy_v1(session)
        report_id = await _persist_report(
            session,
            portfolio_id=PORTFOLIOS[0],
            report=_report(policy.version),
            diagnostic_input_hash=INPUT_HASH,
        )
        item_id = await session.scalar(
            select(PortfolioDiagnosticItemRecord.id).where(
                PortfolioDiagnosticItemRecord.report_id == report_id
            )
        )
        evidence_id = await session.scalar(
            select(PortfolioDiagnosticEvidenceRefRecord.id).where(
                PortfolioDiagnosticEvidenceRefRecord.item_id == item_id
            )
        )
    assert item_id is not None and evidence_id is not None
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


def _report(policy_version: str) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
        trading_day=date(2026, 8, 24),
        policy_version=policy_version,  # type: ignore[arg-type]
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
    )

"""Authenticated read-only portfolio scenario API contracts."""

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
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
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import PositionSnapshot
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from sqlalchemy import func, select

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
OTHER_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000042")
US_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000701")
AS_OF = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)


async def _seed_scenario_authority(database_url: str, *, cash_flow_quality: str = "PASS") -> UUID:
    await seed_portfolio_list(database_url, ready_cash_flow_quality=cash_flow_quality)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=US_INSTRUMENT_ID,
                asset_type="equity",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="us_equity",
                created_at=AS_OF - timedelta(minutes=1),
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=US_INSTRUMENT_ID,
                symbol="SCENARIO",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
                created_at=AS_OF - timedelta(minutes=1),
            )
        )
        session.add(
            PositionSnapshot(
                portfolio_snapshot_id=SNAPSHOT_ID,
                instrument_id=US_INSTRUMENT_ID,
                currency="USD",
                quantity=Decimal("100"),
                available_quantity=Decimal("100"),
                frozen_quantity=Decimal("0"),
                avg_cost=Decimal("35"),
                diluted_cost=Decimal("35"),
                current_price=Decimal("40"),
                provider_as_of=AS_OF,
            )
        )
        report = PortfolioDiagnosticReportRecord(
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=SNAPSHOT_ID,
            snapshot_as_of=AS_OF,
            trading_day=AS_OF.date(),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="RESEARCH_REQUIRED",
            diagnostic_input_hash="d" * 64,
        )
        session.add(report)
        await session.flush()
        item = PortfolioDiagnosticItemRecord(
            report_id=report.id,
            diagnostic_code="CONCENTRATION_TOP1",
            metric_value=Decimal("1"),
            unit="ratio",
            reference_limit=Decimal("0.30"),
            policy_scope="currency:USD",
            severity="critical",
            actionability="reduce_only",
            unknown_reason=None,
        )
        session.add(item)
        await session.flush()
        session.add(
            PortfolioDiagnosticEvidenceRefRecord(
                item_id=item.id,
                source="fixture",
                object_id="scenario-top1",
                available_at=AS_OF,
                location="fixture://scenario-top1",
                content_hash="e" * 64,
            )
        )
        await session.flush()
        return report.id


def _payload(report_id: UUID, *, snapshot_id: UUID = SNAPSHOT_ID) -> bytes:
    return json.dumps(
        {
            "schema_version": "portfolio_scenario_request_v1",
            "portfolio_snapshot_id": str(snapshot_id),
            "diagnostic_report_id": str(report_id),
            "as_of": "2026-08-24T03:59:00Z",
            "constraints": {
                "schema_version": "adjustment_scenario_constraints_v1",
                "objective": "reduce_security_concentration",
                "max_turnover": "0.10",
                "minimum_cash": "0.10",
                "universe_policy": "current_holdings_only",
                "allow_new_positions": False,
            },
        },
        separators=(",", ":"),
    ).encode()


@pytest.mark.asyncio
async def test_scenario_evaluation_returns_one_ephemeral_locked_candidate(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(migrated_database_url)
    body = _payload(report_id)
    path = "/internal/v1/portfolio-scenarios/evaluate"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "scenario-evaluate-1"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)
        replay_headers = headers("POST", path, body)
        replay_headers["Idempotency-Key"] = "scenario-evaluate-1"
        replay = await client.post(path, content=body, headers=replay_headers)
        changed = json.loads(body)
        changed["constraints"]["minimum_cash"] = "0.20"
        changed_body = json.dumps(changed, separators=(",", ":")).encode()
        conflict_headers = headers("POST", path, changed_body)
        conflict_headers["Idempotency-Key"] = "scenario-evaluate-1"
        conflict = await client.post(path, content=changed_body, headers=conflict_headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert replay.status_code == 200
    assert replay.json()["data"]["result_hash"] == data["result_hash"]
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert data["portfolio_snapshot_id"] == str(SNAPSHOT_ID)
    assert data["diagnostic_report_id"] == str(report_id)
    assert data["valuation_scope"] == "currency:USD"
    assert data["analysis_status"] == "partial"
    assert data["execution_locked"] is True
    assert data["unknown_metrics"] == ["estimated_cost"]
    assert data["positions"] == [
        {
            "instrument_id": str(US_INSTRUMENT_ID),
            "current_weight": "0.800000000000",
            "target_weight": "0.800000000000",
            "action": "maintain",
            "reason_code": "BASELINE_WEIGHT",
        }
    ]

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        audit = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "portfolio_scenario_evaluated")
            )
        )
    assert len(audit) == 1
    assert audit[0].payload == {
        "request_hash": data["request_hash"],
        "policy_version": "real_advisory_policy_v1",
        "instrument_count": 1,
        "analysis_status": "partial",
        "objective_status": "infeasible",
    }


@pytest.mark.asyncio
async def test_scenario_rejects_client_weights_and_mismatched_authority(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(migrated_database_url)
    path = "/internal/v1/portfolio-scenarios/evaluate"
    weighted = json.loads(_payload(report_id))
    weighted["positions"] = [{"instrument_id": str(US_INSTRUMENT_ID), "weight": "1"}]
    cases = (
        (json.dumps(weighted, separators=(",", ":")).encode(), 422, "VALIDATION_ERROR"),
        (
            _payload(report_id, snapshot_id=OTHER_SNAPSHOT_ID),
            409,
            "SCENARIO_DIAGNOSTIC_MISMATCH",
        ),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        for index, (body, status, code) in enumerate(cases):
            request_headers = headers("POST", path, body)
            request_headers["Idempotency-Key"] = f"scenario-reject-{index}"
            response = await client.post(path, content=body, headers=request_headers)
            assert response.status_code == status
            assert response.json()["error"]["code"] == code

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "portfolio_scenario_evaluated")
            )
            == 0
        )


@pytest.mark.asyncio
async def test_scenario_rejects_ready_snapshot_without_cash_flow_authority(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(migrated_database_url, cash_flow_quality="PARTIAL")
    body = _payload(report_id)
    path = "/internal/v1/portfolio-scenarios/evaluate"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "scenario-unqualified-baseline"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "SCENARIO_BASELINE_NOT_READY"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "portfolio_scenario_evaluated")
            )
            == 0
        )

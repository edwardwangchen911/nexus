# ruff: noqa: E501
"""Authenticated optimization what-if API contracts."""

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import pytest
from helpers.optimization_api import seed_readable_group
from helpers.portfolio_api import PORTFOLIOS, app, headers, seed_portfolio_list, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.data import DataSnapshot, SourcePolicy
from nexus.persistence.models.diagnostics import DiagnosticPolicy, PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_what_if_writes_only_redacted_audit_summary(migrated_database_url: str) -> None:
    """Persisting requested weights or proposal rows would leak and mutate business state."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        group_id, proposal_id = await seed_readable_group(session)
    payload = {
        "proposal_group_id": str(group_id),
        "proposal_id": str(proposal_id),
        "policy_version": "optimizer_v1",
        "positions": [{"instrument_id": "0198d500-1000-7000-8000-000000000001", "weight": "1"}],
    }
    body = json.dumps(payload, separators=(",", ":")).encode()
    path = "/internal/v1/what-if/evaluate"
    engine_app = app(migrated_database_url)
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "what-if-redacted-summary"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)
    assert response.status_code == 200
    assert response.json()["data"]["policy_version"] == "optimizer_v1"
    assert response.json()["data"]["result"]["diagnostics"]["top1"] == {
        "status": "known", "value": 10000, "unit": "basis_points", "reason": None,
    }
    result = response.json()["data"]["result"]
    assert result["constraints"]["violation_count"]["value"] == 3
    assert result["constraints"]["single_name_critical"]["value"] == 1
    assert result["constraints"]["top3_warning"]["value"] == 1
    assert result["constraints"]["cash_minimum"]["value"] == 1
    assert result["diagnostics"]["sector_exposure"]["reason"] == (
        "SECTOR_EXPOSURE_REQUIRES_IDENTITIES"
    )
    async with factory() as session:
        rows = (
            (
                await session.execute(
                    select(AuditEvent).where(AuditEvent.action == "what_if_evaluated")
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].payload == {
            "policy_version": "optimizer_v1",
            "instrument_count": 1,
            "request_hash": hashlib.sha256(body).hexdigest(),
        }
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1


@pytest.mark.asyncio
async def test_what_if_has_one_redacted_audit_and_no_business_or_idempotency_write(
    migrated_database_url: str,
) -> None:
    """The synchronous endpoint may append one summary, but never retain request values elsewhere."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        group_id, proposal_id = await seed_readable_group(session)
    instrument_id = "0198d500-1000-7000-8000-000000000001"
    exact_weight = "0.10"
    body = json.dumps(
        {
            "proposal_group_id": str(group_id),
            "proposal_id": str(proposal_id),
            "policy_version": "optimizer_v1",
            "positions": [{"instrument_id": instrument_id, "weight": exact_weight}],
        },
        separators=(",", ":"),
    ).encode()
    path = "/internal/v1/what-if/evaluate"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "what-if-no-business-write"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)
    assert response.status_code == 200
    assert instrument_id not in response.text
    assert exact_weight not in response.text
    result = response.json()["data"]["result"]
    assert result["diagnostics"]["top1"]["value"] == 1000
    assert result["constraints"]["single_name_critical"]["value"] == 0
    assert result["constraints"]["violation_count"]["value"] == 0
    async with factory() as session:
        counts = []
        for table in (
            Job,
            IdempotencyRecord,
            OptimizationProposalGroupRecord,
            OptimizationProposalRecord,
            ProposalPositionRecord,
            ProposalComparisonRecord,
        ):
            counts.append(await session.scalar(select(func.count()).select_from(table)))
        audit = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "what_if_evaluated")
            )
        )
    assert tuple(counts) == (0, 1, 1, 3, 2, 3)
    assert len(audit) == 1
    assert exact_weight not in json.dumps(audit[0].payload)
    assert instrument_id not in json.dumps(audit[0].payload)


@pytest.mark.asyncio
async def test_what_if_rejects_oversized_and_more_than_thirty_instruments(
    migrated_database_url: str,
) -> None:
    """Parsing oversized bodies or permitting 31 instruments breaks the bounded what-if contract."""
    path = "/internal/v1/what-if/evaluate"
    too_many = json.dumps(
        {
            "policy_version": "optimizer_v1",
            "positions": [
                {"instrument_id": str(UUID(int=index + 1)), "weight": "0"} for index in range(31)
            ],
        }
    ).encode()
    oversized = b'{"padding":"' + b"x" * (64 * 1024) + b'"}'

    async def chunked_body():
        yield oversized[:1024]
        yield oversized[1024:]

    engine_app = app(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        for body in (too_many, oversized):
            response = await client.post(path, content=body, headers=headers("POST", path, body))
            assert response.status_code == 422
            assert response.json()["error"]["code"] == "VALIDATION_ERROR"
        chunked = await client.post(
            path, content=chunked_body(), headers=headers("POST", path, oversized)
        )
    assert chunked.status_code == 422
    assert chunked.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_proposal_admission_fails_closed_without_persisted_authority(
    migrated_database_url: str,
) -> None:
    """Creating a job from a fabricated data/cost authority must be rejected before persistence."""
    payload = json.dumps(
        {
            "portfolio_snapshot_id": "0198d500-1000-7000-8000-000000000041",
            "data_snapshot_id": "0198d500-1000-7000-8000-000000000699",
            "cost_policy_id": str(COST_POLICY_ID),
        },
        separators=(",", ":"),
    ).encode()
    path = "/internal/v1/optimization-proposals"
    engine_app = app(migrated_database_url)
    request_headers = headers("POST", path, payload)
    request_headers["Idempotency-Key"] = "proposal-missing-authority"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=payload, headers=request_headers)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "changes"),
    (
        ("partial_snapshot", {"snapshot_id": "0198d500-1000-7000-8000-000000000042"}),
        ("diagnostic_hash_conflict", {"diagnostic_input_hash": "c" * 64}),
        ("non_task11_diagnostic", {"alternate_diagnostic_policy": True}),
        ("different_report_snapshot", {"report_snapshot_id": "0198d500-1000-7000-8000-000000000042"}),
        ("future_data", {"data_time_offset": 1}),
        ("incomplete_diagnostic", {"diagnostic_complete": False}),
        ("source_conflict", {"source_conflict": True}),
        ("partial_authority", {"partial": True}),
        ("unresolved_fx", {"requires_fx": True}),
        ("stale_authority", {"authority_stale": True}),
        ("future_source_policy", {"future_source_policy": True}),
        ("noncanonical_source", {"noncanonical_source": True}),
        ("incomplete_cost", {"cost_complete": False}),
        ("cost_policy_conflict", {"request_cost_policy": "0198d500-1000-7000-8000-000000000799"}),
    ),
)
async def test_proposal_admission_rejects_every_unproven_authority_without_writes(
    migrated_database_url: str, case: str, changes: dict[str, object]
) -> None:
    """Any stale, partial, conflicting, or unlinked authority must create neither job nor group."""
    del case
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    partial_snapshot_id = UUID("0198d500-1000-7000-8000-000000000042")
    baseline_id = UUID(str(changes.get("snapshot_id", "0198d500-1000-7000-8000-000000000041")))
    report_snapshot_id = UUID(str(changes.get("report_snapshot_id", baseline_id)))
    report_portfolio = PORTFOLIOS[2] if report_snapshot_id == partial_snapshot_id else PORTFOLIOS[0]
    baseline_time = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
    data_time = baseline_time.replace(hour=4) if changes.get("data_time_offset") else baseline_time
    data_id = UUID("0198d500-1000-7000-8000-000000000721")
    source_policy_id = (
        UUID("0198d500-1000-7000-8000-000000000722")
        if changes.get("noncanonical_source")
        else SOURCE_POLICY_ID
    )
    report_hash = "d" * 64
    report_policy_id = REAL_ADVISORY_POLICY_ID
    async with factory() as session, session.begin():
        if changes.get("future_source_policy"):
            await session.execute(text("ALTER TABLE source_policies DISABLE TRIGGER USER"))
            await session.execute(
                text("UPDATE source_policies SET created_at = :future WHERE id = :id"),
                {"future": baseline_time.replace(day=25), "id": SOURCE_POLICY_ID},
            )
            await session.execute(text("ALTER TABLE source_policies ENABLE TRIGGER USER"))
        if changes.get("alternate_diagnostic_policy"):
            report_policy_id = UUID("0198d500-1000-7000-8000-000000000723")
            session.add(
                DiagnosticPolicy(
                    id=report_policy_id,
                    key="other",
                    version="other_v1",
                    schema_version="1",
                    payload={},
                )
            )
        session.add(
            DataSnapshot(
                id=data_id,
                source="fixture",
                provider_object_id="optimization-authority",
                revision="1",
                observed_at=data_time,
                available_at=data_time,
                received_at=data_time,
                schema_version="1",
                payload={},
            )
        )
        if changes.get("noncanonical_source"):
            session.add(
                SourcePolicy(
                    id=source_policy_id,
                    key="untrusted_source",
                    version="v1",
                    schema_version="1",
                    payload={},
                )
            )
        report = PortfolioDiagnosticReportRecord(
            portfolio_id=report_portfolio,
            portfolio_snapshot_id=report_snapshot_id,
            snapshot_as_of=baseline_time,
            trading_day=date(2026, 8, 24),
                policy_id=report_policy_id,
                policy_version=(
                    "other_v1" if changes.get("alternate_diagnostic_policy") else "real_advisory_policy_v1"
                ),
            optimization_need="OPTIMIZATION_CANDIDATE",
            diagnostic_input_hash=report_hash,
        )
        session.add(report)
        await session.flush()
        session.add(
            OptimizationAdmissionAuthorityRecord(
                portfolio_snapshot_id=baseline_id,
                diagnostic_report_id=report.id,
                data_snapshot_id=data_id,
                source_policy_id=source_policy_id,
                optimizer_policy_id=UUID("e690b91d-1e24-5de3-b2b2-566a463fca25"),
                cost_policy_id=COST_POLICY_ID,
                diagnostic_input_hash=str(changes.get("diagnostic_input_hash", report_hash)),
                valuation_time=baseline_time,
                baseline_ready=True,
                diagnostic_complete=bool(changes.get("diagnostic_complete", True)),
                source_conflict=bool(changes.get("source_conflict", False)),
                partial=bool(changes.get("partial", False)),
                requires_fx=bool(changes.get("requires_fx", False)),
                cost_complete=bool(changes.get("cost_complete", True)),
                authority_stale=bool(changes.get("authority_stale", False)),
            )
        )
    body = json.dumps(
        {
            "portfolio_snapshot_id": str(baseline_id),
            "data_snapshot_id": str(data_id),
            "cost_policy_id": str(changes.get("request_cost_policy", COST_POLICY_ID)),
        },
        separators=(",", ":"),
    ).encode()
    path = "/internal/v1/optimization-proposals"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "reject-unproven-authority"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)
    assert response.status_code == 409
    assert response.json()["error"] == {
        "code": "DATA_PARTIAL",
        "message": "Persisted optimization authority is incomplete.",
        "request_id": response.json()["error"]["request_id"],
        "details": {"reason": "OPTIMIZATION_AUTHORITY_MISSING"},
    }
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord)) == 0


@pytest.mark.asyncio
async def test_proposal_admission_returns_one_queued_job_for_idempotent_replay(
    migrated_database_url: str,
) -> None:
    """Creating two jobs for one key/body or accepting a changed replay breaks admission idempotency."""
    from helpers.portfolio_api import PORTFOLIOS, seed_portfolio_list
    from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID

    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    authority_time = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
    data_snapshot_id = UUID("0198d500-1000-7000-8000-000000000711")
    source_policy_id = SOURCE_POLICY_ID
    async with factory() as session, session.begin():
        session.add(
            DataSnapshot(
                id=data_snapshot_id,
                source="fixture",
                provider_object_id="optimization-711",
                revision="1",
                observed_at=authority_time,
                available_at=authority_time,
                received_at=authority_time,
                schema_version="1",
                payload={},
            )
        )
        report = PortfolioDiagnosticReportRecord(
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
            snapshot_as_of=authority_time,
            trading_day=date(2026, 8, 24),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="OPTIMIZATION_CANDIDATE",
            diagnostic_input_hash="d" * 64,
        )
        session.add(report)
        await session.flush()
        session.add(
            OptimizationAdmissionAuthorityRecord(
                portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
                diagnostic_report_id=report.id,
                data_snapshot_id=data_snapshot_id,
                source_policy_id=source_policy_id,
                optimizer_policy_id=UUID("e690b91d-1e24-5de3-b2b2-566a463fca25"),
                cost_policy_id=COST_POLICY_ID,
                diagnostic_input_hash="d" * 64,
                valuation_time=authority_time,
                baseline_ready=True,
                diagnostic_complete=True,
                source_conflict=False,
                partial=False,
                requires_fx=False,
                cost_complete=True,
            )
        )
    body = json.dumps(
        {
            "portfolio_snapshot_id": "0198d500-1000-7000-8000-000000000041",
            "data_snapshot_id": str(data_snapshot_id),
            "cost_policy_id": str(COST_POLICY_ID),
        },
        separators=(",", ":"),
    ).encode()
    path = "/internal/v1/optimization-proposals"
    engine_app = app(migrated_database_url)
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "proposal-key-711"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        first = await client.post(path, content=body, headers=request_headers)
        replay_headers = headers("POST", path, body)
        replay_headers["Idempotency-Key"] = "proposal-key-711"
        replay = await client.post(path, content=body, headers=replay_headers)
        oversized_headers = headers("POST", path, body)
        oversized_headers["Idempotency-Key"] = "x" * 256
        oversized_key = await client.post(path, content=body, headers=oversized_headers)
        changed = await client.post(
            path,
            content=body + b" ",
            headers={
                **headers("POST", path, body + b" "),
                "Idempotency-Key": "proposal-key-711",
            },
        )
        concurrent_headers = []
        for _ in range(2):
            header = headers("POST", path, body)
            header["Idempotency-Key"] = "proposal-key-concurrent-711"
            concurrent_headers.append(header)
        concurrent = await asyncio.gather(
            *(client.post(path, content=body, headers=header) for header in concurrent_headers)
        )
    assert first.status_code == replay.status_code == 202
    assert first.json()["data"]["job_id"] == replay.json()["data"]["job_id"]
    expected_location = f"/api/v1/jobs/{first.json()['data']['job_id']}"
    assert first.headers["location"] == replay.headers["location"] == expected_location
    assert oversized_key.status_code == 422
    assert oversized_key.json()["error"]["code"] == "VALIDATION_ERROR"
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert {response.status_code for response in concurrent} == {202}
    assert len({response.json()["data"]["job_id"] for response in concurrent}) == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 2
        assert await session.scalar(select(func.count()).select_from(OptimizationProposalGroupRecord)) == 0

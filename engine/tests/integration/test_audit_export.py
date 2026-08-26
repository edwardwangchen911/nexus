"""Deterministic, bounded, redacted audit export contracts."""

import gzip
import json
from datetime import UTC, datetime, timedelta

import pytest
from nexus.audit.export import AuditExportLimitError, export_audit_ndjson_gzip
from nexus.audit.service import append_audit_event
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

_START = datetime(2026, 8, 1, tzinfo=UTC)


async def _seed(database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        await append_audit_event(
            session,
            actor_id="user:private-owner",
            action="quality.freeze.failed",
            schema_version="quality_v1",
            payload={
                "status": "failed",
                "reason_code": "SOURCE_STALE",
                "account_id": "private-account",
                "cash": "90000.00",
                "request_body": {"prompt": "private research"},
            },
            occurred_at=_START + timedelta(hours=1),
        )
        await append_audit_event(
            session,
            actor_id="system:task20",
            action="job.failed",
            schema_version="job_v1",
            payload={
                "outcome": "failed",
                "error_code": "HANDLER_TIMEOUT",
                "order_id": "private-order",
            },
            occurred_at=_START + timedelta(hours=2),
        )
        await append_audit_event(
            session,
            actor_id="broker-worker-private",
            action="broker.reconciliation.mismatch",
            schema_version="reconciliation_v1",
            payload={"status": "partial", "portfolio_id": "private-portfolio"},
            occurred_at=_START + timedelta(days=1),
        )


@pytest.mark.asyncio
async def test_export_is_deterministic_gzip_ndjson_and_redacts_private_fields(
    migrated_database_url: str,
) -> None:
    await _seed(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        first = await export_audit_ndjson_gzip(
            session,
            from_at=_START,
            to_at=_START + timedelta(days=2),
        )
        replay = await export_audit_ndjson_gzip(
            session,
            from_at=_START,
            to_at=_START + timedelta(days=2),
        )
    assert first == replay
    decoded = gzip.decompress(first).decode("utf-8")
    lines = decoded.splitlines()
    assert len(lines) == 3
    documents = [json.loads(line) for line in lines]
    assert [item["event_seq"] for item in documents] == [1, 2, 3]
    assert [item["kind"] for item in documents] == [
        "quality",
        "job",
        "reconciliation",
    ]
    assert documents[0]["actor_kind"] == "user"
    assert documents[1]["actor_kind"] == "system"
    assert documents[2]["actor_kind"] == "service"
    assert documents[0]["outcome"] == {
        "reason_code": "SOURCE_STALE",
        "status": "failed",
    }
    for private in (
        "private-owner",
        "private-account",
        "90000.00",
        "private research",
        "private-order",
        "private-portfolio",
        "broker-worker-private",
    ):
        assert private not in decoded


@pytest.mark.asyncio
async def test_export_kind_filter_and_bounds_fail_closed(
    migrated_database_url: str,
) -> None:
    await _seed(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        job_bytes = await export_audit_ndjson_gzip(
            session,
            from_at=_START,
            to_at=_START + timedelta(days=2),
            kind="job",
        )
        with pytest.raises(AuditExportLimitError, match="row limit"):
            await export_audit_ndjson_gzip(
                session,
                from_at=_START,
                to_at=_START + timedelta(days=2),
                max_rows=1,
            )
        with pytest.raises(ValueError, match="31 days"):
            await export_audit_ndjson_gzip(
                session,
                from_at=_START,
                to_at=_START + timedelta(days=32),
            )
        with pytest.raises(ValueError, match="canonical UTC"):
            await export_audit_ndjson_gzip(
                session,
                from_at=_START.replace(tzinfo=None),
                to_at=_START + timedelta(days=1),
            )
    documents = [
        json.loads(line)
        for line in gzip.decompress(job_bytes).decode("utf-8").splitlines()
    ]
    assert len(documents) == 1
    assert documents[0]["kind"] == "job"

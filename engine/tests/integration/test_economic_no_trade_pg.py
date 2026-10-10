"""No-window results require real qualified no-trade facts, and never write."""

import asyncio
import json
import subprocess
import sys
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest
from nexus.ops import economic_cycle as cli
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisOutputRecord,
    AgentResearchOutputRecord,
)
from nexus.persistence.models.economic import EconomicCommonWindowRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperPositionSnapshotRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, Job
from sqlalchemy import func, select, text

from engine.tests.integration.test_economic_cycle_pg import (
    test_normal_pair_producer_binds_four_research_jobs_and_exact_budget as seed,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "qualified",
        "expired",
        "foreign",
        "trade",
        "mixed",
        "failed",
        "pending",
        "borrowed_job",
        "forged_activation",
    ],
)
async def test_no_window_execution_status_requires_dual_qualified_no_trade(
    migrated_database_url, monkeypatch, tmp_path, case
):
    outcome = (
        "trade"
        if case == "trade"
        else "mixed"
        if case == "mixed"
        else "unknown"
        if case == "failed"
        else False
        if case == "pending"
        else True
    )
    await seed(
        migrated_database_url, monkeypatch, tmp_path, outcome, True, execution_status_test=case
    )


async def verify_no_trade_status(sessions, clock, pair_id, case, config, actual):
    kwargs = dict(database_url_file=config, expected_database=actual, pair_slot_id=pair_id)
    if case in {"trade", "mixed", "failed", "pending"}:
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_REQUIRED"):
            await cli.execute_status(**kwargs)
        return
    if case == "expired":
        clock[0] += timedelta(days=2)
    if case == "foreign":
        async with sessions.begin() as session:
            session.add(
                Job(
                    job_key="foreign-after-no-trade",
                    input_hash="a" * 64,
                    status="queued",
                    schema_version="test_v1",
                    payload={},
                    job_type="test.unrelated",
                    queue="general",
                    scope_kind="global",
                    scope_id="global",
                    effective_at=clock[0],
                    priority=1,
                    retry_policy="never",
                    max_attempts=1,
                    available_at=clock[0],
                )
            )
    tables = (
        AuditEvent,
        Job,
        EconomicCommonWindowRecord,
        PaperOrderRecord,
        PaperFillRecord,
        PaperOrderBatchRecord,
        PaperTargetVersionRecord,
        PaperReservationRecord,
        PaperLedgerTransactionRecord,
        PaperLedgerEntryRecord,
        PaperPositionSnapshotRecord,
        IdempotencyRecord,
        AgentPaperCycleEventRecord,
        StrategyDecisionRecord,
        AgentResearchOutputRecord,
        AgentPortfolioSynthesisOutputRecord,
    )
    async with sessions() as session:
        before = [await session.scalar(select(func.count()).select_from(table)) for table in tables]
    result = await cli.execute_status(**kwargs)
    assert result["status"] == "completed"
    assert result["window_id"] is None
    assert result["orders"] == []
    assert result["comparison_status"] == "not_evaluated"
    assert result["economic_status"] == "not_evaluated"
    assert result["execution_locked"] is True
    assert [(lane["lane"], lane["status"], lane["order_count"]) for lane in result["lanes"]] == [
        ("incumbent", "no_trade", 0),
        ("challenger", "no_trade", 0),
    ]
    assert all(
        lane["cycle_id"] and lane["decision_id"] and lane["outputs"] for lane in result["lanes"]
    )
    assert await cli.execute_status(**kwargs) == result
    completed = await asyncio.to_thread(
        subprocess.run,
        [
            str(Path(sys.executable).with_name("nexus-engine-economic-cycle")),
            "execution-status",
            "--database-url-file",
            str(config),
            "--expected-database",
            actual,
            "--pair-slot-id",
            str(pair_id),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0 and not completed.stderr
    assert json.loads(completed.stdout) == result
    async with sessions.begin() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        after = [await session.scalar(select(func.count()).select_from(table)) for table in tables]
        assert before == after
        counts = dict(zip(tables, after, strict=True))
        assert all(
            counts[table] == 0
            for table in (EconomicCommonWindowRecord, PaperOrderRecord, PaperFillRecord)
        )
        from nexus.paper.economic_execution import read_economic_pair_execution

        # Frozen Fosun authority validation deliberately protects immutable
        # manifest/calendar reads. These are source locks, never execution locks.
        prior_locks = await session.scalar(
            text("SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'")
        )
        assert prior_locks == 0
        assert (
            await read_economic_pair_execution(
                session, pair_slot_id=pair_id, window_seq=None, expected_database=actual
            )
            == result
        )
        allowed = set(
            await session.scalars(
                text("""
            SELECT hashtextextended('price-series-manifest:' || m.id::text, 0)
            FROM price_series_manifests m
            JOIN price_bar_set_authorities a ON a.price_series_manifest_id=m.id
            JOIN agent_data_snapshot_evidence e ON e.evidence_ref_id=a.evidence_ref_id
            JOIN agent_research_runs r ON r.data_snapshot_id=e.data_snapshot_id
            JOIN economic_lane_cycles c ON r.idempotency_key=
                'agent-paper:' || c.cycle_id::text || ':' || r.instrument_id::text
            WHERE c.pair_slot_id=:pair AND m.source='fosun'
            UNION
            SELECT hashtextextended(
                'exchange-calendar-authority:' || m.daily_calendar_authority_id::text, 0)
            FROM price_series_manifests m
            JOIN price_bar_set_authorities a ON a.price_series_manifest_id=m.id
            JOIN agent_data_snapshot_evidence e ON e.evidence_ref_id=a.evidence_ref_id
            JOIN agent_research_runs r ON r.data_snapshot_id=e.data_snapshot_id
            JOIN economic_lane_cycles c ON r.idempotency_key=
                'agent-paper:' || c.cycle_id::text || ':' || r.instrument_id::text
            WHERE c.pair_slot_id=:pair AND m.source='fosun'
        """),
                {"pair": pair_id},
            )
        )
        observed = tuple(
            (
                await session.execute(
                    text(
                        "SELECT classid::bigint,objid::bigint,objsubid,mode,granted "
                        "FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'"
                    )
                )
            ).all()
        )
        expected = {(key & ((1 << 64) - 1)) for key in allowed}
        assert {(row[0] << 32) | row[1] for row in observed} == expected
        assert all(row[2:] == (1, "ExclusiveLock", True) for row in observed)
    async with sessions.begin() as session:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM pg_locks "
                    "WHERE pid=pg_backend_pid() AND locktype='advisory'"
                )
            )
            == 0
        )
    if case == "borrowed_job":
        async with sessions.begin() as session:
            source = await session.scalar(select(Job).order_by(Job.created_at, Job.id).limit(1))
            session.add(
                Job(
                    job_key="borrowed-no-trade-job",
                    input_hash=source.input_hash,
                    status="queued",
                    schema_version=source.schema_version,
                    payload=deepcopy(source.payload),
                    job_type=source.job_type,
                    queue=source.queue,
                    scope_kind=source.scope_kind,
                    scope_id=source.scope_id,
                    effective_at=source.effective_at,
                    priority=source.priority,
                    retry_policy="never",
                    max_attempts=1,
                    available_at=source.available_at,
                )
            )
    if case == "forged_activation":
        async with sessions.begin() as session:
            source = await session.scalar(
                select(IdempotencyRecord)
                .where(IdempotencyRecord.route == "paper-target-activation-v1")
                .order_by(IdempotencyRecord.created_at, IdempotencyRecord.id)
                .limit(1)
            )
            other = await session.scalar(
                select(PaperTargetVersionRecord).where(
                    PaperTargetVersionRecord.account_id
                    != UUID(source.response_payload["account_id"])
                )
            )
            payload = deepcopy(source.response_payload)
            payload["target_version_id"] = str(other.id)
            session.add(
                IdempotencyRecord(
                    actor_id=source.actor_id,
                    route=source.route,
                    idempotency_key="forged-no-trade-activation",
                    request_hash=source.request_hash,
                    schema_version=source.schema_version,
                    response_payload=payload,
                    created_at=source.created_at,
                )
            )
    if case in {"borrowed_job", "forged_activation"}:
        with pytest.raises(ValueError, match="ECONOMIC_PAIR_RESULT_UNKNOWN"):
            await cli.execute_status(**kwargs)

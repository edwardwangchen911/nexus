"""Only proved, untouched incremental capacity rejection can close with zero usage."""

from uuid import UUID, uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_monitor_increment_pg import ready_trigger
from nexus.audit.service import append_audit_event
from nexus.ops.research_monitor import parser
from nexus.ops.research_monitor import run as monitor_entry
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorIncrement,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import Job, JobAttempt
from sqlalchemy import func, select


async def admitted(database_url, monkeypatch, *, dispatch=True):
    factory, settings, trigger, _parent, now = await ready_trigger(database_url, monkeypatch)
    receipt = await monitor_entry(
        settings,
        parser().parse_args(
            ["admit", "--trigger-id", str(trigger.id), "--calendar-authority-id", str(CALENDAR_ID)]
        ),
    )
    assert receipt["status"] == "admitted"
    if dispatch:
        async with factory() as session, session.begin():
            # Dispatch can only follow the newly committed admission.
            now = await session.scalar(select(func.clock_timestamp()))
            await append_audit_event(
                session,
                actor_id="research-monitor",
                action="research.monitor.worker_dispatched",
                schema_version="monitor_dispatch_v1",
                payload={"trigger_id": str(trigger.id), "job_id": receipt["job_id"]},
                occurred_at=now,
            )
    return factory, settings, trigger, UUID(receipt["run_id"]), UUID(receipt["job_id"]), now


@pytest.mark.asyncio
async def test_capacity_rejection_closes_original_without_refunding_calls(
    migrated_database_url, monkeypatch
):
    from nexus.monitoring.admission import require_monitor_account_available
    from nexus.monitoring.preclaim import (
        close_capacity_rejected_increment_once,
        preclaim_rejection_qualified,
    )
    from nexus.monitoring.reconciliation import reconcile_account_results
    from nexus.paper.manual_request import (
        require_manual_account_available,
        require_no_running_manual_request,
    )

    factory, settings, trigger, run_id, job_id, now = await admitted(
        migrated_database_url, monkeypatch
    )
    async with factory() as blocker, blocker.begin():
        await blocker.scalar(select(Job).where(Job.id == job_id).with_for_update())
        with pytest.raises(ValueError, match="MONITOR_PRECLAIM_CLOSE_UNCONFIRMED"):
            await close_capacity_rejected_increment_once(
                settings,
                account_id=trigger.account_id,
                trigger_id=trigger.id,
                job_id=job_id,
                code="CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
            )
    result = await close_capacity_rejected_increment_once(
        settings,
        account_id=trigger.account_id,
        trigger_id=trigger.id,
        job_id=job_id,
        code="CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
    )
    assert result["status"] == "failed" and result["reason"] == "MONITOR_CAPACITY_REJECTED"
    assert (
        await close_capacity_rejected_increment_once(
            settings,
            account_id=trigger.account_id,
            trigger_id=trigger.id,
            job_id=job_id,
            code="CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
        )
        == result
    )
    async with factory() as session, session.begin():
        now = await session.scalar(select(func.clock_timestamp()))
        run = await session.get(AgentResearchRunRecord, run_id)
        job = await session.get(Job, job_id)
        assert run.status == "abstained" and job.status == "skipped"
        assert (run.input_tokens, run.output_tokens, run.settled_cost_usd) == (0, 0, 0)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run_id
            )
        )
        assert reservation.state == "released" and reservation.settled_usd is None
        member = await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.run_id == run_id)
        )
        assert member.reserved_sdk_calls == 3
        assert await preclaim_rejection_qualified(session, run=run, now=now)
        assert (
            await session.scalar(
                select(func.count()).select_from(JobAttempt).where(JobAttempt.job_id == job_id)
            )
            == 1
        )
        assert (await session.get(MonitorTrigger, trigger.id)).reason == "MONITOR_CAPACITY_REJECTED"
        await require_monitor_account_available(session, account_id=trigger.account_id, now=now)
        await require_manual_account_available(session, account_id=trigger.account_id, now=now)
        await require_no_running_manual_request(session, account_id=trigger.account_id)
        await reconcile_account_results(
            session, account_id=trigger.account_id, instrument_ids=(trigger.instrument_id,), now=now
        )
        coverage = await session.scalar(
            select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == run_id)
        )
        assert coverage is not None and coverage.status in {"retained", "gap"}
        assert coverage.quality_status is None
        from nexus.monitoring.source_qualification import load_qualified_source
        from nexus.persistence.models.agents import AgentResearchOutputRecord

        output = await session.scalar(
            select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
        )
        with pytest.raises(ValueError):
            await load_qualified_source(
                session, account_id=trigger.account_id, output_id=output.id, now=now
            )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["account", "job", "unknown_code", "usage", "claimed", "missing_dispatch"]
)
async def test_rejection_never_releases_unproved_or_used_increment(
    migrated_database_url, monkeypatch, fault
):
    from nexus.monitoring.preclaim import close_capacity_rejected_increment_once

    factory, settings, trigger, run_id, job_id, now = await admitted(
        migrated_database_url, monkeypatch, dispatch=fault != "missing_dispatch"
    )
    if fault == "usage":
        async with factory() as session, session.begin():
            run = await session.get(AgentResearchRunRecord, run_id)
            run.input_tokens = 1
    if fault == "claimed":
        from helpers.native_execution import claim_without_dispatch

        async with factory() as session:
            claimed_at = await session.scalar(select(func.clock_timestamp()))
        assert await claim_without_dispatch(settings, monkeypatch, job_id=job_id, now=claimed_at)
    with pytest.raises(ValueError, match="MONITOR_PRECLAIM_CLOSE_UNCONFIRMED"):
        await close_capacity_rejected_increment_once(
            settings,
            account_id=uuid4() if fault == "account" else trigger.account_id,
            trigger_id=trigger.id,
            job_id=uuid4() if fault == "job" else job_id,
            code="CODEX_TIMEOUT"
            if fault == "unknown_code"
            else "CODEX_CAPACITY_POOL_INPUT_EXCEEDED",
        )
    async with factory() as session:
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run_id
            )
        )
        assert reservation.state == "reserved" and reservation.settled_usd is None
        assert (await session.get(MonitorTrigger, trigger.id)).status == "admitted"
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_native_zero_usage_skip_without_capacity_proof_stays_occupied(
    migrated_database_url, monkeypatch
):
    from helpers.native_execution import claim_without_dispatch
    from nexus.agents.execution import _terminal_without_usage
    from nexus.jobs.repository import finalize_job, job_lease_consumed
    from nexus.monitoring.admission import finish_increment, require_monitor_account_available
    from nexus.monitoring.preclaim import ACTION, REASON, RESULT, preclaim_rejection_qualified
    from nexus.persistence.models.runtime import AuditEvent, JobLease

    factory, settings, trigger, run_id, job_id, now = await admitted(
        migrated_database_url, monkeypatch
    )
    async with factory() as session:
        claimed_at = await session.scalar(select(func.clock_timestamp()))
    claim = await claim_without_dispatch(
        settings,
        monkeypatch,
        job_id=job_id,
        now=claimed_at,
        worker_id=f"monitor-preclaim:{trigger.id}",
    )
    assert claim is not None
    async with factory() as session, session.begin():
        now = await session.scalar(select(func.clock_timestamp()))
        run = await session.get(AgentResearchRunRecord, run_id)
        await _terminal_without_usage(
            session,
            run,
            output_sequence=1,
            event_sequence=1,
            status="abstained",
            reason=REASON,
        )
        await finalize_job(session, claim=claim, now=now, outcome="skipped", result_code=RESULT)
        job = await session.get(Job, job_id)
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == job_id))
        assert lease.worker_id == f"monitor-preclaim:{trigger.id}"
        assert await job_lease_consumed(session, job=job, lease=lease, now=now)
        assert not await session.scalar(
            select(AuditEvent.id).where(
                AuditEvent.action == ACTION, AuditEvent.payload["run_id"].astext == str(run_id)
            )
        )
        assert not await preclaim_rejection_qualified(session, run=run, now=now)
        with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
            await require_monitor_account_available(session, account_id=trigger.account_id, now=now)
        finished = await finish_increment(
            session, account_id=trigger.account_id, trigger_id=trigger.id, now=now
        )
        assert finished.status == "admitted"
    await factory.kw["bind"].dispose()

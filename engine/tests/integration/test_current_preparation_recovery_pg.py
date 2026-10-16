"""Current host crash boundaries on isolated PG; no live source/model calls."""

import asyncio
import os
import subprocess
from contextlib import nullcontext
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import headers
from helpers.paper_api import settings as api_settings
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.main import create_engine_app
from nexus.ops import agent_paper_once, manual_paper, manual_paper_host
from nexus.ops.agent_paper_loop import heartbeat_agent_paper_loop
from nexus.ops.openai_local import prepare_database
from nexus.paper import manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import AuditEvent
from nexus.settings import EngineSettings
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.parametrize(
    "failure_point",
    [
        "before_effects",
        "effects_unknown",
        "native_claim_unknown",
        "late_claim_change",
        "unknown_write_failed",
        "cycle_committed",
        "cycle_wrong_execution",
        "cycle_missing_execution",
        "worker_claim_first",
        "maintenance_before_worker",
        "live_producer",
        "audit_rollback",
        "invalid_anchor",
        "concurrent_admission",
        "concurrent_late_effects",
        "concurrent_recovery",
        "accept_unknown",
        "accept_late_claim",
        "accept_audit_rollback",
        "accept_host_diagnostic_rollback",
        "accept_flush_rollback",
        "accept_precommit_rollback",
        "accept_commit_rollback",
        "accept_cleanup_unknown",
        "accept_commit_ack_lost",
        "accept_queued_job",
        "accept_running_job",
        "accept_live_producer",
        "accept_concurrent",
        "accept_direct",
        "accept_check_late",
        "accept_lease",
        "accept_corrupt_audit",
        "accept_late_cycle",
        "accept_other_unknown",
        "accept_writer_contention",
        "accept_audit_contention",
        "accept_open_order",
        "accept_prepared_closed",
        "accept_prepared_missing_job",
        "accept_prepared_identity",
        "accept_prepared_retryable",
        "accept_prepared_live_lease",
        "accept_prepared_missing_attempt",
        "accept_prepared_fence",
        "accept_prepared_future",
        "accept_prepared_succeeded",
        "accept_prepared_running",
    ],
)
@pytest.mark.asyncio
async def test_current_host_interruption_recovers_via_normal_cli(
    migrated_database_url, monkeypatch, tmp_path, failure_point
):
    acceptance_case = failure_point if failure_point.startswith("accept_") else None
    direct_acceptance = acceptance_case in {"accept_direct", "accept_open_order"} or bool(
        acceptance_case and acceptance_case.startswith("accept_prepared_")
    )
    native_trigger = agent_paper_once.trigger_once
    if acceptance_case:
        failure_point = "effects_unknown"
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **V9_OPTIONS,
        formal_flexible_stage_budget=True,
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url,
        monkeypatch,
        decision_time=AT,
        candidate_instruments=INSTRUMENTS,
        occupied=acceptance_case == "accept_open_order",
    )
    account_guard = manual_request.require_manual_account_available
    if acceptance_case == "accept_open_order":
        # Construct a historical unknown alongside independently occupied orders;
        # restore the real guard before exercising the maintenance entry.
        async def seed_historical_request(*_a, **_k):
            pass

        monkeypatch.setattr(
            manual_request, "require_manual_account_available", seed_historical_request
        )
    scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return AT
        return await scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    try:
        async with sessions.begin() as session:
            next_at = AT + timedelta(days=3)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=next_at.date(),
                    decision_at=next_at.replace(hour=21),
                    market_open_at=next_at.replace(hour=13, minute=30),
                    market_close_at=next_at.replace(hour=20),
                    created_at=AT - timedelta(hours=1),
                )
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v10-flexible-stage-budget"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="recovery-batch",
                created_at=AT,
            )
            admission = dict(
                account_id=authority.account_id,
                batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_and_paper",
            )
            request = await manual_request.admit_manual_request(
                session, **admission, idempotency_key="interrupted"
            )
        settings = EngineSettings(
            **api_settings(migrated_database_url).model_dump(
                exclude={
                    "agent_runtime_mode",
                    "release_revision",
                    "agent_paper_account_id",
                    "agent_paper_candidate_batch_id",
                    "agent_paper_formal_model_config_id",
                    "agent_paper_cycle_enabled",
                    "agent_paper_manual_enabled",
                }
            ),
            agent_runtime_mode="codex_sdk",
            agent_paper_account_id=authority.account_id,
            agent_paper_candidate_batch_id=batch.batch_id,
            agent_paper_formal_model_config_id=model.id,
            agent_paper_cycle_enabled=True,
            agent_paper_manual_enabled=True,
            release_revision="a" * 40,
        )
        trigger = agent_paper_once.AgentPaperTriggerSettings.model_validate(
            settings.model_dump()
            | {
                "object_store_endpoint": "http://localhost:9000",
                "alpaca_key": "fixture",
                "alpaca_secret": "fixture",
            }
        )

        # A process cancellation bypasses Exception; the claim transaction is already committed.
        async def interrupted(*args, **kwargs):
            raise asyncio.CancelledError()

        async def no_provider(*args, **kwargs):
            raise AssertionError("external boundary must not have been entered")

        expected_error = asyncio.CancelledError
        if failure_point in {
            "before_effects",
            "live_producer",
            "audit_rollback",
            "invalid_anchor",
            "concurrent_admission",
            "concurrent_late_effects",
            "concurrent_recovery",
        }:
            monkeypatch.setattr(manual_paper_host, "begin_preparation_effects", interrupted)
            monkeypatch.setattr(agent_paper_once, "trigger_once", no_provider)
        elif failure_point == "effects_unknown":
            if direct_acceptance:

                async def failed_before_cycle(*_a, **_k):
                    raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")

                monkeypatch.setattr(agent_paper_once, "trigger_once", failed_before_cycle)
                expected_error = ValueError
            else:
                monkeypatch.setattr(agent_paper_once, "trigger_once", interrupted)
        elif failure_point in {"native_claim_unknown", "late_claim_change"}:
            monkeypatch.setattr(agent_paper_once, "prepare_schwab_decision_authority", interrupted)
            monkeypatch.setattr(
                agent_paper_once, "S3ProviderEvidenceStore", lambda *_a, **_k: object()
            )
        elif failure_point == "unknown_write_failed":

            async def primary_failure(*args, **kwargs):
                raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")

            async def cannot_persist_unknown(*args, **kwargs):
                raise RuntimeError("isolated persistence fault")

            monkeypatch.setattr(agent_paper_once, "trigger_once", primary_failure)
            monkeypatch.setattr(
                manual_paper_host, "record_manual_request_event", cannot_persist_unknown
            )
            expected_error = ValueError
        else:
            original_trigger = agent_paper_once.trigger_once
            if failure_point in {"cycle_wrong_execution", "cycle_missing_execution"}:
                from nexus.paper import agent_cycle

                append_event = agent_cycle._append_event

                async def mismatched_cycle(
                    session, cycle, from_status, to_status, result_code, facts
                ):
                    facts = dict(facts)
                    if to_status == "prepared":
                        if failure_point == "cycle_missing_execution":
                            facts.pop("manual_execution_id", None)
                        else:
                            facts["manual_execution_id"] = "00000000-0000-0000-0000-000000000001"
                    return await append_event(
                        session, cycle, from_status, to_status, result_code, facts
                    )

                monkeypatch.setattr(agent_cycle, "_append_event", mismatched_cycle)

            async def authority_source(*args, **kwargs):
                return authority

            monkeypatch.setattr(
                agent_paper_once, "prepare_schwab_decision_authority", authority_source
            )
            monkeypatch.setattr(
                agent_paper_once, "S3ProviderEvidenceStore", lambda *_a, **_k: object()
            )

            async def lost_response(*args, **kwargs):
                await original_trigger(*args, **kwargs)
                raise asyncio.CancelledError()

            monkeypatch.setattr(agent_paper_once, "trigger_once", lost_response)
        with pytest.raises(expected_error):
            await manual_paper_host.dispatch_manual_request(
                settings,
                trigger,
                request_id=request.request_id,
                repo_root=tmp_path,
                calendar_authority_id=CALENDAR_ID,
                process_environment={},
            )
        async with sessions.begin() as session:
            row = await session.get(ManualPaperRequest, request.request_id)
            assert (await manual_request.read_manual_request(session, row)).status == (
                "active"
                if failure_point
                in {
                    "cycle_committed",
                    "cycle_wrong_execution",
                    "cycle_missing_execution",
                    "worker_claim_first",
                    "maintenance_before_worker",
                }
                else "unknown"
                if direct_acceptance
                else "preparing"
            )
            starts = list(
                await session.scalars(
                    select(AuditEvent).where(
                        AuditEvent.action == "paper.manual_preparation.execution_started"
                    )
                )
            )
            assert len(starts) == 1
        # OS/process ownership and checkout validation are boundary substitutes only.
        from nexus.paper import current_preparation_recovery as recovery

        monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
        monkeypatch.setattr(
            manual_paper, "preparation_service_control_lock", lambda *_: nullcontext()
        )
        args = manual_paper.parser().parse_args(
            [
                "--repo-root",
                str(tmp_path),
                "recover-preparation",
                "--request-id",
                str(request.request_id),
            ]
        )
        if failure_point == "live_producer":
            monkeypatch.setattr(
                "nexus.paper.manual_preparation_close.require_preparation_producers_stopped",
                lambda *_: None,
            )
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await manual_paper.run(settings, args)
            return
        monkeypatch.setattr(recovery, "_require_execution_producer_stopped", lambda *_: None)
        from nexus.paper.history_seal import _row_hash
        from nexus.persistence.models.agents import (
            AgentDailyBudgetReservationRecord,
            AgentResearchRunRecord,
            FormalBudgetBindingRecord,
        )
        from nexus.persistence.models.paper import (
            AgentPaperCycleEventRecord,
            AgentPaperCycleRecord,
            PaperOrderRecord,
        )
        from nexus.persistence.models.runtime import IdempotencyRecord, Job

        async def business_facts():
            async with sessions.begin() as session:
                return {
                    table.__tablename__: sorted(
                        _row_hash(row) for row in await session.scalars(select(table))
                    )
                    for table in (
                        AgentResearchRunRecord,
                        AgentDailyBudgetReservationRecord,
                        FormalBudgetBindingRecord,
                        AgentPaperCycleRecord,
                        AgentPaperCycleEventRecord,
                        PaperOrderRecord,
                        Job,
                        IdempotencyRecord,
                    )
                }

        if acceptance_case:
            async with sessions.begin() as session:
                await _seed_acceptance_budget(session, manifest[0], model, acceptance_case)
        before = await business_facts()
        if direct_acceptance:
            monkeypatch.setattr(manual_request, "require_manual_account_available", account_guard)
            await _check_explicit_acceptance(
                acceptance_case,
                settings,
                args,
                sessions,
                request,
                admission,
                starts[0],
                business_facts,
                before,
                monkeypatch,
                trigger,
                authority,
                tmp_path,
                native_trigger,
            )
            return

        async def claim_original_job():
            from nexus.agents.codex_versions import FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION
            from nexus.jobs.models import QueueName
            from nexus.jobs.repository import claim_next_job

            async def guard(_session, _job):
                return True

            async with sessions.begin() as session:
                job_id = await session.scalar(
                    select(Job.id)
                    .where(Job.job_type == "agent.paper.research")
                    .order_by(Job.id)
                    .limit(1)
                )
                assert job_id is not None
                return await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id="isolated-claim",
                    now=AT,
                    lease_seconds=210,
                    job_ids=(job_id,),
                    job_types=("agent.paper.research",),
                    locked_job_guard=guard,
                    research_budget_profile=FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
                )

        if failure_point in {
            "cycle_wrong_execution",
            "cycle_missing_execution",
            "worker_claim_first",
        }:
            if failure_point == "worker_claim_first":
                assert await claim_original_job() is not None
                before = await business_facts()
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await manual_paper.run(settings, args)
            assert await business_facts() == before
            async with sessions.begin() as session:
                row = await session.get(ManualPaperRequest, request.request_id)
                assert await recovery.read_preparation_recovery(session, row) is None
            return
        if failure_point in {"audit_rollback", "invalid_anchor"}:

            async def broken_audit(*args, **kwargs):
                raise RuntimeError("isolated audit persistence fault")

            append_original = recovery.append_audit_event

            async def wrong_anchor(*a, **kw):
                kw["payload"] = {
                    **kw["payload"],
                    "request_event_hash": starts[0].payload["preparing_hash"],
                }
                return await append_original(*a, **kw)

            with monkeypatch.context() as m:
                m.setattr(
                    recovery,
                    "append_audit_event",
                    broken_audit if failure_point == "audit_rollback" else wrong_anchor,
                )
                with pytest.raises((RuntimeError, ValueError)):
                    await manual_paper.run(settings, args)
            async with sessions.begin() as session:
                row = await session.get(ManualPaperRequest, request.request_id)
                assert (
                    await manual_request.read_manual_request(session, row)
                ).status == "preparing"
                assert await recovery.read_preparation_recovery(session, row) is None
        if failure_point.startswith("concurrent_") or failure_point == "maintenance_before_worker":
            audit_ready, contender_entered, release_audit = (
                asyncio.Event(),
                asyncio.Event(),
                asyncio.Event(),
            )
            append = recovery.append_audit_event
            lock = manual_request.lock_portfolio_account

            async def paused_append(*a, **kw):
                if kw.get("action") == recovery.RECOVERED:
                    audit_ready.set()
                    await release_audit.wait()
                return await append(*a, **kw)

            async def observed_lock(*a, **kw):
                contender_entered.set()
                return await lock(*a, **kw)

            async def contender():
                if failure_point == "maintenance_before_worker":
                    return await claim_original_job()
                if failure_point == "concurrent_recovery":
                    return await manual_paper.run(settings, args)
                async with sessions.begin() as session:
                    if failure_point == "concurrent_admission":
                        return await manual_request.admit_manual_request(
                            session, **admission, idempotency_key="independent"
                        )
                    return await recovery.begin_preparation_effects(
                        session, request_id=request.request_id, execution_id=starts[0].id
                    )

            with monkeypatch.context() as m:
                m.setattr(recovery, "append_audit_event", paused_append)
                task = asyncio.create_task(manual_paper.run(settings, args))
                await asyncio.wait_for(audit_ready.wait(), 5)
                m.setattr(manual_request, "lock_portfolio_account", observed_lock)
                m.setattr(recovery, "lock_portfolio_account", observed_lock)
                other = asyncio.create_task(contender())
                if failure_point == "maintenance_before_worker":
                    assert await asyncio.wait_for(other, 5) is None
                else:
                    await asyncio.wait_for(contender_entered.wait(), 5)
                    assert not other.done()
                release_audit.set()
                receipt = await asyncio.wait_for(task, 5)
                if failure_point == "concurrent_late_effects":
                    with pytest.raises(ValueError, match=recovery.BLOCKED):
                        await asyncio.wait_for(other, 5)
                elif failure_point != "maintenance_before_worker":
                    concurrent_result = await asyncio.wait_for(other, 5)
                    assert (
                        concurrent_result.replayed
                        if failure_point == "concurrent_recovery"
                        else concurrent_result.status == "queued"
                    )
        else:
            receipt = await manual_paper.run(settings, args)
        expected = (
            "cycle_reconciled"
            if failure_point in {"cycle_committed", "maintenance_before_worker"}
            else "unknown_preserved"
            if failure_point
            in {
                "effects_unknown",
                "unknown_write_failed",
                "native_claim_unknown",
                "late_claim_change",
            }
            else "cancelled_before_call"
        )
        assert await business_facts() == before
        assert receipt.disposition == expected
        assert receipt.execution_locked
        assert (await manual_paper.run(settings, args)).replayed
        if acceptance_case:
            await _check_explicit_acceptance(
                acceptance_case,
                settings,
                args,
                sessions,
                request,
                admission,
                starts[0],
                business_facts,
                before,
                monkeypatch,
                trigger,
                authority,
                tmp_path,
                native_trigger,
            )
            return
        async with sessions.begin() as session:
            old = await manual_request.admit_manual_request(
                session, **admission, idempotency_key="interrupted"
            )
            assert old.status == (
                "unavailable"
                if expected == "cancelled_before_call"
                else "active"
                if expected == "cycle_reconciled"
                else "unknown"
            )
            row = await session.get(ManualPaperRequest, request.request_id)
            from nexus.paper.manual_entry import read_configured_manual_request

            presented = await read_configured_manual_request(session, settings, request_id=row.id)
            assert presented.preparation_recovery.disposition == expected
            if expected == "cancelled_before_call":
                new = await manual_request.admit_manual_request(
                    session, **admission, idempotency_key="independent"
                )
                assert new.status == "queued" and new.request_id != old.request_id
            else:
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await manual_request.admit_manual_request(
                        session, **admission, idempotency_key="independent"
                    )
        app = create_engine_app(settings)
        path = f"/internal/v1/paper/manual-requests/{request.request_id}"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, response.text
            assert response.json()["data"]["preparation_recovery"]["disposition"] == expected
        async with sessions.begin() as session:
            from sqlalchemy import text

            await session.execute(text("SET TRANSACTION READ ONLY"))
            row = await session.get(ManualPaperRequest, request.request_id)
            assert (await recovery.read_preparation_recovery(session, row)).disposition == expected
        if failure_point == "late_claim_change":
            async with sessions.begin() as session:
                claim = await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.idempotency_key == str(request.request_id)
                    )
                )
                assert claim is not None
                claim.response_payload = {
                    "outcome": "failed",
                    "error_code": "AGENT_PAPER_RESULT_UNKNOWN",
                }
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                failed = await client.get(path, headers=headers("GET", path))
                assert failed.status_code == 409
                assert failed.json()["error"]["details"]["reason_code"] == recovery.BLOCKED
                assert "data" not in failed.json()
        # An expired execution identity cannot re-enter external work or create a cycle.
        if expected != "cycle_reconciled":
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match=recovery.BLOCKED):
                    await recovery.begin_preparation_effects(
                        session, request_id=request.request_id, execution_id=starts[0].id
                    )
    finally:
        await engine.dispose()


async def _seed_acceptance_budget(session, entry, model, case):
    """Historical unresolved usage, shared by recovery and subsequent execution tests."""
    from decimal import Decimal

    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentPromptVersionRecord,
        AgentResearchRunRecord,
    )

    prompt = await session.scalar(
        select(AgentPromptVersionRecord)
        .where(AgentPromptVersionRecord.role == "orchestrator")
        .limit(1)
    )
    budget_run = AgentResearchRunRecord(
        instrument_id=entry.instrument_id,
        data_snapshot_id=entry.research_snapshot_id,
        prompt_version_id=prompt.id,
        model_config_id=model.id,
        decision_time=AT,
        actor_id="independent-budget-fixture",
        idempotency_key="unsettled-acceptance",
        status="prepared" if case.startswith("accept_prepared_") else "failed",
        schema_hash="a" * 64,
        model_hash=model.config_hash,
        prompt_hash=prompt.content_hash,
        prompt_manifest={},
        evidence_hash="b" * 64,
        evidence_manifest=[{"source": "fixture"}],
        tool_hash="c" * 64,
        input_hash="d" * 64,
        reserved_cost_usd=Decimal("1"),
        settled_cost_usd=None,
        pricing_version="fixture",
        input_tokens=None,
        output_tokens=None,
    )
    session.add(budget_run)
    await session.flush()
    session.add(
        AgentDailyBudgetReservationRecord(
            business_date=AT.date(),
            run_id=budget_run.id,
            pricing_version="fixture",
            reserved_usd=Decimal("1"),
            settled_usd=None,
            state="reserved",
        )
    )
    if case.startswith("accept_prepared_"):
        await _seed_closed_prepared_run(session, budget_run, case)
    return budget_run


async def _seed_closed_prepared_run(session, run, case):
    from uuid import uuid4

    from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
    from nexus.jobs.repository import enqueue_job
    from nexus.persistence.models.runtime import JobAttempt, JobLease

    run.status = "running" if case == "accept_prepared_running" else "prepared"
    if case == "accept_prepared_missing_job":
        return
    digest = job_input_hash(
        {
            "instrument_id": str(run.instrument_id),
            "data_snapshot_id": str(run.data_snapshot_id),
            "decision_time": run.decision_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "request_hash": run.input_hash,
            "schema_hash": run.schema_hash,
            "model_hash": run.model_hash,
            "prompt_hash": run.prompt_hash,
            "evidence_hash": run.evidence_hash,
            "tool_hash": run.tool_hash,
            "pricing_version": run.pricing_version,
        }
    )
    job = await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=JobType.AGENT_RESEARCH,
            scope_kind="instrument",
            scope_id=str(run.instrument_id),
            effective_at=run.decision_time,
            input_hash="e" * 64 if case == "accept_prepared_identity" else digest,
        ),
        schema_version="agent_research_job_v1",
        queue=QueueName.GENERAL,
        payload={"run_id": str(run.id)},
        retry_policy=RetryPolicy.MANUAL_SAFE
        if case == "accept_prepared_retryable"
        else RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=1,
    )
    start, end = AT - timedelta(minutes=5), AT - timedelta(seconds=1)
    if case == "accept_prepared_future":
        end = AT + timedelta(seconds=1)
    job.status = "running"
    job.attempt_count = 1
    job.started_at = start
    job.version += 1
    await session.flush()
    job.status = "succeeded" if case == "accept_prepared_succeeded" else "dead_letter"
    job.finished_at = end
    job.last_error_code = "LEASE_EXPIRED"
    job.version += 1
    token = uuid4()
    session.add(
        JobLease(
            job_id=job.id,
            worker_id="historical-fixture",
            lease_token=token,
            fence_token=2 if case == "accept_prepared_fence" else 1,
            acquired_at=start,
            heartbeat_at=start,
            expires_at=AT + timedelta(seconds=1) if case == "accept_prepared_live_lease" else end,
        )
    )
    if case != "accept_prepared_missing_attempt":
        session.add(
            JobAttempt(
                job_id=job.id,
                attempt_no=1,
                lease_token=token,
                fence_token=1,
                worker_id="historical-fixture",
                started_at=start,
                finished_at=end,
                outcome="lost",
                error_code="LEASE_EXPIRED",
            )
        )


@pytest.mark.asyncio
async def test_prepared_execution_gate_keeps_account_history_seal_required(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_terminal_cycle_cancel_pg import (
        test_legacy_terminal_recovery_skips_only_unused_jobs_and_is_idempotent as legacy_case,
    )
    from nexus.paper.current_preparation_acceptance import require_no_live_research_runs
    from nexus.persistence.session import session_factory
    from sqlalchemy import func

    original = manual_paper.run
    checked = 0
    # This inherited fixture uses a synthetic release; provenance is covered by
    # the normal current-recovery CLI cases, not by this historical seal boundary.
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_args: None)

    async def verify_seal_boundary(settings, args):
        nonlocal checked
        if args.action != "seal-failed-research":
            return await original(settings, args)
        args.repo_root = tmp_path
        factory = session_factory(settings)
        try:
            async with factory.begin() as session:
                now = await session.scalar(select(func.clock_timestamp()))
                await require_no_live_research_runs(session, now=now)
                if checked == 0:
                    with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                        await manual_request.require_manual_account_available(
                            session, account_id=settings.agent_paper_account_id, now=now
                        )
            receipt = await original(settings, args)
            async with factory.begin() as session:
                now = await session.scalar(select(func.clock_timestamp()))
                await require_no_live_research_runs(session, now=now)
                await manual_request.require_manual_account_available(
                    session, account_id=settings.agent_paper_account_id, now=now
                )
            checked += 1
            return receipt
        finally:
            await factory.kw["bind"].dispose()

    monkeypatch.setattr(manual_paper, "run", verify_seal_boundary)
    await legacy_case(migrated_database_url, monkeypatch, "v4", True, False, tmp_path)
    assert checked == 2


async def _check_explicit_acceptance(
    case,
    settings,
    args,
    sessions,
    request,
    admission,
    start,
    business_facts,
    before,
    monkeypatch,
    trigger,
    authority,
    tmp_path,
    native_trigger,
):
    from nexus.jobs.models import job_input_hash
    from nexus.paper import current_preparation_acceptance as acceptance
    from nexus.paper import current_preparation_recovery as recovery
    from nexus.paper.manual_entry import read_configured_manual_request
    from nexus.persistence.models.runtime import IdempotencyRecord

    args = manual_paper.parser().parse_args(
        [
            "--repo-root",
            str(tmp_path),
            "recover-preparation",
            "--request-id",
            str(request.request_id),
            "--accept-unknown-effects",
        ]
    )
    if case.startswith("accept_prepared_") and case != "accept_prepared_closed":
        with pytest.raises(ValueError, match=recovery.BLOCKED):
            await manual_paper.run(settings, args)
        assert await business_facts() == before
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(AuditEvent.id).where(AuditEvent.action == acceptance.ACTION)
                )
                is None
            )
        return
    if case in {"accept_queued_job", "accept_running_job", "accept_lease"}:
        from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy
        from nexus.jobs.repository import enqueue_job

        async with sessions.begin() as session:
            job = await enqueue_job(
                session,
                key=JobKeyV1(
                    job_type=JobType.AGENT_RESEARCH,
                    scope_kind="portfolio",
                    scope_id=str(admission["account_id"]),
                    effective_at=AT,
                    input_hash=job_input_hash({"fixture": "occupied"}),
                ),
                schema_version="isolated_v1",
                queue=QueueName.GENERAL,
                payload={"fixture": "occupied"},
                retry_policy=RetryPolicy.NEVER,
                max_attempts=1,
            )
            if case in {"accept_running_job", "accept_lease"}:
                job.status = "running"
                job.started_at = AT
                job.version += 1
                await session.flush()
            if case == "accept_lease":
                from uuid import uuid4

                from nexus.persistence.models.runtime import JobLease

                job.status = "failed"
                job.version += 1
                job.started_at = job.finished_at = AT
                await session.flush()
                session.add(
                    JobLease(
                        job_id=job.id,
                        worker_id="isolated",
                        lease_token=uuid4(),
                        fence_token=1,
                        acquired_at=AT,
                        heartbeat_at=AT,
                        expires_at=AT + timedelta(seconds=210),
                    )
                )
        check_args = manual_paper.parser().parse_args(
            [
                "--repo-root",
                str(tmp_path),
                "check-preparation-acceptance",
                "--request-id",
                str(request.request_id),
            ]
        )
        with pytest.raises(ValueError, match=recovery.BLOCKED):
            await manual_paper.run(settings, check_args)
        with pytest.raises(ValueError, match=recovery.BLOCKED):
            await manual_paper.run(settings, args)
        return
    if case in {"accept_writer_contention", "accept_audit_contention"}:
        from sqlalchemy import text

        check_args = manual_paper.parser().parse_args(
            [
                "--repo-root",
                str(tmp_path),
                "check-preparation-acceptance",
                "--request-id",
                str(request.request_id),
            ]
        )
        async with sessions.begin() as writer:
            # The normal producer holds this lock after flushing a Run and before
            # enqueuing its Job; keep that deterministic boundary open throughout.
            if case == "accept_audit_contention":
                await writer.execute(
                    text(
                        "SELECT pg_advisory_xact_lock(hashtextextended('nexus:audit-chain', 2020))"
                    )
                )
            else:
                await writer.execute(
                    text("UPDATE agent_research_runs SET status=status WHERE false")
                )
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await asyncio.wait_for(manual_paper.run(settings, check_args), timeout=10)
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await asyncio.wait_for(manual_paper.run(settings, args), timeout=10)
            # The failed maintenance transaction released its earlier jobs lock.
            await writer.execute(text("LOCK TABLE jobs IN ROW EXCLUSIVE MODE NOWAIT"))
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(AuditEvent.id).where(AuditEvent.action == acceptance.ACTION).limit(1)
                )
                is None
            )
        assert await business_facts() == before
        return
    if case == "accept_open_order":
        from nexus.paper.projection import load_paper_projection
        from nexus.paper.target_service import TargetActivationBlockedError

        async with sessions() as session:
            projection = await load_paper_projection(
                session, account_id=admission["account_id"], as_of=AT
            )
            assert projection.open_orders and projection.reserved_cash > 0
        with pytest.raises(TargetActivationBlockedError):
            await manual_paper.run(settings, args)
        assert await business_facts() == before
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(AuditEvent.id).where(AuditEvent.action == acceptance.ACTION).limit(1)
                )
                is None
            )
        return
    if case == "accept_other_unknown":
        async with sessions.begin() as session:
            original = await session.get(ManualPaperRequest, request.request_id)
            other = ManualPaperRequest(
                **{
                    k: getattr(original, k)
                    for k in (
                        "account_id",
                        "candidate_batch_id",
                        "model_config_id",
                        "model_hash",
                        "batch_hash",
                        "mode",
                        "request_hash",
                        "requested_at",
                    )
                },
                key_hash="b" * 64,
            )
            session.add(other)
            await session.flush()
            await manual_request._append(
                session, other, status="queued", code="MANUAL_REQUEST_QUEUED", at=AT
            )
            for status, code in [
                ("preparing", "MANUAL_PREPARATION_STARTED"),
                ("unknown", "MANUAL_RESULT_UNKNOWN"),
            ]:
                await manual_request.record_manual_request_event(
                    session, other, status=status, code=code
                )
        with pytest.raises(ValueError):
            await manual_paper.run(settings, args)
        return
    if case == "accept_live_producer":

        def live(*_):
            raise ValueError(recovery.BLOCKED)

        monkeypatch.setattr(recovery, "_require_execution_producer_stopped", live)
        with pytest.raises(ValueError, match=recovery.BLOCKED):
            await manual_paper.run(settings, args)
        return
    if case == "accept_audit_rollback":

        async def fail(*_a, **_k):
            raise RuntimeError("isolated audit failure")

        with monkeypatch.context() as m:
            m.setattr(acceptance, "append_audit_event", fail)
            with pytest.raises(RuntimeError, match="isolated audit failure"):
                await manual_paper.run(settings, args)
        assert await business_facts() == before
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await manual_request.admit_manual_request(
                    session, **admission, idempotency_key="new"
                )
        return
    if case in {"accept_cleanup_unknown", "accept_commit_ack_lost"}:
        from types import SimpleNamespace

        factory = manual_paper.session_factory
        disposed = []

        def with_failed_cleanup(config):
            real = factory(config)

            async def dispose():
                await real.kw["bind"].dispose()
                disposed.append(True)
                if case == "accept_cleanup_unknown":
                    raise RuntimeError("isolated cleanup after commit")

            class Factory:
                kw = {"bind": SimpleNamespace(dispose=dispose)}

                def __call__(self):
                    session = real()
                    original_commit = session.commit

                    async def commit():
                        await original_commit()
                        # Real commit succeeded but its acknowledgement is lost.
                        if case == "accept_commit_ack_lost":
                            raise RuntimeError("isolated commit acknowledgement lost")

                    session.commit = commit
                    return session

            return Factory()

        with monkeypatch.context() as m:
            m.setattr(manual_paper, "session_factory", with_failed_cleanup)
            with pytest.raises(RuntimeError, match="isolated") as caught:
                await manual_paper.run(settings, args)
            fields = manual_paper._operation_fields(caught.value)
            assert fields == {
                "operation_stage": "engine_dispose"
                if case == "accept_cleanup_unknown"
                else "transaction_commit",
                "operation_code": "UNEXPECTED_EXCEPTION",
                "transaction_outcome": "committed"
                if case == "accept_cleanup_unknown"
                else "unconfirmed",
            }
        assert disposed == [True]
        assert await business_facts() == before
        # An exit/cleanup failure can follow a committed acceptance. Never
        # infer rollback from the caller's failure or retry to discover it.
        async with sessions() as session:
            row = await session.get(ManualPaperRequest, request.request_id)
            assert await acceptance.current_unknown_accepted(session, row)
            audits = list(
                await session.scalars(
                    select(AuditEvent).where(AuditEvent.action == acceptance.ACTION)
                )
            )
            assert len(audits) == 1
        return
    if case in {"accept_flush_rollback", "accept_precommit_rollback", "accept_commit_rollback"}:
        from nexus.paper.history_seal import _row_hash
        from nexus.persistence.models.manual_paper import ManualPaperRequestEvent
        from sqlalchemy import text
        from sqlalchemy.exc import DBAPIError

        async def stored_facts():
            facts = await business_facts()
            async with sessions() as session:
                for table in (ManualPaperRequest, ManualPaperRequestEvent, AuditEvent):
                    facts[table.__tablename__] = sorted(
                        _row_hash(row) for row in await session.scalars(select(table))
                    )
            return facts

        original = await stored_facts()
        appended = []
        append = acceptance.append_audit_event

        async def observed_append(*a, **k):
            result = await append(*a, **k)
            appended.append(True)
            if case == "accept_flush_rollback":
                raise RuntimeError("isolated post-flush failure")
            return result

        def source_recheck(*_args):
            if appended:
                raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")

        if case == "accept_commit_rollback":
            # A real PostgreSQL deferred constraint trigger fires only at COMMIT,
            # after append/flush, producer recheck and proof readback all succeeded.
            async with sessions.begin() as session:
                await session.execute(
                    text("""
                    CREATE FUNCTION reject_acceptance_commit() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN
                    IF NEW.action = 'paper.manual_preparation.unknown_accepted' THEN
                      RAISE EXCEPTION 'isolated deferred commit rejection';
                    END IF; RETURN NEW; END $$
                """)
                )
                await session.execute(
                    text("""
                    CREATE CONSTRAINT TRIGGER reject_acceptance_commit
                    AFTER INSERT ON audit_events DEFERRABLE INITIALLY DEFERRED
                    FOR EACH ROW EXECUTE FUNCTION reject_acceptance_commit()
                """)
                )
        with monkeypatch.context() as m:
            m.setattr(acceptance, "append_audit_event", observed_append)
            if case == "accept_precommit_rollback":
                m.setattr(manual_paper, "_require_maintenance_code", source_recheck)
            expected = (
                DBAPIError
                if case == "accept_commit_rollback"
                else (ValueError if case == "accept_precommit_rollback" else RuntimeError)
            )
            message = {
                "accept_commit_rollback": "isolated deferred commit rejection",
                "accept_precommit_rollback": "MANUAL_CONFIGURATION_UNAVAILABLE",
                "accept_flush_rollback": "isolated post-flush failure",
            }[case]
            with pytest.raises(expected, match=message) as caught:
                await manual_paper.run(settings, args)
            fields = manual_paper._operation_fields(caught.value)
            assert fields["operation_stage"] == (
                "transaction_commit" if case == "accept_commit_rollback" else "transaction_body"
            )
            assert fields["transaction_outcome"] == "unconfirmed"
            assert (
                fields["operation_code"]
                == {
                    "accept_commit_rollback": "DATABASE_ERROR",
                    "accept_precommit_rollback": "VALIDATION_REJECTED",
                    "accept_flush_rollback": "UNEXPECTED_EXCEPTION",
                }[case]
            )
        assert appended == [True]
        assert await stored_facts() == original
        return
    if case == "accept_host_diagnostic_rollback":
        from types import SimpleNamespace

        from nexus.paper import manual_preparation_close as close
        from nexus.paper.history_seal import _row_hash
        from nexus.persistence.models.manual_paper import ManualPaperRequestEvent

        async def stored_facts():
            facts = await business_facts()
            async with sessions() as session:
                for table in (ManualPaperRequest, ManualPaperRequestEvent, AuditEvent):
                    facts[table.__tablename__] = sorted(
                        _row_hash(row) for row in await session.scalars(select(table))
                    )
            return facts

        original = await stored_facts()
        audit_appended = []
        append = acceptance.append_audit_event

        async def append_then_fail_inspection(*a, **k):
            result = await append(*a, **k)
            audit_appended.append(True)
            return result

        def status(argv, **kwargs):
            assert "status" in argv and kwargs["timeout"] == 15
            return subprocess.CompletedProcess(
                argv, 1 if audit_appended else 0, '{"status":"inactive"}', "private-stderr"
            )

        with monkeypatch.context() as m:
            m.setattr(acceptance, "append_audit_event", append_then_fail_inspection)
            # Only OS boundaries are substituted; run the real status classifier
            # before and after the real audit append in the normal CLI transaction.
            m.setattr(close, "subprocess", SimpleNamespace(run=status))
            m.setattr(close, "require_research_processes_quiescent", lambda: None)
            m.setattr(
                recovery,
                "_require_execution_producer_stopped",
                lambda _, root, env: close.require_preparation_producers_stopped(root, env),
            )
            with pytest.raises(ValueError, match="MANUAL_PREPARATION_CLOSE_BLOCKED") as failure:
                await manual_paper.run(settings, args)
        assert isinstance(failure.value, close.PreparationCloseDiagnosticError)
        assert failure.value.safe_diagnostics() == {
            "diagnostic_stage": "host_status",
            "diagnostic_code": "INSPECTION_FAILED",
            "diagnostic_point": "accept_producer_recheck",
        }
        assert audit_appended == [True]
        assert await stored_facts() == original
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await manual_request.admit_manual_request(
                    session, **admission, idempotency_key="new-after-diagnostic-failure"
                )
        return
    if case in {"accept_prepared_closed", "accept_check_late"}:
        from nexus.paper.history_seal import _row_hash
        from nexus.persistence.models.manual_paper import ManualPaperRequestEvent
        from nexus.persistence.models.runtime import JobAttempt, JobLease

        async def stored_facts():
            facts = await business_facts()
            async with sessions() as session:
                for table in (
                    ManualPaperRequest,
                    ManualPaperRequestEvent,
                    AuditEvent,
                    JobAttempt,
                    JobLease,
                ):
                    facts[table.__tablename__] = sorted(
                        _row_hash(row) for row in await session.scalars(select(table))
                    )
            return facts

        check_args = manual_paper.parser().parse_args(
            [
                "--repo-root",
                str(tmp_path),
                "check-preparation-acceptance",
                "--request-id",
                str(request.request_id),
            ]
        )
        original = await stored_facts()

        async def forbidden_write(*_a, **_k):
            raise AssertionError("inspection must not call the audit writer")

        with monkeypatch.context() as m:
            m.setattr(acceptance, "append_audit_event", forbidden_write)
            missing_args = manual_paper.parser().parse_args(
                [
                    "--repo-root",
                    str(tmp_path),
                    "check-preparation-acceptance",
                    "--request-id",
                    str(UUID(int=1)),
                ]
            )
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await manual_paper.run(settings, missing_args)
            checked = await manual_paper.run(settings, check_args)
        assert checked.status == "eligible_at_check"
        assert checked.request_id == request.request_id
        assert checked.check_only and checked.execution_locked and checked.budget_preserved
        assert checked.checked_at == AT
        assert await stored_facts() == original
        if case == "accept_check_late":
            # A check is an observation, never a capability consumed by accept.
            async with sessions.begin() as session:
                session.add(
                    IdempotencyRecord(
                        actor_id="local-agent-paper-schwab:" + str(admission["account_id"]),
                        idempotency_key=str(request.request_id),
                        route="agent-paper-explicit-manual-v1",
                        schema_version="agent_paper_explicit_manual_v1",
                        request_hash=job_input_hash({"late_after_check": True}),
                        response_payload={},
                        created_at=AT,
                    )
                )
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await manual_paper.run(settings, args)
            changed = await stored_facts()
            assert changed["audit_events"] == original["audit_events"]
            return
    if case == "accept_concurrent":
        first, second = await asyncio.gather(
            manual_paper.run(settings, args),
            manual_paper.run(settings, args),
        )
        assert sorted([first.replayed, second.replayed]) == [False, True]
    else:
        first = await manual_paper.run(settings, args)
        assert first.replayed is False
    assert first.status == "preparation_unknown_acknowledged"
    assert await business_facts() == before
    assert (await manual_paper.run(settings, args)).replayed
    if case == "accept_corrupt_audit":
        from sqlalchemy import text

        async with sessions.begin() as session:
            await session.execute(text("ALTER TABLE audit_events DISABLE TRIGGER USER"))
            audit = await session.scalar(
                select(AuditEvent).where(AuditEvent.action == acceptance.ACTION)
            )
            audit.payload = {**audit.payload, "budget_preserved": False}
            await session.flush()
            await session.execute(text("ALTER TABLE audit_events ENABLE TRIGGER USER"))
        async with sessions.begin() as session:
            row = await session.get(ManualPaperRequest, request.request_id)
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await read_configured_manual_request(session, settings, request_id=row.id)
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await recovery.require_preparation_execution(session, row, execution_id=start.id)
        return
    async with sessions.begin() as session:
        old = await read_configured_manual_request(session, settings, request_id=request.request_id)
        assert old.status == "unknown" and old.cycle_id is None
        assert old.preparation_disposition == "unknown_acknowledged"
        assert old.preparation_recovery is None
        assert (
            await manual_paper_host.peek_manual_request(session, account_id=admission["account_id"])
            is None
        )
        row = await session.get(ManualPaperRequest, request.request_id)
        with pytest.raises(ValueError, match=recovery.BLOCKED):
            await recovery.require_preparation_execution(session, row, execution_id=start.id)
        assert (
            await manual_request.admit_manual_request(
                session, **admission, idempotency_key="interrupted"
            )
        ).status == "unknown"
    if case == "accept_late_claim":
        async with sessions.begin() as session:
            session.add(
                IdempotencyRecord(
                    actor_id="local-agent-paper-schwab:" + str(admission["account_id"]),
                    idempotency_key=str(request.request_id),
                    route="agent-paper-explicit-manual-v1",
                    schema_version="agent_paper_explicit_manual_v1",
                    request_hash=job_input_hash({"late": True}),
                    response_payload={},
                    created_at=AT,
                )
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await read_configured_manual_request(
                    session, settings, request_id=request.request_id
                )
            row = await session.get(ManualPaperRequest, request.request_id)
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await recovery.require_preparation_execution(session, row, execution_id=start.id)
            with pytest.raises(ValueError):
                await manual_request.admit_manual_request(
                    session, **admission, idempotency_key="new"
                )
        return
    # Recovery must compose with the configured user entry, including the host,
    # version and trading-session gates, not only the lower-level admission helper.
    browser_entry = (
        os.environ.get("NEXUS_CONTROLLED_BROWSER") == "1" and case == "accept_prepared_closed"
    )
    if browser_entry:
        settings.release_revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    await heartbeat_agent_paper_loop(
        settings,
        account_id=admission["account_id"],
        worker_id=f"agent-paper-loop-{UUID(int=1)}",
        started_at=AT - timedelta(minutes=1),
        seen_at=AT,
    )
    if browser_entry:
        from helpers.controlled_browser import verify_controlled_browser

        new_id = await verify_controlled_browser(
            settings.database_url,
            api_now=AT,
            cycle_id=None,
            order_ids=(),
            manual_settings=settings,
            submit_manual=True,
            recovered_manual_request_id=request.request_id,
        )
    else:
        app = create_engine_app(settings)
        path = "/internal/v1/paper/manual-requests"
        body = b'{"mode":"research_and_paper"}'
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    path,
                    content=body,
                    headers={**headers("POST", path, body), "Idempotency-Key": "new"},
                )
            assert response.status_code == 200, response.text
            assert response.json()["data"]["status"] == "queued"
            new_id = UUID(response.json()["data"]["request_id"])
        finally:
            await app.state.session_factory.kw["bind"].dispose()
    assert new_id is not None and new_id != request.request_id
    assert await business_facts() == before
    async with sessions.begin() as session:
        new = await read_configured_manual_request(session, settings, request_id=new_id)
        assert new.status == "queued" and new.mode == "research_and_paper"

    async def source(*_a, **_k):
        return authority

    monkeypatch.setattr(agent_paper_once, "trigger_once", native_trigger)
    monkeypatch.setattr(agent_paper_once, "prepare_schwab_decision_authority", source)
    monkeypatch.setattr(agent_paper_once, "S3ProviderEvidenceStore", lambda *_a, **_k: object())
    await manual_paper_host.dispatch_manual_request(
        settings,
        trigger,
        request_id=new.request_id,
        repo_root=tmp_path,
        calendar_authority_id=CALENDAR_ID,
        process_environment={},
    )
    after = await business_facts()
    for table, original_hashes in before.items():
        assert set(original_hashes).issubset(after[table]), table
    async with sessions.begin() as session:
        current = await read_configured_manual_request(session, settings, request_id=new.request_id)
        assert current.status == "active" and current.cycle_id is not None
        old = await read_configured_manual_request(session, settings, request_id=request.request_id)
        assert old.preparation_disposition == "unknown_acknowledged"
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await manual_request.admit_manual_request(session, **admission, idempotency_key="third")

    def live_again(*_):
        raise AssertionError("replay must not require the new producer to stop")

    monkeypatch.setattr(recovery, "_require_execution_producer_stopped", live_again)
    assert (await manual_paper.run(settings, args)).replayed
    if case == "accept_prepared_closed":
        before_check = await stored_facts()
        checked = await manual_paper.run(settings, check_args)
        assert checked.status == "already_acknowledged" and checked.check_only
        assert await stored_facts() == before_check
    if case == "accept_late_cycle":
        from nexus.persistence.models.paper import AgentPaperCycleEventRecord
        from sqlalchemy import text

        async with sessions.begin() as session:
            await session.execute(text("ALTER TABLE agent_paper_cycle_events DISABLE TRIGGER USER"))
            event = await session.scalar(
                select(AgentPaperCycleEventRecord).where(
                    AgentPaperCycleEventRecord.cycle_id == current.cycle_id,
                    AgentPaperCycleEventRecord.sequence == 1,
                )
            )
            event.fact_ids = {**event.fact_ids, "manual_request_id": str(request.request_id)}
            await session.flush()
            await session.execute(text("ALTER TABLE agent_paper_cycle_events ENABLE TRIGGER USER"))
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await read_configured_manual_request(
                    session, settings, request_id=request.request_id
                )
            row = await session.get(ManualPaperRequest, request.request_id)
            with pytest.raises(ValueError, match=recovery.BLOCKED):
                await recovery.require_preparation_execution(session, row, execution_id=start.id)
        return
    # GET uses the same validated presentation, including the original unknown.
    app = create_engine_app(settings)
    path = f"/internal/v1/paper/manual-requests/{request.request_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        assert response.json()["data"]["preparation_disposition"] == "unknown_acknowledged"

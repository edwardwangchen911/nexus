"""Current host crash boundaries on isolated PG; no live source/model calls."""

import asyncio
from contextlib import nullcontext
from datetime import timedelta

import httpx
import pytest
from helpers.paper_api import headers
from helpers.paper_api import settings as api_settings
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.main import create_engine_app
from nexus.ops import agent_paper_once, manual_paper, manual_paper_host
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
    ],
)
@pytest.mark.asyncio
async def test_current_host_interruption_recovers_via_normal_cli(
    migrated_database_url, monkeypatch, tmp_path, failure_point
):
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **V9_OPTIONS,
        formal_flexible_stage_budget=True,
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT, candidate_instruments=INSTRUMENTS
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

        before = await business_facts()

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

"""Normal maintenance preserves a scheduled unknown and fences its old writes."""

import asyncio
from argparse import Namespace
from contextlib import nullcontext
from datetime import timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import frozen_cycle, seed_normal_path
from nexus.ops import manual_paper
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle, manual_preparation_close, manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "wrong_hash",
        "other_unknown",
        "queued",
        "live",
        "changed_claim",
        "damaged_audit",
        "old_snapshot",
        "late_finish",
        "audit_failure",
        "account_lock",
        "budget_lock",
        "schema_lock",
        "accept_first",
        "writer_first",
        "next_cycle",
        "next_natural_slot",
        "late_cycle",
        "damaged_batch",
        "damaged_audit_identity",
        "damaged_audit_action",
    ],
)
@pytest.mark.asyncio
async def test_normal_scheduled_check_and_accept_preserve_unknown(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **(V9_OPTIONS | {"formal_flexible_stage_budget": True}),
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT, candidate_instruments=INSTRUMENTS
    )
    original_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return AT + timedelta(minutes=10)
        if getattr(statement, "is_insert", False) and statement.table.name == "idempotency_records":
            statement = statement.values(created_at=AT)
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lambda *_: nullcontext())
    monkeypatch.setattr(
        manual_preparation_close, "require_preparation_producers_stopped", lambda *_a, **_k: None
    )
    try:
        async with sessions.begin() as s:
            model = await s.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v10-flexible-stage-budget"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                s,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(i.instrument_id for i in manifest),
                idempotency_key="scheduled-close-batch",
                created_at=AT,
            )
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                s,
                account_id=authority.account_id,
                candidate_batch_id=batch.batch_id,
                now=AT,
                client_idempotency_key=f"agent-paper-loop:{AT.isoformat()}",
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            claim = await s.get(IdempotencyRecord, reservation.claim_id)
            await agent_cycle.finish_schwab_agent_paper_trigger(
                s, reservation=reservation, error_code="AGENT_PAPER_TRIGGER_FAILED"
            )
            old_hash = claim.request_hash
            settings = EngineSettings(
                database_url=migrated_database_url,
                agent_runtime_mode="codex_sdk",
                agent_paper_account_id=authority.account_id,
                agent_paper_candidate_batch_id=batch.batch_id,
                agent_paper_formal_model_config_id=model.id,
            )
        args = Namespace(
            action="check-scheduled-acceptance",
            claim_id=reservation.claim_id,
            request_id=reservation.claim_id,
            repo_root=tmp_path,
            env_file=tmp_path / "env",
        )
        async with sessions() as s:
            before = await s.scalar(select(func.count()).select_from(AuditEvent))
            with pytest.raises(ValueError, match="MANUAL_PREPARATION_ACK_BLOCKED"):
                await manual_request.require_manual_account_available(
                    s, account_id=authority.account_id, now=AT + timedelta(minutes=10)
                )
        if fault in {"wrong_hash", "other_unknown", "queued"}:
            async with sessions.begin() as s:
                if fault == "wrong_hash":
                    c = await s.get(IdempotencyRecord, reservation.claim_id)
                    c.request_hash = "a" * 64
                elif fault == "other_unknown":
                    s.add(
                        IdempotencyRecord(
                            actor_id=f"local-agent-paper-schwab:{authority.account_id}",
                            route="agent-paper-schwab-trigger-v2",
                            schema_version="agent_paper_schwab_trigger_v2",
                            idempotency_key=(AT - timedelta(days=1)).isoformat(),
                            request_hash="b" * 64,
                            response_payload={},
                        )
                    )
                else:
                    from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy
                    from nexus.jobs.repository import enqueue_job

                    await enqueue_job(
                        s,
                        key=JobKeyV1(
                            job_type=JobType.AGENT_RESEARCH,
                            scope_kind="portfolio",
                            scope_id=str(authority.account_id),
                            effective_at=AT,
                            input_hash="c" * 64,
                        ),
                        schema_version="isolated_v1",
                        queue=QueueName.GENERAL,
                        payload={},
                        retry_policy=RetryPolicy.NEVER,
                        max_attempts=1,
                    )
        if fault == "live":

            def live(*_a, **_k):
                raise ValueError("SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED")

            monkeypatch.setattr(
                manual_preparation_close, "require_preparation_producers_stopped", live
            )
        if fault in {"wrong_hash", "other_unknown", "queued", "live"}:
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
            async with sessions() as s:
                assert await s.scalar(select(func.count()).select_from(AuditEvent)) == before
            return
        if fault in {"account_lock", "budget_lock", "schema_lock"}:
            async with sessions() as owner:
                if fault == "account_lock":
                    await owner.execute(
                        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,1818))"),
                        {"scope": f"internal-paper:{authority.account_id}"},
                    )
                elif fault == "schema_lock":
                    await owner.execute(text("LOCK TABLE alembic_version IN ACCESS EXCLUSIVE MODE"))
                else:
                    await owner.execute(
                        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                        {"key": f"agent-budget:{AT.date().isoformat()}"},
                    )
                with pytest.raises(ValueError):
                    await asyncio.wait_for(manual_paper.run(settings, args), timeout=5)
                await owner.rollback()
            return
        check = await manual_paper.run(settings, args)
        assert check is not None and check.status == "eligible_at_check"
        async with sessions() as s:
            assert await s.scalar(select(func.count()).select_from(AuditEvent)) == before
        args.action = "accept-scheduled-unknown"
        args.accept_unknown_effects = False
        with pytest.raises(ValueError):
            await manual_paper.run(settings, args)
        args.accept_unknown_effects = True
        if fault == "late_finish":
            async with sessions.begin() as s:
                claim = await s.get(IdempotencyRecord, reservation.claim_id)
                claim.response_payload = {"outcome": "failed", "error_code": "SCHWAB_L1_INCOMPLETE"}
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
            async with sessions() as s:
                assert await s.scalar(select(func.count()).select_from(AuditEvent)) == before
            return
        if fault == "audit_failure":
            from nexus.paper import scheduled_unknown_acceptance as acceptance

            async def reject(*_a, **_k):
                raise ValueError("injected audit failure")

            monkeypatch.setattr(acceptance, "append_audit_event", reject)
            with pytest.raises(ValueError, match="injected audit failure"):
                await manual_paper.run(settings, args)
            async with sessions() as s:
                assert await s.scalar(select(func.count()).select_from(AuditEvent)) == before
                assert (await s.get(IdempotencyRecord, reservation.claim_id)).response_payload[
                    "external_effects"
                ] == "unknown"
            return
        if fault == "old_snapshot":
            from nexus.paper.scheduled_unknown_acceptance import require_scheduled_execution_open

            async with sessions() as stale:
                await stale.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                await stale.scalar(select(func.count()).select_from(AuditEvent))
                await manual_paper.run(settings, args)
                with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await require_scheduled_execution_open(
                        stale, account_id=authority.account_id, slot=AT
                    )
                await stale.rollback()
            return
        if fault == "writer_first":
            async with sessions.begin() as writer:
                # A pre-acceptance writer already owns the account fence and commits
                # a late old cycle; acceptance must reject both during and after it.
                from nexus.paper.target_service import lock_portfolio_account

                await lock_portfolio_account(writer, authority.account_id)
                late = frozen_cycle(authority, manifest)
                late.candidate_batch_id = batch.batch_id
                late.sizing_policy_hash = agent_cycle.PORTFOLIO_ALLOCATION_POLICY_HASH
                writer.add(late)
                await writer.flush()
                with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await asyncio.wait_for(manual_paper.run(settings, args), timeout=10)
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
            async with sessions() as s:
                assert await s.scalar(select(func.count()).select_from(AuditEvent)) == before
            return
        if fault == "accept_first":
            from nexus.paper import scheduled_unknown_acceptance as acceptance

            audit_written, release_accept, writer_started = (asyncio.Event() for _ in range(3))
            original_append = acceptance.append_audit_event
            original_lock = acceptance.lock_portfolio_account

            async def paused_append(*a, **kw):
                result = await original_append(*a, **kw)
                audit_written.set()
                await release_accept.wait()
                return result

            async def observed_lock(*a, **kw):
                writer_started.set()
                return await original_lock(*a, **kw)

            async def late_writer():
                async with sessions.begin() as writer:
                    with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                        await agent_cycle.finish_schwab_agent_paper_trigger(
                            writer, reservation=reservation, error_code="SCHWAB_L1_INCOMPLETE"
                        )

            monkeypatch.setattr(acceptance, "append_audit_event", paused_append)
            monkeypatch.setattr(acceptance, "lock_portfolio_account", observed_lock)
            accept_task = asyncio.create_task(manual_paper.run(settings, args))
            await asyncio.wait_for(audit_written.wait(), timeout=10)
            writer_task = asyncio.create_task(late_writer())
            await asyncio.wait_for(writer_started.wait(), timeout=10)
            release_accept.set()
            accepted = await asyncio.wait_for(accept_task, timeout=10)
            await asyncio.wait_for(writer_task, timeout=10)
        else:
            accepted = await manual_paper.run(settings, args)
        assert accepted.status == "scheduled_unknown_acknowledged" and not accepted.replayed
        repeated = await manual_paper.run(settings, args)
        assert repeated.replayed
        async with sessions() as s:
            assert await s.scalar(select(func.count()).select_from(AuditEvent)) == before + 1
            claim = await s.get(IdempotencyRecord, reservation.claim_id)
            assert claim.request_hash == old_hash
            assert claim.response_payload == {
                "outcome": "failed",
                "error_code": "AGENT_PAPER_TRIGGER_FAILED",
                "external_effects": "unknown",
            }
            await manual_request.require_manual_account_available(
                s, account_id=authority.account_id, now=AT + timedelta(minutes=10)
            )
        if fault == "next_natural_slot":
            async with sessions.begin() as s:
                with pytest.raises(ValueError, match="AGENT_PAPER_OUTSIDE_WINDOW"):
                    await agent_cycle.reserve_schwab_agent_paper_trigger(
                        s,
                        account_id=authority.account_id,
                        candidate_batch_id=batch.batch_id,
                        now=AT + timedelta(minutes=5),
                        client_idempotency_key="extra-same-day",
                        formal_stage_capacity=True,
                        formal_model_config_id=model.id,
                    )
                next_slot = AT + timedelta(days=3)  # Friday to the next Monday, 10:00 New York.
                later = await agent_cycle.reserve_schwab_agent_paper_trigger(
                    s,
                    account_id=authority.account_id,
                    candidate_batch_id=batch.batch_id,
                    now=next_slot,
                    client_idempotency_key=f"agent-paper-loop:{next_slot.isoformat()}",
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                )
                assert later.claim_id != reservation.claim_id
                assert later.decision_slot_at == next_slot
            args.action = "check-scheduled-acceptance"
            assert (await manual_paper.run(settings, args)).status == "already_acknowledged"
            return
        if fault == "next_cycle":
            # Normal native creation for another slot preserves the prior acceptance proof.
            async with sessions.begin() as s:
                started = await agent_cycle.start_schwab_agent_paper_cycle(
                    s,
                    authority=authority,
                    decision_slot_at=AT + timedelta(minutes=5),
                    idempotency_key="independent-later-slot",
                    requested_instrument_ids=tuple(i.instrument_id for i in manifest),
                    candidate_batch_id=batch.batch_id,
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                )
                assert started.cycle_id is not None
            args.action = "check-scheduled-acceptance"
            assert (await manual_paper.run(settings, args)).status == "already_acknowledged"
            return
        # A new independent manual request is admitted; it cannot consume the old slot.
        async with sessions.begin() as s:
            newer = await manual_request.admit_manual_request(
                s,
                account_id=authority.account_id,
                batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_and_paper",
                idempotency_key="independent-new-request",
            )
            assert newer.status == "queued"
        args.action = "check-scheduled-acceptance"
        assert (await manual_paper.run(settings, args)).status == "already_acknowledged"
        if fault in {
            "changed_claim",
            "damaged_audit",
            "damaged_batch",
            "damaged_audit_identity",
            "damaged_audit_action",
            "late_cycle",
        }:
            async with sessions.begin() as s:
                if fault == "changed_claim":
                    claim = await s.get(IdempotencyRecord, reservation.claim_id)
                    claim.response_payload = {
                        "outcome": "failed",
                        "error_code": "SCHWAB_L1_INCOMPLETE",
                    }
                elif fault == "late_cycle":
                    late = frozen_cycle(authority, manifest)
                    late.candidate_batch_id = batch.batch_id
                    late.sizing_policy_hash = agent_cycle.PORTFOLIO_ALLOCATION_POLICY_HASH
                    s.add(late)
                    await s.flush()
                    late_id = late.id
                elif fault == "damaged_batch":
                    await s.execute(
                        text("ALTER TABLE agent_paper_candidate_batches DISABLE TRIGGER USER")
                    )
                    await s.execute(
                        text(
                            "UPDATE agent_paper_candidate_batches "
                            "SET candidate_instrument_ids=candidate_instrument_ids[1:1] "
                            "WHERE id=:id"
                        ),
                        {"id": batch.batch_id},
                    )
                    await s.execute(
                        text("ALTER TABLE agent_paper_candidate_batches ENABLE TRIGGER USER")
                    )
                else:
                    audit = await s.scalar(
                        select(AuditEvent).where(
                            AuditEvent.action == "paper.scheduled_unknown.accepted"
                        )
                    )
                    # Deliberate isolated corruption, outside normal immutable APIs.
                    await s.execute(text("ALTER TABLE audit_events DISABLE TRIGGER USER"))
                    await s.execute(
                        text(
                            "UPDATE audit_events SET payload='{}' WHERE id=:id"
                            if fault == "damaged_audit_identity"
                            else "UPDATE audit_events SET action='damaged' WHERE id=:id"
                            if fault == "damaged_audit_action"
                            else "UPDATE audit_events SET canonical_event='{}' WHERE id=:id"
                        ),
                        {"id": audit.id},
                    )
                    await s.execute(text("ALTER TABLE audit_events ENABLE TRIGGER USER"))
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
        from nexus.ops.formal_research_preparation import (
            FormalPreparationContext,
            require_preparation_scope,
        )
        from nexus.paper.scheduled_unknown_acceptance import require_scheduled_execution_open

        async with sessions() as s:
            if fault == "none":
                for same_slot in (
                    AT,
                    AT.astimezone(ZoneInfo("America/New_York")),
                    AT.replace(tzinfo=None),
                ):
                    with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                        await require_scheduled_execution_open(
                            s, account_id=authority.account_id, slot=same_slot
                        )
            if fault == "late_cycle":
                with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await agent_cycle.resume_schwab_agent_paper_cycle(
                        s, cycle_id=late_id, idempotency_key="old-resume", now=AT
                    )
            with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                await agent_cycle.start_schwab_agent_paper_cycle(
                    s,
                    authority=authority,
                    decision_slot_at=AT,
                    idempotency_key="old-start",
                    requested_instrument_ids=tuple(i.instrument_id for i in manifest),
                    candidate_batch_id=batch.batch_id,
                )
            with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                await require_scheduled_execution_open(
                    s, account_id=authority.account_id, slot=AT, claim_id=reservation.claim_id
                )
            with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                await require_preparation_scope(
                    s,
                    FormalPreparationContext(
                        authority.account_id, reservation, f"agent-paper-loop:{AT.isoformat()}"
                    ),
                    now=AT,
                )
            with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                await agent_cycle._start_cycle(
                    s,
                    authority=authority,
                    decision_slot_at=AT,
                    daily_limit_usd=Decimal("3"),
                    requested_instrument_ids=tuple(i.instrument_id for i in manifest),
                    candidate_batch_id=batch.batch_id,
                )
            with pytest.raises(ValueError, match="SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    s,
                    account_id=authority.account_id,
                    candidate_batch_id=batch.batch_id,
                    now=AT,
                    client_idempotency_key=f"agent-paper-loop:{AT.isoformat()}",
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                )
        async with sessions.begin() as s:
            with pytest.raises(ValueError):
                await agent_cycle.finish_schwab_agent_paper_trigger(
                    s, reservation=reservation, error_code="AGENT_PAPER_TRIGGER_FAILED"
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["none", "worker", "token", "fence", "finished", "missing", "active", "unknown_usage"]
)
async def test_consumed_future_lease_uses_native_terminal_proof(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    from uuid import UUID, uuid4

    from nexus.ops.manual_paper_host import finish_configured_manual_request
    from nexus.paper.history_seal import _row_hash
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.manual_paper import ManualPaperRequest
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.persistence.models.runtime import Job, JobAttempt, JobLease

    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **(V9_OPTIONS | {"formal_flexible_stage_budget": True}),
    )
    now = AT - timedelta(seconds=30)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=now, candidate_instruments=INSTRUMENTS
    )
    original_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return now
        if getattr(statement, "is_insert", False) and statement.table.name == "idempotency_records":
            statement = statement.values(created_at=now)
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lambda *_: nullcontext())
    monkeypatch.setattr(
        manual_preparation_close, "require_preparation_producers_stopped", lambda *_a, **_k: None
    )
    try:
        async with sessions.begin() as s:
            model = await s.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v10-flexible-stage-budget"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                s,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(i.instrument_id for i in manifest),
                idempotency_key="pre-slot-manual",
                created_at=now,
            )
            settings = EngineSettings(
                database_url=migrated_database_url,
                agent_runtime_mode="codex_sdk",
                agent_paper_account_id=authority.account_id,
                agent_paper_candidate_batch_id=batch.batch_id,
                agent_paper_formal_model_config_id=model.id,
            )
            manual = await manual_request.admit_manual_request(
                s,
                account_id=authority.account_id,
                batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_and_paper",
                idempotency_key="before-natural-slot",
            )
            _, claimed = await manual_request.claim_manual_request(s, request_id=manual.request_id)
            assert claimed
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                s,
                account_id=authority.account_id,
                candidate_batch_id=batch.batch_id,
                now=now,
                client_idempotency_key=f"manual-entry:{manual.request_id}",
                trigger_mode="manual",
                manual_request_id=manual.request_id,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            result = await agent_cycle.start_schwab_agent_paper_cycle(
                s,
                authority=authority,
                decision_slot_at=now,
                idempotency_key=f"manual-entry:{manual.request_id}",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
                manual_request_id=manual.request_id,
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                s, reservation=reservation, result=result
            )
            cycle = await s.get(AgentPaperCycleRecord, result.cycle_id)
            await agent_cycle._stop_failed(
                s, cycle, await agent_cycle._events(s, cycle.id), "RESEARCH_OUTPUT_NOT_ACTIONABLE"
            )
        now += timedelta(seconds=5)
        async with sessions.begin() as s:
            closed = await finish_configured_manual_request(
                s, settings, request_id=manual.request_id, cancel_unused=True
            )
            assert closed.status == "unavailable"
            assert (await s.get(ManualPaperRequest, manual.request_id)) is not None
        now = AT
        async with sessions.begin() as s:
            unknown = await agent_cycle.reserve_schwab_agent_paper_trigger(
                s,
                account_id=authority.account_id,
                candidate_batch_id=batch.batch_id,
                now=now,
                client_idempotency_key=f"agent-paper-loop:{AT.isoformat()}",
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                s, reservation=unknown, error_code="AGENT_PAPER_TRIGGER_FAILED"
            )
        now = AT + timedelta(seconds=10)
        async with sessions.begin() as s:
            lease = await s.scalar(select(JobLease).order_by(JobLease.id))
            assert lease.expires_at > now
            attempt = await s.scalar(
                select(JobAttempt).where(JobAttempt.lease_token == lease.lease_token)
            )
            job = await s.get(Job, lease.job_id)
            assert job.status == attempt.outcome == "skipped"
            # Deliberate isolated corruption; normal immutable writers remain unchanged.
            if fault != "none":
                for table in ("job_attempts", "jobs", "agent_research_runs"):
                    await s.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
            if fault == "worker":
                attempt.worker_id = "wrong-worker"
            elif fault == "token":
                attempt.lease_token = uuid4()
            elif fault == "fence":
                attempt.fence_token += 1
            elif fault == "finished":
                attempt.finished_at = lease.expires_at + timedelta(seconds=1)
            elif fault == "missing":
                await s.delete(attempt)
            elif fault == "active":
                attempt.outcome, attempt.finished_at = "running", None
                job.status, job.finished_at = "running", None
            elif fault == "unknown_usage":
                run = await s.get(AgentResearchRunRecord, UUID(job.payload["run_id"]))
                run.settled_cost_usd = None
            await s.flush()
            if fault != "none":
                for table in ("job_attempts", "jobs", "agent_research_runs"):
                    await s.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))

        async def preserved():
            async with sessions() as s:
                return {
                    table.__tablename__: sorted(
                        _row_hash(r) for r in await s.scalars(select(table))
                    )
                    for table in (
                        Job,
                        JobAttempt,
                        JobLease,
                        AgentResearchRunRecord,
                        AgentDailyBudgetReservationRecord,
                        IdempotencyRecord,
                    )
                }

        before = await preserved()
        args = Namespace(
            action="check-scheduled-acceptance",
            claim_id=unknown.claim_id,
            repo_root=tmp_path,
            env_file=tmp_path / "env",
        )
        if fault == "none":
            check = await manual_paper.run(settings, args)
            assert check.status == "eligible_at_check"
            args.action, args.accept_unknown_effects = "accept-scheduled-unknown", True
            assert (
                await manual_paper.run(settings, args)
            ).status == "scheduled_unknown_acknowledged"
            async with sessions.begin() as s:
                await manual_request.require_manual_account_available(
                    s, account_id=authority.account_id, now=now
                )
        else:
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
        assert await preserved() == before
    finally:
        await engine.dispose()

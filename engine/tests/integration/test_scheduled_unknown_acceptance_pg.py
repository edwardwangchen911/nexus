"""Normal maintenance preserves a scheduled unknown and fences its old writes."""

import asyncio
from argparse import Namespace
from contextlib import nullcontext
from datetime import timedelta
from decimal import Decimal

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

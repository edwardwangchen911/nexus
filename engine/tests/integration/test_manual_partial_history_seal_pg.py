"""Explicit failure disposition traverses real manual/Worker/PG paths, never a model."""

from argparse import Namespace
from datetime import timedelta

import pytest
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from integration.test_terminal_cycle_cancel_pg import AT, event_values
from nexus.agents.codex_sdk import CodexInvocationError
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops import manual_paper
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle, manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL, manual_host_scope
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.persistence.models.runtime import AuditEvent, Job, WorkerHeartbeat
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.agents.test_codex_sdk import _staged_batch
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_stage", ["critique", "synthesis"])
async def test_explicit_manual_seal_retains_known_lower_bound_and_closes_unused_jobs(
    migrated_database_url, monkeypatch, failed_stage
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT
    )
    clock = [AT]
    original_scalar = AsyncSession.scalar

    async def database_clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return clock[0]
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", database_clock)
    from datetime import datetime

    class WorkerClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0] if tz is None else clock[0].astimezone(tz)

    monkeypatch.setattr("nexus.worker.datetime", WorkerClock)
    monkeypatch.setattr("nexus.worker._ONCE_STARTED_AT", AT)
    calls = []

    async def invoke(payload, **kwargs):
        stage = payload["stage"]
        calls.append(stage)
        if stage == failed_stage:
            failure = CodexInvocationError(
                "CODEX_TIMEOUT", phase="turn_run", provider_call_started=True
            )
            failure.timeout_source = "stage_deadline"
            raise failure
        import json

        return json.dumps(
            _staged_batch(stage, [p["role"] for p in payload["prompts"]])
        ), UsageSnapshot(input_tokens=100, output_tokens=80)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    monkeypatch.setattr(
        "nexus.paper.history_seal.require_research_processes_quiescent", lambda: None
    )
    try:
        await _set_test_insert_clock(sessions, AT - timedelta(minutes=1))
        await prepare_database(settings, **V9_OPTIONS)
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v9-full-input-capacity"
                )
            )
            assert model is not None
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="partial-seal-batch",
                created_at=AT,
            )
            request = await manual_request.admit_manual_request(
                session,
                account_id=authority.account_id,
                batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_and_paper",
                idempotency_key="partial-seal",
            )
            _, claimed = await manual_request.claim_manual_request(
                session, request_id=request.request_id
            )
            assert claimed
            result = await agent_cycle.start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key=f"manual-entry:{request.request_id}",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
                manual_request_id=request.request_id,
            )
            job = await session.scalar(select(Job).order_by(Job.id).limit(1))
            job_id = job.id
        await _set_test_insert_clock(sessions, AT)
        assert await run_agent_worker_once(settings, job_ids=(job_id,), formal_stage_capacity=True)
        async with sessions.begin() as session:
            job = await session.get(Job, job_id)
            assert job.status == "failed" and job.attempt_count == 1
            from uuid import UUID

            run_id = UUID(job.payload["run_id"])
            run = await session.get(AgentResearchRunRecord, run_id)
            assert run.status == "failed" and run.settled_cost_usd is None
            lower_bound = (run.input_tokens, run.output_tokens)
            assert lower_bound == ((100, 80) if failed_stage == "critique" else (200, 160))
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            await agent_cycle._stop_failed(
                session,
                cycle,
                await agent_cycle._events(session, cycle.id),
                "RESEARCH_OUTPUT_NOT_ACTIONABLE",
            )
            original_events = event_values(await agent_cycle._events(session, cycle.id))
            runtime = settings.model_copy(
                update={
                    "agent_paper_account_id": authority.account_id,
                    "agent_paper_candidate_batch_id": batch.batch_id,
                    "agent_paper_formal_model_config_id": model.id,
                    "agent_paper_manual_enabled": True,
                    "agent_paper_cycle_enabled": True,
                    "release_revision": "a" * 40,
                }
            )
            clock[0] = AT + timedelta(days=3)
            session.add(
                WorkerHeartbeat(
                    worker_id=f"agent-paper-loop-{UUID(int=555)}:manual:{manual_host_scope(runtime)}",
                    queue="agent_paper",
                    seen_at=clock[0],
                    started_at=AT,
                    process_version=f"{MANUAL_HOST_PROTOCOL}+{'a' * 40}",
                    migration_head=CURRENT_MIGRATION_HEAD,
                )
            )
        args = Namespace(
            action="seal-failed-research",
            cycle_id=result.cycle_id,
            run_ids=(run_id,),
            accept_unknown_usage=True,
        )
        receipt = await manual_paper.run(runtime, args)
        assert receipt.sealed_research == 1 and not receipt.replayed
        assert (await manual_paper.run(runtime, args)).replayed
        async with sessions.begin() as session:
            row = await session.get(ManualPaperRequest, request.request_id)
            assert (await manual_request.read_manual_request(session, row)).status == "unavailable"
            await manual_request.require_manual_account_available(
                session, account_id=authority.account_id, now=clock[0]
            )
            run = await session.get(AgentResearchRunRecord, run_id)
            assert (run.input_tokens, run.output_tokens) == lower_bound
            assert run.settled_cost_usd is None and run.retry_count == 0
            reservation = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run_id
                )
            )
            assert reservation.settled_usd is None
            jobs = list(await session.scalars(select(Job)))
            assert sum(j.status == "skipped" for j in jobs) == len(manifest) - 1
            assert (await session.get(Job, job_id)).attempt_count == 1
            assert (
                event_values(await agent_cycle._events(session, result.cycle_id)) == original_events
            )
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "paper.legacy_research.sealed")
                )
                == 1
            )
        assert calls == (
            ["research", "critique"]
            if failed_stage == "critique"
            else ["research", "critique", "synthesis"]
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "confirmation",
        "usage",
        "request-hash",
        "unknown-request",
        "sibling-attempt",
        "late-close-failure",
    ],
)
async def test_manual_partial_seal_rejection_preserves_all_facts(
    migrated_database_url, monkeypatch, fault
):
    import hashlib
    import json
    from copy import deepcopy
    from types import SimpleNamespace

    from nexus.paper import history_seal
    from nexus.persistence.session import session_factory

    original_entry = manual_paper.run
    original_proof = history_seal._require_partial_usage_proof
    original_close = manual_request.close_failed_manual_request
    original_stop = agent_cycle._stop_failed

    class VerifiedRejection(Exception):
        pass

    if fault == "sibling-attempt":

        async def claim_sibling(session, cycle, events, code):
            from nexus.agents.codex_formal_stage import formal_stage_job_qualified
            from nexus.agents.codex_versions import FULL_INPUT_CAPACITY_CONFIG_VERSION
            from nexus.jobs.models import QueueName
            from nexus.jobs.repository import claim_next_job, fail_job

            other = await session.scalar(select(Job).where(Job.status == "queued").limit(1))
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="controlled-sibling-attempt",
                now=AT,
                job_ids=(other.id,),
                job_types=("agent.paper.research",),
                research_budget_profile=FULL_INPUT_CAPACITY_CONFIG_VERSION,
                locked_job_guard=formal_stage_job_qualified,
                lease_seconds=210,
            )
            assert claim is not None
            await fail_job(session, claim=claim, now=AT, error_code="AGENT_RESEARCH_RESULT_UNKNOWN")
            return await original_stop(session, cycle, events, code)

        monkeypatch.setattr(agent_cycle, "_stop_failed", claim_sibling)

    async def checked_entry(settings, args):
        factory = session_factory(settings)
        try:
            if fault == "unknown-request":
                async with factory.begin() as session:
                    row = await session.scalar(select(ManualPaperRequest))
                    await manual_request.record_manual_request_event(
                        session, row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
                    )
            async with factory() as session:
                jobs_before = [
                    (j.id, j.status, j.attempt_count)
                    for j in await session.scalars(select(Job).order_by(Job.id))
                ]
                row = await session.scalar(select(ManualPaperRequest))
                request_before = await manual_request.read_manual_request(session, row)
                run_before = await session.get(AgentResearchRunRecord, args.run_ids[0])
                usage_before = (
                    run_before.input_tokens,
                    run_before.output_tokens,
                    run_before.settled_cost_usd,
                )
            with monkeypatch.context() as scoped:
                if fault in {"usage", "request-hash"}:

                    async def altered_proof(session, run, outputs, model):
                        if fault == "usage":
                            values = {
                                c.key: getattr(run, c.key) for c in run.__mapper__.column_attrs
                            }
                            values["input_tokens"] += 1
                            return await original_proof(
                                session, SimpleNamespace(**values), outputs, model
                            )
                        last = outputs[-1]
                        values = {c.key: getattr(last, c.key) for c in last.__mapper__.column_attrs}
                        payload = deepcopy(last.payload)
                        payload["stage_input"]["request_hash"] = "0" * 64
                        values["payload"] = payload
                        values["content_hash"] = hashlib.sha256(
                            json.dumps(
                                {
                                    "status": last.status,
                                    "role": None,
                                    "payload": payload,
                                    "intent": None,
                                },
                                sort_keys=True,
                                separators=(",", ":"),
                                default=str,
                            ).encode()
                        ).hexdigest()
                        return await original_proof(
                            session, run, (*outputs[:-1], SimpleNamespace(**values)), model
                        )

                    scoped.setattr(history_seal, "_require_partial_usage_proof", altered_proof)
                if fault == "late-close-failure":

                    async def failed_after_close(session, **kwargs):
                        await original_close(session, **kwargs)
                        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")

                    scoped.setattr(
                        manual_request, "close_failed_manual_request", failed_after_close
                    )
                if fault == "confirmation":
                    args = Namespace(**{**vars(args), "accept_unknown_usage": False})
                with pytest.raises(
                    ValueError, match="MANUAL_(ACCOUNT_BUSY_OR_UNKNOWN|REQUEST_INVALID)"
                ):
                    await original_entry(settings, args)
            async with factory() as session:
                assert [
                    (j.id, j.status, j.attempt_count)
                    for j in await session.scalars(select(Job).order_by(Job.id))
                ] == jobs_before
                row = await session.scalar(select(ManualPaperRequest))
                assert await manual_request.read_manual_request(session, row) == request_before
                run = await session.get(AgentResearchRunRecord, args.run_ids[0])
                assert (run.input_tokens, run.output_tokens, run.settled_cost_usd) == usage_before
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(AuditEvent)
                        .where(AuditEvent.action == "paper.legacy_research.sealed")
                    )
                    == 0
                )
                assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            raise VerifiedRejection
        finally:
            await factory.kw["bind"].dispose()

    monkeypatch.setattr(manual_paper, "run", checked_entry)
    with pytest.raises(VerifiedRejection):
        await test_explicit_manual_seal_retains_known_lower_bound_and_closes_unused_jobs(
            migrated_database_url, monkeypatch, "critique"
        )

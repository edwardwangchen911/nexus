from argparse import Namespace
from datetime import timedelta
from uuid import UUID

import pytest
from integration.test_terminal_cycle_cancel_pg import (
    AT,
)
from integration.test_terminal_cycle_cancel_pg import (
    test_legacy_terminal_recovery_skips_only_unused_jobs_and_is_idempotent as legacy_case,
)
from nexus.ops import manual_paper
from nexus.persistence.models.agents import (
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


class VerifiedRejection(Exception):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "live-lease",
        "missing-confirmation",
        "last-run-invalid",
        "fact-changed",
        "process-live",
        "forged-seal",
        "research-event-added",
    ],
)
async def test_history_seal_guards_and_whole_transaction_rollback(
    migrated_database_url, monkeypatch, fault, tmp_path
):
    original = manual_paper.run

    async def checked(settings, args):
        if args.action != "seal-failed-research":
            return await original(settings, args)
        factory = session_factory(settings)
        try:
            if fault in {"fact-changed", "research-event-added"}:
                await original(settings, args)
            async with factory.begin() as session:
                run = await session.get(AgentResearchRunRecord, args.run_ids[0])
                job = await session.scalar(
                    select(Job).where(Job.payload["run_id"].as_string() == str(run.id))
                )
                if fault == "live-lease":
                    lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
                    lease.expires_at = AT + timedelta(days=4)
                elif fault == "fact-changed":
                    run.status = "failed"
                elif fault == "research-event-added":
                    from nexus.agents.execution import _append_event

                    await _append_event(
                        session, run, 1, "failed", {"reason": "model_usage_uncertain"}
                    )
                elif fault == "forged-seal":
                    from nexus.audit.service import append_audit_event

                    await append_audit_event(
                        session,
                        actor_id="local-manual-history-seal",
                        action="paper.legacy_research.sealed",
                        schema_version="paper_legacy_seal_v1",
                        payload={"run_id": str(run.id), "fact_hash": "0" * 64},
                        occurred_at=AT + timedelta(days=3),
                    )
                if fault == "missing-confirmation":
                    args = Namespace(**{**vars(args), "accept_unknown_usage": False})
                elif fault == "last-run-invalid":
                    args = Namespace(**{**vars(args), "run_ids": [*args.run_ids, UUID(int=999)]})
                elif fault == "process-live":

                    def blocked():
                        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")

                    monkeypatch.setattr(
                        "nexus.paper.history_seal.require_research_processes_quiescent", blocked
                    )
                await session.flush()
                before = await session.scalar(select(func.count()).select_from(AuditEvent))
            with pytest.raises(
                ValueError, match="MANUAL_(ACCOUNT_BUSY_OR_UNKNOWN|REQUEST_INVALID)"
            ):
                await original(settings, args)
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(AuditEvent)) == before
                run = await session.get(AgentResearchRunRecord, args.run_ids[0])
                assert run.status == ("failed" if fault == "fact-changed" else "prepared")
                assert run.settled_cost_usd is run.input_tokens is run.output_tokens is None
            raise VerifiedRejection
        finally:
            await factory.kw["bind"].dispose()

    monkeypatch.setattr(manual_paper, "run", checked)
    with pytest.raises(VerifiedRejection):
        await legacy_case(migrated_database_url, monkeypatch, "v4", True, False, tmp_path)


@pytest.mark.asyncio
async def test_failed_unknown_usage_and_released_budget_remain_unknown(
    migrated_database_url, monkeypatch, tmp_path
):
    original = manual_paper.run
    cancellations = 0

    async def historical_failure(settings, args):
        nonlocal cancellations
        result = await original(settings, args)
        if args.action != "cancel-unused-research":
            return result
        cancellations += 1
        if cancellations != 2:
            return result
        factory = session_factory(settings)
        try:
            async with factory.begin() as session:
                from nexus.agents.execution import _persist_terminal
                from nexus.agents.persistence import release_daily_budget_if_reserved

                run = await session.scalar(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.settled_cost_usd.is_(None)
                    )
                )
                await release_daily_budget_if_reserved(session, run_id=run.id)
                await _persist_terminal(
                    session,
                    run,
                    output_sequence=1,
                    event_sequence=1,
                    status="failed",
                    reason="model_usage_uncertain",
                )
        finally:
            await factory.kw["bind"].dispose()
        return result

    monkeypatch.setattr(manual_paper, "run", historical_failure)
    await legacy_case(migrated_database_url, monkeypatch, "v4", True, False, tmp_path)

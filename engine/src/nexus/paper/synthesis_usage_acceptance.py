"""Accept one failed synthesis scheduling hold; never settle usage or replay it."""

import json
from datetime import datetime
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.synthesis_lifecycle import SYNTHESIS_JOB_VERSIONS
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.jobs.models import JobKeyV1, JobType, canonical_job_key, job_input_hash
from nexus.paper.agent_cycle_models import CycleStatus, canonical_cycle_event_hash
from nexus.paper.history_seal import _row_hash as _legacy_row_hash
from nexus.paper.manual_preparation_close import require_preparation_producers_stopped
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.base import Base
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import (
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job, JobAttempt, JobLease

ACTION = "paper.synthesis.unknown_usage_accepted"
SCHEMA = "synthesis_usage_acceptance_v1"
ACTOR = "local-user:synthesis-usage-acceptance"
BLOCKED = "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"


def _row_hash(row: Base) -> str:
    # The v1 acceptance proof predates diagnostics. Freeze its original business
    # columns so nullable additions cannot revoke historical acceptance or make
    # diagnostic metadata a budget/scheduling-release authority.
    if isinstance(row, AgentPortfolioSynthesisOutputRecord):
        values = {
            key: getattr(row, key)
            for key in (
                "id",
                "run_id",
                "status",
                "intent",
                "content_hash",
                "binding_hash",
                "created_at",
            )
        }
        return job_input_hash(json.loads(json.dumps(values, default=str)))
    return _legacy_row_hash(row)


class SynthesisUsageDiagnosticError(ValueError):
    """Fixed phase/code only; never surface source data or exception text."""

    def __init__(self, stage: str, code: str) -> None:
        super().__init__(BLOCKED)
        self.stage, self.code = stage, code

    def safe_diagnostics(self) -> dict[str, str]:
        pairs = {
            ("synthesis_history", "BINDING_INVALID"),
            ("synthesis_acceptance", "PROOF_INVALID"),
            ("synthesis_occupancy", "ACCOUNT_NOT_IDLE"),
            ("synthesis_fence", "WRITER_BUSY"),
            ("synthesis_recovery_target", "SOURCE_CONFIGURATION_UNSAFE"),
            ("synthesis_recovery_target", "SOURCE_CONFIGURATION_CHANGED"),
            ("synthesis_recovery_target", "SOURCE_TARGET_DATABASE_IDENTICAL"),
            ("synthesis_recovery_target", "ACTUAL_DATABASE_MISMATCH"),
            ("synthesis_recovery_target", "PRODUCER_ARGUMENT_PAIR_REQUIRED"),
            ("synthesis_recovery_target", "RECOVERY_DATABASE_REQUIRED"),
        }
        if (self.stage, self.code) not in pairs:
            return {}
        return {"diagnostic_stage": self.stage, "diagnostic_code": self.code}


class SynthesisUsageDisposition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Literal["not_accepted", "accepted", "invalid"]
    usage: Literal["unknown"] = "unknown"
    budget_preserved: Literal[True] = True
    execution_allowed: Literal[False] = False


class SynthesisUsageReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    run_id: UUID
    status: Literal["eligible_at_check", "accepted_usage_unknown"]
    replayed: bool = False
    check_only: bool = False
    usage: Literal["unknown"] = "unknown"
    budget_preserved: Literal[True] = True
    execution_locked: Literal[True] = True


async def _audits(session: AsyncSession, run_id: UUID) -> list[AuditEvent]:
    return list(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION, AuditEvent.payload["run_id"].astext == str(run_id)
            )
        )
    )


async def require_synthesis_not_accepted(session: AsyncSession, run_id: UUID) -> None:
    # A damaged disposition still cannot resurrect the original execution identity.
    if await _audits(session, run_id):
        raise ValueError(BLOCKED)


async def _collect_proof(
    session: AsyncSession, *, cycle_id: UUID, run_id: UUID, now: datetime
) -> dict[str, object]:
    from nexus.agents.persistence import load_actionable_research_output
    from nexus.paper.historical_assessment import source_for_cycle
    from nexus.paper.management_read_authority import load_synthesis_read_authority

    require_utc(now)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
    if (
        cycle is None
        or run is None
        or run.cycle_id != cycle_id
        or not cycle.execution_locked
        or cycle.decision_time > now
        or run.status != "failed"
        or run.completed_at is None
        or run.completed_at > now
        or run.input_tokens is not None
        or run.output_tokens is not None
        or run.settled_cost_usd is not None
        or await source_for_cycle(session, cycle_id) is not None
    ):
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    all_runs = list(
        await session.scalars(
            select(AgentPortfolioSynthesisRunRecord).where(
                AgentPortfolioSynthesisRunRecord.cycle_id == cycle_id
            )
        )
    )
    if len(all_runs) != 1 or all_runs[0].id != run_id:
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    events = list(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence)
        )
    )
    prior = None
    research_ids: set[UUID] = set()
    for sequence, event in enumerate(events, 1):
        if (
            event.sequence != sequence
            or event.from_status != prior
            or event.occurred_at > now
            or event.to_status not in {"prepared", "researching", "unavailable", "failed"}
            or event.event_hash
            != canonical_cycle_event_hash(
                cycle_id=cycle_id,
                sequence=sequence,
                from_status=cast(CycleStatus | None, prior),
                to_status=cast(CycleStatus, event.to_status),
                result_code=event.result_code,
                fact_ids=event.fact_ids,
                occurred_at=event.occurred_at,
            )
        ):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        raw = event.fact_ids.get("research_run_ids", [])
        if not isinstance(raw, list):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        try:
            research_ids.update(UUID(str(value)) for value in raw)
        except (ValueError, TypeError):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID") from None
        prior = event.to_status
    if (
        prior not in {"failed", "unavailable"}
        or not 1 <= len(research_ids) <= 5
        or events[-1].result_code != "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
    ):
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    authority = await load_synthesis_read_authority(session, cycle=cycle, read_as_of=now)
    if authority is None or authority.run.id != run_id or authority.intent is not None:
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    frozen = authority.frozen
    batch = await session.get(AgentPaperCandidateBatchRecord, run.candidate_batch_id)
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    prompt = await session.get(AgentPromptVersionRecord, run.prompt_version_id)
    reserves = list(
        await session.scalars(
            select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == run_id
            )
        )
    )
    outputs = list(
        await session.scalars(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run_id
            )
        )
    )
    if (
        batch is None
        or model is None
        or prompt is None
        or len(reserves) != 1
        or len(outputs) != 1
        or outputs[0].id != authority.output.id
        or outputs[0].status != "failed"
        or outputs[0].intent is not None
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
        or reserves[0].state != "reserved"
        or reserves[0].settled_usd is not None
        or reserves[0].reserved_usd != model.worst_case_cost_usd
        or reserves[0].pricing_version != run.pricing_version
        or reserves[0].business_date != frozen.admitted_at.date()
    ):
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    # No execution product from this failed cycle, including a later inserted decision.
    if (
        await session.scalar(
            select(StrategyDecisionRecord.id)
            .where(StrategyDecisionRecord.agent_paper_cycle_id == cycle_id)
            .limit(1)
        )
        is not None
    ):
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    rows: list[Base] = [cycle, run, batch, model, prompt, *reserves, *outputs, *events]
    observed_research: set[UUID] = set()
    formal = bool(events[0].fact_ids.get("formal_stage_capacity") is True)
    for source in frozen.security_outputs:
        output = await session.get(AgentResearchOutputRecord, source.research_output_id)
        research = (
            None if output is None else await session.get(AgentResearchRunRecord, output.run_id)
        )
        if (
            research is None
            or output is None
            or research.id not in research_ids
            or research.id in observed_research
            or research.status != "completed"
            or research.settled_cost_usd is None
            or research.input_tokens is None
            or research.output_tokens is None
            or research.idempotency_key != f"agent-paper:{cycle_id}:{source.instrument_id}"
        ):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        observed_research.add(research.id)
        action = await load_actionable_research_output(
            session,
            run_id=research.id,
            decision_time=frozen.admitted_at,
            allow_safe_no_trade=True,
            formal_stage_capacity=formal,
        )
        if (
            action.output_id != output.id
            or action.intent_hash != source.intent_hash
            or output.content_hash != source.content_hash
            or action.evidence_ref_ids != source.evidence_ref_ids
        ):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        research_reserves = list(
            await session.scalars(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == research.id
                )
            )
        )
        if (
            len(research_reserves) != 1
            or research_reserves[0].state != "settled"
            or research_reserves[0].settled_usd != research.settled_cost_usd
        ):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        research_outputs = list(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .where(AgentResearchOutputRecord.run_id == research.id)
                .order_by(AgentResearchOutputRecord.sequence)
            )
        )
        research_events = list(
            await session.scalars(
                select(AgentResearchEventRecord)
                .where(AgentResearchEventRecord.run_id == research.id)
                .order_by(AgentResearchEventRecord.sequence)
            )
        )
        rows.extend([research, *research_reserves, *research_outputs, *research_events])
    associated_research = set(
        await session.scalars(
            select(AgentResearchRunRecord.id).where(
                AgentResearchRunRecord.idempotency_key.like(f"agent-paper:{cycle_id}:%")
            )
        )
    )
    if associated_research != research_ids:
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    if observed_research != research_ids:
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    # Query the entire associated sets, not just IDs recorded by the original audit.
    for identity in sorted(research_ids | {run_id}, key=str):
        jobs = list(
            await session.scalars(select(Job).where(Job.payload["run_id"].astext == str(identity)))
        )
        if len(jobs) != 1:
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        job = jobs[0]
        synthesis = identity == run_id
        if (
            job.payload != {"run_id": str(identity)}
            or job.attempt_count != 1
            or job.max_attempts != 1
            or job.manual_retry_count != 0
            or job.finished_at is None
            or job.finished_at > now
            or job.status not in ({"failed", "dead_letter"} if synthesis else {"succeeded"})
            or job.job_type
            not in ({"agent.portfolio_synthesis"} if synthesis else {"agent.paper.research"})
        ):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        if synthesis and (
            job.retry_policy != "never"
            or job.input_hash != run.input_hash
            or job.scope_kind != "portfolio"
            or job.scope_id != str(cycle.portfolio_id)
            or job.schema_version not in SYNTHESIS_JOB_VERSIONS
            or job.effective_at != run.decision_time
            or (job.status, job.last_error_code)
            not in {
                ("failed", "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN"),
                ("dead_letter", "LEASE_EXPIRED"),
            }
            or job.job_key
            != canonical_job_key(
                JobKeyV1(
                    job_type=JobType.AGENT_PORTFOLIO_SYNTHESIS,
                    scope_kind="portfolio",
                    scope_id=str(cycle.portfolio_id),
                    effective_at=run.decision_time,
                    input_hash=run.input_hash,
                )
            )
        ):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        attempts = list(
            await session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id))
        )
        leases = list(await session.scalars(select(JobLease).where(JobLease.job_id == job.id)))
        if len(attempts) != 1 or len(leases) != 1:
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        attempt, lease = attempts[0], leases[0]
        if (
            attempt.attempt_no != 1
            or attempt.outcome
            != (
                "lost"
                if synthesis and job.status == "dead_letter"
                else "failed"
                if synthesis
                else "succeeded"
            )
            or attempt.error_code != job.last_error_code
            or attempt.started_at != job.started_at
            or attempt.finished_at != job.finished_at
            or attempt.worker_id != lease.worker_id
            or attempt.lease_token != lease.lease_token
            or attempt.fence_token != lease.fence_token
            or lease.expires_at > now
        ):
            raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
        rows.extend([job, attempt, lease])
    if any(getattr(row, "created_at", now) > now for row in rows):
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID")
    return {
        "account_id": str(cycle.account_id),
        "cycle_id": str(cycle_id),
        "run_id": str(run_id),
        "usage": "unknown",
        "budget_preserved": True,
        "disposition": "scheduling_hold_accepted_without_usage_settlement",
        "fact_hash": job_input_hash({"rows": [_row_hash(row) for row in rows]}),
    }


async def _proof(
    session: AsyncSession, *, cycle_id: UUID, run_id: UUID, now: datetime
) -> dict[str, object]:
    try:
        return await _collect_proof(session, cycle_id=cycle_id, run_id=run_id, now=now)
    except SynthesisUsageDiagnosticError:
        raise
    except ValueError:
        raise SynthesisUsageDiagnosticError("synthesis_history", "BINDING_INVALID") from None


async def synthesis_usage_accepted(
    session: AsyncSession, *, cycle_id: UUID, run_id: UUID, now: datetime
) -> bool:
    audits = await _audits(session, run_id)
    if not audits:
        return False
    if len(audits) != 1:
        raise SynthesisUsageDiagnosticError("synthesis_acceptance", "PROOF_INVALID")
    row = audits[0]
    if row.occurred_at > now or row.schema_version != SCHEMA or row.actor_id != ACTOR:
        raise SynthesisUsageDiagnosticError("synthesis_acceptance", "PROOF_INVALID")
    proof = await _proof(session, cycle_id=cycle_id, run_id=run_id, now=row.occurred_at)
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=row.event_seq,
            occurred_at=row.occurred_at,
            actor_id=row.actor_id,
            action=row.action,
            schema_version=row.schema_version,
            payload=row.payload,
        )
    )
    previous = await session.scalar(
        select(AuditEvent.event_hash).where(AuditEvent.event_seq == row.event_seq - 1)
    )
    if (
        row.payload != proof
        or row.chain_version != "audit_jcs_sha256_v1"
        or row.canonical_event != canonical
        or row.previous_hash != ("0" * 64 if row.event_seq == 1 else previous)
        or row.event_hash != audit_event_hash(row.previous_hash, canonical)
    ):
        raise SynthesisUsageDiagnosticError("synthesis_acceptance", "PROOF_INVALID")
    return True


async def read_synthesis_usage_disposition(
    session: AsyncSession, *, cycle_id: UUID, now: datetime
) -> SynthesisUsageDisposition | None:
    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord).where(
            AgentPortfolioSynthesisRunRecord.cycle_id == cycle_id
        )
    )
    if run is None:
        return None
    audits = await _audits(session, run.id)
    if not audits and (run.status != "failed" or run.settled_cost_usd is not None):
        return None
    try:
        accepted = await synthesis_usage_accepted(
            session, cycle_id=cycle_id, run_id=run.id, now=now
        )
    except ValueError:
        return SynthesisUsageDisposition(status="invalid")
    return SynthesisUsageDisposition(status="accepted" if accepted else "not_accepted")


async def _eligibility(
    session: AsyncSession, *, account_id: UUID, cycle_id: UUID, run_id: UUID, now: datetime
) -> dict[str, object]:
    from nexus.paper.current_preparation_acceptance import require_no_live_research_runs
    from nexus.paper.manual_request import (
        _require_cycle_jobs_settled,
        require_manual_request_history,
    )
    from nexus.paper.preparation_unknown_ack import require_acknowledged_native_claims
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.target_service import (
        require_activation_projection_unoccupied,
        require_no_unknown_cancel_requests,
    )

    proof = await _proof(session, cycle_id=cycle_id, run_id=run_id, now=now)
    if proof["account_id"] != str(account_id):
        raise SynthesisUsageDiagnosticError("synthesis_occupancy", "ACCOUNT_NOT_IDLE")
    await require_manual_request_history(session, account_id=account_id, now=now)
    await require_acknowledged_native_claims(session, account_id=account_id, now=now)
    cycles = list(
        await session.scalars(
            select(AgentPaperCycleRecord).where(AgentPaperCycleRecord.account_id == account_id)
        )
    )
    for cycle in cycles:
        if cycle.id == cycle_id:
            continue
        status = await session.scalar(
            select(AgentPaperCycleEventRecord.to_status)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle.id)
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        if status not in {"completed", "failed", "unavailable"}:
            raise SynthesisUsageDiagnosticError("synthesis_occupancy", "ACCOUNT_NOT_IDLE")
        await _require_cycle_jobs_settled(
            session, cycle_id=cycle.id, now=now, allow_accepted_synthesis=True
        )
    kinds = ("agent.research", "agent.paper.research", "agent.portfolio_synthesis")
    if (
        await session.scalar(
            select(Job.id)
            .where(Job.job_type.in_(kinds), Job.status.in_(["queued", "running"]))
            .limit(1)
        )
        is not None
    ):
        raise SynthesisUsageDiagnosticError("synthesis_occupancy", "ACCOUNT_NOT_IDLE")
    if (
        await session.scalar(
            select(JobLease.job_id)
            .join(Job, Job.id == JobLease.job_id)
            .where(Job.job_type.in_(kinds), JobLease.expires_at > now)
            .limit(1)
        )
        is not None
    ):
        raise SynthesisUsageDiagnosticError("synthesis_occupancy", "ACCOUNT_NOT_IDLE")
    await require_no_live_research_runs(session, now=now)
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    await require_no_unknown_cancel_requests(session, account_id=account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=account_id, projection=projection
    )
    return proof


async def check_failed_synthesis_usage(
    session: AsyncSession,
    *,
    account_id: UUID,
    cycle_id: UUID,
    run_id: UUID,
    now: datetime,
    repo_root: Path,
    env_file: Path,
    inspector_repo_root: Path | None = None,
    inspector_revision: str | None = None,
) -> SynthesisUsageReceipt:
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or cycle.account_id != account_id:
        raise ValueError(BLOCKED)
    accepted = await synthesis_usage_accepted(session, cycle_id=cycle_id, run_id=run_id, now=now)
    if not accepted:
        require_preparation_producers_stopped(
            repo_root,
            env_file,
            inspector_repo_root=inspector_repo_root,
            inspector_revision=inspector_revision,
        )
        await _eligibility(
            session, account_id=account_id, cycle_id=cycle_id, run_id=run_id, now=now
        )
    return SynthesisUsageReceipt(
        cycle_id=cycle_id,
        run_id=run_id,
        status="accepted_usage_unknown" if accepted else "eligible_at_check",
        replayed=accepted,
        check_only=True,
    )


async def accept_failed_synthesis_usage(
    session: AsyncSession,
    *,
    account_id: UUID,
    cycle_id: UUID,
    run_id: UUID,
    now: datetime,
    repo_root: Path,
    env_file: Path,
    accept_unknown_usage: bool,
    inspector_repo_root: Path | None = None,
    inspector_revision: str | None = None,
) -> SynthesisUsageReceipt:
    if accept_unknown_usage is not True:
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or cycle.account_id != account_id:
        raise ValueError(BLOCKED)
    if await synthesis_usage_accepted(session, cycle_id=cycle_id, run_id=run_id, now=now):
        return SynthesisUsageReceipt(
            cycle_id=cycle_id, run_id=run_id, status="accepted_usage_unknown", replayed=True
        )
    require_preparation_producers_stopped(
        repo_root,
        env_file,
        inspector_repo_root=inspector_repo_root,
        inspector_revision=inspector_revision,
    )
    # Match the existing maintenance lock order; never wait behind a writer with a
    # different table order. Table fences also cover previously absent associated rows.
    if not await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtextextended('nexus:audit-chain', 2020))")
    ):
        raise ValueError(BLOCKED)
    try:
        await session.execute(
            text(
                "LOCK TABLE jobs, job_leases, job_attempts, agent_research_runs, "
                "agent_research_outputs, agent_research_events, agent_daily_budget_reservations, "
                "agent_portfolio_synthesis_runs, agent_portfolio_synthesis_outputs, "
                "agent_portfolio_synthesis_budget_reservations, agent_paper_cycle_events, "
                "strategy_decisions IN SHARE MODE NOWAIT"
            )
        )
    except DBAPIError as error:
        if getattr(error.orig, "sqlstate", None) == "55P03":
            raise SynthesisUsageDiagnosticError("synthesis_fence", "WRITER_BUSY") from None
        raise
    proof = await _eligibility(
        session, account_id=account_id, cycle_id=cycle_id, run_id=run_id, now=now
    )
    await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        payload=proof,
        occurred_at=now,
    )
    from nexus.paper.manual_request import require_manual_account_available

    await require_manual_account_available(session, account_id=account_id, now=now)
    require_preparation_producers_stopped(
        repo_root,
        env_file,
        inspector_repo_root=inspector_repo_root,
        inspector_revision=inspector_revision,
    )
    return SynthesisUsageReceipt(cycle_id=cycle_id, run_id=run_id, status="accepted_usage_unknown")

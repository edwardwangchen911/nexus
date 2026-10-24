"""Reconcile terminal native research into immutable monitor coverage, without I/O."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import String, cast, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import MONITORING_SCOPE_PROMPT_VERSIONS
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.synthesis import domain_evidence
from nexus.audit.service import append_audit_event
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.jobs.repository import job_lease_consumed
from nexus.monitoring.admission import finish_increment, require_increment_binding
from nexus.monitoring.installation import install_plan_from_output
from nexus.monitoring.storage import lock_security
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorIncrement,
    MonitorPlan,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobLease

_CONTEXT_SOURCES = frozenset({"nexus_agent_paper_context", "nexus_price_monitor_increment"})


def can_retain_plan(
    *,
    prior_evidence: tuple[EvidenceRef, ...],
    current_evidence: tuple[EvidenceRef, ...],
    valid_until: datetime,
    now: datetime,
    vetoed: bool,
    previously_active: bool = True,
) -> bool:
    # Exact factual inputs, not an LLM inference that a new disclosure is harmless.
    def facts(refs: tuple[EvidenceRef, ...]) -> set[str]:
        return {ref.model_dump_json() for ref in refs if ref.source not in _CONTEXT_SOURCES}

    prior, current = facts(prior_evidence), facts(current_evidence)
    return (
        previously_active and not vetoed and now < valid_until and bool(prior) and prior == current
    )


async def latest_coverage(
    session: AsyncSession, *, account_id: UUID, instrument_id: UUID, now: datetime
) -> MonitorCoverageReceipt | None:
    row: MonitorCoverageReceipt | None = await session.scalar(
        select(MonitorCoverageReceipt)
        .where(
            MonitorCoverageReceipt.kind == "price",
            MonitorCoverageReceipt.account_id == account_id,
            MonitorCoverageReceipt.instrument_id == instrument_id,
            MonitorCoverageReceipt.observed_at <= now,
            MonitorCoverageReceipt.status != "superseded",
        )
        .order_by(
            MonitorCoverageReceipt.research_as_of.desc(), MonitorCoverageReceipt.created_at.desc()
        )
        .limit(1)
    )
    return row


async def plan_covered(session: AsyncSession, *, plan: MonitorPlan, now: datetime) -> bool:
    if plan.kind != "price":
        return False
    receipt = await latest_coverage(
        session, account_id=plan.account_id, instrument_id=plan.instrument_id, now=now
    )
    source = await session.get(AgentResearchRunRecord, plan.source_run_id)
    if source is None:
        return False
    # Native completion and monitoring adoption are separate. Until a newer
    # result is reconciled, pending old triggers cannot bypass its possible veto.
    successor = await session.scalar(
        select(AgentResearchRunRecord)
        .join(DataSnapshot, DataSnapshot.id == AgentResearchRunRecord.data_snapshot_id)
        .outerjoin(
            AgentPaperCycleRecord,
            DataSnapshot.payload["agent_paper_context"]["cycle_id"].astext
            == cast(AgentPaperCycleRecord.id, String),
        )
        .outerjoin(MonitorIncrement, MonitorIncrement.run_id == AgentResearchRunRecord.id)
        .where(
            or_(
                AgentPaperCycleRecord.account_id == plan.account_id,
                MonitorIncrement.account_id == plan.account_id,
            ),
            AgentResearchRunRecord.instrument_id == plan.instrument_id,
            AgentResearchRunRecord.status.in_(("completed", "failed", "abstained")),
            AgentResearchRunRecord.decision_time > source.decision_time,
            AgentResearchRunRecord.decision_time <= now,
            AgentResearchRunRecord.prompt_manifest["portfolio"]["version"].astext.in_(
                MONITORING_SCOPE_PROMPT_VERSIONS
            ),
        )
        .order_by(AgentResearchRunRecord.decision_time.desc(), AgentResearchRunRecord.id.desc())
        .limit(1)
    )
    if successor is not None:
        successor_receipt = await session.scalar(
            select(MonitorCoverageReceipt).where(
                MonitorCoverageReceipt.kind == "price",
                MonitorCoverageReceipt.account_id == plan.account_id,
                MonitorCoverageReceipt.run_id == successor.id,
            )
        )
        if successor_receipt is None or successor_receipt.active_plan_id != plan.id:
            return False
    if receipt is None or receipt.active_plan_id == plan.id:
        return True
    # An explicitly installed newer source is not stopped by an older gap receipt.
    return source.decision_time > receipt.research_as_of


async def _native_finished(
    session: AsyncSession, run: AgentResearchRunRecord, now: datetime
) -> bool:
    from nexus.agents.codex_formal_stage import _formal_stage_job_hash

    jobs = tuple(
        await session.scalars(
            select(Job).where(Job.payload["run_id"].astext == str(run.id)).limit(2)
        )
    )
    if len(jobs) != 1:
        return False
    job = jobs[0]
    if job.status == "skipped":
        from nexus.monitoring.preclaim import preclaim_rejection_qualified

        return await preclaim_rejection_qualified(session, run=run, now=now)
    leases = tuple(await session.scalars(select(JobLease).where(JobLease.job_id == job.id)))
    return bool(
        job.job_type == "agent.paper.research"
        and job.schema_version == "agent_research_job_v1"
        and job.payload == {"run_id": str(run.id)}
        and job.max_attempts == 1
        and job.manual_retry_count == 0
        and job.attempt_count == 1
        and job.scope_kind == "instrument"
        and job.scope_id == str(run.instrument_id)
        and job.effective_at == run.decision_time
        and job.input_hash == _formal_stage_job_hash(run)
        and len(leases) == 1
        and await job_lease_consumed(session, job=job, lease=leases[0], now=now)
    )


async def _scope_evidence(
    session: AsyncSession, *, run: AgentResearchRunRecord, account_id: UUID
) -> tuple[tuple[EvidenceRef, ...], MonitorIncrement | None]:
    from nexus.agents.paper_context import (
        FrozenAgentPaperContextStatus,
        read_agent_paper_research_context,
        revalidate_frozen_agent_paper_context,
    )
    from nexus.monitoring.context import is_monitor_snapshot

    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    rows = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    member = None
    if is_monitor_snapshot(snapshot):
        member = await require_increment_binding(session, run)
        if member.account_id != account_id:
            raise ValueError("MONITOR_SOURCE_INVALID")
    else:
        bound = await read_agent_paper_research_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        cycle = (
            None if bound is None else await session.get(AgentPaperCycleRecord, bound[0].cycle_id)
        )
        status = await revalidate_frozen_agent_paper_context(
            session, run=run, evidence_records=rows
        )
        if (
            cycle is None
            or cycle.account_id != account_id
            or status is not FrozenAgentPaperContextStatus.VALID
        ):
            raise ValueError("MONITOR_SOURCE_INVALID")
    return tuple(domain_evidence(row) for row in rows), member


async def reconcile_account_results(
    session: AsyncSession, *, account_id: UUID, instrument_ids: tuple[UUID, ...], now: datetime
) -> dict[str, object]:
    from nexus.paper.candidate_batch import CandidateBatchInput

    now = require_utc(now)
    instrument_ids = CandidateBatchInput(instrument_ids=instrument_ids).instrument_ids
    await lock_portfolio_account(session, account_id)
    # Exact persisted account lineage; do not infer ownership from symbol/model.
    candidates = tuple(
        await session.scalars(
            select(AgentResearchRunRecord)
            .join(DataSnapshot, DataSnapshot.id == AgentResearchRunRecord.data_snapshot_id)
            .outerjoin(
                AgentPaperCycleRecord,
                DataSnapshot.payload["agent_paper_context"]["cycle_id"].astext
                == cast(AgentPaperCycleRecord.id, String),
            )
            .outerjoin(MonitorIncrement, MonitorIncrement.run_id == AgentResearchRunRecord.id)
            .where(
                or_(
                    AgentPaperCycleRecord.account_id == account_id,
                    MonitorIncrement.account_id == account_id,
                ),
                AgentResearchRunRecord.instrument_id.in_(instrument_ids),
                AgentResearchRunRecord.status.in_(("completed", "failed", "abstained")),
                AgentResearchRunRecord.decision_time <= now,
                AgentResearchRunRecord.prompt_manifest["portfolio"]["version"].astext.in_(
                    MONITORING_SCOPE_PROMPT_VERSIONS
                ),
            )
            .distinct(AgentResearchRunRecord.instrument_id)
            .order_by(
                AgentResearchRunRecord.instrument_id,
                AgentResearchRunRecord.decision_time.desc(),
                AgentResearchRunRecord.id.desc(),
            )
            .limit(6)
        )
    )
    if len(candidates) > 5:
        raise ValueError("MONITOR_SCOPE_EXCEEDED")
    counts = {
        "recorded": 0,
        "installed": 0,
        "retained": 0,
        "gap": 0,
        "unknown": 0,
        "unverified": 0,
        "superseded": 0,
        "awaiting_native_finish": 0,
    }
    for run in candidates:
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        paper = None if snapshot is None else snapshot.payload.get("agent_paper_context")
        if isinstance(paper, dict) and paper.get("pending_monitoring_v2") is not None:
            from nexus.monitoring.daily_v2_reconciliation import (
                DailyEventReplacementRejected,
                reconcile_daily_event_run,
                record_daily_event_replacement_gap,
            )

            try:
                async with session.begin_nested():
                    daily_receipts = await reconcile_daily_event_run(
                        session, account_id=account_id, run=run, now=now
                    )
            except DailyEventReplacementRejected:
                daily_receipts = await record_daily_event_replacement_gap(
                    session, account_id=account_id, run=run, now=now
                )
            except ValueError:
                # Existing damaged/partial proof is a refusal, not an empty queue
                # that may dispatch another research job.
                counts["unverified"] += 1
                continue
            if daily_receipts is None:
                counts["awaiting_native_finish"] += 1
            else:
                for daily_receipt in daily_receipts:
                    counts["recorded"] += 1
                    counts[daily_receipt.status] += 1
            continue
        existing = await session.scalar(
            select(MonitorCoverageReceipt).where(
                MonitorCoverageReceipt.kind == "price",
                MonitorCoverageReceipt.account_id == account_id,
                MonitorCoverageReceipt.run_id == run.id,
            )
        )
        if existing is not None:
            continue
        mixed_member = await session.scalar(
            select(MonitorIncrement).where(
                MonitorIncrement.run_id == run.id, MonitorIncrement.batch_id.is_not(None)
            )
        )
        if mixed_member is not None:
            from nexus.monitoring.mixed_reconciliation import (
                MixedReplacementRejected,
                reconcile_mixed_run,
                record_replacement_gap,
            )

            try:
                async with session.begin_nested():
                    mixed_receipts = await reconcile_mixed_run(
                        session, account_id=account_id, run=run, member=mixed_member, now=now
                    )
            except MixedReplacementRejected as rejected:
                await finish_increment(
                    session, account_id=account_id, trigger_id=mixed_member.trigger_id, now=now
                )
                mixed_receipts = await record_replacement_gap(
                    session, account_id=account_id, run=run, rejected=rejected, now=now
                )

            if mixed_receipts is None:
                counts["awaiting_mixed_review"] = counts.get("awaiting_mixed_review", 0) + 1
            else:
                for mixed_receipt in mixed_receipts:
                    counts["recorded"] += 1
                    counts[mixed_receipt.status] += 1
            continue
        if not await _native_finished(session, run, now):
            counts["awaiting_native_finish"] += 1
            continue
        try:
            async with session.begin_nested():
                receipt = await _reconcile_run(session, account_id=account_id, run=run, now=now)
        except ValueError:
            # This records a failed verification of the declared account lineage,
            # never an approved source. It withdraws this security's monitoring only.
            receipt = MonitorCoverageReceipt(
                account_id=account_id,
                instrument_id=run.instrument_id,
                run_id=run.id,
                output_id=None,
                prior_plan_id=None,
                active_plan_id=None,
                research_as_of=run.decision_time,
                observed_at=now,
                status="unverified",
                reason="MONITOR_SOURCE_UNVERIFIED",
                quality_status=None,
            )
            session.add(receipt)
            await session.flush()
            await append_audit_event(
                session,
                actor_id="research-monitor",
                action="research.monitor.coverage_recorded",
                schema_version="monitor_coverage_v1",
                occurred_at=now,
                payload={
                    "receipt_id": str(receipt.id),
                    "run_id": str(run.id),
                    "status": "unverified",
                    "reason": "MONITOR_SOURCE_UNVERIFIED",
                    "active_plan_id": None,
                },
            )
        if receipt is None:
            counts["awaiting_native_finish"] += 1
            continue
        status = receipt.status
        counts["recorded"] += 1
        counts[status] += 1
    return {
        "status": "reconciled",
        **counts,
        "execution_locked": True,
        "model_requests": 0,
        "provider_requests": 0,
    }


async def _reconcile_run(
    session: AsyncSession, *, account_id: UUID, run: AgentResearchRunRecord, now: datetime
) -> MonitorCoverageReceipt | None:
    await lock_security(session, account_id=account_id, instrument_id=run.instrument_id)
    # Revalidate frozen lineage before recording even an unavailable result.
    evidence, member = await _scope_evidence(session, run=run, account_id=account_id)
    if member is not None:
        trigger = await finish_increment(
            session, account_id=account_id, trigger_id=member.trigger_id, now=now
        )
        if trigger.status not in {"completed", "failed", "unknown"}:
            return None
    prior = await session.scalar(
        select(MonitorPlan)
        .where(
            MonitorPlan.kind == "price",
            MonitorPlan.account_id == account_id,
            MonitorPlan.instrument_id == run.instrument_id,
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    previous = await latest_coverage(
        session, account_id=account_id, instrument_id=run.instrument_id, now=now
    )
    prior_run = (
        None if prior is None else await session.get(AgentResearchRunRecord, prior.source_run_id)
    )
    superseded = (
        previous is not None
        and previous.research_as_of >= run.decision_time
        or prior_run is not None
        and prior_run.id != run.id
        and prior_run.decision_time >= run.decision_time
        or member is not None
        and prior is not None
        and prior.id != member.plan_id
        and prior.source_run_id != run.id
    )
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run.id
        )
    )
    from nexus.monitoring.preclaim import preclaim_rejection_qualified

    rejected_before_claim = await preclaim_rejection_qualified(session, run=run, now=now)
    known = rejected_before_claim or (
        reservation is not None
        and reservation.state == "settled"
        and run.input_tokens is not None
        and run.output_tokens is not None
        and run.settled_cost_usd is not None
        and reservation.settled_usd == run.settled_cost_usd
    )
    status, reason, quality, active, output_id = "gap", "MONITOR_RUN_FAILED", None, None, None
    if rejected_before_claim:
        reason = "MONITOR_CAPACITY_REJECTED"
    vetoed = False
    if superseded:
        status, reason = "superseded", "MONITOR_SOURCE_SUPERSEDED"
    elif not known:
        status, reason = "unknown", "MONITOR_UNKNOWN_USAGE"
    elif run.status == "completed":
        from nexus.monitoring.source_qualification import load_qualified_source

        terminal = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == run.id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
            )
        )
        if terminal is not None:
            output_id = terminal.id
            try:
                qualified = await load_qualified_source(
                    session, account_id=account_id, output_id=terminal.id, now=now
                )
                from nexus.monitoring.daily_consumption import record_daily_consumption

                async with session.begin_nested():
                    await record_daily_consumption(
                        session, qualified=qualified, account_id=account_id, now=now
                    )
                quality = qualified.intent.quality_status
                vetoed = any(
                    row.role == "evidence_audit"
                    and row.vote != "support"
                    or row.monitoring_review is not None
                    and row.monitoring_review.decision == "rejected"
                    for row in qualified.role_outputs
                )
                if vetoed:
                    reason = "MONITOR_AUDIT_REJECTED"
                else:
                    try:
                        async with session.begin_nested():
                            installed = await install_plan_from_output(
                                session,
                                account_id=account_id,
                                output_id=terminal.id,
                                expected_predecessor_id=(
                                    member.plan_id
                                    if member is not None
                                    else None
                                    if prior is None
                                    else prior.predecessor_id
                                    if prior.source_run_id == run.id
                                    else prior.id
                                ),
                                now=now,
                            )
                        active, status = installed.id, "installed"
                        reason = (
                            "MONITOR_PARTIAL_CONDITION_APPROVED"
                            if quality == "partial"
                            else "MONITOR_INSTALLED"
                        )
                    except ValueError as error:
                        if str(error) in {"MONITOR_PLAN_CONFLICT", "MONITOR_SOURCE_NOT_NEWER"}:
                            status, reason = "superseded", "MONITOR_SOURCE_SUPERSEDED"
                        else:
                            reason = "MONITOR_NO_APPROVED_CONDITION"
            except ValueError:
                vetoed, reason = True, "MONITOR_SOURCE_INVALID"
    if status in {"gap", "unknown"} and prior is not None and not vetoed:
        assert prior_run is not None
        old = tuple(
            domain_evidence(row)
            for row in await load_frozen_evidence(session, prior_run, allow_fixture_evidence=False)
        )
        if can_retain_plan(
            prior_evidence=old,
            current_evidence=evidence,
            valid_until=prior.valid_until,
            now=now,
            vetoed=vetoed,
            previously_active=(
                previous is None
                or previous.active_plan_id == prior.id
                or prior_run.decision_time > previous.research_as_of
            ),
        ):
            active = prior.id
            if status != "unknown":
                status, reason = "retained", "MONITOR_PRIOR_EVIDENCE_UNCHANGED"
    receipt = MonitorCoverageReceipt(
        account_id=account_id,
        instrument_id=run.instrument_id,
        run_id=run.id,
        output_id=output_id,
        prior_plan_id=None if prior is None else prior.id,
        active_plan_id=active,
        research_as_of=run.decision_time,
        observed_at=now,
        status=status,
        reason=reason,
        quality_status=quality,
    )
    session.add(receipt)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.coverage_recorded",
        schema_version="monitor_coverage_v1",
        occurred_at=now,
        payload={
            "receipt_id": str(receipt.id),
            "run_id": str(run.id),
            "status": status,
            "reason": reason,
            "active_plan_id": None if active is None else str(active),
        },
    )
    return receipt

"""Install an original audited SEC discovery proposal without recapturing its baseline."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.paper_context import AgentPaperResearchContext
from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.disclosure_contracts import compile_disclosure_proposal
from nexus.monitoring.mixed_context import MixedMonitorContext
from nexus.monitoring.source_qualification import load_qualified_source
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorPlan


async def _install_mixed_or_legacy_disclosure(
    session: AsyncSession,
    *,
    account_id: UUID,
    output_id: UUID,
    expected_predecessor_id: UUID | None,
    now: datetime,
) -> MonitorPlan:
    now = require_utc(now)
    source = await load_qualified_source(
        session, account_id=account_id, output_id=output_id, now=now
    )
    context = source.context
    by_role = {row.role: row for row in source.role_outputs}
    proposal = by_role["market_state"].disclosure_proposal
    if (
        not isinstance(context, AgentPaperResearchContext | MixedMonitorContext)
        or context.disclosure_monitoring is None
        or proposal is None
        or source.intent.disclosure_proposal != proposal
        or now >= proposal.review_due_at
    ):
        raise ValueError("MONITOR_DISCLOSURE_SOURCE_INVALID")
    compile_disclosure_proposal(
        proposal,
        {
            str(role): row.disclosure_review
            for role, row in by_role.items()
            if role in {"bear", "evidence_audit"} and row.disclosure_review is not None
        },
        context=context.disclosure_monitoring.model_dump(mode="json"),
        instrument_id=source.run.instrument_id,
        now=source.run.decision_time,
        evidence_refs=source.evidence,
        audit_vote=by_role["evidence_audit"].vote,
        adoption_hash=by_role["portfolio"].disclosure_adoption_hash,
    )
    if isinstance(context, MixedMonitorContext):
        from nexus.monitoring.mixed_reconciliation import frozen_predecessor

        if expected_predecessor_id != frozen_predecessor(context, "disclosure"):
            raise ValueError("MONITOR_PLAN_CONFLICT")
    await lock_security(session, account_id=account_id, instrument_id=source.run.instrument_id)
    existing = await session.scalar(
        select(MonitorPlan).where(
            MonitorPlan.kind == "disclosure",
            MonitorPlan.account_id == account_id,
            MonitorPlan.source_output_id == output_id,
        )
    )
    if existing is not None:
        if existing.predecessor_id != expected_predecessor_id:
            raise ValueError("MONITOR_PLAN_CONFLICT")
        return existing
    prior = await session.scalar(
        select(MonitorPlan)
        .where(
            MonitorPlan.kind == "disclosure",
            MonitorPlan.account_id == account_id,
            MonitorPlan.instrument_id == source.run.instrument_id,
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if (None if prior is None else prior.id) != expected_predecessor_id:
        raise ValueError("MONITOR_PLAN_CONFLICT")
    if prior is not None:
        prior_run = await session.get(AgentResearchRunRecord, prior.source_run_id)
        if (
            prior_run is None
            or source.run.decision_time <= prior_run.decision_time
            or now < prior.effective_at
        ):
            raise ValueError("MONITOR_SOURCE_NOT_NEWER")
    plan = MonitorPlan(
        kind="disclosure",
        account_id=account_id,
        instrument_id=source.run.instrument_id,
        generation=1 if prior is None else prior.generation + 1,
        source_run_id=source.run.id,
        source_output_id=source.output.id,
        data_snapshot_id=source.run.data_snapshot_id,
        predecessor_id=expected_predecessor_id,
        condition_hash=proposal.condition.semantic_hash,
        condition=proposal.condition.model_dump(mode="json"),
        source_hash=source.output.content_hash,
        baseline_snapshot_id=proposal.baseline_snapshot_id,
        baseline_as_of=proposal.baseline_as_of,
        effective_at=now,
        review_due_at=proposal.review_due_at,
        valid_until=proposal.valid_until,
    )
    session.add(plan)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.disclosure_plan_installed",
        schema_version="disclosure_plan_installation_v1",
        occurred_at=now,
        payload={
            "plan_id": str(plan.id),
            "source_output_id": str(output_id),
            "predecessor_id": None if prior is None else str(prior.id),
            "baseline_snapshot_id": str(proposal.baseline_snapshot_id),
            "proposal_hash": proposal.content_hash,
        },
    )
    return plan


async def install_disclosure_plan(
    session: AsyncSession,
    *,
    account_id: UUID,
    output_id: UUID,
    expected_predecessor_id: UUID | None,
    now: datetime,
) -> MonitorPlan:
    """Single-kind public entry; typed batches close only through atomic reconciliation."""
    source = await load_qualified_source(
        session, account_id=account_id, output_id=output_id, now=now
    )
    if isinstance(source.context, MixedMonitorContext):
        raise ValueError("MONITOR_MIXED_ATOMIC_RECONCILIATION_REQUIRED")
    return await _install_mixed_or_legacy_disclosure(
        session,
        account_id=account_id,
        output_id=output_id,
        expected_predecessor_id=expected_predecessor_id,
        now=now,
    )

"""Install only an original, audited, fully settled research proposal; no model I/O."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.paper_context import AgentPaperResearchContext
from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.context import MonitorResearchContext
from nexus.monitoring.contracts import PriceMonitoringProposal, PriceReviewCondition
from nexus.monitoring.evaluation import EpisodeState, Result, advance_episode, compile_proposal
from nexus.monitoring.mixed_context import MixedMonitorContext
from nexus.monitoring.source_qualification import load_qualified_source
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.agents import (
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorEpisode, MonitorPlan


@dataclass(frozen=True)
class ApprovedSource:
    run: AgentResearchRunRecord
    output: AgentResearchOutputRecord
    proposal: PriceMonitoringProposal
    context: AgentPaperResearchContext | MonitorResearchContext | MixedMonitorContext


def consume_episode_baseline(
    state: EpisodeState,
    baseline: Result,
    *,
    last_checked_at: datetime | None,
    observed_at: datetime,
) -> tuple[EpisodeState, bool]:
    if baseline == "unknown" or (last_checked_at is not None and last_checked_at > observed_at):
        return state, False
    updated, _cause = advance_episode(state, baseline)
    return updated, True


def consumed_baseline(
    condition: PriceReviewCondition,
    context: AgentPaperResearchContext | MonitorResearchContext | MixedMonitorContext,
) -> Result:
    """Only evidence actually frozen into this research can seed a replacement latch."""
    quote = context.decision_quote
    try:
        observed = require_utc(quote.observed_at)
        available = require_utc(quote.available_at)
        received = require_utc(quote.received_at)
        decision = require_utc(context.decision_time)
        if quote.bid is None or quote.ask is None:
            return "unknown"
        bid, ask = Decimal(quote.bid), Decimal(quote.ask)
        if (
            not observed <= available <= received <= decision
            or decision - observed > timedelta(seconds=condition.max_age_seconds)
            or not bid.is_finite()
            or not ask.is_finite()
            or not Decimal(0) < bid <= ask
        ):
            return "unknown"
        with localcontext() as arithmetic:
            arithmetic.prec = 50
            midpoint = (bid + ask) / Decimal(2)
        matched = (
            midpoint >= Decimal(condition.threshold)
            if condition.operator == "gte"
            else midpoint <= Decimal(condition.threshold)
        )
        return "true" if matched else "false"
    except (InvalidOperation, ValueError, TypeError):
        return "unknown"


async def load_approved_source(
    session: AsyncSession, *, account_id: UUID, output_id: UUID, now: datetime
) -> ApprovedSource:
    """Require the original role approvals in addition to qualified research facts."""
    source = await load_qualified_source(
        session, account_id=account_id, output_id=output_id, now=now
    )
    by_role = {row.role: row for row in source.role_outputs}
    proposal = by_role["market_state"].monitoring_proposal
    if (
        proposal is None
        or source.intent.monitoring_proposal != proposal
        or by_role["evidence_audit"].vote != "support"
        or by_role["portfolio"].monitoring_adoption_hash != proposal.content_hash
    ):
        raise ValueError("MONITOR_SOURCE_INVALID")
    compile_proposal(
        proposal,
        {
            str(role): row.monitoring_review
            for role, row in by_role.items()
            if role in {"bear", "evidence_audit"} and row.monitoring_review is not None
        },
        now=now,
        evidence_refs=source.evidence,
    )
    return ApprovedSource(source.run, source.output, proposal, source.context)


async def install_plan_from_output(
    session: AsyncSession,
    *,
    account_id: UUID,
    output_id: UUID,
    expected_predecessor_id: UUID | None,
    now: datetime,
) -> MonitorPlan:
    now = require_utc(now)
    source = await load_approved_source(
        session, account_id=account_id, output_id=output_id, now=now
    )
    if (
        isinstance(source.context, MonitorResearchContext | MixedMonitorContext)
        and expected_predecessor_id != source.context.plan_id
    ):
        raise ValueError("MONITOR_PLAN_CONFLICT")
    await lock_security(session, account_id=account_id, instrument_id=source.run.instrument_id)
    existing = await session.scalar(
        select(MonitorPlan).where(
            MonitorPlan.kind == "price",
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
            MonitorPlan.kind == "price",
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
    proposal = source.proposal
    plan = MonitorPlan(
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
        effective_at=now,
        review_due_at=proposal.review_due_at,
        valid_until=proposal.valid_until,
    )
    session.add(plan)
    baseline: Result | None = None
    baseline_applied = False
    if prior is not None:
        state = await session.get(
            MonitorEpisode, (account_id, plan.instrument_id, plan.condition_hash)
        )
        # Includes A->B->A: existing state is consumed only if this frozen quote
        # is newer than its last check. Never eat post-freeze trigger evidence.
        baseline = consumed_baseline(proposal.condition, source.context)
        previous = (
            EpisodeState()
            if state is None
            else EpisodeState(
                sequence=state.sequence, latched=state.latched, observed_false=state.observed_false
            )
        )
        updated, baseline_applied = consume_episode_baseline(
            previous,
            baseline,
            last_checked_at=None if state is None else state.checked_at,
            observed_at=source.context.decision_quote.observed_at,
        )
        if baseline_applied:
            if state is None:
                state = MonitorEpisode(
                    account_id=account_id,
                    instrument_id=plan.instrument_id,
                    condition_hash=plan.condition_hash,
                    checked_at=now,
                )
                session.add(state)
            state.sequence, state.latched = updated.sequence, updated.latched
            state.observed_false, state.checked_at = updated.observed_false, now
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.plan_installed",
        schema_version="monitor_plan_installation_v1",
        occurred_at=now,
        payload={
            "plan_id": str(plan.id),
            "source_output_id": str(output_id),
            "predecessor_id": None if prior is None else str(prior.id),
            "generation": plan.generation,
            "proposal_hash": proposal.content_hash,
            "consumed_snapshot_id": str(plan.data_snapshot_id),
            "baseline": baseline,
            "baseline_applied": baseline_applied,
        },
    )
    return plan

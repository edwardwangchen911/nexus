"""Install only an original, audited, fully settled research proposal; no model I/O."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.actionable_research import actionable_intent_shape_valid
from nexus.agents.committee import MONITORING_SCOPE_PROMPT_VERSION, ROLE_ORDER, CommitteeRoleOutput
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_prompts
from nexus.agents.paper_context import AgentPaperResearchContext, read_agent_paper_research_context
from nexus.agents.persistence import _schema_hash, load_actionable_research_output
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.synthesis import domain_evidence, synthesize_intent
from nexus.api.research_stages import validate_stage_trace
from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.contracts import PriceMonitoringProposal, PriceReviewCondition
from nexus.monitoring.evaluation import EpisodeState, Result, advance_episode, compile_proposal
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorEpisode, MonitorPlan
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.strategy.intents import ResearchIntent


@dataclass(frozen=True)
class ApprovedSource:
    run: AgentResearchRunRecord
    output: AgentResearchOutputRecord
    proposal: PriceMonitoringProposal
    context: AgentPaperResearchContext


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
    condition: PriceReviewCondition, context: AgentPaperResearchContext
) -> Result:
    """Only evidence actually frozen into this research can seed a replacement latch."""
    quote = context.decision_quote
    try:
        observed = require_utc(quote.observed_at)
        available = require_utc(quote.available_at)
        received = require_utc(quote.received_at)
        decision = require_utc(context.decision_time)
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
    """Reconstruct the original intent rather than trusting synthesis's approval claim."""
    invalid = "MONITOR_SOURCE_INVALID"
    output = await session.get(AgentResearchOutputRecord, output_id)
    run = None if output is None else await session.get(AgentResearchRunRecord, output.run_id)
    if (
        output is None
        or run is None
        or output.status != "completed"
        or output.role is not None
        or run.status != "completed"
        or run.settled_cost_usd is None
        or run.input_tokens is None
        or run.output_tokens is None
        or output.intent is None
        or max(output.created_at, run.created_at) > now
    ):
        raise ValueError(invalid)
    prompts = await load_frozen_prompts(session, run)
    if (
        prompts is None
        or prompts["portfolio"].version != MONITORING_SCOPE_PROMPT_VERSION
        or run.schema_hash != _schema_hash(portfolio_prompt_version=MONITORING_SCOPE_PROMPT_VERSION)
    ):
        raise ValueError(invalid)
    admitted = await load_actionable_research_output(
        session,
        run_id=run.id,
        decision_time=now,
        allow_safe_no_trade=True,
        formal_stage_capacity=True,
    )
    intent = ResearchIntent.model_validate(output.intent)
    if admitted.output_id != output.id or not actionable_intent_shape_valid(intent):
        raise ValueError(invalid)
    bound = await read_agent_paper_research_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    cycle = None if bound is None else await session.get(AgentPaperCycleRecord, bound[0].cycle_id)
    if (
        bound is None
        or cycle is None
        or cycle.account_id != account_id
        or not cycle.execution_locked
    ):
        raise ValueError(invalid)
    records = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord)
            .where(
                AgentResearchOutputRecord.run_id == run.id,
                AgentResearchOutputRecord.role.is_not(None),
            )
            .order_by(AgentResearchOutputRecord.sequence)
            .limit(9)
        )
    )
    evidence = tuple(
        domain_evidence(row)
        for row in await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    )
    if (
        len(records) != len(ROLE_ORDER)
        or any(
            row.status != "running" or row.intent is not None or row.created_at > now
            for row in records
        )
        or validate_stage_trace(
            run,
            records,
            evidence,
            {str(role): prompt.content_hash for role, prompt in prompts.items()},
        ).status
        != "verified"
    ):
        raise ValueError(invalid)
    outputs = tuple(CommitteeRoleOutput.model_validate(row.payload) for row in records)
    by_role = {row.role: row for row in outputs}
    proposal = by_role["market_state"].monitoring_proposal
    if (
        proposal is None
        or intent.monitoring_proposal != proposal
        or by_role["evidence_audit"].vote != "support"
        or by_role["portfolio"].monitoring_adoption_hash != proposal.content_hash
    ):
        raise ValueError(invalid)
    compile_proposal(
        proposal,
        {
            str(role): row.monitoring_review
            for role, row in by_role.items()
            if role in {"bear", "evidence_audit"} and row.monitoring_review is not None
        },
        now=now,
        evidence_refs=evidence,
    )
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    if model is None or snapshot is None or model.config_hash != run.model_hash:
        raise ValueError(invalid)
    reconstructed = synthesize_intent(
        run,
        intent.display_symbol,
        snapshot,
        prompts,
        model,
        evidence,
        outputs,
        price_authority_valid=True,  # The formal frozen authority was revalidated above.
        portfolio_context=await read_portfolio_context(
            session, run.data_snapshot_id, run.instrument_id, run.decision_time
        ),
    )
    if reconstructed.model_dump(mode="json") != output.intent:
        raise ValueError(invalid)
    return ApprovedSource(run, output, proposal, bound[0])


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
    await lock_security(session, account_id=account_id, instrument_id=source.run.instrument_id)
    existing = await session.scalar(
        select(MonitorPlan).where(
            MonitorPlan.account_id == account_id, MonitorPlan.source_output_id == output_id
        )
    )
    if existing is not None:
        if existing.predecessor_id != expected_predecessor_id:
            raise ValueError("MONITOR_PLAN_CONFLICT")
        return existing
    prior = await session.scalar(
        select(MonitorPlan)
        .where(
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

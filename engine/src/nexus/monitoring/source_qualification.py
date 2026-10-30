"""Original research facts for monitoring only; never grants formal execution authority."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.actionable_research import (
    _all_nested_evidence_bound,
    _binding_identity,
    _evidence_identity,
    _hash,
    actionable_intent_shape_valid,
    safe_no_trade_intent_shape_valid,
)
from nexus.agents.codex_formal_stage import (
    _formal_stage_job_hash,
    formal_preparation_model_matches,
    formal_stage_output_qualified,
)
from nexus.agents.committee import MONITORING_SCOPE_PROMPT_VERSIONS, ROLE_ORDER, CommitteeRoleOutput
from nexus.agents.execution_authority import (
    load_frozen_evidence,
    load_frozen_prompts,
    load_frozen_symbol,
)
from nexus.agents.paper_context import AgentPaperResearchContext, read_agent_paper_research_context
from nexus.agents.persistence import _schema_hash
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.research_only import require_formal_research
from nexus.agents.synthesis import domain_evidence, synthesize_intent
from nexus.api.research_stages import validate_stage_trace
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.jobs.repository import job_lease_consumed
from nexus.monitoring.admission import require_increment_binding
from nexus.monitoring.context import (
    MonitorResearchContext,
    is_monitor_snapshot,
    read_monitor_context,
)
from nexus.monitoring.independent_context import IndependentMonitorContext, is_independent_snapshot
from nexus.monitoring.independent_storage import require_independent_run
from nexus.monitoring.mixed_context import MixedMonitorContext
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobLease
from nexus.strategy.intents import ResearchIntent


async def _qualify_source(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    output: AgentResearchOutputRecord,
    intent: ResearchIntent,
    account_id: UUID,
    now: datetime,
) -> tuple[
    AgentPaperResearchContext
    | MonitorResearchContext
    | MixedMonitorContext
    | IndependentMonitorContext,
    tuple[EvidenceRef, ...],
]:
    from nexus.agents.comparison_manifest import require_business_run

    require_business_run(run.actor_id)
    invalid = "MONITOR_SOURCE_INVALID"
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    if snapshot is None:
        raise ValueError(invalid)
    context: (
        AgentPaperResearchContext
        | MonitorResearchContext
        | MixedMonitorContext
        | IndependentMonitorContext
    )
    if is_monitor_snapshot(snapshot):
        member = await require_increment_binding(session, run)
        monitor = await read_monitor_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        trigger = await session.get(MonitorTrigger, member.trigger_id)
        if (
            monitor is None
            or member.account_id != account_id
            or trigger is None
            or (trigger.kind != "price" and member.batch_id is None)
            or trigger.status == "unknown"
        ):
            raise ValueError(invalid)
        context = monitor[0]
    elif is_independent_snapshot(snapshot):
        context = await require_independent_run(session, run)
        if context.account_id != account_id:
            raise ValueError(invalid)
    else:
        await require_formal_research(session, run)
        if not await formal_stage_output_qualified(session, run.id, decision_time=now):
            raise ValueError(invalid)
        paper = await read_agent_paper_research_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        cycle = (
            None if paper is None else await session.get(AgentPaperCycleRecord, paper[0].cycle_id)
        )
        if (
            paper is None
            or cycle is None
            or cycle.account_id != account_id
            or not cycle.execution_locked
        ):
            raise ValueError(invalid)
        context = paper[0]
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    if isinstance(context, IndependentMonitorContext):
        checked_at = await session.scalar(select(func.clock_timestamp()))
        if (
            model is None
            or not isinstance(checked_at, datetime)
            or now > checked_at
            or not model.pricing_available_at <= checked_at < model.pricing_expires_at
        ):
            raise ValueError(invalid)
    jobs = tuple(
        await session.scalars(
            select(Job).where(Job.payload["run_id"].astext == str(run.id)).limit(2)
        )
    )
    if len(jobs) != 1:
        raise ValueError(invalid)
    job = jobs[0]
    leases = tuple(
        await session.scalars(select(JobLease).where(JobLease.job_id == job.id).limit(2))
    )
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run.id
        )
    )
    if (
        model is None
        or not formal_preparation_model_matches(model)
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
        or not model.pricing_available_at <= now < model.pricing_expires_at
        or model.created_at > run.decision_time
        or run.status != "completed"
        or run.retry_count != 0
        or run.settled_cost_usd is None
        or run.input_tokens is None
        or run.output_tokens is None
        or reservation is None
        or reservation.state != "settled"
        or reservation.settled_usd != run.settled_cost_usd
        or reservation.pricing_version != run.pricing_version
        or job.job_type != "agent.paper.research"
        or job.queue != "general"
        or job.schema_version != "agent_research_job_v1"
        or job.retry_policy != "automatic_safe"
        or job.status != "succeeded"
        or job.max_attempts != 1
        or job.attempt_count != 1
        or job.manual_retry_count != 0
        or job.payload != {"run_id": str(run.id)}
        or job.scope_kind != "instrument"
        or job.scope_id != str(run.instrument_id)
        or job.effective_at != run.decision_time
        or job.input_hash != _formal_stage_job_hash(run)
        or len(leases) != 1
        or not await job_lease_consumed(session, job=job, lease=leases[0], now=now)
    ):
        raise ValueError(invalid)
    terminals = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord)
            .where(
                AgentResearchOutputRecord.run_id == run.id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status.in_(("completed", "failed", "abstained")),
            )
            .limit(2)
        )
    )
    bindings = tuple(
        await session.scalars(
            select(AgentResearchOutputEvidenceRecord)
            .where(AgentResearchOutputEvidenceRecord.output_id == output.id)
            .limit(101)
        )
    )
    records = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    from nexus.agents.paper_context import (
        FrozenAgentPaperContextStatus,
        revalidate_frozen_agent_paper_context,
    )
    from nexus.agents.price_authority import (
        FrozenPriceAuthorityStatus,
        revalidate_frozen_price_authority,
    )
    from nexus.monitoring.context import FrozenMonitorContextStatus, revalidate_monitor_context

    price_status = await revalidate_frozen_price_authority(
        session, run=run, evidence_records=records, allow_fixture=False
    )
    if price_status is FrozenPriceAuthorityStatus.INVALID:
        raise ValueError(invalid)
    if isinstance(context, MonitorResearchContext | MixedMonitorContext):
        if (
            await revalidate_monitor_context(session, run=run, evidence_records=records)
            is not FrozenMonitorContextStatus.VALID
        ):
            raise ValueError(invalid)
    elif not isinstance(context, IndependentMonitorContext) and (
        await revalidate_frozen_agent_paper_context(session, run=run, evidence_records=records)
        is not FrozenAgentPaperContextStatus.VALID
    ):
        raise ValueError(invalid)
    current = {row.id: row for row in records}
    identities = {_evidence_identity(ref) for ref in intent.evidence_refs}
    if (
        len(terminals) != 1
        or terminals[0].id != output.id
        or output.status != "completed"
        or output.role is not None
        or output.run_id != run.id
        or output.content_hash
        != _hash(
            {
                "status": output.status,
                "role": output.role,
                "payload": output.payload,
                "intent": output.intent,
            }
        )
        or output.intent != intent.model_dump(mode="json")
        or intent.instrument_id != run.instrument_id
        or intent.data_snapshot_id != run.data_snapshot_id
        or intent.decision_time != run.decision_time
        or not run.decision_time <= now <= intent.valid_until
        or max(run.created_at, output.created_at) > now
        or output.payload.get("quality_status") != intent.quality_status
        or not (actionable_intent_shape_valid(intent) or safe_no_trade_intent_shape_valid(intent))
        or await load_frozen_symbol(session, run) != intent.display_symbol
        or snapshot.available_at > run.decision_time
        or snapshot.received_at > run.decision_time
        or not current
        or not bindings
        or len(bindings) > 100
        or len(current) != len(records)
        or len(bindings) != len(current)
        or set(current) != {row.evidence_ref_id for row in bindings}
        or len(identities) != len(intent.evidence_refs)
        or len({_binding_identity(row) for row in bindings}) != len(bindings)
        or identities != {_binding_identity(row) for row in bindings}
        or not _all_nested_evidence_bound(intent, identities)
        or any(
            (
                row.source,
                row.provider_object_id,
                row.revision,
                row.available_at,
                row.location,
                row.content_hash,
            )
            != (
                current[row.evidence_ref_id].source,
                current[row.evidence_ref_id].provider_object_id,
                current[row.evidence_ref_id].revision,
                current[row.evidence_ref_id].available_at,
                current[row.evidence_ref_id].location,
                current[row.evidence_ref_id].content_hash,
            )
            for row in bindings
        )
    ):
        raise ValueError(invalid)
    return context, tuple(domain_evidence(row) for row in records)


@dataclass(frozen=True)
class QualifiedSource:
    run: AgentResearchRunRecord
    output: AgentResearchOutputRecord
    context: (
        AgentPaperResearchContext
        | MonitorResearchContext
        | MixedMonitorContext
        | IndependentMonitorContext
    )
    intent: ResearchIntent
    evidence: tuple[EvidenceRef, ...]
    role_outputs: tuple[CommitteeRoleOutput, ...]


async def load_qualified_source(
    session: AsyncSession, *, account_id: UUID, output_id: UUID, now: datetime
) -> QualifiedSource:
    """Verify original complete/partial research facts without approving a proposal."""
    now = require_utc(now)
    invalid = "MONITOR_SOURCE_INVALID"
    output = await session.get(AgentResearchOutputRecord, output_id)
    run = None if output is None else await session.get(AgentResearchRunRecord, output.run_id)
    if output is None or run is None or output.intent is None:
        raise ValueError(invalid)
    intent = ResearchIntent.model_validate(output.intent)
    prompts = await load_frozen_prompts(session, run)
    if (
        prompts is None
        or prompts["portfolio"].version not in MONITORING_SCOPE_PROMPT_VERSIONS
        or run.schema_hash != _schema_hash(portfolio_prompt_version=prompts["portfolio"].version)
    ):
        raise ValueError(invalid)
    context, evidence = await _qualify_source(
        session, run=run, output=output, intent=intent, account_id=account_id, now=now
    )
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
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    if model is None or snapshot is None:
        raise ValueError(invalid)
    reconstructed = synthesize_intent(
        run,
        intent.display_symbol,
        snapshot,
        prompts,
        model,
        evidence,
        outputs,
        price_authority_valid=(
            not isinstance(context, IndependentMonitorContext)
            or await _independent_price_valid(session, run, evidence)
        ),
        portfolio_context=await read_portfolio_context(
            session, run.data_snapshot_id, run.instrument_id, run.decision_time
        ),
    )
    if reconstructed.model_dump(mode="json") != output.intent:
        raise ValueError(invalid)
    return QualifiedSource(run, output, context, intent, evidence, outputs)


async def _independent_price_valid(
    session: AsyncSession, run: AgentResearchRunRecord, evidence: tuple[EvidenceRef, ...]
) -> bool:
    from nexus.agents.price_authority import (
        FrozenPriceAuthorityStatus,
        revalidate_frozen_price_authority,
    )

    records = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    return (
        await revalidate_frozen_price_authority(
            session, run=run, evidence_records=records, allow_fixture=False
        )
        is FrozenPriceAuthorityStatus.VALID
    )

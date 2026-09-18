"""Exactly-once portfolio synthesis admission and terminal accounting, without model I/O."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Literal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.agents.persistence import (
    _lock_budget_day,
    _shared_daily_budget_used,
    _validate_budget_inputs,
    load_actionable_research_output,
)
from nexus.agents.portfolio_synthesis_contracts import (
    SYNTHESIS_PROMPT_VERSION,
    SYNTHESIS_TOOL_HASH,
    PortfolioSecurityOutputV1,
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    PortfolioTargetWeightV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.synthesis import usage_cost
from nexus.domain.time import require_utc
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy
from nexus.jobs.repository import enqueue_job
from nexus.paper.agent_cycle_models import candidate_manifest_hash, parse_candidate_manifest
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_POLICY_HASH, PORTFOLIO_ALLOCATION_V1
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
    StrategyDecisionTargetRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.strategy.intents import ResearchIntent


@dataclass(frozen=True)
class PortfolioSynthesisAdmission:
    run: AgentPortfolioSynthesisRunRecord
    replayed: bool

    @property
    def run_id(self) -> UUID:
        return self.run.id


async def _lock_account(session: AsyncSession, account_id: UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{account_id}"},
    )


def _frozen_run(run: AgentPortfolioSynthesisRunRecord) -> PortfolioSynthesisInputV1:
    frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
    if (
        canonical_portfolio_synthesis_hash(frozen) != run.input_hash
        or frozen.cycle_id != run.cycle_id
        or frozen.candidate_batch_id != run.candidate_batch_id
        or tuple(item.research_output_id for item in frozen.security_outputs)
        != tuple(run.research_output_ids)
        or frozen.active_target_version_id != run.active_target_version_id
        or frozen.projection_hash != run.projection_hash
        or frozen.prompt_version_id != run.prompt_version_id
        or frozen.prompt_hash != run.prompt_hash
        or frozen.model_config_id != run.model_config_id
        or frozen.model_hash != run.model_hash
        or frozen.pricing_version != run.pricing_version
        or frozen.decision_time != run.decision_time
        or run.idempotency_key != f"portfolio-synthesis:{run.cycle_id}"
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_INPUT_CONFLICT")
    return frozen


async def admit_portfolio_synthesis(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    research_output_ids: tuple[UUID, ...],
    active_target_version_id: UUID | None,
    projection_hash: str,
    daily_limit_usd: Decimal,
    now: datetime,
) -> PortfolioSynthesisAdmission:
    """Freeze a complete research set and atomically reserve one non-retryable Job."""
    require_utc(now)
    if not 1 <= len(research_output_ids) <= 5 or len(set(research_output_ids)) != len(
        research_output_ids
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_INPUT_CONFLICT")
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or cycle.candidate_batch_id is None:
        raise ValueError("PORTFOLIO_SYNTHESIS_CYCLE_INVALID")
    await _lock_account(session, cycle.account_id)
    existing = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord)
        .where(AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id)
        .execution_options(populate_existing=True)
    )
    if existing is not None:
        if (
            set(existing.research_output_ids) != set(research_output_ids)
            or existing.active_target_version_id != active_target_version_id
            or existing.projection_hash != projection_hash
        ):
            raise ValueError("PORTFOLIO_SYNTHESIS_INPUT_CONFLICT")
        _frozen_run(existing)
        return PortfolioSynthesisAdmission(existing, True)

    if (
        cycle.sizing_policy_version != PORTFOLIO_ALLOCATION_V1.version
        or cycle.sizing_policy_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
        or not cycle.execution_locked
        or cycle.decision_time > now
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_POLICY_INVALID")
    batch, _ = await validate_agent_paper_candidate_batch(
        session,
        batch_id=cycle.candidate_batch_id,
        account_id=cycle.account_id,
        as_of=cycle.decision_time,
        universe_snapshot_id=cycle.universe_snapshot_id,
    )
    manifest = parse_candidate_manifest(
        cycle.candidate_manifest,
        allow_legacy=cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
    )
    if (
        tuple(row.instrument_id for row in manifest) != batch.instrument_ids
        or candidate_manifest_hash(manifest) != cycle.candidate_hash
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_CANDIDATES_INVALID")
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle.id)
            .order_by(AgentPaperCycleEventRecord.sequence)
        )
    )
    admitted = [event for event in events if event.result_code == "RESEARCH_JOBS_PREPARED"]
    if not events or events[-1].to_status != "researching" or len(admitted) != 1:
        raise ValueError("PORTFOLIO_SYNTHESIS_RESEARCH_INVALID")
    raw_ids = admitted[0].fact_ids.get("research_run_ids")
    if not isinstance(raw_ids, list):
        raise ValueError("PORTFOLIO_SYNTHESIS_RESEARCH_INVALID")
    run_ids = tuple(UUID(str(value)) for value in raw_ids)
    outputs = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.id.in_(research_output_ids)
            )
        )
    )
    runs = tuple(
        await session.scalars(
            select(AgentResearchRunRecord).where(AgentResearchRunRecord.id.in_(run_ids))
        )
    )
    if (
        len(set(run_ids)) != len(batch.instrument_ids)
        or len(runs) != len(run_ids)
        or len(outputs) != len(run_ids)
        or {row.run_id for row in outputs} != set(run_ids)
        or {row.instrument_id for row in runs} != set(batch.instrument_ids)
        or any(
            row.decision_time != cycle.decision_time
            or row.actor_id != "local-agent-paper-cycle"
            or row.idempotency_key != f"agent-paper:{cycle.id}:{row.instrument_id}"
            for row in runs
        )
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_RESEARCH_INVALID")
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.job_type.in_(
                    (JobType.AGENT_RESEARCH.value, JobType.AGENT_PAPER_RESEARCH.value)
                ),
                Job.payload["run_id"].as_string().in_([str(value) for value in run_ids]),
            )
        )
    )
    if len(jobs) != len(run_ids) or any(
        job.status != "succeeded" or job.finished_at is None or job.finished_at > now
        for job in jobs
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_RESEARCH_INVALID")
    source_by_run = {row.run_id: row for row in outputs}
    security = []
    for research_run in sorted(runs, key=lambda row: row.instrument_id.int):
        action = await load_actionable_research_output(
            session, run_id=research_run.id, decision_time=now, allow_safe_no_trade=True
        )
        output = source_by_run[research_run.id]
        if action.output_id != output.id:
            raise ValueError("PORTFOLIO_SYNTHESIS_RESEARCH_INVALID")
        security.append(
            PortfolioSecurityOutputV1(
                instrument_id=research_run.instrument_id,
                research_output_id=output.id,
                content_hash=output.content_hash,
                intent_hash=action.intent_hash,
                evidence_ref_ids=action.evidence_ref_ids,
                intent=ResearchIntent.model_validate(output.intent),
            )
        )
    projection = await load_paper_projection(
        session, account_id=cycle.account_id, as_of=cycle.decision_time
    )
    if (
        projection.projection_hash != projection_hash
        or projection.portfolio_id != cycle.portfolio_id
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_PROJECTION_INVALID")
    for research_run in runs:
        context = await read_agent_paper_research_context(
            session,
            snapshot_id=research_run.data_snapshot_id,
            instrument_id=research_run.instrument_id,
            as_of=cycle.decision_time,
        )
        if (
            context is None
            and cycle.decision_price_policy_id != PAPER_DECISION_SOURCE_POLICY_ID
            or context is not None
            and (
                context[0].cycle_id != cycle.id
                or context[0].paper_portfolio.projection_hash != projection_hash
            )
        ):
            raise ValueError("PORTFOLIO_SYNTHESIS_PROJECTION_INVALID")
    target = await session.scalar(
        select(PaperTargetVersionRecord)
        .where(PaperTargetVersionRecord.account_id == cycle.account_id)
        .order_by(PaperTargetVersionRecord.version_seq.desc())
        .limit(1)
    )
    if (None if target is None else target.id) != active_target_version_id or (
        target is not None
        and (target.activated_at > cycle.decision_time or target.portfolio_id != cycle.portfolio_id)
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_TARGET_INVALID")
    target_rows = (
        ()
        if target is None
        else tuple(
            await session.scalars(
                select(StrategyDecisionTargetRecord)
                .where(
                    StrategyDecisionTargetRecord.strategy_decision_id == target.strategy_decision_id
                )
                .order_by(StrategyDecisionTargetRecord.instrument_id)
            )
        )
    )
    target_decision = (
        None
        if target is None
        else await session.get(StrategyDecisionRecord, target.strategy_decision_id)
    )
    if target is not None and (
        target_decision is None or target_decision.account_id != cycle.account_id
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_TARGET_INVALID")
    calendar_row = (
        await session.execute(
            select(ExchangeCalendarAuthorityRecord, ExchangeCalendarSessionRecord)
            .join(
                ExchangeCalendarSessionRecord,
                ExchangeCalendarSessionRecord.calendar_authority_id
                == ExchangeCalendarAuthorityRecord.id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= cycle.decision_time,
                ExchangeCalendarAuthorityRecord.created_at <= cycle.decision_time,
                ExchangeCalendarSessionRecord.session == cycle.decision_session,
                ExchangeCalendarSessionRecord.created_at <= cycle.decision_time,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(1)
        )
    ).one_or_none()
    if calendar_row is None or now >= calendar_row[1].market_close_at:
        raise ValueError("PORTFOLIO_SYNTHESIS_SESSION_EXPIRED")
    prompt = await session.scalar(
        select(AgentPromptVersionRecord).where(
            AgentPromptVersionRecord.role == "portfolio_decision",
            AgentPromptVersionRecord.version == SYNTHESIS_PROMPT_VERSION,
            AgentPromptVersionRecord.release_channel == "active",
        )
    )
    if (
        prompt is None
        or prompt.created_at > now
        or sha256(prompt.content.encode()).hexdigest() != prompt.content_hash
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_PROMPT_INVALID")
    model_ids = {row.model_config_id for row in runs}
    model = await session.get(AgentModelConfigRecord, next(iter(model_ids)))
    if (
        len(model_ids) != 1
        or model is None
        or model.created_at > now
        or model.pricing_available_at > now
        or model.pricing_expires_at <= now
        or any(
            row.model_hash != model.config_hash or row.pricing_version != model.version
            for row in runs
        )
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_MODEL_INVALID")
    frozen = PortfolioSynthesisInputV1(
        schema_version="portfolio_synthesis_input_v1",
        cycle_id=cycle.id,
        account_id=cycle.account_id,
        portfolio_id=cycle.portfolio_id,
        candidate_batch_id=batch.batch_id,
        candidate_batch_hash=batch.batch_hash,
        candidate_instrument_ids=batch.instrument_ids,
        security_outputs=tuple(security),
        active_target_version_id=active_target_version_id,
        active_target_content_hash=None if target is None else target.target_content_hash,
        active_target_instrument_ids=tuple(row.instrument_id for row in target_rows),
        active_target_weights=tuple(
            PortfolioTargetWeightV1(
                instrument_id=row.instrument_id, target_weight=row.target_weight
            )
            for row in target_rows
        ),
        active_target_cash_weight=None if target_decision is None else target_decision.cash_weight,
        projection=projection,
        projection_hash=projection_hash,
        policy_version=PORTFOLIO_ALLOCATION_V1.version,
        policy_hash=PORTFOLIO_ALLOCATION_POLICY_HASH,
        prompt_version_id=prompt.id,
        prompt_version=SYNTHESIS_PROMPT_VERSION,
        prompt_hash=prompt.content_hash,
        model_config_id=model.id,
        model_hash=model.config_hash,
        pricing_version=model.version,
        tool_hash=SYNTHESIS_TOOL_HASH,
        intent_schema_hash=canonical_portfolio_synthesis_hash(
            PortfolioSynthesisIntentV1.model_json_schema()
        ),
        calendar_authority_id=calendar_row[0].id,
        decision_time=cycle.decision_time,
        admitted_at=now,
        expires_at=calendar_row[1].market_close_at,
    )
    worst = model.worst_case_cost_usd
    _validate_budget_inputs(now, daily_limit_usd, worst, model.version)
    await _lock_budget_day(session, now.date().isoformat())
    used = await _shared_daily_budget_used(session, business_date=now.date())
    if used + worst > daily_limit_usd:
        raise BudgetExhausted("daily research budget exhausted")
    async with session.begin_nested():
        run = AgentPortfolioSynthesisRunRecord(
            cycle_id=cycle.id,
            candidate_batch_id=batch.batch_id,
            research_output_ids=[row.research_output_id for row in security],
            active_target_version_id=active_target_version_id,
            projection_hash=projection_hash,
            prompt_version_id=prompt.id,
            model_config_id=model.id,
            prompt_hash=prompt.content_hash,
            model_hash=model.config_hash,
            input_hash=canonical_portfolio_synthesis_hash(frozen),
            frozen_input=frozen.model_dump(mode="json"),
            idempotency_key=f"portfolio-synthesis:{cycle.id}",
            status="prepared",
            pricing_version=model.version,
            decision_time=cycle.decision_time,
        )
        session.add(run)
        await session.flush()
        session.add(
            AgentPortfolioSynthesisBudgetReservationRecord(
                business_date=now.date(),
                run_id=run.id,
                pricing_version=model.version,
                reserved_usd=worst,
                state="reserved",
            )
        )
        await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.AGENT_PORTFOLIO_SYNTHESIS,
                scope_kind="portfolio",
                scope_id=str(cycle.portfolio_id),
                effective_at=cycle.decision_time,
                input_hash=run.input_hash,
            ),
            schema_version="portfolio_synthesis_job_v1",
            payload={"run_id": str(run.id)},
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.NEVER,
            max_attempts=1,
            available_at=now,
        )
        await session.flush()
    return PortfolioSynthesisAdmission(run, False)


async def persist_portfolio_synthesis_terminal(
    session: AsyncSession,
    *,
    run_id: UUID,
    input_hash: str,
    status: Literal["completed", "abstained", "failed"],
    intent: PortfolioSynthesisIntentV1 | None,
    input_tokens: int | None,
    output_tokens: int | None,
    now: datetime,
) -> AgentPortfolioSynthesisOutputRecord:
    """Commit one bound terminal; unknown post-call usage retains the full reservation."""
    require_utc(now)
    run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
    cycle = None if run is None else await session.get(AgentPaperCycleRecord, run.cycle_id)
    if run is None or cycle is None:
        raise ValueError("PORTFOLIO_SYNTHESIS_RUN_INVALID")
    await _lock_account(session, cycle.account_id)
    await session.refresh(run)
    frozen = _frozen_run(run)
    if input_hash != run.input_hash:
        raise ValueError("PORTFOLIO_SYNTHESIS_INPUT_CONFLICT")
    if status not in {"completed", "failed", "abstained"} or (status == "completed") != (
        intent is not None
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_TERMINAL_INVALID")
    if (input_tokens is None) != (output_tokens is None) or any(
        value is not None and (type(value) is not int or value < 0)
        for value in (input_tokens, output_tokens)
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_USAGE_INVALID")
    if intent is not None:
        intent = PortfolioSynthesisIntentV1.model_validate(intent.model_dump(mode="json"))
        validate_portfolio_synthesis_intent(intent, frozen)
    content = {
        "schema_version": "portfolio_synthesis_terminal_v1",
        "status": status,
        "intent": None if intent is None else intent.model_dump(mode="json"),
    }
    content_hash = canonical_portfolio_synthesis_hash(content)
    binding_hash = canonical_portfolio_synthesis_hash(
        {
            "schema_version": "portfolio_synthesis_binding_v1",
            "run_id": run.id,
            "input_hash": input_hash,
            "content_hash": content_hash,
        }
    )
    existing = await session.scalar(
        select(AgentPortfolioSynthesisOutputRecord).where(
            AgentPortfolioSynthesisOutputRecord.run_id == run.id
        )
    )
    if existing is not None:
        if (
            existing.content_hash != content_hash
            or existing.binding_hash != binding_hash
            or run.input_tokens != input_tokens
            or run.output_tokens != output_tokens
        ):
            raise ValueError("PORTFOLIO_SYNTHESIS_TERMINAL_CONFLICT")
        return existing
    if (
        run.status not in {"prepared", "running"}
        or now < frozen.admitted_at
        or run.status == "prepared"
        and (status != "failed" or input_tokens is not None)
        or status != "failed"
        and (input_tokens is None or now >= frozen.expires_at)
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_TERMINAL_INVALID")
    reservation = await session.scalar(
        select(AgentPortfolioSynthesisBudgetReservationRecord).where(
            AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
        )
    )
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    if (
        reservation is None
        or reservation.state != "reserved"
        or model is None
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
        or reservation.pricing_version != run.pricing_version
        or reservation.reserved_usd != model.worst_case_cost_usd
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_BUDGET_INVALID")
    await _lock_budget_day(session, reservation.business_date.isoformat())
    cost = (
        None
        if input_tokens is None or output_tokens is None
        else usage_cost(model, input_tokens, output_tokens)
    )
    async with session.begin_nested():
        if cost is not None:
            reservation.state = "settled"
            reservation.settled_usd = cost
        elif run.status == "prepared":
            reservation.state = "released"
            reservation.settled_usd = Decimal("0")
            cost = Decimal("0")
        run.status = status
        run.input_tokens = input_tokens
        run.output_tokens = output_tokens
        run.settled_cost_usd = cost
        run.completed_at = now
        output = AgentPortfolioSynthesisOutputRecord(
            run_id=run.id,
            status=status,
            intent=content["intent"],
            content_hash=content_hash,
            binding_hash=binding_hash,
        )
        session.add(output)
        await session.flush()
    return output

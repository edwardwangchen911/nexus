"""Normal mixed batch admission under the same account, formal data and finite call guards."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.jobs.models import job_input_hash
from nexus.monitoring.admission import (
    _freeze_increment_snapshot,
    increment_manifest_hash,
    require_increment_binding,
)
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import Job


async def admit_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    materials_snapshot_id: UUID,
    candidate_batch_id: UUID,
    calendar_authority_id: UUID,
    model_config_id: UUID,
    daily_limit_usd: Decimal,
    daily_sdk_call_limit: int,
    now: datetime,
) -> MonitorIncrement:
    async with session.begin_nested():
        return await _admit_batch(
            session,
            account_id=account_id,
            batch_id=batch_id,
            materials_snapshot_id=materials_snapshot_id,
            candidate_batch_id=candidate_batch_id,
            calendar_authority_id=calendar_authority_id,
            model_config_id=model_config_id,
            daily_limit_usd=daily_limit_usd,
            daily_sdk_call_limit=daily_sdk_call_limit,
            now=now,
        )


async def _admit_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    materials_snapshot_id: UUID,
    candidate_batch_id: UUID,
    calendar_authority_id: UUID,
    model_config_id: UUID,
    daily_limit_usd: Decimal,
    daily_sdk_call_limit: int,
    now: datetime,
) -> MonitorIncrement:
    import hashlib
    from decimal import Decimal, localcontext

    from sqlalchemy import func

    from nexus.agents.models import ResearchRequest
    from nexus.agents.paper_context import AgentPaperPortfolioContext
    from nexus.agents.persistence import admit_research_run
    from nexus.domain.time import require_utc
    from nexus.jobs.models import JobType
    from nexus.monitoring.contracts import PriceReviewCondition
    from nexus.monitoring.disclosure_context import freeze_disclosure_baseline
    from nexus.monitoring.evaluation import evaluate_price
    from nexus.monitoring.mixed_context import MixedMonitorContext, MixedTriggerCheck, content
    from nexus.monitoring.observations import read_latest_price
    from nexus.paper.agent_cycle_authority import (
        load_agent_paper_service_calendar_authority,
        load_quasi_realtime_cycle_authority,
        prepare_candidates,
    )
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
    from nexus.paper.manual_request import require_manual_account_available
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.target_service import lock_portfolio_account
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import MonitorPlan
    from nexus.strategy.intents import ResearchIntent

    now = require_utc(now)
    if type(daily_sdk_call_limit) is not int or not 3 <= daily_sdk_call_limit <= 90:
        raise ValueError("MONITOR_CALL_LIMIT_INVALID")
    await lock_portfolio_account(session, account_id)
    frozen_now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(frozen_now, datetime):
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    now = require_utc(frozen_now)
    from nexus.monitoring.batch_inputs import _inputs, read_batch_inputs
    from nexus.persistence.models.monitoring import MonitorObservationBatch

    batch_row = await session.get(MonitorObservationBatch, batch_id)
    if batch_row is None or batch_row.account_id != account_id:
        raise ValueError("MONITOR_BATCH_UNAVAILABLE")
    await lock_security(session, account_id=account_id, instrument_id=batch_row.instrument_id)
    prior = await session.scalar(
        select(MonitorIncrement).where(MonitorIncrement.batch_id == batch_id)
    )
    if prior is not None:
        run = await session.get(AgentResearchRunRecord, prior.run_id)
        if run is None:
            raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
        return await require_increment_binding(session, run)
    await require_manual_account_available(
        session, account_id=account_id, now=now, allow_scheduled_completion=True
    )
    from nexus.monitoring.mixed_readback import unadmitted_stock_count

    if (
        await unadmitted_stock_count(
            session, account_id=account_id, instrument_id=batch_row.instrument_id
        )
        > 32
    ):
        raise ValueError("MONITOR_PENDING_OVERFLOW")
    materials = await read_batch_inputs(session, snapshot_id=materials_snapshot_id, as_of=now)
    material_row = await session.get(DataSnapshot, materials_snapshot_id)
    if materials is None or material_row is None or materials.batch_id != batch_id:
        raise ValueError("MONITOR_CONTEXT_INVALID")
    current = await _inputs(session, batch_row, now)
    if (current.parents, current.members, current.contents) != (
        materials.parents,
        materials.members,
        materials.contents,
    ):
        raise ValueError("MONITOR_PREPARED_INPUTS_CHANGED")
    triggers = tuple(
        await session.scalars(
            select(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch_id)
            .order_by(MonitorTrigger.id)
        )
    )
    if len(triggers) != len(materials.members) or any(
        t.status not in {"pending", "blocked"} or t.research_run_id is not None for t in triggers
    ):
        raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
    trigger = triggers[0]
    parent_plan = next((p for p in materials.parents if p["kind"] == "price"), materials.parents[0])
    plan = await session.get(MonitorPlan, UUID(str(parent_plan["plan_id"])))
    if plan is None:
        raise ValueError("MONITOR_SOURCE_INVALID")
    calendar = await load_agent_paper_service_calendar_authority(
        session, now=now, expected_calendar_authority_id=calendar_authority_id
    )
    if not calendar.market_open_at <= now < calendar.market_close_at:
        raise ValueError("MONITOR_MARKET_CLOSED")
    from nexus.monitoring.reconciliation import plan_covered

    if plan.kind == "price" and not await plan_covered(session, plan=plan, now=now):
        raise ValueError("MONITOR_TRIGGER_SUPERSEDED_OR_EXPIRED")
    batch, _identities = await validate_agent_paper_candidate_batch(
        session, batch_id=candidate_batch_id, account_id=account_id, as_of=now
    )
    if trigger.instrument_id not in batch.instrument_ids:
        raise ValueError("MONITOR_SCOPE_INVALID")
    authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=calendar_authority_id,
        expected_universe_snapshot_id=batch.universe_snapshot_id,
    )
    candidates = await prepare_candidates(
        session,
        authority=authority,
        requested_instrument_ids=(trigger.instrument_id,),
        max_instruments=1,
    )
    quote = await read_latest_price(session, instrument_id=trigger.instrument_id, now=now)
    if (
        len(candidates) != 1
        or quote is None
        or quote.quote_id != candidates[0].decision_quote_id
        or plan.kind == "price"
        and evaluate_price(
            PriceReviewCondition.model_validate(plan.condition),
            quote,
            instrument_id=trigger.instrument_id,
            now=now,
        ).result
        == "unknown"
    ):
        raise ValueError("MONITOR_CURRENT_INPUTS_UNAVAILABLE")
    # Separate finite subscription calls from USD budget; unknown never refunds calls.
    used, prior_limit = (
        await session.execute(
            select(
                func.coalesce(func.sum(MonitorIncrement.reserved_sdk_calls), 0),
                func.min(MonitorIncrement.daily_sdk_call_limit),
            ).where(
                MonitorIncrement.account_id == account_id,
                MonitorIncrement.session_date == authority.decision_session,
            )
        )
    ).one()
    limit = daily_sdk_call_limit if prior_limit is None else min(daily_sdk_call_limit, prior_limit)
    if used + 3 > limit:
        raise ValueError("MONITOR_DAILY_CALL_LIMIT")
    parent = await session.get(AgentResearchRunRecord, plan.source_run_id)
    output = await session.get(AgentResearchOutputRecord, plan.source_output_id)
    if (
        parent is None
        or output is None
        or output.intent is None
        or parent.model_config_id != model_config_id
    ):
        raise ValueError("MONITOR_SOURCE_INVALID")
    projection = await load_paper_projection(
        session, account_id=account_id, as_of=now, derived_snapshots_known_at=now
    )
    position = next(
        (p for p in projection.positions if p.instrument_id == trigger.instrument_id), None
    )
    if projection.nav <= 0:
        raise ValueError("MONITOR_CURRENT_INPUTS_UNAVAILABLE")
    with localcontext() as arithmetic:
        arithmetic.prec = 38
        weight = (
            Decimal(0)
            if position is None
            else position.quantity * position.reference_close / projection.nav
        )
        cash = (projection.available_cash + projection.reserved_cash) / projection.nav
    context = MixedMonitorContext(
        batch_id=batch_id,
        materials_snapshot_id=materials_snapshot_id,
        materials_hash=material_row.revision,
        materials=materials,
        disclosure_monitoring=await freeze_disclosure_baseline(
            session, instrument_id=trigger.instrument_id, as_of=now
        ),
        account_id=account_id,
        instrument_id=trigger.instrument_id,
        parent_run_id=parent.id,
        parent_output_id=output.id,
        parent_output_hash=output.content_hash,
        parent_intent=ResearchIntent.model_validate(output.intent),
        plan_id=plan.id,
        plan_generation=plan.generation,
        decision_time=now,
        decision_quote=quote,
        candidate=candidates[0],
        authority=authority,
        trigger_checks=tuple(
            MixedTriggerCheck.model_validate(
                {key: value for key, value in member.items() if key != "plan_id"}
            )
            for member in materials.members
        ),
        paper_portfolio=AgentPaperPortfolioContext(
            candidate_position_weight=str(weight),
            cash_weight=str(cash),
            projection_hash=projection.projection_hash,
        ),
    )
    snapshot = await _freeze_increment_snapshot(session, context)
    request_hash = job_input_hash(
        {
            "purpose": context.purpose,
            "trigger_id": str(trigger.id),
            "snapshot_id": str(snapshot.id),
            "context_hash": hashlib.sha256(content(context)).hexdigest(),
        }
    )
    admitted = await admit_research_run(
        session,
        actor_id=f"monitor:{account_id}",
        route="research-monitor/admit-batch",
        idempotency_key=f"monitor-batch:{batch_id}",
        request_hash=request_hash,
        request=ResearchRequest(
            instrument_id=trigger.instrument_id, data_snapshot_id=snapshot.id, decision_time=now
        ),
        prompt_version_id=parent.prompt_version_id,
        model_config_id=parent.model_config_id,
        daily_limit_usd=daily_limit_usd,
        now=now,
        job_type=JobType.AGENT_PAPER_RESEARCH,
        formal_stage_capacity=True,
        monitor_increment=True,
    )
    if admitted.replayed or admitted.run.prompt_hash != parent.prompt_hash:
        raise ValueError("MONITOR_VERSION_BINDING_INVALID")
    job = await session.scalar(
        select(Job).where(Job.payload["run_id"].astext == str(admitted.run.id))
    )
    if job is None:
        raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
    member = MonitorIncrement(
        account_id=account_id,
        instrument_id=trigger.instrument_id,
        batch_id=batch_id,
        trigger_id=trigger.id,
        plan_id=plan.id,
        run_id=admitted.run.id,
        job_id=job.id,
        snapshot_id=snapshot.id,
        research_as_of=now,
        session_date=authority.decision_session,
        reserved_sdk_calls=3,
        daily_sdk_call_limit=limit,
        context_hash=hashlib.sha256(content(context)).hexdigest(),
        manifest_hash="0" * 64,
    )
    member.manifest_hash = increment_manifest_hash(member, admitted.run, job)
    session.add(member)
    for item in triggers:
        item.status, item.reason, item.research_run_id = "admitted", None, admitted.run.id
    await session.flush()
    await require_increment_binding(session, admitted.run)
    from nexus.audit.service import append_audit_event

    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.increment_admitted",
        schema_version="monitor_increment_admission_v1",
        occurred_at=now,
        payload={
            "trigger_id": str(trigger.id),
            "run_id": str(member.run_id),
            "manifest_hash": member.manifest_hash,
            "reserved_sdk_calls": 3,
        },
    )
    from nexus.agents.research_stages.routes import register_new_prepared_runs

    await register_new_prepared_runs(session, (member.run_id,))
    return member

"""Read-only ownership checks for an already registered isolated evaluation."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import _formal_stage_job_hash as research_job_hash
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.agents.portfolio_synthesis_persistence import load_frozen_synthesis_authority
from nexus.jobs.models import JobType
from nexus.paper.economic_budget_hold import HoldRequest, require_protocol_budget_day
from nexus.paper.economic_cycle import (
    EconomicLaneAuthority,
    EconomicLaneRef,
    read_economic_cycle,
    require_economic_lane,
)
from nexus.paper.portfolio_economic_protocol import (
    require_economic_database_target,
    require_isolated_database,
)
from nexus.persistence.base import Base
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord, AgentResearchRunRecord
from nexus.persistence.models.economic import EconomicEvaluationRecord, EconomicPairSlotRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import Job

_FORBIDDEN = "ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"


async def require_running_isolation(
    session: AsyncSession, *, authority: EconomicLaneAuthority, expected_database: str | None = None
) -> None:
    """Allow owned historical/pending facts; do not requalify their old pricing."""
    actual = await session.scalar(text("SELECT current_database()"))
    if not isinstance(actual, str):
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    await require_economic_database_target(session, expected=expected_database or actual)
    for name in ("broker_account_bindings", "account_external_id_envelopes"):
        if await session.scalar(select(func.count()).select_from(Base.metadata.tables[name])):
            raise ValueError(_FORBIDDEN)

    owners: dict[UUID, EconomicLaneAuthority] = {}
    cycles = tuple(await session.scalars(select(AgentPaperCycleRecord)))
    for cycle in cycles:
        owned = await read_economic_cycle(session, cycle_id=cycle.id)
        if owned is None or owned[1].identity.id != authority.identity.id:
            raise ValueError(_FORBIDDEN)
        owners[cycle.id] = owned[1]

    research = tuple(await session.scalars(select(AgentResearchRunRecord)))
    research_by_id = {}
    for run in research:
        context = await read_agent_paper_research_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        owner = None if context is None else owners.get(context[0].cycle_id)
        if (
            context is None
            or owner is None
            or run.actor_id != "local-agent-paper-cycle"
            or run.idempotency_key != f"agent-paper:{context[0].cycle_id}:{run.instrument_id}"
            or run.decision_time != owner.slot.decision_at
            or run.instrument_id
            not in {item.instrument_id for item in owner.protocol.request.instruments}
            or run.model_config_id != owner.binding.shared_model_id
            or run.model_hash != owner.binding.shared_model_hash
            or run.pricing_version != owner.binding.shared_model_version
        ):
            raise ValueError(_FORBIDDEN)
        research_by_id[run.id] = run

    synthesis = tuple(await session.scalars(select(AgentPortfolioSynthesisRunRecord)))
    synthesis_by_id = {}
    for synthesis_record in synthesis:
        if synthesis_record.cycle_id not in owners:
            raise ValueError(_FORBIDDEN)
        try:
            await load_frozen_synthesis_authority(session, synthesis_record)
        except ValueError:
            raise ValueError(_FORBIDDEN) from None
        synthesis_by_id[synthesis_record.id] = synthesis_record

    cycle_by_id = {item.id: item for item in cycles}
    seen: set[tuple[str, UUID]] = set()
    for job in await session.scalars(select(Job)):
        try:
            run_id = UUID(str(job.payload.get("run_id")))
        except (ValueError, TypeError, AttributeError):
            raise ValueError(_FORBIDDEN) from None
        key = (job.job_type, run_id)
        if key in seen or job.max_attempts != 1:
            raise ValueError(_FORBIDDEN)
        if job.job_type == JobType.AGENT_PAPER_RESEARCH.value:
            research_run = research_by_id.get(run_id)
            if research_run is None or (
                job.schema_version,
                job.input_hash,
                job.scope_kind,
                job.scope_id,
                job.effective_at,
            ) != (
                "agent_research_job_v1",
                research_job_hash(research_run),
                "instrument",
                str(research_run.instrument_id),
                research_run.decision_time,
            ):
                raise ValueError(_FORBIDDEN)
        elif job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value:
            synthesis_run = synthesis_by_id.get(run_id)
            synthesis_cycle = (
                None if synthesis_run is None else cycle_by_id.get(synthesis_run.cycle_id)
            )
            if (
                synthesis_run is None
                or synthesis_cycle is None
                or (
                    job.schema_version,
                    job.input_hash,
                    job.scope_kind,
                    job.scope_id,
                    job.effective_at,
                )
                != (
                    "portfolio_synthesis_job_v1",
                    synthesis_run.input_hash,
                    "portfolio",
                    str(synthesis_cycle.portfolio_id),
                    synthesis_cycle.decision_time,
                )
            ):
                raise ValueError(_FORBIDDEN)
        else:
            raise ValueError(_FORBIDDEN)
        seen.add(key)
    if seen != (
        {(JobType.AGENT_PAPER_RESEARCH.value, run_id) for run_id in research_by_id}
        | {(JobType.AGENT_PORTFOLIO_SYNTHESIS.value, run_id) for run_id in synthesis_by_id}
    ):
        raise ValueError(_FORBIDDEN)

    for order in await session.scalars(select(PaperOrderRecord)):
        decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
        order_cycle = (
            None
            if decision is None or decision.agent_paper_cycle_id is None
            else cycle_by_id.get(decision.agent_paper_cycle_id)
        )
        if (
            decision is None
            or order_cycle is None
            or (order.account_id, decision.account_id, decision.portfolio_id)
            != (order_cycle.account_id, order_cycle.account_id, order_cycle.portfolio_id)
        ):
            raise ValueError(_FORBIDDEN)


async def require_hold_isolation(
    session: AsyncSession, *, request: HoldRequest, expected_database: str, now: datetime
) -> None:
    """Unregistered diagnostics remain empty; registered holds bind frozen facts."""
    identities = tuple(await session.scalars(select(EconomicEvaluationRecord).limit(2)))
    if not identities:
        await require_isolated_database(session, expected=expected_database)
        return
    if len(identities) != 1 or identities[0].protocol_hash != request.protocol_hash:
        raise ValueError("ECONOMIC_HOLD_AUTHORITY_MISMATCH")
    slot = await session.scalar(
        select(EconomicPairSlotRecord).where(
            EconomicPairSlotRecord.evaluation_id == identities[0].id,
            EconomicPairSlotRecord.ordinal == request.slot_ordinal,
        )
    )
    if slot is None:
        raise ValueError("ECONOMIC_HOLD_AUTHORITY_MISMATCH")
    authority = await require_economic_lane(session, ref=EconomicLaneRef(slot.id, "incumbent"))
    frozen = authority.protocol.request
    if (
        request.evaluation_key,
        request.research_cost_usd,
        request.synthesis_cost_usd,
        request.global_limit_usd,
        request.uncovered_formal_usd,
    ) != (
        frozen.evaluation_key,
        frozen.research_cost_usd,
        frozen.synthesis_cost_usd,
        frozen.global_daily_limit_usd,
        Decimal(0),
    ):
        raise ValueError("ECONOMIC_HOLD_AUTHORITY_MISMATCH")
    require_protocol_budget_day(authority.protocol, request, observed_at=now)
    await require_running_isolation(
        session, authority=authority, expected_database=expected_database
    )

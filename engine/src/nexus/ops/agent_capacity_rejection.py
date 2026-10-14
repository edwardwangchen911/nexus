"""Close a freshly rejected, entirely untouched formal v9 research pool."""

from uuid import UUID

from sqlalchemy import func, select

from nexus.agents.capacity_cycle import derive_cycle_members
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.codex_versions import ISOLATED_CAPACITY_CONFIG_VERSIONS
from nexus.agents.persistence import try_lock_budget_day
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.paper.agent_cycle import (
    _append_event,
    _events,
    _fact_uuids,
    _require_cycle_candidate_profile,
)
from nexus.paper.manual_cancel import cancel_unused_manual_research
from nexus.paper.manual_request import _require_cycle_jobs_settled
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.economic import EconomicLaneCycleRecord
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

CAPACITY_REJECTION_CODES = frozenset(
    {"CODEX_CAPACITY_POOL_WIRE_EXCEEDED", "CODEX_CAPACITY_POOL_INPUT_EXCEEDED"}
)
_BLOCKED = "AGENT_PAPER_LOOP_RESULT_UNKNOWN"


async def close_capacity_rejected_cycle_once(
    settings: EngineSettings, *, account_id: UUID, cycle_id: UUID, job_id: UUID, code: str
) -> None:
    """Called only after this dispatch's deterministic preclaim rejection.

    Unknown transport/usage or any claimed member refuses the entire transaction.
    Native cancellation records zero model usage and releases reservations. It
    does not retry a Worker, publish research, create a decision or place orders.
    """
    if (
        code not in CAPACITY_REJECTION_CODES
        or settings.agent_paper_formal_model_config_id is None
        or settings.agent_paper_account_id != account_id
    ):
        raise ValueError(_BLOCKED)
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await lock_portfolio_account(session, account_id)
            cycle = await session.scalar(
                select(AgentPaperCycleRecord)
                .where(
                    AgentPaperCycleRecord.id == cycle_id,
                    AgentPaperCycleRecord.account_id == account_id,
                )
                .with_for_update()
            )
            if cycle is None or cycle.execution_locked is not True:
                raise ValueError(_BLOCKED)
            if await session.scalar(
                select(EconomicLaneCycleRecord.id).where(
                    EconomicLaneCycleRecord.cycle_id == cycle_id
                )
            ):
                raise ValueError(_BLOCKED)
            now = await session.scalar(select(func.clock_timestamp()))
            if now is None or now < cycle.decision_time:
                raise ValueError(_BLOCKED)
            events = await _events(session, cycle_id)
            ids = _fact_uuids(events, "research_run_ids")
            runs = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord).where(AgentResearchRunRecord.id.in_(ids))
                )
            )
            derive_cycle_members(cycle, events, runs)
            await _require_cycle_candidate_profile(
                session,
                cycle=cycle,
                candidate_stage_capacity=False,
                formal_stage_capacity=True,
                expected_formal_model_id=settings.agent_paper_formal_model_config_id,
                now=cycle.decision_time,
            )
            if any(run.pricing_version not in ISOLATED_CAPACITY_CONFIG_VERSIONS for run in runs):
                raise ValueError(_BLOCKED)
            days = tuple(
                await session.scalars(
                    select(AgentDailyBudgetReservationRecord.business_date)
                    .where(AgentDailyBudgetReservationRecord.run_id.in_(ids))
                    .distinct()
                    .order_by(AgentDailyBudgetReservationRecord.business_date)
                )
            )
            for day in days:
                if not await try_lock_budget_day(session, day.isoformat()):
                    raise ValueError(_BLOCKED)
            # Never wait behind a Worker that owns a member or its budget lock.
            locked_runs = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord)
                    .where(AgentResearchRunRecord.id.in_(ids))
                    .order_by(AgentResearchRunRecord.id)
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
            )
            if len(locked_runs) != len(ids) or any(
                run.model_config_id != settings.agent_paper_formal_model_config_id
                or run.model_hash != events[0].fact_ids.get("formal_model_hash")
                for run in locked_runs
            ):
                raise ValueError(_BLOCKED)
            jobs = tuple(
                await session.scalars(
                    select(Job)
                    .where(
                        Job.job_type == "agent.paper.research",
                        Job.payload["run_id"].as_string().in_([str(rid) for rid in ids]),
                    )
                    .order_by(Job.id)
                    .with_for_update(skip_locked=True)
                )
            )
            if len(jobs) != len(ids) or job_id not in {job.id for job in jobs}:
                raise ValueError(_BLOCKED)
            last = events[-1]
            if (
                last.to_status == "unavailable"
                and last.fact_ids.get("failure_phase") == "capacity_preflight"
                and last.fact_ids.get("failure_code") == code
                and last.fact_ids.get("preclaim_job_id") == str(job_id)
            ):
                await _require_cycle_jobs_settled(session, cycle_id=cycle_id, now=now)
                return
            if last.to_status != "researching":
                raise ValueError(_BLOCKED)
            for job in jobs:
                if not await formal_stage_job_qualified(session, job):
                    raise ValueError(_BLOCKED)
            cancelled = await cancel_unused_manual_research(
                session, cycle=cycle, events=events, now=now
            )
            if cancelled != len(ids):
                raise ValueError(_BLOCKED)
            await _append_event(
                session,
                cycle,
                "researching",
                "unavailable",
                "RESEARCH_OUTPUT_NOT_ACTIONABLE",
                {
                    "failure_phase": "capacity_preflight",
                    "failure_code": code,
                    "preclaim_job_id": str(job_id),
                    "unused_research_cancelled": cancelled,
                    "observed_at": now.isoformat(),
                },
            )
    finally:
        await factory.kw["bind"].dispose()

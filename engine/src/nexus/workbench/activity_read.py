"""Bounded request-aware selection, using persisted facts and no admission locks."""

from datetime import date, datetime, time, timedelta
from typing import cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import String, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import CompoundSelect

from nexus.api.errors import NexusError
from nexus.market.current_identity import identity_parse_scope
from nexus.paper.manual_preparation_close import preparation_closed
from nexus.paper.manual_request import ManualMode, _require_cycle_jobs_settled, read_manual_request
from nexus.paper.read_service import PaperReadService
from nexus.paper.research_request import require_research_jobs_settled
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord, AgentResearchRunRecord
from nexus.persistence.models.manual_paper import (
    ManualPaperRequest,
    ManualPaperRequestEvent,
    ResearchRequestManifest,
    ResearchRequestMember,
)
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobLease
from nexus.workbench.activity import ActivityObject, choose_activity
from nexus.workbench.activity_schema import (
    WorkbenchActivityResponse,
    WorkbenchActivityScopeResponse,
)
from nexus.workbench.evidence_read_scope import evidence_read_scope
from nexus.workbench.research_summary import cycle_candidates, research_candidates

_LIMIT = 20
_NY = ZoneInfo("America/New_York")


def _job_unsettled(run_id: InstrumentedAttribute[UUID], now: datetime) -> ColumnElement[bool]:
    jobs = (
        select(Job.id)
        .where(Job.payload["run_id"].as_string() == run_id.cast(String))
        .correlate(run_id.class_)
    )
    return or_(
        ~exists(jobs),
        exists(jobs.where(Job.status.not_in(("succeeded", "failed", "skipped", "dead_letter")))),
        exists(select(JobLease.id).where(JobLease.job_id.in_(jobs), JobLease.expires_at > now)),
    )


def _unsettled_run(now: datetime) -> ColumnElement[bool]:
    return or_(
        AgentResearchRunRecord.settled_cost_usd.is_(None),
        AgentResearchRunRecord.status.not_in(("completed", "failed", "abstained")),
        _job_unsettled(AgentResearchRunRecord.id, now),
    )


def _unsettled_cycles(now: datetime) -> CompoundSelect[tuple[UUID]]:
    research = (
        select(AgentPaperCycleEventRecord.cycle_id)
        .join(
            AgentResearchRunRecord,
            AgentPaperCycleEventRecord.fact_ids["research_run_ids"].op("?")(
                AgentResearchRunRecord.id.cast(String)
            ),
        )
        .where(_unsettled_run(now))
    )
    synthesis = select(AgentPortfolioSynthesisRunRecord.cycle_id).where(
        or_(
            AgentPortfolioSynthesisRunRecord.settled_cost_usd.is_(None),
            AgentPortfolioSynthesisRunRecord.status.not_in(("completed", "failed", "abstained")),
            _job_unsettled(AgentPortfolioSynthesisRunRecord.id, now),
        )
    )
    return research.union(synthesis)


async def _cycle_settled(session: AsyncSession, cycle_id: UUID, now: datetime) -> bool:
    try:
        await _require_cycle_jobs_settled(
            session, cycle_id=cycle_id, now=now, allow_accepted_synthesis=True, read_only=True
        )
        return True
    except ValueError as error:
        if str(error) != "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN":
            raise
        return False


async def read_activity(
    session: AsyncSession, *, read_at: datetime, decision_session: date | None
) -> WorkbenchActivityResponse:
    # Cache only source parsing within this single consistent read transaction.
    # Every reference and persisted proof is still validated independently.
    with identity_parse_scope(), evidence_read_scope(session):
        return await _read_activity(session, read_at=read_at, decision_session=decision_session)


async def _read_activity(
    session: AsyncSession, *, read_at: datetime, decision_session: date | None
) -> WorkbenchActivityResponse:
    paper = PaperReadService(session, read_at)
    account, _ = await paper.account()
    scopes = []
    for mode in ("research_and_paper", "research_only", "research_and_monitor"):
        try:
            scopes.append(
                await _scope(
                    session, paper, account.id, cast(ManualMode, mode), read_at, decision_session
                )
            )
        except (ValueError, TypeError, NexusError):
            scopes.append(
                WorkbenchActivityScopeResponse(
                    mode=cast(ManualMode, mode),
                    registration_date=read_at.astimezone(_NY).date(),
                    decision_session=decision_session if mode == "research_and_paper" else None,
                    selection="unavailable",
                    occupancy="unverified",
                    reason_code="WORKBENCH_ACTIVITY_UNVERIFIED",
                    portfolio_status="unavailable"
                    if mode == "research_and_paper"
                    else "not_applicable",
                )
            )
    return WorkbenchActivityResponse(
        read_at=read_at,
        read_status="partial"
        if any(
            s.selection in {"unavailable", "ambiguous"}
            or any(c.read_status == "unavailable" for c in s.candidates)
            for s in scopes
        )
        else "complete",
        scopes=tuple(scopes),
    )


async def _scope(
    session: AsyncSession,
    paper: PaperReadService,
    account_id: UUID,
    mode: ManualMode,
    now: datetime,
    decision_session: date | None,
) -> WorkbenchActivityScopeResponse:
    day = now.astimezone(_NY).date()
    start = datetime.combine(day, time.min, _NY)
    end = start + timedelta(days=1)
    base = (
        ManualPaperRequest.account_id == account_id,
        ManualPaperRequest.mode == mode,
        ManualPaperRequest.requested_at <= now,
        ManualPaperRequest.created_at <= now,
    )
    count = await session.scalar(
        select(func.count())
        .select_from(ManualPaperRequest)
        .where(
            *base, ManualPaperRequest.requested_at >= start, ManualPaperRequest.requested_at < end
        )
    )
    latest_status = (
        select(ManualPaperRequestEvent.status)
        .where(
            ManualPaperRequestEvent.request_id == ManualPaperRequest.id,
            ManualPaperRequestEvent.occurred_at <= now,
            ManualPaperRequestEvent.created_at <= now,
        )
        .order_by(ManualPaperRequestEvent.sequence.desc())
        .limit(1)
        .correlate(ManualPaperRequest)
        .scalar_subquery()
    )
    unsettled_cycle = exists().where(
        ManualPaperRequestEvent.request_id == ManualPaperRequest.id,
        ManualPaperRequestEvent.occurred_at <= now,
        ManualPaperRequestEvent.created_at <= now,
        ManualPaperRequestEvent.cycle_id == AgentPaperCycleRecord.id,
        AgentPaperCycleRecord.account_id == ManualPaperRequest.account_id,
        AgentPaperCycleRecord.account_id == account_id,
        AgentPaperCycleRecord.decision_time <= now,
        AgentPaperCycleRecord.created_at <= now,
        ManualPaperRequest.mode == mode,
        AgentPaperCycleRecord.id.in_(_unsettled_cycles(now)),
    )
    unsettled_pure = exists(
        select(ResearchRequestMember.id)
        .join(
            ResearchRequestManifest, ResearchRequestManifest.id == ResearchRequestMember.manifest_id
        )
        .join(AgentResearchRunRecord, AgentResearchRunRecord.id == ResearchRequestMember.run_id)
        .where(ResearchRequestManifest.request_id == ManualPaperRequest.id, _unsettled_run(now))
    )
    pending = tuple(
        await session.scalars(
            select(ManualPaperRequest)
            .where(
                *base,
                or_(
                    latest_status.in_(("queued", "preparing", "active", "unknown")),
                    latest_status.is_(None),
                    unsettled_cycle,
                    unsettled_pure,
                ),
            )
            .order_by(ManualPaperRequest.requested_at.desc())
            .limit(_LIMIT + 1)
        )
    )
    result = WorkbenchActivityScopeResponse(
        mode=mode,
        today_registered=count,
        registration_date=day,
        decision_session=decision_session if mode == "research_and_paper" else None,
        selection="none",
        portfolio_status="not_applicable" if mode != "research_and_paper" else None,
    )
    if len(pending) > _LIMIT:
        return result.model_copy(
            update={
                "selection": "unavailable",
                "reason_code": "WORKBENCH_ACTIVITY_LIMIT",
                "occupancy": "unverified",
                "portfolio_status": "unavailable"
                if mode == "research_and_paper"
                else "not_applicable",
            }
        )
    latest = await session.scalar(
        select(ManualPaperRequest)
        .where(*base)
        .order_by(ManualPaperRequest.requested_at.desc(), ManualPaperRequest.id.desc())
        .limit(1)
    )
    requests = {row.id: row for row in pending}
    if latest is not None:
        requests[latest.id] = latest
    objects = []
    views = {}
    released = {}
    for row in requests.values():
        view = await read_manual_request(session, row)
        event_future = await session.scalar(
            select(
                exists().where(
                    ManualPaperRequestEvent.request_id == row.id,
                    or_(
                        ManualPaperRequestEvent.occurred_at > now,
                        ManualPaperRequestEvent.created_at > now,
                    ),
                )
            )
        )
        if event_future:
            raise ValueError("WORKBENCH_ACTIVITY_UNVERIFIED")
        closed = view.status == "unknown" and await preparation_closed(session, row, now=now)
        unresolved = (
            view.status in {"queued", "preparing", "active"}
            or view.status == "unknown"
            and not closed
        )
        if view.cycle_id is not None:
            unresolved = unresolved or not await _cycle_settled(session, view.cycle_id, now)
        if view.research is not None:
            try:
                await require_research_jobs_settled(session, row=row, view=view.research, now=now)
            except ValueError as error:
                if str(error) != "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN":
                    raise
                unresolved = True
        objects.append(
            ActivityObject(row.id, row.requested_at, start <= row.requested_at < end, unresolved)
        )
        views[row.id] = view
        released[row.id] = closed
    # Scheduled cycles are independently scoped. Manual cycles are represented once via request.
    linked_cycle = (
        select(ManualPaperRequestEvent.id)
        .join(ManualPaperRequest, ManualPaperRequest.id == ManualPaperRequestEvent.request_id)
        .where(
            ManualPaperRequestEvent.cycle_id == AgentPaperCycleRecord.id,
            ManualPaperRequestEvent.occurred_at <= now,
            ManualPaperRequestEvent.created_at <= now,
            ManualPaperRequest.requested_at <= now,
            ManualPaperRequest.created_at <= now,
            ManualPaperRequest.account_id == account_id,
            ManualPaperRequest.account_id == AgentPaperCycleRecord.account_id,
            ManualPaperRequest.mode.in_(("research_only", "research_and_paper")),
        )
        .correlate(AgentPaperCycleRecord)
    )
    manual_cycle = exists(linked_cycle)
    cyclebase = (
        AgentPaperCycleRecord.account_id == account_id,
        AgentPaperCycleRecord.decision_time <= now,
        AgentPaperCycleRecord.created_at <= now,
    )
    started_cycles = (
        select(func.count())
        .select_from(AgentPaperCycleRecord)
        .where(*cyclebase, AgentPaperCycleRecord.decision_session == decision_session)
    )
    pure_cycle = exists(linked_cycle.where(ManualPaperRequest.mode == "research_only"))
    if mode == "research_and_paper":
        result.today_started = (
            await session.scalar(started_cycles.where(~pure_cycle)) if decision_session else None
        )
    else:
        active_event = exists().where(
            ManualPaperRequestEvent.request_id == ManualPaperRequest.id,
            ManualPaperRequestEvent.status == "active",
            ManualPaperRequestEvent.occurred_at <= now,
            ManualPaperRequestEvent.created_at <= now,
        )
        result.today_started = await session.scalar(
            select(func.count())
            .select_from(ManualPaperRequest)
            .where(
                *base,
                ManualPaperRequest.requested_at >= start,
                ManualPaperRequest.requested_at < end,
                active_event,
            )
        )
    cycle_rows = {}
    if mode == "research_and_paper":
        status = (
            select(AgentPaperCycleEventRecord.to_status)
            .where(
                AgentPaperCycleEventRecord.cycle_id == AgentPaperCycleRecord.id,
                AgentPaperCycleEventRecord.occurred_at <= now,
                AgentPaperCycleEventRecord.created_at <= now,
            )
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
            .correlate(AgentPaperCycleRecord)
            .scalar_subquery()
        )
        scheduled = tuple(
            await session.scalars(
                select(AgentPaperCycleRecord)
                .where(
                    *cyclebase,
                    ~manual_cycle,
                    or_(
                        status.not_in(("completed", "unavailable", "failed")),
                        status.is_(None),
                        AgentPaperCycleRecord.id.in_(_unsettled_cycles(now)),
                    ),
                )
                .order_by(AgentPaperCycleRecord.decision_time.desc())
                .limit(_LIMIT + 1)
            )
        )
        if len(scheduled) > _LIMIT:
            return result.model_copy(
                update={
                    "selection": "unavailable",
                    "reason_code": "WORKBENCH_ACTIVITY_LIMIT",
                    "occupancy": "unverified",
                    "portfolio_status": "unavailable"
                    if mode == "research_and_paper"
                    else "not_applicable",
                }
            )
        last_cycle = await session.scalar(
            select(AgentPaperCycleRecord)
            .where(*cyclebase, ~manual_cycle)
            .order_by(AgentPaperCycleRecord.decision_time.desc(), AgentPaperCycleRecord.id.desc())
            .limit(1)
        )
        cycle_rows = {row.id: row for row in scheduled}
        if last_cycle is not None:
            cycle_rows[last_cycle.id] = last_cycle
        for cycle_row in cycle_rows.values():
            detail = await paper.agent_cycle_detail(cycle_row.id)
            if detail is None:
                raise ValueError("WORKBENCH_ACTIVITY_UNVERIFIED")
            objects.append(
                ActivityObject(
                    cycle_row.id,
                    cycle_row.decision_time,
                    cycle_row.decision_session == decision_session,
                    detail.cycle.status not in {"completed", "unavailable", "failed"}
                    or not await _cycle_settled(session, cycle_row.id, now),
                )
            )
    selected, selection = choose_activity(objects)
    result.selection = selection
    if selected is None:
        if selection == "ambiguous":
            result.occupancy = "unverified"
            result.reason_code = "WORKBENCH_ACTIVITY_AMBIGUOUS"
            result.portfolio_status = (
                "unavailable" if mode == "research_and_paper" else "not_applicable"
            )
        return result
    result.historical = selection in {"historical", "historical_unresolved"}
    result.occupancy = "unresolved" if selected.unresolved else "not_indicated"
    cycle = None
    if selected.identity in requests:
        row = requests[selected.identity]
        view = views[row.id]
        result.request_id = row.id
        result.requested_at = row.requested_at
        result.status = view.status
        result.reason_code = view.code
        result.trigger = "manual"
        result.cycle_id = view.cycle_id
        if mode == "research_and_paper" and view.cycle_id is None:
            result.portfolio_status = "awaiting_cycle"
        if released[row.id]:
            result.occupancy = "released"
        if view.cycle_id is not None:
            cycle = await session.get(AgentPaperCycleRecord, view.cycle_id)
            if (
                cycle is None
                or cycle.account_id != account_id
                or cycle.created_at > now
                or cycle.decision_time > now
            ):
                raise ValueError("WORKBENCH_ACTIVITY_UNVERIFIED")
        elif view.research is not None:
            result.candidates = await research_candidates(session, row, view, read_at=now)
    else:
        cycle = cycle_rows[selected.identity]
        result.cycle_id = cycle.id
        result.trigger = "scheduled"
    if cycle is not None:
        detail = await paper.agent_cycle_detail(cycle.id)
        if detail is None:
            raise ValueError("WORKBENCH_ACTIVITY_UNVERIFIED")
        result.selected_decision_session = cycle.decision_session
        result.href = f"/decisions/{cycle.id}"
        if result.trigger == "scheduled":
            result.status = detail.cycle.status
        result.candidates = await cycle_candidates(session, cycle, read_at=now)
        if mode == "research_and_paper":
            code = detail.cycle.result_code
            # Only fixed code classification, no free-text inference of a no-trade conclusion.
            known = {
                "PORTFOLIO_ASSESSMENT_DEFERRED",
                "NO_ORDER_REQUIRED",
                "PAPER_CYCLE_COMPLETED",
                "RESEARCH_OUTPUT_NOT_ACTIONABLE",
                "RESEARCH_JOBS_PREPARED",
                "PORTFOLIO_DECISION_RECORDED",
            }
            result.portfolio_reason_code = (
                code if code in known else "WORKBENCH_PORTFOLIO_DETAIL_REQUIRED"
            )
            result.portfolio_status = (
                "deferred"
                if code == "PORTFOLIO_ASSESSMENT_DEFERRED"
                else "no_order_required"
                if code == "NO_ORDER_REQUIRED"
                else "recorded"
                if detail.cycle.decision is not None
                else "unavailable"
                if detail.cycle.status in {"unavailable", "failed"}
                else "pending"
            )
    return result

"""Read completed five-minute price windows without invoking the scanner."""

from datetime import UTC, datetime, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.research_job_identity import research_run_job_input_hash
from nexus.api.schemas.research_monitoring import PriceCheckWindows
from nexus.monitoring.reconciliation import terminal_successors
from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority
from nexus.paper.formal_budget_protection import _validate_binding
from nexus.persistence.models.agents import (
    AgentResearchEventRecord,
    AgentResearchRunRecord,
    FormalBudgetBindingRecord,
)
from nexus.persistence.models.monitoring import MonitorCheck, MonitorCoverageReceipt, MonitorPlan
from nexus.persistence.models.paper import AgentPaperCandidateBatchRecord as Batch
from nexus.persistence.models.runtime import AuditEvent, Job

_STEP = timedelta(minutes=5)
_DAY = timedelta(days=1)
_NY = ZoneInfo("America/New_York")


def price_check_windows(
    periods: list[tuple[datetime, datetime]],
    now: datetime,
    observed: set[datetime],
    uncertain_periods: list[tuple[datetime, datetime]],
    scope_complete: bool = True,
) -> PriceCheckWindows:
    expected: set[datetime] = set()
    uncertain: set[datetime] = set()
    for start, end in periods:
        slot = start.replace(minute=start.minute // 5 * 5, second=0, microsecond=0)
        while slot < end and slot + _STEP <= now:
            if (
                start <= slot
                and slot + _STEP <= end
                and not any(a < slot + _STEP and slot < b for a, b in uncertain_periods)
            ):
                expected.add(slot)
            else:
                uncertain.add(slot)
            slot += _STEP
    return PriceCheckWindows(
        scope_complete=scope_complete,
        recorded_outside_verified_windows=len(observed - expected),
        expected=len(expected),
        recorded=len(expected & observed),
        missing=tuple(sorted(expected - observed)),
        uncertain=len(uncertain - expected),
    )


async def read_run_price_windows(
    session: AsyncSession, run: AgentResearchRunRecord, plan_id: UUID, now: datetime
) -> PriceCheckWindows | None:
    plan = await session.get(MonitorPlan, plan_id)
    if plan is None or plan.kind != "price" or plan.source_run_id != run.id:
        return None
    start = max(plan.created_at, plan.effective_at)
    stop = min(now, plan.valid_until)
    replaced = await session.scalar(
        select(func.min(MonitorPlan.created_at)).where(
            MonitorPlan.account_id == plan.account_id,
            MonitorPlan.instrument_id == plan.instrument_id,
            MonitorPlan.kind == "price",
            MonitorPlan.generation > plan.generation,
            MonitorPlan.created_at <= stop,
        )
    )
    if replaced is not None:
        stop = min(stop, replaced)
    first_slot = start.replace(minute=start.minute // 5 * 5, second=0, microsecond=0)
    if stop <= start or first_slot + _STEP > now:
        return price_check_windows([], now, set(), [])
    # Bound the entire read; never silently truncate the denominator.
    if stop - start > 32 * _DAY:
        return None
    rows = list(
        await session.scalars(
            select(MonitorCoverageReceipt)
            .where(
                MonitorCoverageReceipt.account_id == plan.account_id,
                MonitorCoverageReceipt.instrument_id == plan.instrument_id,
                MonitorCoverageReceipt.kind == "price",
                MonitorCoverageReceipt.status != "superseded",
                MonitorCoverageReceipt.research_as_of >= run.decision_time,
                MonitorCoverageReceipt.created_at <= stop,
                MonitorCoverageReceipt.observed_at <= stop,
            )
            .limit(257)
        )
    )
    successors = list(
        await session.scalars(
            terminal_successors(plan, run, stop)
            .where(AgentResearchRunRecord.created_at <= stop)
            .limit(257)
        )
    )
    if max(len(rows), len(successors)) > 256:
        return None
    # Event.created_at may be the start of a long transaction. Pair the
    # terminal event with its exact native job finish before dating a veto.
    terminal_at: dict[UUID, datetime] = {}
    if successors:
        events = await session.execute(
            select(AgentResearchEventRecord.run_id, func.min(AgentResearchEventRecord.created_at))
            .where(
                AgentResearchEventRecord.run_id.in_([child.id for child in successors]),
                AgentResearchEventRecord.state.in_(("completed", "failed", "abstained")),
            )
            .group_by(AgentResearchEventRecord.run_id)
        )
        event_times = {run_id: at for run_id, at in events}
        members = {str(child.id): child for child in successors}
        jobs = await session.scalars(
            select(Job).where(Job.payload["run_id"].astext.in_(tuple(members))).limit(257)
        )
        seen = set()
        for job in jobs:
            key = job.payload.get("run_id")
            if not isinstance(key, str) or key not in members:
                return None
            child = members[key]
            if (
                key in seen
                or job.payload != {"run_id": key}
                or (
                    job.job_type != "agent.paper.research"
                    or job.schema_version != "agent_research_job_v1"
                    or job.scope_kind != "instrument"
                    or job.scope_id != str(child.instrument_id)
                    or job.effective_at != child.decision_time
                    or job.input_hash
                    != research_run_job_input_hash(
                        child, decision_time=child.decision_time.astimezone(UTC)
                    )
                    or job.finished_at is None
                    or child.id not in event_times
                )
            ):
                return None
            seen.add(key)
            terminal_at[child.id] = max(job.finished_at, event_times[child.id])
        if len(seen) != len(successors):
            return None
    covered, uncertain = _coverage_periods(plan.id, start, stop, rows, successors, terminal_at)
    periods, scope_complete = await _authorized_periods(session, plan, covered)
    checked = set(
        await session.scalars(
            select(MonitorCheck.scheduled_at).where(
                MonitorCheck.plan_id == plan.id,
                MonitorCheck.kind == "price",
                MonitorCheck.checked_at <= now,
                MonitorCheck.created_at <= now,
                MonitorCheck.scheduled_at >= start - _STEP,
                MonitorCheck.scheduled_at < stop,
            )
        )
    )
    elapsed = {slot for slot in checked if slot + _STEP <= now}
    return price_check_windows(periods, now, elapsed, uncertain, scope_complete)


def _coverage_periods(
    plan_id: UUID,
    start: datetime,
    stop: datetime,
    rows: list[MonitorCoverageReceipt],
    successors: list[AgentResearchRunRecord],
    terminal_at: dict[UUID, datetime],
) -> tuple[list[tuple[datetime, datetime]], list[tuple[datetime, datetime]]]:
    boundaries = {start, stop}
    for receipt in rows:
        boundaries.add(max(start, receipt.created_at, receipt.observed_at))
    for child in successors:
        at = max(child.created_at, child.decision_time, terminal_at[child.id])
        terminal_at[child.id] = at
        if at < stop:
            boundaries.add(max(start, at))
    ordered = sorted(boundaries)
    covered: list[tuple[datetime, datetime]] = []
    uncertain: list[tuple[datetime, datetime]] = []
    for a, b in zip(ordered, ordered[1:], strict=False):
        visible = [r for r in rows if max(r.created_at, r.observed_at) <= a]
        latest = max(visible, key=lambda r: (r.research_as_of, r.created_at), default=None)
        if latest is not None and latest.active_plan_id != plan_id and latest.status != "unknown":
            continue
        latest_child = next((r for r in successors if terminal_at[r.id] <= a), None)
        if (
            latest is not None and latest.status == "unknown" and latest.active_plan_id != plan_id
        ) or (
            latest_child is not None
            and not any(
                r.run_id == latest_child.id and r.active_plan_id == plan_id for r in visible
            )
        ):
            uncertain.append((a, b))
        if covered and covered[-1][1] == a:
            covered[-1] = covered[-1][0], b
        else:
            covered.append((a, b))
    return covered, uncertain


async def _authorized_periods(
    session: AsyncSession, plan: MonitorPlan, covered: list[tuple[datetime, datetime]]
) -> tuple[list[tuple[datetime, datetime]], bool]:
    if not covered:
        return [], True
    # Use the same immutable daily scope that the protected host consumes.
    # A current environment or an available batch alone cannot prove past eligibility.
    start, stop = covered[0][0], covered[-1][1]
    bindings = list(
        await session.scalars(
            select(FormalBudgetBindingRecord)
            .where(
                FormalBudgetBindingRecord.business_date.between(start.date(), stop.date()),
                FormalBudgetBindingRecord.applied_at <= stop,
                FormalBudgetBindingRecord.created_at <= stop,
            )
            .order_by(FormalBudgetBindingRecord.sequence.desc())
            .limit(65)
        )
    )
    if len(bindings) > 64:
        return [], False
    authorities = []
    for binding in bindings:
        audit = await session.get(AuditEvent, binding.audit_event_id)
        visible = max(
            binding.applied_at,
            binding.created_at,
            audit.created_at if audit is not None else binding.created_at,
        )
        scope = None
        try:
            await _validate_binding(session, binding)
            payload = binding.payload
            batch = await session.get(Batch, UUID(str(payload["batch_id"])))
            if (
                batch is not None
                and str(batch.account_id) == payload["account_id"]
                and batch.batch_hash == payload["batch_hash"]
                and batch.created_at <= visible
                and isinstance(payload["enabled"], bool)
            ):
                enabled = (
                    payload["enabled"]
                    and batch.account_id == plan.account_id
                    and plan.instrument_id in batch.candidate_instrument_ids
                )
                scope = (enabled, UUID(str(payload["calendar_id"])))
        except (ValueError, KeyError, TypeError):
            pass  # Without a valid audited binding the schedule is unknown.
        authorities.append((binding, visible, scope))
    periods: list[tuple[datetime, datetime]] = []
    complete = True
    for left, right in covered:
        edges = {left, right}
        for _, at, _ in authorities:
            if left < at < right:
                edges.add(at)
        for zone in (UTC, _NY):
            date = left.astimezone(zone).date() + _DAY
            while date <= right.astimezone(zone).date():
                edges.add(datetime.combine(date, datetime.min.time(), zone).astimezone(UTC))
                date += _DAY
        ordered = sorted(edges)
        for a, b in zip(ordered, ordered[1:], strict=False):
            scope = next(
                (
                    scope
                    for binding, at, scope in authorities
                    if binding.business_date == a.date() and at <= a
                ),
                None,
            )
            if scope is None:
                complete = False
                continue
            enabled, cal_id = scope
            if not enabled:
                continue
            try:
                cal = await load_agent_paper_service_calendar_authority(
                    session, now=a, expected_calendar_authority_id=cal_id
                )
            except ValueError:
                complete = False
                continue
            first, last = max(a, cal.market_open_at), min(b, cal.market_close_at)
            if first < last:
                if periods and periods[-1][1] == first:
                    periods[-1] = periods[-1][0], last
                else:
                    periods.append((first, last))
    return periods, complete

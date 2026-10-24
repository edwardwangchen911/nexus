"""Atomically reconcile a new daily study and its exact historical event consumption."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.monitoring.daily_contracts import compile_daily_pending_proposal
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan


def frozen_daily_predecessor(context: BaseModel, kind: str) -> tuple[UUID | None, int | None]:
    try:
        value = context.model_dump(mode="json")["predecessors"][kind]
        identity = value["plan_id"]
        generation = value["generation"]
        if kind not in {"price", "disclosure"} or (identity is None) != (generation is None):
            raise ValueError
        return (None, None) if identity is None else (UUID(str(identity)), int(generation))
    except (KeyError, TypeError, ValueError):
        raise ValueError("MONITOR_DAILY_PREDECESSOR_INVALID") from None


async def reconcile_daily_event_run(
    session: AsyncSession, *, account_id: UUID, run: AgentResearchRunRecord, now: datetime
) -> tuple[MonitorCoverageReceipt, ...] | None:
    from nexus.agents.paper_context import read_agent_paper_research_context
    from nexus.audit.service import append_audit_event
    from nexus.monitoring.daily_v2_consumption import (
        record_daily_event_consumption,
        validate_daily_event_receipt,
    )
    from nexus.monitoring.disclosure_installation import _install_mixed_or_legacy_disclosure
    from nexus.monitoring.installation import _install_mixed_or_legacy_plan
    from nexus.monitoring.reconciliation import _native_finished, _scope_evidence
    from nexus.monitoring.source_qualification import load_qualified_source
    from nexus.monitoring.storage import lock_security
    from nexus.persistence.models.monitoring import MonitorDailyEventReceipt

    await lock_security(session, account_id=account_id, instrument_id=run.instrument_id)
    await _scope_evidence(session, run=run, account_id=account_id)
    paper = await read_agent_paper_research_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if paper is None or paper[0].pending_monitoring_v2 is None:
        raise ValueError("MONITOR_DAILY_EVENT_CONTEXT_INVALID")
    context = paper[0].pending_monitoring_v2
    prior_receipts = tuple(
        await session.scalars(
            select(MonitorCoverageReceipt).where(
                MonitorCoverageReceipt.account_id == account_id,
                MonitorCoverageReceipt.run_id == run.id,
            )
        )
    )
    if prior_receipts:
        if {row.kind for row in prior_receipts} != {"price", "disclosure"}:
            raise ValueError("MONITOR_DAILY_EVENT_RESULT_UNVERIFIED")
        for proof in await session.scalars(
            select(MonitorDailyEventReceipt).where(MonitorDailyEventReceipt.run_id == run.id)
        ):
            if not await validate_daily_event_receipt(session, proof.id):
                raise ValueError("MONITOR_DAILY_EVENT_RESULT_UNVERIFIED")
        return prior_receipts
    if not await _native_finished(session, run, now):
        return None
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run.id
        )
    )
    known = (
        reservation is not None
        and reservation.state == "settled"
        and run.input_tokens is not None
        and run.output_tokens is not None
        and run.settled_cost_usd is not None
        and reservation.settled_usd == run.settled_cost_usd
    )
    terminal = await session.scalar(
        select(AgentResearchOutputRecord).where(
            AgentResearchOutputRecord.run_id == run.id,
            AgentResearchOutputRecord.role.is_(None),
            AgentResearchOutputRecord.status == "completed",
        )
    )
    source = None
    proposal = None
    reason = "MONITOR_RUN_FAILED" if known else "MONITOR_UNKNOWN_USAGE"
    if known and terminal is not None and run.status == "completed":
        try:
            source = await load_qualified_source(
                session, account_id=account_id, output_id=terminal.id, now=now
            )
            roles = {row.role: row for row in source.role_outputs}
            candidate = source.intent.daily_pending_proposal
            if candidate is None or candidate != roles["orchestrator"].daily_pending_proposal:
                raise ValueError("MONITOR_DAILY_EVENT_ADOPTION_INVALID")
            proposal = compile_daily_pending_proposal(
                candidate,
                {
                    str(role): row.daily_pending_review
                    for role, row in roles.items()
                    if role in {"bear", "evidence_audit"} and row.daily_pending_review is not None
                },
                context_hash=context.content_hash,
                trigger_ids=tuple(row.trigger_id for row in context.trigger_checks),
                evidence_refs=source.evidence,
                audit_vote=roles["evidence_audit"].vote,
                adoption_hash=roles["portfolio"].daily_pending_adoption_hash,
            )
            reason = "MONITOR_NO_APPROVED_CONDITION"
        except ValueError:
            reason = "MONITOR_DAILY_EVENT_REVIEW_UNVERIFIED"
    priors = {}
    superseded = False
    for kind in ("price", "disclosure"):
        prior = await session.scalar(
            select(MonitorPlan)
            .where(
                MonitorPlan.account_id == account_id,
                MonitorPlan.instrument_id == run.instrument_id,
                MonitorPlan.kind == kind,
            )
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        priors[kind] = prior
        expected_id, expected_generation = frozen_daily_predecessor(context, kind)
        if (None if prior is None else prior.id, None if prior is None else prior.generation) != (
            expected_id,
            expected_generation,
        ):
            superseded = True
    receipts = []
    for kind in ("price", "disclosure"):
        prior = priors[kind]
        active, status, fixed = None, "gap" if known else "unknown", reason
        if superseded:
            status, fixed = "superseded", "MONITOR_SOURCE_SUPERSEDED"
        elif proposal is not None and source is not None:
            kinds = {row.trigger_id: row.kind for row in context.trigger_checks}
            roles = {row.role: row for row in source.role_outputs}
            available = (
                source.intent.monitoring_proposal
                if kind == "price"
                else source.intent.disclosure_proposal
            )
            if any(
                item.status == "unresolved" and kinds[item.trigger_id] == kind
                for item in proposal.items
            ):
                fixed = "MONITOR_KIND_EVENTS_UNRESOLVED"
            elif available is not None:
                installer = (
                    _install_mixed_or_legacy_plan
                    if kind == "price"
                    else _install_mixed_or_legacy_disclosure
                )
                try:
                    installed = await installer(
                        session,
                        account_id=account_id,
                        output_id=source.output.id,
                        expected_predecessor_id=None if prior is None else prior.id,
                        now=now,
                    )
                except ValueError:
                    raise DailyEventReplacementRejected from None
                active, status, fixed = installed.id, "installed", "MONITOR_INSTALLED"
        row = MonitorCoverageReceipt(
            kind=kind,
            account_id=account_id,
            instrument_id=run.instrument_id,
            run_id=run.id,
            output_id=None if terminal is None else terminal.id,
            prior_plan_id=None if prior is None else prior.id,
            active_plan_id=active,
            research_as_of=run.decision_time,
            observed_at=now,
            status=status,
            reason=fixed,
            quality_status=None if source is None else source.intent.quality_status,
        )
        session.add(row)
        receipts.append(row)
    await session.flush()
    consumed = 0
    if proposal is not None and source is not None and not superseded:
        consumed = await record_daily_event_consumption(
            session, qualified=source, account_id=account_id, now=now
        )
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.daily_events_reconciled",
        schema_version="daily_monitor_consumption_v2",
        occurred_at=now,
        payload={
            "run_id": str(run.id),
            "coverage_ids": [str(row.id) for row in receipts],
            "consumed_count": consumed,
            "execution_locked": True,
        },
    )
    return tuple(receipts)


class DailyEventReplacementRejected(ValueError):
    def __init__(self) -> None:
        super().__init__("MONITOR_DAILY_REPLACEMENT_REJECTED")


async def record_daily_event_replacement_gap(
    session: AsyncSession, *, account_id: UUID, run: AgentResearchRunRecord, now: datetime
) -> tuple[MonitorCoverageReceipt, ...]:
    """After the atomic savepoint rolls back, preserve safe closeout without replay."""
    receipts = []
    for kind in ("price", "disclosure"):
        prior = await session.scalar(
            select(MonitorPlan)
            .where(
                MonitorPlan.account_id == account_id,
                MonitorPlan.instrument_id == run.instrument_id,
                MonitorPlan.kind == kind,
            )
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        row = MonitorCoverageReceipt(
            kind=kind,
            account_id=account_id,
            instrument_id=run.instrument_id,
            run_id=run.id,
            output_id=None,
            prior_plan_id=None if prior is None else prior.id,
            active_plan_id=None,
            research_as_of=run.decision_time,
            observed_at=now,
            status="gap",
            reason="MONITOR_DAILY_REPLACEMENT_REJECTED",
            quality_status=None,
        )
        session.add(row)
        receipts.append(row)
    await session.flush()
    return tuple(receipts)

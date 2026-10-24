"""Atomic per-event consumption and per-kind replacement of one settled mixed job."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.monitoring.mixed_context import MixedMonitorContext
from nexus.monitoring.mixed_contracts import canonical_hash, compile_mixed_proposal
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorIncrement,
    MonitorPlan,
)


def frozen_predecessor(context: MixedMonitorContext, kind: str) -> UUID | None:
    rows = [row for row in context.materials.parents if row["kind"] == kind]
    if len(rows) > 1:
        raise ValueError("MONITOR_PLAN_CONFLICT")
    return None if not rows else UUID(str(rows[0]["plan_id"]))


def frozen_predecessor_changed(
    context: MixedMonitorContext, kind: str, current: MonitorPlan | None
) -> bool:
    expected = frozen_predecessor(context, kind)
    if expected is not None:
        return current is None or current.id != expected
    # Missing parents mean this kind was not frozen, not that no plan existed.
    # It cannot be replaced here. A plan first visible/effective after the
    # material cutoff still fences the entire old result from consumption.
    return current is not None and max(current.created_at, current.effective_at) > (
        context.materials.prepared_at
    )


async def reconcile_mixed_run(
    session: AsyncSession,
    *,
    account_id: UUID,
    run: AgentResearchRunRecord,
    member: MonitorIncrement,
    now: datetime,
) -> tuple[MonitorCoverageReceipt, ...] | None:
    from nexus.monitoring.admission import finish_increment
    from nexus.monitoring.disclosure_installation import _install_mixed_or_legacy_disclosure
    from nexus.monitoring.installation import _install_mixed_or_legacy_plan
    from nexus.monitoring.reconciliation import _native_finished, _scope_evidence
    from nexus.monitoring.source_qualification import load_qualified_source
    from nexus.monitoring.storage import lock_security
    from nexus.persistence.models.monitoring import MonitorMixedEventReceipt

    await lock_security(session, account_id=account_id, instrument_id=run.instrument_id)
    await _scope_evidence(session, run=run, account_id=account_id)
    trigger = await finish_increment(
        session, account_id=account_id, trigger_id=member.trigger_id, now=now
    )
    if trigger.status not in {"completed", "failed"} or not await _native_finished(
        session, run, now
    ):
        return None
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run.id
        )
    )
    from nexus.monitoring.preclaim import preclaim_rejection_qualified

    preclaim = await preclaim_rejection_qualified(session, run=run, now=now)
    known = preclaim or (
        reservation is not None
        and reservation.state == "settled"
        and run.input_tokens is not None
        and run.output_tokens is not None
        and run.settled_cost_usd is not None
        and reservation.settled_usd == run.settled_cost_usd
    )
    if not known:
        return None
    terminal = await session.scalar(
        select(AgentResearchOutputRecord).where(
            AgentResearchOutputRecord.run_id == run.id,
            AgentResearchOutputRecord.role.is_(None),
            AgentResearchOutputRecord.status == "completed",
        )
    )
    source = None
    proposal = None
    review_hash = None
    reason = "MONITOR_CAPACITY_REJECTED" if preclaim else "MONITOR_RUN_FAILED"
    if run.status == "completed" and terminal is not None:
        try:
            source = await load_qualified_source(
                session, account_id=account_id, output_id=terminal.id, now=now
            )
            if not isinstance(source.context, MixedMonitorContext):
                raise ValueError("MONITOR_SOURCE_INVALID")
            roles = {row.role: row for row in source.role_outputs}
            candidate = roles["market_state"].mixed_event_proposal
            if candidate is None:
                raise ValueError("MIXED_EVENT_PROPOSAL_INVALID")
            reviews = {
                str(role): row.mixed_event_review
                for role, row in roles.items()
                if role in {"bear", "evidence_audit"} and row.mixed_event_review is not None
            }
            proposal = compile_mixed_proposal(
                candidate,
                reviews,
                context=source.context.model_dump(mode="json"),
                evidence_refs=source.evidence,
                audit_vote=roles["evidence_audit"].vote,
                adoption_hash=roles["portfolio"].mixed_event_adoption_hash,
            )
            review_hash = canonical_hash(
                {key: value.model_dump(mode="json") for key, value in reviews.items()}
            )
            reason = "MONITOR_NO_APPROVED_CONDITION"
        except ValueError:
            reason = "MONITOR_AUDIT_OR_EVENT_REVIEW_UNVERIFIED"
    receipts = []
    superseded = False
    priors = {}
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
        if source is not None and isinstance(source.context, MixedMonitorContext):
            if frozen_predecessor_changed(source.context, kind, prior):
                superseded = True
    # Predecessor conflicts apply to the entire transaction, never just one kind.
    for kind in ("price", "disclosure"):
        prior = priors[kind]
        active, status, fixed_reason = None, "gap", reason
        if superseded:
            status, fixed_reason = "superseded", "MONITOR_SOURCE_SUPERSEDED"
        elif proposal is not None and source is not None:
            assert isinstance(source.context, MixedMonitorContext)
            roles = {row.role: row for row in source.role_outputs}
            available = (
                roles["market_state"].monitoring_proposal
                if kind == "price"
                else roles["market_state"].disclosure_proposal
            )
            if frozen_predecessor(source.context, kind) is None:
                fixed_reason = "MONITOR_KIND_PARENT_NOT_FROZEN"
            elif any(item.kind == kind and item.status == "unresolved" for item in proposal.items):
                fixed_reason = "MONITOR_KIND_EVENTS_UNRESOLVED"
            elif available is not None:
                installer = (
                    _install_mixed_or_legacy_plan
                    if kind == "price"
                    else _install_mixed_or_legacy_disclosure
                )
                # An invalid non-null proposal aborts both replacements and consumption.
                try:
                    installed = await installer(
                        session,
                        account_id=account_id,
                        output_id=source.output.id,
                        expected_predecessor_id=None if prior is None else prior.id,
                        now=now,
                    )
                except ValueError:
                    raise MixedReplacementRejected(
                        source.output.id, source.intent.quality_status
                    ) from None
                active, status, fixed_reason = installed.id, "installed", "MONITOR_INSTALLED"
        receipt = MonitorCoverageReceipt(
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
            reason=fixed_reason,
            quality_status=None if source is None else source.intent.quality_status,
        )
        session.add(receipt)
        receipts.append(receipt)
    if proposal is not None and source is not None and not superseded:
        context = source.context
        assert isinstance(context, MixedMonitorContext)
        for item in proposal.items:
            if item.status == "addressed":
                session.add(
                    MonitorMixedEventReceipt(
                        account_id=account_id,
                        instrument_id=run.instrument_id,
                        batch_id=member.batch_id,
                        trigger_id=item.trigger_id,
                        run_id=run.id,
                        output_id=source.output.id,
                        materials_snapshot_id=context.materials_snapshot_id,
                        context_hash=proposal.context_hash,
                        proposal_hash=proposal.content_hash,
                        review_hash=review_hash,
                        observed_at=now,
                    )
                )
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.mixed_result_reconciled",
        schema_version="monitor_mixed_reconciliation_v1",
        occurred_at=now,
        payload={
            "run_id": str(run.id),
            "batch_id": str(member.batch_id),
            "coverage_receipt_ids": [str(row.id) for row in receipts],
            "addressed_events": 0
            if proposal is None or superseded
            else sum(item.status == "addressed" for item in proposal.items),
            "execution_locked": True,
        },
    )
    return tuple(receipts)


class MixedReplacementRejected(ValueError):
    def __init__(self, output_id: UUID, quality: str):
        super().__init__("MONITOR_CONDITION_REPLACEMENT_REJECTED")
        self.output_id = output_id
        self.quality = quality


async def record_replacement_gap(
    session: AsyncSession,
    *,
    account_id: UUID,
    run: AgentResearchRunRecord,
    rejected: MixedReplacementRejected,
    now: datetime,
) -> tuple[MonitorCoverageReceipt, ...]:
    """Called only after rolling back the entire verified replacement savepoint."""
    from nexus.monitoring.storage import lock_security

    await lock_security(session, account_id=account_id, instrument_id=run.instrument_id)
    receipts = []
    for kind in ("price", "disclosure"):
        prior = await session.scalar(
            select(MonitorPlan)
            .where(
                MonitorPlan.kind == kind,
                MonitorPlan.account_id == account_id,
                MonitorPlan.instrument_id == run.instrument_id,
            )
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        row = MonitorCoverageReceipt(
            kind=kind,
            account_id=account_id,
            instrument_id=run.instrument_id,
            run_id=run.id,
            output_id=rejected.output_id,
            prior_plan_id=None if prior is None else prior.id,
            active_plan_id=None,
            research_as_of=run.decision_time,
            observed_at=now,
            status="gap",
            reason="MONITOR_CONDITION_REPLACEMENT_REJECTED",
            quality_status=rejected.quality,
        )
        session.add(row)
        receipts.append(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.mixed_replacement_rejected",
        schema_version="monitor_mixed_reconciliation_v1",
        occurred_at=now,
        payload={
            "run_id": str(run.id),
            "coverage_receipt_ids": [str(row.id) for row in receipts],
            "reason": "MONITOR_CONDITION_REPLACEMENT_REJECTED",
        },
    )
    return tuple(receipts)

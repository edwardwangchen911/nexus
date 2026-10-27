"""Seal current stock observations before notification or any research admission."""

import hashlib
import json
from datetime import datetime, timedelta
from typing import TypedDict
from uuid import UUID

from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.monitoring.observations import read_latest_price
from nexus.monitoring.storage import lock_security, record_check
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorIncrement,
    MonitorObservationBatch,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import OutboxEvent


class BatchScanReport(TypedDict):
    impact_review: str
    coverage: str
    unresolved_event_count: int
    stock_unresolved_event_count: int
    status: str
    batch_id: str
    checks: int
    triggers: int
    notifications: int
    new_triggers: int
    automatic_review_capacity: str
    pending_triggers: int
    true: int
    false: int
    unknown: int
    model_requests: int
    provider_requests: int
    execution_locked: bool


async def _report(
    session: AsyncSession, batch: MonitorObservationBatch, *, new_triggers: int, status: str
) -> BatchScanReport:
    from nexus.monitoring.daily_consumption import not_daily_covered
    from nexus.monitoring.mixed_readback import mixed_batch_readback, not_mixed_consumed

    checks = tuple(
        await session.scalars(
            select(MonitorCheck).where(
                MonitorCheck.id.in_(tuple(UUID(value) for value in batch.check_ids))
            )
        )
    )
    pending = (
        await session.scalar(
            select(func.count())
            .select_from(MonitorTrigger)
            .where(
                MonitorTrigger.account_id == batch.account_id,
                MonitorTrigger.instrument_id == batch.instrument_id,
                MonitorTrigger.status.in_(("pending", "blocked")),
                MonitorTrigger.research_run_id.is_(None),
                not_daily_covered(),
                not_mixed_consumed(),
            )
        )
        or 0
    )
    unresolved = (
        await session.scalar(
            select(func.count())
            .select_from(MonitorTrigger)
            .where(
                MonitorTrigger.account_id == batch.account_id,
                MonitorTrigger.instrument_id == batch.instrument_id,
                MonitorTrigger.batch_id.is_not(None),
                not_mixed_consumed(),
            )
        )
        or 0
    )
    return {
        **await mixed_batch_readback(session, batch.id),
        "stock_unresolved_event_count": unresolved,
        "status": status,
        "batch_id": str(batch.id),
        "checks": len(checks),
        "triggers": len(batch.trigger_ids),
        "notifications": int(new_triggers > 0),
        "new_triggers": new_triggers,
        "automatic_review_capacity": "blocked" if pending > 32 else "within_limit",
        "pending_triggers": pending,
        "true": sum(row.result == "true" for row in checks),
        "false": sum(row.result == "false" for row in checks),
        "unknown": sum(row.result == "unknown" for row in checks),
        "model_requests": 0,
        "provider_requests": 0,
        "execution_locked": True,
    }


async def disclosure_source_current(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> bool:
    """An unreconciled newer daily result cannot leave an old SEC plan silently active."""
    from nexus.agents.committee import MONITORING_SCOPE_PROMPT_VERSIONS

    latest = await session.scalar(
        select(AgentResearchRunRecord.id)
        .join(DataSnapshot, DataSnapshot.id == AgentResearchRunRecord.data_snapshot_id)
        .outerjoin(
            AgentPaperCycleRecord,
            DataSnapshot.payload["agent_paper_context"]["cycle_id"].astext
            == cast(AgentPaperCycleRecord.id, String),
        )
        .outerjoin(MonitorIncrement, MonitorIncrement.run_id == AgentResearchRunRecord.id)
        .where(
            or_(
                AgentPaperCycleRecord.account_id == plan.account_id,
                MonitorIncrement.account_id == plan.account_id,
                DataSnapshot.payload["independent_monitor_context"]["account_id"].astext
                == str(plan.account_id),
            ),
            AgentResearchRunRecord.instrument_id == plan.instrument_id,
            AgentResearchRunRecord.decision_time <= now,
            AgentResearchRunRecord.created_at <= now,
            AgentResearchRunRecord.prompt_manifest["portfolio"]["version"].astext.in_(
                MONITORING_SCOPE_PROMPT_VERSIONS
            ),
        )
        .order_by(
            AgentResearchRunRecord.decision_time.desc(),
            AgentResearchRunRecord.created_at.desc(),
            AgentResearchRunRecord.id.desc(),
        )
        .limit(1)
    )
    if latest == plan.source_run_id:
        return True
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt

    return (
        await session.scalar(
            select(MonitorCoverageReceipt.id).where(
                MonitorCoverageReceipt.kind == "disclosure",
                MonitorCoverageReceipt.account_id == plan.account_id,
                MonitorCoverageReceipt.run_id == latest,
                MonitorCoverageReceipt.active_plan_id == plan.id,
                MonitorCoverageReceipt.status.in_(("installed", "retained")),
                MonitorCoverageReceipt.observed_at <= now,
            )
        )
        is not None
    )


async def _check(
    session: AsyncSession,
    plan: MonitorPlan,
    scheduled: datetime,
    now: datetime,
    *,
    key: str,
    result: str,
    reason: str | None,
    observation: dict[str, object] | None,
) -> MonitorCheck:
    existing = await session.scalar(
        select(MonitorCheck).where(
            MonitorCheck.account_id == plan.account_id,
            MonitorCheck.instrument_id == plan.instrument_id,
            MonitorCheck.kind == plan.kind,
            MonitorCheck.scheduled_at == scheduled,
            MonitorCheck.event_key == key,
        )
    )
    if existing is not None:
        return existing
    row = MonitorCheck(
        account_id=plan.account_id,
        instrument_id=plan.instrument_id,
        plan_id=plan.id,
        kind=plan.kind,
        event_key=key,
        scheduled_at=scheduled,
        checked_at=now,
        result=result,
        reason=reason,
        observation=observation,
    )
    session.add(row)
    await session.flush()
    return row


async def scan_stock_batch(
    session: AsyncSession, *, account_id: UUID, instrument_id: UUID, now: datetime
) -> BatchScanReport:
    from nexus.monitoring.disclosure_observations import read_disclosure_observation
    from nexus.monitoring.reconciliation import plan_covered
    from nexus.notifications.research_monitor_batch import ResearchMonitorBatchNotification

    now = require_utc(now)
    scheduled = now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
    if not scheduled <= now < scheduled + timedelta(minutes=5):
        raise ValueError("MONITOR_TICK_TIME_INVALID")
    await lock_security(session, account_id=account_id, instrument_id=instrument_id)
    plans = tuple(
        await session.scalars(
            select(MonitorPlan)
            .where(MonitorPlan.account_id == account_id, MonitorPlan.instrument_id == instrument_id)
            .distinct(MonitorPlan.kind)
            .order_by(MonitorPlan.kind, MonitorPlan.generation.desc())
        )
    )
    if not plans or len(plans) > 2:
        raise ValueError("MONITOR_PLAN_UNAVAILABLE")
    by_kind = {plan.kind: plan for plan in plans}
    facts = {}
    reasons: dict[str, str | None] = {}
    quote = None
    disclosure = None
    for kind, plan in by_kind.items():
        reason = None
        if not plan.effective_at <= now < plan.valid_until:
            reason = "MONITOR_PLAN_EXPIRED_OR_NOT_EFFECTIVE"
        elif kind == "price":
            if not await plan_covered(session, plan=plan, now=now):
                reason = "MONITOR_AWAITING_RECONCILIATION"
            else:
                quote = await read_latest_price(session, instrument_id=instrument_id, now=now)
        elif not await disclosure_source_current(session, plan, now):
            reason = "MONITOR_AWAITING_RECONCILIATION"
        else:
            try:
                disclosure = await read_disclosure_observation(session, plan=plan, now=now)
            except ValueError as error:
                if str(error) != "MONITOR_DISCLOSURE_PLAN_INVALID":
                    raise
                reason = "MONITOR_DISCLOSURE_PLAN_INVALID"
        reasons[kind] = reason
        facts[kind] = {
            "plan_id": str(plan.id),
            "reason": reason,
            "observation": (quote.model_dump(mode="json") if quote else None)
            if kind == "price"
            else disclosure.model_dump(mode="json")
            if disclosure
            else None,
        }
    signature = hashlib.sha256(
        json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    existing = await session.scalar(
        select(MonitorObservationBatch).where(
            MonitorObservationBatch.account_id == account_id,
            MonitorObservationBatch.instrument_id == instrument_id,
            MonitorObservationBatch.scheduled_at == scheduled,
            MonitorObservationBatch.observation_hash == signature,
        )
    )
    if existing is not None:
        return await _report(session, existing, new_triggers=0, status="existing")
    batch_id, outbox_id = new_uuid7(), new_uuid7()
    checks = []
    price = by_kind.get("price")
    if price is not None:
        if facts["price"]["reason"] is None:
            checks.append(
                await record_check(
                    session,
                    plan_id=price.id,
                    scheduled_at=scheduled,
                    checked_at=now,
                    observation=quote,
                    batch_id=batch_id,
                    batch_outbox_id=outbox_id,
                )
            )
        else:
            checks.append(
                await _check(
                    session,
                    price,
                    scheduled,
                    now,
                    key="",
                    result="unknown",
                    reason=reasons["price"],
                    observation=None,
                )
            )
    sec = by_kind.get("disclosure")
    if sec is not None:
        document = disclosure.model_dump(mode="json") if disclosure else None
        key = f"capture:{getattr(disclosure, 'current_snapshot_id', None)}:{sec.id}"
        checks.append(
            await _check(
                session,
                sec,
                scheduled,
                now,
                key=key,
                result=disclosure.result if disclosure else "unknown",
                reason=disclosure.reason if disclosure else reasons["disclosure"],
                observation=document,
            )
        )
        if disclosure is not None and disclosure.result == "true":
            assert document is not None
            for entry in disclosure.added:
                event_key = f"sec:{disclosure.issuer_cik}:{entry.accession_number}"
                seen = await session.scalar(
                    select(MonitorTrigger.id).where(
                        MonitorTrigger.account_id == account_id,
                        MonitorTrigger.instrument_id == instrument_id,
                        MonitorTrigger.kind == "disclosure",
                        MonitorTrigger.event_key == event_key,
                    )
                )
                if seen is not None:
                    continue
                observed = {
                    **{key: value for key, value in document.items() if key != "added"},
                    "entry": entry.model_dump(mode="json"),
                }
                checked = await _check(
                    session,
                    sec,
                    scheduled,
                    now,
                    key=event_key,
                    result="true",
                    reason=None,
                    observation=observed,
                )
                if checked.plan_id != sec.id:
                    raise ValueError("MONITOR_DISCLOSURE_IDENTITY_CONFLICT")
                checks.append(checked)
                session.add(
                    MonitorTrigger(
                        account_id=account_id,
                        instrument_id=instrument_id,
                        kind="disclosure",
                        event_key=event_key,
                        episode=None,
                        condition_hash=sec.condition_hash,
                        check_id=checked.id,
                        cause="disclosure_added",
                        status="pending",
                        reason=None,
                        batch_id=batch_id,
                        outbox_id=outbox_id,
                    )
                )
            await session.flush()
    new = tuple(
        await session.scalars(
            select(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch_id)
            .order_by(MonitorTrigger.id)
        )
    )
    members = sorted(str(row.id) for row in new)
    check_ids = sorted({str(row.id) for row in checks})
    actual_plans = tuple(
        await session.scalars(
            select(MonitorPlan).where(MonitorPlan.id.in_({row.plan_id for row in checks}))
        )
    )
    sealed = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(sealed, datetime) or sealed < now:
        raise ValueError("MONITOR_BATCH_CLOCK_INVALID")
    if new:
        trigger_checks = {row.id: row for row in checks}
        plan_map = {row.id: row for row in actual_plans}
        payload = {
            "schema_version": "research_monitor_batch_v1",
            "batch_id": str(batch_id),
            "instrument_id": str(instrument_id),
            "trigger_ids": members,
            "event_ids": sorted(
                "price:" + str(row.id) if row.kind == "price" else row.event_key for row in new
            ),
            "source_run_ids": sorted(
                {str(plan_map[trigger_checks[row.check_id].plan_id].source_run_id) for row in new}
            ),
            "occurred_at": now.isoformat(),
        }
        ResearchMonitorBatchNotification.model_validate(payload)
        session.add(
            OutboxEvent(
                id=outbox_id,
                aggregate_id=batch_id,
                event_type="research.monitor.batch_triggered",
                schema_version="research_monitor_batch_v1",
                payload=payload,
                available_at=sealed,
            )
        )
        await session.flush()
    batch = MonitorObservationBatch(
        id=batch_id,
        account_id=account_id,
        instrument_id=instrument_id,
        scheduled_at=scheduled,
        checked_at=now,
        sealed_at=sealed,
        observation_hash=signature,
        plan_ids=sorted(str(row.id) for row in actual_plans),
        check_ids=check_ids,
        trigger_ids=members,
        outbox_id=outbox_id if new else None,
    )
    session.add(batch)
    await session.flush()
    return await _report(session, batch, new_triggers=len(new), status="sealed")

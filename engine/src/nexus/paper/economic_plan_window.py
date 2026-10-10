"""Post-sale window scope from a canonical, committed pair plan receipt."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.paper.service import load_paper_order_batch_result
from nexus.persistence.models.economic import (
    EconomicCommonWindowRecord,
    EconomicLaneCycleRecord,
    EconomicWindowOrderRecord,
)
from nexus.persistence.models.paper import (
    PaperOrderBatchRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import AuditEvent

PLAN_STAGE = "post_sale_v1"
PLAN_ACTION = "economic.plan.continuation_finished"


async def read_plan_receipt(
    session: AsyncSession,
    event: AuditEvent,
    pair_id: UUID,
) -> tuple[dict[str, object], list[EconomicWindowOrderRecord], list[datetime]]:
    from nexus.paper.economic_execution import _history, _orders

    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=event.event_seq,
            occurred_at=event.occurred_at,
            actor_id=event.actor_id,
            action=event.action,
            schema_version=event.schema_version,
            payload=event.payload,
        )
    )
    if (
        canonical != event.canonical_event
        or audit_event_hash(event.previous_hash, canonical) != event.event_hash
    ):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    payload = event.payload
    result = payload.get("result")
    if (
        event.action != PLAN_ACTION
        or event.actor_id != "local-economic-cycle"
        or event.schema_version != "economic_plan_continuation_v1"
        or payload.get("pair_slot_id") != str(pair_id)
        or not isinstance(result, dict)
        or set(result) != {"status", "lanes", "execution_locked"}
        or result.get("status") != "plans_continued"
        or result.get("execution_locked") is not True
    ):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    source = await session.get(
        EconomicCommonWindowRecord, UUID(str(payload.get("source_window_id")))
    )
    if (
        source is None
        or source.pair_slot_id != pair_id
        or source.content_hash != payload.get("source_window_hash")
        or source.qualification.get("plan_stage") is not None
    ):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    window, _, requests = await _orders(session, pair_id, source.window_seq, lock=False)
    if (await _history(session, window, requests))["status"] != "completed":
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    lanes = tuple(
        await session.scalars(
            select(EconomicLaneCycleRecord)
            .where(EconomicLaneCycleRecord.pair_slot_id == pair_id)
            .order_by(EconomicLaneCycleRecord.lane)
        )
    )
    saved = result.get("lanes")
    if not isinstance(saved, list) or len(saved) != 2 or len(lanes) != 2:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    links: list[EconomicWindowOrderRecord] = []
    times = [event.created_at, event.occurred_at, source.window_end]
    for lane, item in zip(lanes, saved, strict=True):
        if not isinstance(item, dict) or item.get("lane") != lane.lane:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        parent = await session.scalar(
            select(PaperOrderBatchRecord)
            .join(
                StrategyDecisionRecord,
                StrategyDecisionRecord.id == PaperOrderBatchRecord.strategy_decision_id,
            )
            .where(
                StrategyDecisionRecord.agent_paper_cycle_id == lane.cycle_id,
                PaperOrderBatchRecord.previous_batch_id.is_(None),
            )
        )
        if item.get("status") == "not_required":
            if item.get("batch_id") is not None:
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            if (
                parent is not None
                and await session.scalar(
                    select(PaperOrderBatchRecord.id).where(
                        PaperOrderBatchRecord.previous_batch_id == parent.id
                    )
                )
                is not None
            ):
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            continue
        if item.get("status") != "created" or parent is None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        child = await session.get(PaperOrderBatchRecord, UUID(str(item.get("batch_id"))))
        if (
            child is None
            or child.previous_batch_id != parent.id
            or child.account_id != lane.account_id
            or child.strategy_decision_id != parent.strategy_decision_id
            or child.target_version_id != parent.target_version_id
        ):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        loaded = await load_paper_order_batch_result(
            session,
            account_id=lane.account_id,
            strategy_decision_id=child.strategy_decision_id,
            batch_id=child.id,
        )
        if loaded is None or [str(order) for order in loaded.order_ids] != item.get("order_ids"):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        times.extend((child.created_at, child.evaluated_at))
        orders = tuple(
            await session.scalars(
                select(PaperOrderRecord)
                .where(PaperOrderRecord.batch_id == child.id)
                .order_by(PaperOrderRecord.id)
            )
        )
        if not orders or any(order.side != "buy" for order in orders):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        for order in orders:
            links.append(
                EconomicWindowOrderRecord(
                    lane=lane.lane,
                    order_id=order.id,
                    account_id=order.account_id,
                    instrument_id=order.instrument_id,
                )
            )
            times.append(order.created_at)
    if max(times) > max(event.created_at, event.occurred_at):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    return result, sorted(links, key=lambda link: link.order_id), times


async def plan_window_scope(
    session: AsyncSession,
    pair_id: UUID,
    saved: dict[str, object] | None = None,
) -> tuple[AuditEvent | None, list[EconomicWindowOrderRecord], list[datetime]]:
    if saved is None:
        event = await session.scalar(
            select(AuditEvent)
            .where(
                AuditEvent.action == PLAN_ACTION,
                AuditEvent.payload["pair_slot_id"].astext == str(pair_id),
            )
            .order_by(AuditEvent.event_seq)
            .limit(1)
        )
    else:
        event = await session.get(AuditEvent, UUID(str(saved.get("plan_event_id"))))
    if event is None:
        if saved is not None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        return None, [], []
    _, links, times = await read_plan_receipt(session, event, pair_id)
    if saved is not None and event.event_hash != saved.get("plan_event_hash"):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    return event, links, times

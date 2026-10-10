"""Normal pair-owned post-sale plan continuation; never borrows peer proceeds."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.paper.economic_cycle import EconomicLaneRef, require_economic_lane
from nexus.paper.economic_execution import _history, _orders
from nexus.paper.economic_isolation import require_running_isolation
from nexus.paper.target_service import continue_portfolio_plan, lock_portfolio_account
from nexus.persistence.models.economic import EconomicLaneCycleRecord
from nexus.persistence.models.paper import (
    PaperOrderBatchRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import AuditEvent


async def continue_economic_plans(
    session: AsyncSession, *, pair_slot_id: UUID, expected_database: str
) -> dict[str, object]:
    authority = await require_economic_lane(session, ref=EconomicLaneRef(pair_slot_id, "incumbent"))
    if await session.scalar(text("SELECT current_database()")) != expected_database:
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    for account_id in sorted(
        (authority.identity.incumbent_account_id, authority.identity.challenger_account_id)
    ):
        await lock_portfolio_account(session, account_id)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,2026))"),
        {"scope": f"economic-pair:{pair_slot_id}"},
    )
    existing = await session.scalar(
        select(AuditEvent)
        .where(
            AuditEvent.action == "economic.plan.continuation_finished",
            AuditEvent.payload["pair_slot_id"].astext == str(pair_slot_id),
        )
        .order_by(AuditEvent.event_seq)
        .limit(1)
    )
    if existing is not None:
        from nexus.paper.economic_plan_window import read_plan_receipt

        result, _, _ = await read_plan_receipt(session, existing, pair_slot_id)
        return result
    from nexus.persistence.models.economic import EconomicCommonWindowRecord

    row = await session.scalar(
        select(EconomicCommonWindowRecord)
        .where(EconomicCommonWindowRecord.pair_slot_id == pair_slot_id)
        .order_by(EconomicCommonWindowRecord.window_seq.desc())
        .limit(1)
    )
    if row is None:
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    window, _, requests = await _orders(session, pair_slot_id, row.window_seq)
    history = await _history(session, window, requests)
    if history["status"] != "completed":
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    output: list[dict[str, object]] = []
    async with session.begin_nested():
        lanes = tuple(
            await session.scalars(
                select(EconomicLaneCycleRecord)
                .where(EconomicLaneCycleRecord.pair_slot_id == pair_slot_id)
                .order_by(EconomicLaneCycleRecord.lane)
            )
        )
        for lane in lanes:
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
            if parent is None:
                output.append({"lane": lane.lane, "status": "not_required", "batch_id": None})
                continue
            sales = await session.scalar(
                select(PaperOrderRecord.id)
                .where(PaperOrderRecord.batch_id == parent.id, PaperOrderRecord.side == "sell")
                .limit(1)
            )
            if sales is None:
                output.append({"lane": lane.lane, "status": "not_required", "batch_id": None})
                continue
            existing_child = await session.scalar(
                select(PaperOrderBatchRecord.id).where(
                    PaperOrderBatchRecord.previous_batch_id == parent.id
                )
            )
            if existing_child is None:
                # Historical children remain readable after later isolation drift.
                # Any missing side still requires the full new-write gate; the
                # nested pair transaction rolls back earlier new children on failure.
                await require_running_isolation(session, authority=authority)
            child = await continue_portfolio_plan(
                session,
                account_id=lane.account_id,
                previous_batch_id=parent.id,
            )
            output.append(
                {
                    "lane": lane.lane,
                    "status": "created" if child else "not_required",
                    "batch_id": str(child.batch_id) if child else None,
                    "order_ids": [str(item) for item in child.order_ids] if child else [],
                }
            )
    result = {"status": "plans_continued", "lanes": output, "execution_locked": True}
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    await append_audit_event(
        session,
        actor_id="local-economic-cycle",
        action="economic.plan.continuation_finished",
        schema_version="economic_plan_continuation_v1",
        payload={
            "pair_slot_id": str(pair_slot_id),
            "source_window_id": str(window.id),
            "source_window_hash": window.content_hash,
            "result": result,
        },
        occurred_at=now,
    )
    return result

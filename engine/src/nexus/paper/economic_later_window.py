"""Subsequent windows anchored in committed prior execution, never a retry clock."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from nexus.paper.economic_cycle import EconomicLaneAuthority
    from nexus.paper.economic_window import EconomicWindowResult

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchOutputRecord,
)
from nexus.persistence.models.economic import EconomicCommonWindowRecord, EconomicWindowOrderRecord
from nexus.persistence.models.paper import (
    PaperExecutionWindowAuthorityRecord,
    PaperFillRecord,
    PaperOrderEventRecord,
)
from nexus.persistence.models.runtime import AuditEvent


async def _parent(
    session: AsyncSession, pair_id: UUID, seq: int
) -> tuple[EconomicCommonWindowRecord, AuditEvent]:
    from nexus.paper.economic_execution import _history, _orders

    parent, _, requests = await _orders(session, pair_id, seq - 1, lock=False)
    result = await _history(session, parent, requests)
    finished = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == "economic.window.execution_finished",
            AuditEvent.payload["window_id"].astext == str(parent.id),
        )
    )
    if finished is None or result["status"] not in {"partial", "completed"}:
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    orders = result["orders"]
    if not isinstance(orders, list) or any(
        not isinstance(o, dict) or o.get("action") not in {"filled", "partially_filled"}
        for o in orders
    ):
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    return parent, finished


async def _states(
    session: AsyncSession,
    root: EconomicCommonWindowRecord,
    saved: object = None,
    *,
    plan_links: list[EconomicWindowOrderRecord] | None = None,
) -> tuple[list[dict[str, object]], list[EconomicWindowOrderRecord], list[datetime]]:
    links = (
        tuple(plan_links)
        if plan_links is not None
        else tuple(
            await session.scalars(
                select(EconomicWindowOrderRecord)
                .where(EconomicWindowOrderRecord.window_id == root.id)
                .order_by(EconomicWindowOrderRecord.order_id)
            )
        )
    )
    if saved is not None and (not isinstance(saved, list) or len(saved) != len(links)):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    refs, active, times = [], [], [root.eligible_at]
    for index, link in enumerate(links):
        query = select(PaperOrderEventRecord).where(PaperOrderEventRecord.order_id == link.order_id)
        if saved is not None:
            ref = saved[index]
            if not isinstance(ref, dict) or ref.get("order_id") != str(link.order_id):
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            query = query.where(PaperOrderEventRecord.event_seq == ref.get("event_seq"))
        event = await session.scalar(
            query.order_by(PaperOrderEventRecord.event_seq.desc()).limit(1)
        )
        allowed = {"submitted", "partially_filled", "filled"}
        if plan_links is not None:
            allowed.add("risk_rejected")
        if event is None or event.to_status not in allowed:
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        if plan_links is not None and event.to_status in {"submitted", "risk_rejected"}:
            from nexus.paper.economic_window import _rejection
            from nexus.persistence.models.paper import PaperOrderRecord

            order = await session.get(PaperOrderRecord, link.order_id)
            if order is None:
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            if event.to_status == "risk_rejected":
                rejected = await _rejection(session, order)
                if rejected.id != event.id:
                    raise ValueError("ECONOMIC_WINDOW_INVALID")
            elif order.risk_outcome == "rejected" or event.source_fill_id is not None:
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            if (
                saved is None
                and await session.scalar(
                    select(PaperExecutionWindowAuthorityRecord.id)
                    .where(PaperExecutionWindowAuthorityRecord.order_id == link.order_id)
                    .limit(1)
                )
                is not None
            ):
                raise ValueError("ECONOMIC_WINDOW_NOT_READY")
            record = {
                "order_id": str(link.order_id),
                "lane": link.lane,
                "event_seq": event.event_seq,
                "event_hash": event.payload_hash,
                "status": event.to_status,
                "native_id": None,
                "native_hash": None,
            }
            if saved is not None and record != saved[index]:
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            refs.append(record)
            times.extend((event.created_at, event.occurred_at))
            if event.to_status == "submitted":
                active.append(link)
            continue
        native_query = select(PaperExecutionWindowAuthorityRecord).where(
            PaperExecutionWindowAuthorityRecord.order_id == link.order_id
        )
        if saved is not None:
            native_query = native_query.where(
                PaperExecutionWindowAuthorityRecord.id == UUID(str(ref.get("native_id")))
            )
        native = await session.scalar(
            native_query.order_by(PaperExecutionWindowAuthorityRecord.window_seq.desc()).limit(1)
        )
        fill = (
            await session.get(PaperFillRecord, event.source_fill_id)
            if event.source_fill_id is not None
            else None
        )
        if native is None or fill is None or fill.execution_window_authority_id != native.id:
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        record = {
            "order_id": str(link.order_id),
            "lane": link.lane,
            "event_seq": event.event_seq,
            "event_hash": event.payload_hash,
            "status": event.to_status,
            "native_id": str(native.id),
            "native_hash": native.content_hash,
        }
        if saved is not None and record != saved[index]:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        refs.append(record)
        times.extend(
            (
                event.created_at,
                event.occurred_at,
                native.created_at,
                native.evaluated_at,
                native.data_available_at,
            )
        )
        if event.to_status in {"submitted", "partially_filled"}:
            active.append(link)
    return refs, active, times


async def read_later_window(session: AsyncSession, row: EconomicCommonWindowRecord) -> None:
    from nexus.paper.agent_market import agent_paper_window_bounds
    from nexus.paper.economic_window import _hash, _read_window

    parent, finished = await _parent(session, row.pair_slot_id, row.window_seq)
    root = await session.scalar(
        select(EconomicCommonWindowRecord).where(
            EconomicCommonWindowRecord.pair_slot_id == row.pair_slot_id,
            EconomicCommonWindowRecord.window_seq == 1,
        )
    )
    if root is None:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    await _read_window(session, root)
    doc = row.qualification
    plan_event = None
    if doc.get("plan_stage") is not None:
        from nexus.paper.economic_plan_window import PLAN_STAGE, plan_window_scope

        if doc.get("plan_stage") != PLAN_STAGE:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        plan_event, plan_links, plan_times = await plan_window_scope(session, row.pair_slot_id, doc)
        refs, active, times = await _states(
            session, root, doc.get("order_states"), plan_links=plan_links
        )
        times.extend(plan_times)
    else:
        refs, active, times = await _states(session, root, doc.get("order_states"))
    times.extend((finished.created_at, finished.occurred_at))
    expected = {
        "pair_slot_id": str(row.pair_slot_id),
        "window_seq": row.window_seq,
        "status": row.status,
        "eligible_at": max(times).isoformat(),
        "window_start": row.window_start.isoformat(),
        "window_end": row.window_end.isoformat(),
        "frozen_at": row.frozen_at.isoformat(),
        "lanes": root.qualification["lanes"],
        "orders": sorted(str(link.order_id) for link in active),
        "order_states": refs,
        "parent_id": str(parent.id),
        "parent_hash": parent.content_hash,
        "completion_id": str(finished.id),
        "completion_hash": finished.event_hash,
    }
    if plan_event is not None:
        expected.update(
            plan_stage=PLAN_STAGE,
            plan_event_id=str(plan_event.id),
            plan_event_hash=plan_event.event_hash,
        )
    links = tuple(
        await session.scalars(
            select(EconomicWindowOrderRecord).where(EconomicWindowOrderRecord.window_id == row.id)
        )
    )
    if (
        doc != expected
        or row.content_hash != _hash(doc)
        or row.eligible_at != max(times)
        or row.eligible_at < parent.window_end
        or row.frozen_at < row.eligible_at
        or not active
    ):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    if {(link.order_id, link.account_id, link.instrument_id, link.lane) for link in links} != {
        (link.order_id, link.account_id, link.instrument_id, link.lane) for link in active
    }:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    # The original slot is the only calendar authority. No caller clock or sliding.
    from nexus.paper.economic_cycle import EconomicLaneRef, require_economic_lane

    authority = await require_economic_lane(
        session, ref=EconomicLaneRef(row.pair_slot_id, "incumbent")
    )
    model = await session.get(AgentModelConfigRecord, authority.binding.shared_model_id)
    if (
        model is None
        or row.window_end > model.pricing_expires_at
        or row.eligible_at < model.pricing_available_at
    ):
        raise ValueError("ECONOMIC_WINDOW_SOURCE_EXPIRED")
    lanes = root.qualification.get("lanes")
    if not isinstance(lanes, list):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    for lane in lanes:
        if not isinstance(lane, dict):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(str(lane["run_id"])))
        if run is None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        for output_id in run.research_output_ids:
            output = await session.get(AgentResearchOutputRecord, output_id)
            if (
                output is None
                or output.intent is None
                or row.window_end
                > datetime.fromisoformat(str(output.intent["valid_until"]).replace("Z", "+00:00"))
            ):
                raise ValueError("ECONOMIC_WINDOW_SOURCE_EXPIRED")
    if agent_paper_window_bounds(row.eligible_at, authority.slot.market_close_at) != (
        row.window_start,
        row.window_end,
    ):
        raise ValueError("ECONOMIC_WINDOW_INVALID")


async def freeze_later_window(
    session: AsyncSession, *, authority: EconomicLaneAuthority, window_seq: int
) -> EconomicWindowResult:
    from nexus.paper.agent_market import agent_paper_window_bounds
    from nexus.paper.economic_window import EconomicWindowResult, _hash, _read_window, _result

    pair_id = authority.slot.id
    # The caller already owns both account and pair locks and has checked isolation.
    parent, finished = await _parent(session, pair_id, window_seq)
    root = await session.scalar(
        select(EconomicCommonWindowRecord).where(
            EconomicCommonWindowRecord.pair_slot_id == pair_id,
            EconomicCommonWindowRecord.window_seq == 1,
        )
    )
    if root is None:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    await _read_window(session, root)
    plan_event = None
    if parent.qualification.get("plan_stage") is not None:
        from nexus.paper.economic_plan_window import PLAN_STAGE, plan_window_scope

        plan_event, plan_links, plan_times = await plan_window_scope(
            session, pair_id, parent.qualification
        )
        refs, active, times = await _states(session, root, plan_links=plan_links)
        times.extend(plan_times)
    else:
        refs, active, times = await _states(session, root)
        if not active:
            from nexus.paper.economic_plan_window import PLAN_STAGE, plan_window_scope

            plan_event, plan_links, plan_times = await plan_window_scope(session, pair_id)
            if plan_event is not None:
                refs, active, times = await _states(session, root, plan_links=plan_links)
                times.extend(plan_times)
    times.extend((finished.created_at, finished.occurred_at))
    eligible = max(times)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now < eligible:
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    if not active:
        if plan_event is not None and any(ref["status"] == "risk_rejected" for ref in refs):
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        # Child plans need their own eligibility/window contract. Until that producer
        # exists, do not turn submitted new orders into a false no-work receipt.
        from nexus.persistence.models.economic import EconomicLaneCycleRecord
        from nexus.persistence.models.paper import (
            PaperOrderBatchRecord,
            PaperOrderRecord,
            StrategyDecisionRecord,
        )

        children = tuple(
            await session.scalars(
                select(PaperOrderRecord.id)
                .join(PaperOrderBatchRecord, PaperOrderBatchRecord.id == PaperOrderRecord.batch_id)
                .join(
                    StrategyDecisionRecord,
                    StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
                )
                .join(
                    EconomicLaneCycleRecord,
                    EconomicLaneCycleRecord.cycle_id == StrategyDecisionRecord.agent_paper_cycle_id,
                )
                .where(
                    EconomicLaneCycleRecord.pair_slot_id == pair_id,
                    PaperOrderBatchRecord.previous_batch_id.is_not(None),
                )
            )
        )
        for order_id in children:
            latest = await session.scalar(
                select(PaperOrderEventRecord.to_status)
                .where(PaperOrderEventRecord.order_id == order_id)
                .order_by(PaperOrderEventRecord.event_seq.desc())
                .limit(1)
            )
            if latest in {"submitted", "partially_filled"} or latest is None:
                raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        return EconomicWindowResult("not_required", None, None, None, False)
    start, end = agent_paper_window_bounds(eligible, authority.slot.market_close_at)
    model = await session.get(AgentModelConfigRecord, authority.binding.shared_model_id)
    if (
        model is None
        or not model.pricing_available_at <= now < model.pricing_expires_at
        or end > model.pricing_expires_at
    ):
        raise ValueError("ECONOMIC_WINDOW_SOURCE_EXPIRED")
    lanes = root.qualification.get("lanes")
    if not isinstance(lanes, list):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    for lane in lanes:
        if not isinstance(lane, dict):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(lane["run_id"]))
        if run is None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        for output_id in run.research_output_ids:
            output = await session.get(AgentResearchOutputRecord, output_id)
            if (
                output is None
                or output.intent is None
                or end
                > datetime.fromisoformat(str(output.intent["valid_until"]).replace("Z", "+00:00"))
            ):
                raise ValueError("ECONOMIC_WINDOW_SOURCE_EXPIRED")
    status = "frozen" if now < start else "missed"
    doc = {
        "pair_slot_id": str(pair_id),
        "window_seq": window_seq,
        "status": status,
        "eligible_at": eligible.isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "frozen_at": now.isoformat(),
        "lanes": root.qualification["lanes"],
        "orders": sorted(str(link.order_id) for link in active),
        "order_states": refs,
        "parent_id": str(parent.id),
        "parent_hash": parent.content_hash,
        "completion_id": str(finished.id),
        "completion_hash": finished.event_hash,
    }
    if plan_event is not None:
        doc.update(
            plan_stage=PLAN_STAGE,
            plan_event_id=str(plan_event.id),
            plan_event_hash=plan_event.event_hash,
        )
    row = EconomicCommonWindowRecord(
        pair_slot_id=pair_id,
        window_seq=window_seq,
        status=status,
        eligible_at=eligible,
        window_start=start,
        window_end=end,
        frozen_at=now,
        qualification=doc,
        content_hash=_hash(doc),
    )
    session.add(row)
    await session.flush()
    session.add_all(
        EconomicWindowOrderRecord(
            window_id=row.id,
            lane=link.lane,
            order_id=link.order_id,
            account_id=link.account_id,
            instrument_id=link.instrument_id,
        )
        for link in active
    )
    await session.flush()
    await _read_window(session, row)
    return _result(row, replayed=False)

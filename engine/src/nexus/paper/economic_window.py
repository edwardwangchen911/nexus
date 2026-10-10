"""Database-owned first common window; no model, collector or execution calls."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, cast
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_synthesis_persistence import load_frozen_synthesis_authority
from nexus.paper.economic_cycle import EconomicLaneRef, read_economic_cycle, require_economic_lane
from nexus.paper.economic_isolation import require_running_isolation
from nexus.paper.errors import PaperExecutionError, PaperExecutionErrorCode
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.economic import (
    EconomicCommonWindowRecord,
    EconomicEvaluationRecord,
    EconomicLaneCycleRecord,
    EconomicWindowOrderRecord,
)
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    AgentPaperMarketWindowRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import Job

if TYPE_CHECKING:
    from nexus.paper.agent_market import AgentPaperMarketScope
    from nexus.paper.fills import QualifiedExecutionWindow
    from nexus.persistence.models.data import PriceBarSetAuthority


@dataclass(frozen=True)
class EconomicWindowResult:
    status: str
    window_id: UUID | None
    window_start: datetime | None
    window_end: datetime | None
    replayed: bool


def _hash(document: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def _result(row: EconomicCommonWindowRecord, *, replayed: bool) -> EconomicWindowResult:
    if row.content_hash != _hash(row.qualification):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    return EconomicWindowResult(row.status, row.id, row.window_start, row.window_end, replayed)


async def _root_orders(session: AsyncSession, decision_id: UUID) -> tuple[PaperOrderRecord, ...]:
    return tuple(
        await session.scalars(
            select(PaperOrderRecord)
            .join(PaperOrderBatchRecord, PaperOrderBatchRecord.id == PaperOrderRecord.batch_id)
            .where(
                PaperOrderRecord.strategy_decision_id == decision_id,
                PaperOrderBatchRecord.previous_batch_id.is_(None),
            )
            .order_by(PaperOrderRecord.id)
        )
    )


async def _rejection(session: AsyncSession, order: PaperOrderRecord) -> PaperOrderEventRecord:
    events = tuple(
        await session.scalars(
            select(PaperOrderEventRecord)
            .where(PaperOrderEventRecord.order_id == order.id)
            .order_by(PaperOrderEventRecord.event_seq)
        )
    )
    if (
        order.risk_outcome != "rejected"
        or len(events) != 2
        or events[0].to_status != "proposed"
        or events[1].from_status != "proposed"
        or events[1].to_status != "risk_rejected"
        or events[1].reason_code != "PAPER_RISK_REJECTED"
    ):
        raise ValueError("ECONOMIC_WINDOW_ORDER_NOT_READY")
    return events[1]


def _rejection_ref(order: PaperOrderRecord, event: PaperOrderEventRecord) -> dict[str, object]:
    return {
        "order_id": str(order.id),
        "event_seq": event.event_seq,
        "event_hash": event.payload_hash,
    }


async def _read_window(session: AsyncSession, row: EconomicCommonWindowRecord) -> None:
    if row.window_seq > 1:
        from nexus.paper.economic_later_window import read_later_window

        await read_later_window(session, row)
        return
    _result(row, replayed=True)
    document = row.qualification
    wanted = {
        "pair_slot_id": str(row.pair_slot_id),
        "window_seq": row.window_seq,
        "status": row.status,
        "eligible_at": row.eligible_at.isoformat(),
        "window_start": row.window_start.isoformat(),
        "window_end": row.window_end.isoformat(),
        "frozen_at": row.frozen_at.isoformat(),
    }
    if any(document.get(key) != value for key, value in wanted.items()):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    lanes = document.get("lanes")
    orders = document.get("orders")
    if (
        not isinstance(lanes, list)
        or len(lanes) != 2
        or not isinstance(orders, list)
        or not orders
        or len(set(orders)) != len(orders)
    ):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    links = tuple(
        await session.scalars(
            select(EconomicWindowOrderRecord).where(EconomicWindowOrderRecord.window_id == row.id)
        )
    )
    if set(orders) != {str(link.order_id) for link in links}:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    cycles = tuple(
        await session.scalars(
            select(EconomicLaneCycleRecord).where(
                EconomicLaneCycleRecord.pair_slot_id == row.pair_slot_id
            )
        )
    )
    if {str(lane.get("lane")) for lane in lanes if isinstance(lane, dict)} != {
        "incumbent",
        "challenger",
    }:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    lane_times: list[datetime] = []
    for lane in lanes:
        if not isinstance(lane, dict):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        cycle = next((item for item in cycles if item.lane == lane["lane"]), None)
        if cycle is None or str(cycle.cycle_id) != lane.get("cycle_id"):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        event = await session.get(
            AgentPaperCycleEventRecord, (cycle.cycle_id, lane.get("event_sequence"))
        )
        decision = await session.get(StrategyDecisionRecord, UUID(str(lane.get("decision_id"))))
        if (
            event is None
            or event.event_hash != lane.get("event_hash")
            or decision is None
            or decision.agent_paper_cycle_id != cycle.cycle_id
            or decision.decision_hash != lane.get("decision_hash")
        ):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        lane_orders = [link for link in links if link.lane == cycle.lane]
        if (lane_orders and event.to_status != "awaiting_market") or (
            not lane_orders
            and (event.to_status, event.result_code) != ("completed", "NO_ORDER_REQUIRED")
        ):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(str(lane.get("run_id"))))
        if (
            run is None
            or run.cycle_id != cycle.cycle_id
            or run.status != "completed"
            or run.completed_at is None
            or run.input_hash != lane.get("input_hash")
        ):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        await load_frozen_synthesis_authority(session, run)
        output = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run.id
            )
        )
        if (
            output is None
            or output.status != "completed"
            or decision.portfolio_synthesis_output_id != output.id
        ):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        refs = [{"id": str(output.id), "hash": output.content_hash}]
        times = [
            run.created_at,
            run.completed_at,
            output.created_at,
            decision.created_at,
            event.created_at,
            event.occurred_at,
        ]
        run_ids = [str(run.id)]
        for output_id in run.research_output_ids:
            research = await session.get(AgentResearchOutputRecord, output_id)
            research_run = (
                None
                if research is None
                else await session.get(AgentResearchRunRecord, research.run_id)
            )
            if (
                research is None
                or research_run is None
                or research.status != "completed"
                or research_run.status != "completed"
            ):
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            refs.append({"id": str(output_id), "hash": research.content_hash})
            times.extend((research.created_at, research_run.created_at))
            run_ids.append(str(research_run.id))
        if lane.get("outputs") != refs:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        jobs = tuple(
            await session.scalars(select(Job).where(Job.payload["run_id"].as_string().in_(run_ids)))
        )
        if len(jobs) != len(run_ids) or any(
            job.status != "succeeded" or job.finished_at is None for job in jobs
        ):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        times.extend(job.created_at for job in jobs)
        times.extend(job.finished_at for job in jobs if job.finished_at is not None)
        for link in links:
            if link.lane != cycle.lane:
                continue
            order = await session.get(PaperOrderRecord, link.order_id)
            if (
                order is None
                or order.strategy_decision_id != decision.id
                or order.account_id != cycle.account_id
                or link.account_id != cycle.account_id
                or order.instrument_id != link.instrument_id
            ):
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            submitted = await session.scalar(
                select(PaperOrderEventRecord)
                .where(
                    PaperOrderEventRecord.order_id == order.id,
                    PaperOrderEventRecord.to_status == "submitted",
                )
                .order_by(PaperOrderEventRecord.event_seq)
                .limit(1)
            )
            if submitted is None:
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            times.extend((order.created_at, submitted.created_at, submitted.occurred_at))
        root_orders = await _root_orders(session, decision.id)
        linked = {link.order_id for link in lane_orders}
        rejected = []
        for order in root_orders:
            if order.id in linked:
                if order.risk_outcome == "rejected":
                    raise ValueError("ECONOMIC_WINDOW_INVALID")
                continue
            try:
                rejection = await _rejection(session, order)
            except ValueError as exc:
                raise ValueError("ECONOMIC_WINDOW_INVALID") from exc
            rejected.append(_rejection_ref(order, rejection))
            times.extend((order.created_at, rejection.created_at, rejection.occurred_at))
        if (
            linked != {order.id for order in root_orders if order.risk_outcome != "rejected"}
            or lane.get("rejected_orders", []) != rejected
        ):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        if max(times).isoformat() != lane.get("eligible_at"):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        lane_times.append(max(times))
    if max(lane_times) != row.eligible_at:
        raise ValueError("ECONOMIC_WINDOW_INVALID")


class _FreezeCrossedStart(Exception):
    """Known uncommitted DB work only; never a model/provider retry."""


async def freeze_common_window(
    session: AsyncSession, *, pair_slot_id: UUID, window_seq: int | None
) -> EconomicWindowResult:
    try:
        async with session.begin_nested():
            result = await _freeze(session, pair_slot_id=pair_slot_id, window_seq=window_seq)
            if result.status == "frozen" and not result.replayed:
                now = await session.scalar(select(func.clock_timestamp()))
                assert isinstance(now, datetime) and result.window_start is not None
                if now >= result.window_start:
                    raise _FreezeCrossedStart
            return result
    except _FreezeCrossedStart:
        # The savepoint rolled back the known, uncommitted window and links.
        # Re-derive the same first window from immutable qualifications; now it
        # records missed, or reads another transaction's exact frozen receipt.
        async with session.begin_nested():
            return await _freeze(session, pair_slot_id=pair_slot_id, window_seq=window_seq)


async def _freeze(
    session: AsyncSession, *, pair_slot_id: UUID, window_seq: int | None
) -> EconomicWindowResult:
    from nexus.paper.agent_market import agent_paper_window_bounds

    if window_seq is not None and window_seq < 1:
        raise ValueError("ECONOMIC_WINDOW_SEQUENCE_UNSUPPORTED")
    authority = await require_economic_lane(session, ref=EconomicLaneRef(pair_slot_id, "incumbent"))
    for account in sorted(
        (authority.identity.incumbent_account_id, authority.identity.challenger_account_id),
        key=lambda item: item.int,
    ):
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,1818))"),
            {"scope": f"internal-paper:{account}"},
        )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,2026))"),
        {"scope": f"economic-pair:{pair_slot_id}"},
    )
    if window_seq is None:
        from nexus.persistence.models.runtime import AuditEvent

        latest = await session.scalar(
            select(EconomicCommonWindowRecord)
            .where(EconomicCommonWindowRecord.pair_slot_id == pair_slot_id)
            .order_by(EconomicCommonWindowRecord.window_seq.desc())
            .limit(1)
        )
        completed = (
            None
            if latest is None
            else await session.scalar(
                select(AuditEvent.id).where(
                    AuditEvent.action == "economic.window.execution_finished",
                    AuditEvent.payload["window_id"].astext == str(latest.id),
                )
            )
        )
        window_seq = (
            1
            if latest is None
            else latest.window_seq
            + (1 if completed is not None and latest.status == "frozen" else 0)
        )
    existing = await session.scalar(
        select(EconomicCommonWindowRecord).where(
            EconomicCommonWindowRecord.pair_slot_id == pair_slot_id,
            EconomicCommonWindowRecord.window_seq == window_seq,
        )
    )
    if existing is not None:
        await _read_window(session, existing)
        return _result(existing, replayed=True)
    await require_running_isolation(session, authority=authority)
    if window_seq > 1:
        from nexus.paper.economic_later_window import freeze_later_window

        return await freeze_later_window(session, authority=authority, window_seq=window_seq)
    links = tuple(
        await session.scalars(
            select(EconomicLaneCycleRecord)
            .where(EconomicLaneCycleRecord.pair_slot_id == pair_slot_id)
            .order_by(EconomicLaneCycleRecord.lane)
        )
    )
    if len(links) != 2:
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    model = await session.get(AgentModelConfigRecord, authority.binding.shared_model_id)
    if model is None or not model.pricing_available_at <= now < model.pricing_expires_at:
        raise ValueError("ECONOMIC_WINDOW_SOURCE_EXPIRED")
    lanes = []
    order_links = []
    eligible = []
    expiries = []
    for link in links:
        await read_economic_cycle(session, cycle_id=link.cycle_id)
        event = await session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == link.cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        decision = await session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.agent_paper_cycle_id == link.cycle_id
            )
        )
        run = await session.scalar(
            select(AgentPortfolioSynthesisRunRecord).where(
                AgentPortfolioSynthesisRunRecord.cycle_id == link.cycle_id
            )
        )
        if (
            event is None
            or decision is None
            or run is None
            or run.status != "completed"
            or run.completed_at is None
            or event.to_status not in {"awaiting_market", "completed"}
        ):
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        await load_frozen_synthesis_authority(session, run)
        output = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run.id
            )
        )
        if (
            output is None
            or output.status != "completed"
            or decision.portfolio_synthesis_output_id != output.id
        ):
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        times = [
            run.completed_at,
            run.created_at,
            output.created_at,
            decision.created_at,
            event.occurred_at,
            event.created_at,
        ]
        refs = [{"id": str(output.id), "hash": output.content_hash}]
        run_ids = [str(run.id)]
        for output_id in run.research_output_ids:
            research_output = await session.get(AgentResearchOutputRecord, output_id)
            research_run = (
                None
                if research_output is None
                else await session.get(AgentResearchRunRecord, research_output.run_id)
            )
            if (
                research_output is None
                or research_run is None
                or research_run.status != "completed"
                or research_output.status != "completed"
                or research_output.intent is None
            ):
                raise ValueError("ECONOMIC_WINDOW_NOT_READY")
            expiries.append(
                datetime.fromisoformat(
                    str(research_output.intent["valid_until"]).replace("Z", "+00:00")
                )
            )
            run_ids.append(str(research_run.id))
            refs.append({"id": str(output_id), "hash": research_output.content_hash})
            times.extend((research_output.created_at, research_run.created_at))
        jobs = tuple(
            await session.scalars(select(Job).where(Job.payload["run_id"].as_string().in_(run_ids)))
        )
        if len(jobs) != len(run.research_output_ids) + 1 or any(
            job.status != "succeeded" or job.finished_at is None for job in jobs
        ):
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        times.extend(job.finished_at for job in jobs if job.finished_at is not None)
        times.extend(job.created_at for job in jobs)
        orders = await _root_orders(session, decision.id)
        rejected = []
        if orders and all(order.risk_outcome == "rejected" for order in orders):
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        if not orders and (event.to_status, event.result_code) != (
            "completed",
            "NO_ORDER_REQUIRED",
        ):
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        if orders and event.to_status != "awaiting_market":
            raise ValueError("ECONOMIC_WINDOW_NOT_READY")
        for order in orders:
            if order.risk_outcome == "rejected":
                rejection = await _rejection(session, order)
                rejected.append(_rejection_ref(order, rejection))
                times.extend((order.created_at, rejection.created_at, rejection.occurred_at))
                continue
            events = tuple(
                await session.scalars(
                    select(PaperOrderEventRecord)
                    .where(PaperOrderEventRecord.order_id == order.id)
                    .order_by(PaperOrderEventRecord.event_seq)
                )
            )
            submitted = next((item for item in events if item.to_status == "submitted"), None)
            if submitted is None or events[-1].to_status != "submitted":
                raise ValueError("ECONOMIC_WINDOW_ORDER_NOT_READY")
            times.extend((order.created_at, submitted.created_at, submitted.occurred_at))
            order_links.append((link, order))
        lane_at = max(times)
        eligible.append(lane_at)
        lanes.append(
            {
                "lane": link.lane,
                "cycle_id": str(link.cycle_id),
                "event_sequence": event.sequence,
                "event_hash": event.event_hash,
                "decision_id": str(decision.id),
                "decision_hash": decision.decision_hash,
                "run_id": str(run.id),
                "input_hash": run.input_hash,
                "eligible_at": lane_at.isoformat(),
                "outputs": refs,
            }
        )
        if rejected:
            lanes[-1]["rejected_orders"] = rejected
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    if max(eligible) > now:
        raise ValueError("ECONOMIC_WINDOW_NOT_READY")
    if not order_links:
        return EconomicWindowResult("not_required", None, None, None, False)
    start, end = agent_paper_window_bounds(max(eligible), authority.slot.market_close_at)
    if min(expiries) < end or model.pricing_expires_at < end:
        raise ValueError("ECONOMIC_WINDOW_SOURCE_EXPIRED")
    status = "frozen" if now < start else "missed"
    document = {
        "pair_slot_id": str(pair_slot_id),
        "window_seq": window_seq,
        "status": status,
        "eligible_at": max(eligible).isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "frozen_at": now.isoformat(),
        "lanes": lanes,
        "orders": sorted(str(order.id) for _, order in order_links),
    }
    row = EconomicCommonWindowRecord(
        pair_slot_id=pair_slot_id,
        window_seq=window_seq,
        status=status,
        eligible_at=max(eligible),
        window_start=start,
        window_end=end,
        frozen_at=now,
        qualification=document,
        content_hash=_hash(document),
    )
    session.add(row)
    await session.flush()
    session.add_all(
        EconomicWindowOrderRecord(
            window_id=row.id,
            lane=link.lane,
            order_id=order.id,
            account_id=order.account_id,
            instrument_id=order.instrument_id,
        )
        for link, order in order_links
    )
    await session.flush()
    await _read_window(session, row)
    return _result(row, replayed=False)


async def economic_market_scope(
    session: AsyncSession, *, cycle_id: UUID, instrument_id: UUID
) -> "AgentPaperMarketScope | None":
    from nexus.paper.agent_market import AgentPaperMarketScope
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    owner = await read_economic_cycle(session, cycle_id=cycle_id)
    if owner is None:
        return None
    row = await session.scalar(
        select(EconomicCommonWindowRecord)
        .where(EconomicCommonWindowRecord.pair_slot_id == owner[0].pair_slot_id)
        .order_by(EconomicCommonWindowRecord.window_seq.desc())
        .limit(1)
    )
    if row is None:
        raise ValueError("ECONOMIC_WINDOW_REQUIRED")
    await _read_window(session, row)
    if row.status != "frozen":
        raise ValueError("ECONOMIC_WINDOW_MISSED")
    bound = await session.scalar(
        select(EconomicWindowOrderRecord.id).where(
            EconomicWindowOrderRecord.window_id == row.id,
            EconomicWindowOrderRecord.account_id == owner[1].identity.incumbent_account_id
            if owner[0].lane == "incumbent"
            else EconomicWindowOrderRecord.account_id == owner[1].identity.challenger_account_id,
            EconomicWindowOrderRecord.instrument_id == instrument_id,
        )
    )
    if bound is None:
        raise ValueError("ECONOMIC_WINDOW_ORDER_REQUIRED")
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    return AgentPaperMarketScope(
        cycle_id=cycle_id,
        instrument_id=instrument_id,
        execution_source_policy_id=cycle.execution_price_policy_id,
        window_start=row.window_start,
        window_end=row.window_end,
    )


async def require_economic_consumption(
    session: AsyncSession, *, order_id: UUID, evaluated_at: datetime, market_window_id: UUID | None
) -> tuple["QualifiedExecutionWindow", "PriceBarSetAuthority"] | None:
    try:
        return await _require_economic_consumption(
            session, order_id=order_id, evaluated_at=evaluated_at, market_window_id=market_window_id
        )
    except ValueError as error:
        code = str(error)
        if code in ECONOMIC_CONSUMPTION_CODES:
            raise PaperExecutionError(cast(PaperExecutionErrorCode, code)) from None
        raise PaperExecutionError("PAPER_EXECUTION_AUTHORITY_INCOMPLETE") from None


ECONOMIC_CONSUMPTION_CODES = {
    "ECONOMIC_PAIR_CYCLE_NOT_BOUND",
    "ECONOMIC_WINDOW_REQUIRED",
    "ECONOMIC_WINDOW_INVALID",
    "ECONOMIC_WINDOW_MISSED",
    "ECONOMIC_WINDOW_ORDER_REQUIRED",
    "ECONOMIC_WINDOW_ARCHIVES_REQUIRED",
    "ECONOMIC_WINDOW_ARCHIVE_CONFLICT",
    "ECONOMIC_WINDOW_BINDING_REQUIRED",
    "ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE",
}


async def _require_economic_consumption(
    session: AsyncSession, *, order_id: UUID, evaluated_at: datetime, market_window_id: UUID | None
) -> tuple["QualifiedExecutionWindow", "PriceBarSetAuthority"] | None:
    order = await session.get(PaperOrderRecord, order_id)
    decision = (
        None
        if order is None
        else await session.get(StrategyDecisionRecord, order.strategy_decision_id)
    )
    if order is None:
        return None
    registered = await session.scalar(
        select(EconomicEvaluationRecord.id).where(
            (EconomicEvaluationRecord.incumbent_account_id == order.account_id)
            | (EconomicEvaluationRecord.challenger_account_id == order.account_id)
        )
    )
    if decision is None or decision.agent_paper_cycle_id is None:
        if registered is not None:
            raise ValueError("ECONOMIC_PAIR_CYCLE_NOT_BOUND")
        return None
    owner = await read_economic_cycle(session, cycle_id=decision.agent_paper_cycle_id)
    if owner is None:
        if registered is not None:
            raise ValueError("ECONOMIC_PAIR_CYCLE_NOT_BOUND")
        return None
    scope = await economic_market_scope(
        session, cycle_id=decision.agent_paper_cycle_id, instrument_id=order.instrument_id
    )
    row = await session.scalar(
        select(EconomicCommonWindowRecord)
        .where(EconomicCommonWindowRecord.pair_slot_id == owner[0].pair_slot_id)
        .order_by(EconomicCommonWindowRecord.window_seq.desc())
        .limit(1)
        .with_for_update()
    )
    if row is None or scope is None:
        raise ValueError("ECONOMIC_WINDOW_REQUIRED")
    bindings = tuple(
        await session.scalars(
            select(EconomicWindowOrderRecord).where(EconomicWindowOrderRecord.window_id == row.id)
        )
    )
    if order.id not in {binding.order_id for binding in bindings}:
        raise ValueError("ECONOMIC_WINDOW_ORDER_REQUIRED")
    seen: dict[UUID, tuple[UUID, str, str]] = {}
    selected = None
    for binding in bindings:
        raw = await session.scalar(
            select(AgentPaperMarketWindowRecord).where(
                AgentPaperMarketWindowRecord.order_id == binding.order_id,
                AgentPaperMarketWindowRecord.window_start_at == scope.window_start,
                AgentPaperMarketWindowRecord.window_end_at == scope.window_end,
                AgentPaperMarketWindowRecord.purpose == "paper_fill",
            )
        )
        if raw is None:
            raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")
        if raw.evaluated_at > evaluated_at or raw.data_available_at > evaluated_at:
            raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")
        identity = (raw.raw_artifact_id, raw.raw_content_hash, raw.source_content_hash)
        if binding.instrument_id in seen and seen[binding.instrument_id] != identity:
            raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        seen[binding.instrument_id] = identity
        if binding.order_id == order.id:
            selected = raw.id
    if market_window_id is None or selected != market_window_id:
        raise ValueError("ECONOMIC_WINDOW_BINDING_REQUIRED")
    return await _qualify_economic_consumption(
        session, row=row, order_id=order.id, evaluated_at=evaluated_at
    )


async def _qualify_economic_consumption(
    session: AsyncSession,
    *,
    row: EconomicCommonWindowRecord,
    order_id: UUID,
    evaluated_at: datetime,
) -> tuple["QualifiedExecutionWindow", "PriceBarSetAuthority"]:
    # Use the exact collection receipt and the native execution qualifier for
    # every required order before permitting the first independent transaction.
    from nexus.paper.economic_collection import _existing, _require_new_collection, _scope
    from nexus.paper.execution_service import _qualified_window
    from nexus.paper.fills import ExecutionCalendarSession, ExecutionModel, execution_model_hash
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.paper import (
        PaperExecutionModelRecord,
        PaperExecutionWindowAuthorityRecord,
    )

    window, scopes, order_scopes = await _scope(session, row.pair_slot_id, row.window_seq)
    if not await _existing(session, window, order_scopes):
        raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")
    await _require_new_collection(session, scopes)
    if evaluated_at < window.window_end:
        raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")
    if (
        await session.scalar(
            select(PaperExecutionWindowAuthorityRecord.id).where(
                PaperExecutionWindowAuthorityRecord.order_id == order_id,
                PaperExecutionWindowAuthorityRecord.window_start_at == window.window_start,
            )
        )
        is not None
    ):
        # Only exact original receipts, read before this gate, may replay.
        raise ValueError("ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE")
    model_row = await session.scalar(
        select(PaperExecutionModelRecord).where(
            PaperExecutionModelRecord.version == "execution_model_v1"
        )
    )
    if model_row is None:
        raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
    document = dict(model_row.document)
    document["volume_participation"] = Decimal(str(document.get("volume_participation")))
    model = ExecutionModel.model_validate(document)
    if execution_model_hash(model) != model_row.content_hash:
        raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
    database_now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(database_now, datetime) or evaluated_at > database_now:
        raise ValueError("ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE")
    # Fresh execution cannot reuse a research intent after its original expiry.
    # This check does not apply to an exact already-committed receipt.
    lanes = window.qualification.get("lanes")
    if not isinstance(lanes, list):
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    for lane in lanes:
        if not isinstance(lane, dict) or not isinstance(lane.get("run_id"), str):
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(lane["run_id"]))
        if run is None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        for output_id in run.research_output_ids:
            research = await session.get(AgentResearchOutputRecord, output_id)
            if research is None or research.intent is None:
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            expiry = datetime.fromisoformat(
                str(research.intent["valid_until"]).replace("Z", "+00:00")
            )
            if max(evaluated_at, database_now) >= expiry:
                raise ValueError("ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE")
    for account_id in {
        link.account_id
        for link in await session.scalars(
            select(EconomicWindowOrderRecord).where(
                EconomicWindowOrderRecord.window_id == window.id
            )
        )
    }:
        await load_paper_projection(session, account_id=account_id, as_of=evaluated_at)
    selected_qualified = None
    shared_qualifications: dict[UUID, tuple[UUID, tuple[object, ...]]] = {}
    for peer_id, scope in order_scopes.items():
        peer = await session.get(PaperOrderRecord, peer_id)
        if peer is None:
            raise ValueError("ECONOMIC_WINDOW_ORDER_REQUIRED")
        decision = await session.get(StrategyDecisionRecord, peer.strategy_decision_id)
        if decision is None:
            raise ValueError("ECONOMIC_WINDOW_ORDER_REQUIRED")
        events = tuple(
            await session.scalars(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == peer_id)
                .order_by(PaperOrderEventRecord.event_seq)
            )
        )
        if not events or events[-1].to_status not in {"submitted", "partially_filled", "filled"}:
            raise ValueError("ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE")
        calendar_row = await session.get(
            ExchangeCalendarSessionRecord,
            (decision.calendar_authority_id, decision.execution_session),
        )
        if calendar_row is None or max(evaluated_at, database_now) >= calendar_row.market_close_at:
            raise ValueError("ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE")
        archive = await session.scalar(
            select(AgentPaperMarketWindowRecord).where(
                AgentPaperMarketWindowRecord.order_id == peer_id,
                AgentPaperMarketWindowRecord.window_start_at == window.window_start,
                AgentPaperMarketWindowRecord.purpose == "paper_fill",
            )
        )
        if archive is None:
            raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")
        prior = await session.scalar(
            select(PaperExecutionWindowAuthorityRecord)
            .where(
                PaperExecutionWindowAuthorityRecord.order_id == peer_id,
                PaperExecutionWindowAuthorityRecord.window_start_at < window.window_start,
            )
            .order_by(PaperExecutionWindowAuthorityRecord.window_seq.desc())
            .limit(1)
        )
        qualified = await _qualified_window(
            session,
            order=peer,
            source_policy_id=scope.execution_source_policy_id,
            calendar=ExecutionCalendarSession(
                session=calendar_row.session,
                market_open_at=calendar_row.market_open_at,
                market_close_at=calendar_row.market_close_at,
            ),
            model=model,
            window_seq=1 if prior is None else prior.window_seq + 1,
            evaluated_at=evaluated_at,
            prior_window=prior,
            expected_agent_market_window_id=archive.id,
            economic_scope=scope,
        )
        if qualified is None:
            raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")

        result, source = qualified
        # Same archived bars and same quote/no-quote inputs must produce the same
        # deterministic source metrics on both lanes. An immutable committed
        # native window pins them; later quote changes cause a safe conflict.
        names = (
            "source",
            "feed",
            "delivery_mode",
            "vwap",
            "volume",
            "capacity",
            "quote_id",
            "quote_revision",
            "bid",
            "ask",
            "half_spread",
        )
        identity = (source.id, tuple(getattr(result, key) for key in names))
        if (
            peer.instrument_id in shared_qualifications
            and shared_qualifications[peer.instrument_id] != identity
        ):
            raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        shared_qualifications[peer.instrument_id] = identity
        committed = tuple(
            await session.scalars(
                select(PaperExecutionWindowAuthorityRecord).where(
                    PaperExecutionWindowAuthorityRecord.order_id.in_(order_scopes),
                    PaperExecutionWindowAuthorityRecord.instrument_id == peer.instrument_id,
                    PaperExecutionWindowAuthorityRecord.window_start_at == window.window_start,
                )
            )
        )
        for previous in committed:
            if previous.source_authority_id != source.id or any(
                getattr(previous, key) != getattr(result, key) for key in names
            ):
                raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        if peer_id == order_id:
            selected_qualified = qualified
    if selected_qualified is None:
        raise ValueError("ECONOMIC_WINDOW_ORDER_REQUIRED")
    return selected_qualified


async def lock_economic_order_window(session: AsyncSession, *, order: PaperOrderRecord) -> None:
    """Serialize an owned cancellation with consumption without requiring archives."""
    decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
    if decision is None or decision.agent_paper_cycle_id is None:
        return
    owned = await read_economic_cycle(session, cycle_id=decision.agent_paper_cycle_id)
    if owned is None:
        return
    await session.scalar(
        select(EconomicCommonWindowRecord)
        .where(EconomicCommonWindowRecord.pair_slot_id == owned[0].pair_slot_id)
        .order_by(EconomicCommonWindowRecord.window_seq.desc())
        .limit(1)
        .with_for_update()
    )

"""One persisted pair attempt, independently committed native order operations."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.audit.service import append_audit_event
from nexus.ops.paper_once import (
    PaperAdvanceRequest,
    PaperOnceError,
    _advance_agent_market_window,
    _coordinator_agent_window_result,
    _exact_agent_window_fill_output,
)
from nexus.paper.agent_market import AgentPaperMarketScope
from nexus.paper.economic_collection import _existing, _require_new_collection, _scope
from nexus.paper.errors import PaperExecutionError
from nexus.persistence.models.economic import EconomicCommonWindowRecord, EconomicWindowOrderRecord
from nexus.persistence.models.paper import AgentPaperMarketWindowRecord, PaperOrderRecord
from nexus.persistence.models.runtime import AuditEvent

_CLAIM = "economic.window.execution_claimed"
_RESULT = "economic.window.execution_finished"
_SCHEMA = "economic_window_execution_v1"
_UNKNOWN = "ECONOMIC_PAIR_RESULT_UNKNOWN"
RequestRefs = tuple[tuple[str, PaperAdvanceRequest], ...]
OrderFacts = list[dict[str, str]]


async def _orders(
    session: AsyncSession, pair_slot_id: UUID, window_seq: int, *, lock: bool = True
) -> tuple[EconomicCommonWindowRecord, dict[UUID, AgentPaperMarketScope], RequestRefs]:
    window, scopes, order_scopes = await _scope(session, pair_slot_id, window_seq, lock=lock)
    if not await _existing(session, window, order_scopes):
        raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")
    links = tuple(
        await session.scalars(
            select(EconomicWindowOrderRecord)
            .where(EconomicWindowOrderRecord.window_id == window.id)
            .order_by(
                EconomicWindowOrderRecord.lane.desc(),
                EconomicWindowOrderRecord.instrument_id,
                EconomicWindowOrderRecord.order_id,
            )
        )
    )
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError(_UNKNOWN)
    requests = []
    for link in links:
        order = await session.get(PaperOrderRecord, link.order_id)
        binding = await session.scalar(
            select(AgentPaperMarketWindowRecord).where(
                AgentPaperMarketWindowRecord.order_id == link.order_id,
                AgentPaperMarketWindowRecord.window_start_at == window.window_start,
                AgentPaperMarketWindowRecord.purpose == "paper_fill",
            )
        )
        if order is None or binding is None:
            raise ValueError(_UNKNOWN)
        requests.append(
            (
                link.lane,
                PaperAdvanceRequest(
                    action="advance",
                    account_id=order.account_id,
                    order_id=order.id,
                    agent_cycle_id=binding.cycle_id,
                    agent_market_window_id=binding.id,
                    evaluated_at=now,
                    idempotency_key=f"agent-paper-loop-advance-v2:{order.id}:{binding.id}",
                ),
            )
        )
    return window, scopes, tuple(requests)


async def _facts(session: AsyncSession, requests: RequestRefs) -> OrderFacts:
    outcomes: OrderFacts = []
    for lane, request in requests:
        if request.agent_cycle_id is None:
            raise ValueError(_UNKNOWN)
        binding = await session.get(AgentPaperMarketWindowRecord, request.agent_market_window_id)
        if binding is None:
            raise ValueError(_UNKNOWN)
        exists, result = await _coordinator_agent_window_result(
            session,
            account_id=request.account_id,
            cycle_id=request.agent_cycle_id,
            order_id=request.order_id,
            window=binding,
            idempotency_key=request.idempotency_key,
        )
        if not exists:
            result = await _exact_agent_window_fill_output(session, request=request, window=binding)
        outcomes.append(
            {
                "lane": lane,
                "order_id": str(request.order_id),
                "status": result.status if result is not None else "unknown",
                "action": result.action if result is not None else "unknown",
                "code": "OK" if result is not None else _UNKNOWN,
            }
        )
    return outcomes


def _summary(
    pair_slot_id: UUID, window: EconomicCommonWindowRecord, orders: OrderFacts, *, replayed: bool
) -> dict[str, object]:
    actions = {item["action"] for item in orders}
    has_fill = bool(actions & {"filled", "partially_filled"})
    failed = bool(actions & {"unavailable", "not_attempted", "unknown"})
    status = "unknown" if "unknown" in actions else "unavailable" if failed else "completed"
    if not failed and actions != {"filled"}:
        status = "partial"
    states = window.qualification.get("order_states", [])
    rejected_lanes = {
        state.get("lane")
        for state in (states if isinstance(states, list) else [])
        if isinstance(state, dict) and state.get("status") == "risk_rejected"
    }
    if rejected_lanes and not failed:
        status = "partial"
    comparison = (
        "incomparable"
        if has_fill and failed
        else "incomplete"
        if status != "completed"
        else "not_evaluated"
    )
    lanes = []
    for lane in ("incumbent", "challenger"):
        subset = [item for item in orders if item["lane"] == lane]
        lane_actions = {item["action"] for item in subset}
        # _read_window validates either the original NO_ORDER_REQUIRED chain
        # or the exact post-sale plan receipt; a new stage is not a new no-trade cycle.
        previously_trading = isinstance(states, list) and any(
            isinstance(state, dict) and state.get("lane") == lane for state in states
        )
        lane_status = (
            "unknown"
            if "unknown" in lane_actions
            else "unavailable"
            if lane_actions & {"unavailable", "not_attempted"}
            else ("partial" if subset else "risk_rejected")
            if lane in rejected_lanes
            else "filled"
            if not subset and previously_trading
            else "not_required"
            if not subset and window.qualification.get("plan_stage") == "post_sale_v1"
            else "no_trade"
            if not subset
            else "filled"
            if lane_actions == {"filled"}
            else (
                "unknown"
                if "unknown" in lane_actions
                else "unavailable"
                if lane_actions & {"unavailable", "not_attempted"}
                else "partial"
            )
        )
        lanes.append({"lane": lane, "status": lane_status, "order_count": len(subset)})
    return {
        "status": status,
        "pair_slot_id": str(pair_slot_id),
        "window_id": str(window.id),
        "lanes": lanes,
        "orders": orders,
        "replayed": replayed,
        "comparison_status": comparison,
        "economic_status": "not_evaluated",
        "execution_locked": True,
    }


async def _history(
    session: AsyncSession, window: EconomicCommonWindowRecord, requests: RequestRefs
) -> dict[str, object]:
    finished = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == _RESULT, AuditEvent.payload["window_id"].astext == str(window.id)
        )
    )
    facts = await _facts(session, requests)
    if finished is not None:
        if (
            finished.schema_version != _SCHEMA
            or finished.payload.get("window_hash") != window.content_hash
        ):
            raise ValueError(_UNKNOWN)
        saved = finished.payload.get("orders")
        if not isinstance(saved, list) or len(saved) != len(facts):
            raise ValueError(_UNKNOWN)
        for fact, previous in zip(facts, saved, strict=True):
            if not isinstance(previous, dict) or (
                previous.get("lane"),
                previous.get("order_id"),
            ) != (fact["lane"], fact["order_id"]):
                raise ValueError(_UNKNOWN)
            if previous.get("action") in {"unavailable", "not_attempted"}:
                from nexus.ops.paper_once import _SAFE_CODES

                if previous.get("code") not in _SAFE_CODES | {"ECONOMIC_PAIR_EXECUTION_STOPPED"}:
                    raise ValueError(_UNKNOWN)
                if fact["action"] != "unknown":
                    raise ValueError(_UNKNOWN)
                fact.update(status="unavailable", action=previous["action"], code=previous["code"])
            elif fact != previous:
                raise ValueError(_UNKNOWN)
    return _summary(window.pair_slot_id, window, facts, replayed=True)


async def _consume_order(
    session_factory: async_sessionmaker[AsyncSession], request: PaperAdvanceRequest
) -> dict[str, object]:
    async with session_factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(now, datetime):
            raise ValueError(_UNKNOWN)
        return await _advance_agent_market_window(
            session, request=request.model_copy(update={"evaluated_at": now})
        )


async def advance_economic_pair(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    pair_slot_id: UUID,
    window_seq: int,
    expected_database: str,
) -> dict[str, object]:
    """Never retry a committed attempt; a lost receipt is resolved from native facts."""
    from nexus.ops.paper_once import _SAFE_CODES
    from nexus.paper.portfolio_economic_protocol import require_economic_database_target

    async with session_factory.begin() as session:
        await require_economic_database_target(session, expected=expected_database)
        window, scopes, requests = await _orders(session, pair_slot_id, window_seq)
        window_id, window_hash = window.id, window.content_hash
        claimed = await session.scalar(
            select(AuditEvent).where(
                AuditEvent.action == _CLAIM,
                AuditEvent.payload["window_id"].astext == str(window_id),
            )
        )
        if claimed is not None:
            if (
                claimed.schema_version != _SCHEMA
                or claimed.payload.get("window_hash") != window_hash
            ):
                raise ValueError(_UNKNOWN)
            return await _history(session, window, requests)
        await _require_new_collection(session, scopes)
        now = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(now, datetime) or now < window.window_end:
            raise ValueError("ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE")
        await append_audit_event(
            session,
            actor_id="economic-window",
            action=_CLAIM,
            schema_version=_SCHEMA,
            occurred_at=now,
            payload={"window_id": str(window_id), "window_hash": window_hash},
        )

    outcomes: OrderFacts = []
    failed: str | None = None
    for lane, request in requests:
        if failed is not None:
            outcomes.append(
                {
                    "lane": lane,
                    "order_id": str(request.order_id),
                    "status": "unavailable",
                    "action": "not_attempted",
                    "code": "ECONOMIC_PAIR_EXECUTION_STOPPED",
                }
            )
            continue
        try:
            output = await _consume_order(session_factory, request)
        except (PaperOnceError, PaperExecutionError, ValueError) as error:
            code = str(error)
            # A fixed error code does not prove the native transaction failed:
            # a response can be lost after its independent commit.
            async with session_factory.begin() as session:
                window, _, current = await _orders(session, pair_slot_id, window_seq)
                facts = await _facts(session, current)
                committed = next(
                    item for item in facts if item["order_id"] == str(request.order_id)
                )
                if committed["action"] != "unknown" or code not in _SAFE_CODES or "UNKNOWN" in code:
                    return await _history(session, window, current)
            failed = code
            output = {"status": "unavailable", "action": "unavailable"}
        except Exception:
            async with session_factory.begin() as session:
                window, _, current = await _orders(session, pair_slot_id, window_seq)
                return await _history(session, window, current)
        outcomes.append(
            {
                "lane": lane,
                "order_id": str(request.order_id),
                "status": str(output["status"]),
                "action": str(output["action"]),
                "code": failed or "OK",
            }
        )
    async with session_factory.begin() as session:
        window, _, current = await _orders(session, pair_slot_id, window_seq)
        now = await session.scalar(select(func.clock_timestamp()))
        if (
            window.id != window_id
            or window.content_hash != window_hash
            or not isinstance(now, datetime)
        ):
            raise ValueError(_UNKNOWN)
        await append_audit_event(
            session,
            actor_id="economic-window",
            action=_RESULT,
            schema_version=_SCHEMA,
            occurred_at=now,
            payload={"window_id": str(window_id), "window_hash": window_hash, "orders": outcomes},
        )
        verified = await _history(session, window, current)
        verified["replayed"] = False
        return verified


async def read_economic_pair_execution(
    session: AsyncSession, *, pair_slot_id: UUID, window_seq: int | None, expected_database: str
) -> dict[str, object]:
    """Read exact native facts and prior attempt state; cannot create a claim."""
    from nexus.paper.portfolio_economic_protocol import require_economic_database_target

    await require_economic_database_target(session, expected=expected_database)
    if window_seq is None:
        window_seq = await session.scalar(
            select(func.max(EconomicCommonWindowRecord.window_seq)).where(
                EconomicCommonWindowRecord.pair_slot_id == pair_slot_id
            )
        )
        if window_seq is None:
            from nexus.paper.economic_no_trade import read_no_trade_pair

            result = await read_no_trade_pair(session, pair_slot_id=pair_slot_id)
            if result is None:
                raise ValueError("ECONOMIC_WINDOW_REQUIRED")
            return result
    window, _, requests = await _orders(session, pair_slot_id, window_seq, lock=False)
    claim = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == _CLAIM, AuditEvent.payload["window_id"].astext == str(window.id)
        )
    )
    if claim is not None and (
        claim.schema_version != _SCHEMA or claim.payload.get("window_hash") != window.content_hash
    ):
        raise ValueError(_UNKNOWN)
    result = await _history(session, window, requests)
    if claim is None and all(
        item["action"] == "unknown" for item in await _facts(session, requests)
    ):
        result["status"] = "not_started"
    return result

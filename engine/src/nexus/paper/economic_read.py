"""Read-only, server-resolved economic identity; no ordinary-account fallback."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from nexus.api.schemas.economic_pair import EconomicLaneResponse, EconomicPairResponse
from nexus.api.schemas.paper import PaperOrderResponse
from nexus.paper.agent_cycle_models import CycleStatus, canonical_cycle_event_hash
from nexus.paper.economic_cycle import (
    EconomicLaneRef,
    lane_account,
    read_economic_cycle,
    require_economic_lane,
)
from nexus.paper.economic_execution import read_economic_pair_execution
from nexus.paper.portfolio_economic_protocol import require_economic_database_target
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.economic import EconomicLaneCycleRecord, EconomicPairSlotRecord
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, PaperAccountRecord
from nexus.persistence.models.portfolio import Account, Portfolio


async def read_economic_pair(
    session: AsyncSession, *, pair_slot_id: UUID, expected_database: str, now: datetime
) -> EconomicPairResponse | None:
    return await _read_economic_pair(
        session, pair_slot_id=pair_slot_id, expected_database=expected_database, now=now
    )


async def _read_economic_pair(
    session: AsyncSession,
    *,
    pair_slot_id: UUID,
    expected_database: str,
    now: datetime,
    services: dict[str, _EconomicLaneReadService] | None = None,
) -> EconomicPairResponse | None:
    await require_economic_database_target(session, expected=expected_database)
    slot = await session.get(EconomicPairSlotRecord, pair_slot_id)
    if slot is None:
        return None
    authority = await require_economic_lane(session, ref=EconomicLaneRef(pair_slot_id, "incumbent"))
    links = tuple(
        await session.scalars(
            select(EconomicLaneCycleRecord).where(
                EconomicLaneCycleRecord.pair_slot_id == pair_slot_id
            )
        )
    )
    if len(links) not in {0, 2} or (
        links and {link.lane for link in links} != {"incumbent", "challenger"}
    ):
        raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
    refs = []
    for lane in ("incumbent", "challenger"):
        account_id, portfolio_id = lane_account(authority, lane)
        paper = await session.get(PaperAccountRecord, account_id)
        portfolio = await session.get(Portfolio, portfolio_id)
        account = None if paper is None else await session.get(Account, paper.account_id)
        if (
            paper is None
            or portfolio is None
            or account is None
            or paper.portfolio_id != portfolio_id
            or portfolio.account_id != account.id
            or account.provider != "internal"
            or account.status != "ACTIVE"
            or paper.capability != "simulation_write_internal"
            or paper.code != f"economic:{authority.identity.id.hex}:{lane[0]}"
        ):
            raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
        link = next((item for item in links if item.lane == lane), None)
        status = None
        if link is not None:
            bound = await read_economic_cycle(session, cycle_id=link.cycle_id)
            if (
                bound is None
                or bound[0] != EconomicLaneRef(pair_slot_id, lane)
                or (link.account_id, link.portfolio_id) != (account_id, portfolio_id)
            ):
                raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
            events = tuple(
                await session.scalars(
                    select(AgentPaperCycleEventRecord)
                    .where(AgentPaperCycleEventRecord.cycle_id == link.cycle_id)
                    .order_by(AgentPaperCycleEventRecord.sequence)
                )
            )
            if not events:
                raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
            for seq, event in enumerate(events, 1):
                if (
                    event.sequence != seq
                    or event.from_status != (None if seq == 1 else events[seq - 2].to_status)
                    or event.event_hash
                    != canonical_cycle_event_hash(
                        cycle_id=link.cycle_id,
                        sequence=seq,
                        from_status=cast(CycleStatus | None, event.from_status),
                        to_status=cast(CycleStatus, event.to_status),
                        result_code=event.result_code,
                        fact_ids=event.fact_ids,
                        occurred_at=event.occurred_at,
                    )
                ):
                    raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
            service = (
                services[lane]
                if services is not None
                else _EconomicLaneReadService(
                    session,
                    now,
                    ref=EconomicLaneRef(pair_slot_id, lane),
                    expected_database=expected_database,
                )
            )
            if (
                service._session is not session
                or service._request_time != now
                or service._economic_ref != EconomicLaneRef(pair_slot_id, lane)
            ):
                raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
            projected = await service.agent_cycle_detail(link.cycle_id)
            if projected is None:
                raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
            status = projected.cycle.status
        refs.append(
            {
                "lane": lane,
                "cycle_id": None if link is None else link.cycle_id,
                "cycle_status": status,
            }
        )
    gap = None
    try:
        result = await read_economic_pair_execution(
            session, pair_slot_id=pair_slot_id, window_seq=None, expected_database=expected_database
        )
        execution = {
            key: result[key] for key in ("status", "window_id", "orders", "comparison_status")
        }
        raw_lanes = result["lanes"]
        if not isinstance(raw_lanes, list) or not all(isinstance(item, dict) for item in raw_lanes):
            raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
        execution["lanes"] = [
            {key: item[key] for key in ("lane", "status", "order_count")} for item in raw_lanes
        ]
    except ValueError as error:
        if str(error) != "ECONOMIC_WINDOW_REQUIRED":
            raise
        execution = None
        gap = "ECONOMIC_WINDOW_REQUIRED"
    return EconomicPairResponse.model_validate(
        {
            "planned_slots": 24,
            "execution_locked": True,
            "economic_status": "not_evaluated",
            "pair_slot_id": slot.id,
            "evaluation_id": slot.evaluation_id,
            "ordinal": slot.ordinal,
            "decision_session": slot.session,
            "decision_at": slot.decision_at,
            "lanes": refs,
            "execution": execution,
            "gap_code": gap,
        }
    )


class _EconomicLaneReadService(PaperReadService):
    """Server-only scope, resolved from the registered pair inside this read snapshot."""

    def __init__(
        self,
        session: AsyncSession,
        request_time: datetime,
        *,
        ref: EconomicLaneRef,
        expected_database: str,
        snapshot_transaction: AsyncSessionTransaction | None = None,
    ) -> None:
        super().__init__(session, request_time)
        self._economic_ref = ref
        self._expected_database = expected_database
        self._snapshot_transaction = snapshot_transaction
        self._snapshot_orders: dict[UUID, tuple[PaperOrderResponse, ...]] = {}

    async def _orders_for_cycle(self, cycle_id: UUID) -> tuple[PaperOrderResponse, ...]:
        transaction = self._session.get_transaction()
        reusable = (
            self._snapshot_transaction is not None and transaction is self._snapshot_transaction
        )
        if reusable and cycle_id in self._snapshot_orders:
            return self._snapshot_orders[cycle_id]
        orders = await super()._orders_for_cycle(cycle_id)
        if reusable and self._session.get_transaction() is transaction:
            self._snapshot_orders[cycle_id] = orders
        return orders

    async def account(self) -> tuple[PaperAccountRecord, Portfolio]:
        await require_economic_database_target(self._session, expected=self._expected_database)
        authority = await require_economic_lane(self._session, ref=self._economic_ref)
        account_id, portfolio_id = lane_account(authority, self._economic_ref.lane)
        paper = await self._session.get(PaperAccountRecord, account_id)
        portfolio = await self._session.get(Portfolio, portfolio_id)
        account = None if paper is None else await self._session.get(Account, paper.account_id)
        if (
            paper is None
            or portfolio is None
            or account is None
            or paper.portfolio_id != portfolio_id
            or portfolio.account_id != account.id
            or account.provider != "internal"
            or account.status != "ACTIVE"
            or paper.capability != "simulation_write_internal"
            or paper.code != f"economic:{authority.identity.id.hex}:{self._economic_ref.lane[0]}"
        ):
            raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
        return paper, portfolio


async def read_economic_lane(
    session: AsyncSession,
    *,
    pair_slot_id: UUID,
    lane: Literal["incumbent", "challenger"],
    expected_database: str,
    now: datetime,
) -> EconomicLaneResponse | None:
    await require_economic_database_target(session, expected=expected_database)
    isolation = (
        await session.execute(
            text(
                "SELECT current_setting('transaction_isolation'), "
                "current_setting('transaction_read_only')"
            )
        )
    ).one()
    snapshot = session.get_transaction() if tuple(isolation) == ("repeatable read", "on") else None
    sides: tuple[Literal["incumbent", "challenger"], ...] = ("incumbent", "challenger")
    services: dict[str, _EconomicLaneReadService] = {
        side: _EconomicLaneReadService(
            session,
            now,
            ref=EconomicLaneRef(pair_slot_id, side),
            expected_database=expected_database,
            snapshot_transaction=snapshot,
        )
        for side in sides
    }
    pair = await _read_economic_pair(
        session,
        pair_slot_id=pair_slot_id,
        expected_database=expected_database,
        now=now,
        services=services,
    )
    if pair is None:
        return None
    service = services[lane]
    cycle_ref = next(ref for ref in pair.lanes if ref.lane == lane)
    detail = (
        None
        if cycle_ref.cycle_id is None
        else await service.management_cycle_detail(cycle_ref.cycle_id)
    )
    if cycle_ref.cycle_id is not None and (
        detail is None or detail.cycle.status != cycle_ref.cycle_status
    ):
        raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
    return EconomicLaneResponse(
        pair=pair,
        lane=lane,
        detail=detail,
        actual=await service.current_actual(),
        portfolio=await service.portfolio(),
    )

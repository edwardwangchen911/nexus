from __future__ import annotations

import asyncio
from collections import deque
from uuid import UUID

from nexus.providers.broker.base import BrokerOrderFact, BrokerPage, BrokerPosition, CashBalance
from nexus.reconciliation.facts import BrokerReadSet

from helpers.broker_facts import NOW, cash


class ScriptedBrokerProvider:
    """Synthetic provider that exposes every logical read in call order."""

    def __init__(
        self,
        attempts: list[tuple[BrokerReadSet, BrokerReadSet] | Exception],
        *,
        page_size: int = 100,
        call_delay: float = 0,
        page_quality: str = "PASS",
    ) -> None:
        self._attempts = deque(attempts)
        self._page_size = page_size
        self._call_delay = call_delay
        self._page_quality = page_quality
        self.calls: list[tuple[str, UUID, UUID, str | None, int | None]] = []
        self._active: tuple[BrokerReadSet, BrokerReadSet] | None = None
        self._step = 0

    async def read_cash(self, account_id: UUID, *, sync_run_id: UUID) -> BrokerPage[CashBalance]:
        await self._before("cash", account_id, sync_run_id, None, None)
        if self._step != 5:
            scripted = self._attempts.popleft()
            if isinstance(scripted, Exception):
                raise scripted
            self._active = scripted
            self._step = 1
            return self._cash_page(scripted[0].cash)
        assert self._active is not None and self._step == 5
        self._step = 0
        return self._cash_page(self._active[1].cash)

    async def read_positions_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerPosition]:
        await self._before("positions", account_id, sync_run_id, cursor, limit)
        assert self._active is not None and self._step in {1, 3}
        read = self._active[0 if self._step == 1 else 1]
        return self._page(read.positions, cursor, after_step=2 if self._step == 1 else 4)

    async def read_open_orders_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerOrderFact]:
        await self._before("orders", account_id, sync_run_id, cursor, limit)
        assert self._active is not None and self._step in {2, 4}
        read = self._active[0 if self._step == 2 else 1]
        return self._page(read.open_orders, cursor, after_step=3 if self._step == 2 else 5)

    async def _before(
        self,
        operation: str,
        account_id: UUID,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int | None,
    ) -> None:
        self.calls.append((operation, account_id, sync_run_id, cursor, limit))
        if self._call_delay:
            await asyncio.sleep(self._call_delay)

    def _page(
        self, items: tuple[object, ...], cursor: str | None, *, after_step: int
    ) -> BrokerPage:
        start = int(cursor or "0")
        page_items = items[start : start + self._page_size]
        next_offset = start + len(page_items)
        next_cursor = str(next_offset) if next_offset < len(items) else None
        if next_cursor is None:
            self._step = after_step
        return BrokerPage(
            items=page_items,
            has_more=next_cursor is not None,
            next_cursor=next_cursor,
            source_request_id=f"synthetic-{after_step}-{start}",
            as_of=NOW,
            quality=self._page_quality,
        )

    def _cash_page(self, items: tuple[CashBalance, ...]) -> BrokerPage[CashBalance]:
        return BrokerPage(
            items=items,
            has_more=False,
            next_cursor=None,
            source_request_id="synthetic-cash",
            as_of=NOW,
            quality=self._page_quality,
        )


class InterruptedPaginationProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def read_cash(self, account_id: UUID, *, sync_run_id: UUID) -> BrokerPage[CashBalance]:
        del account_id, sync_run_id
        self.calls.append("cash")
        return BrokerPage(
            items=(cash("1000"),),
            has_more=False,
            next_cursor=None,
            source_request_id="synthetic-cash",
            as_of=NOW,
        )

    async def read_positions_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerPosition]:
        del account_id, sync_run_id, cursor, limit
        self.calls.append("positions")
        return BrokerPage(
            items=(),
            has_more=True,
            next_cursor="1",
            source_request_id="synthetic-interrupted",
            as_of=NOW,
        )


class AfterCashTimeoutProvider(InterruptedPaginationProvider):
    async def read_positions_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerPosition]:
        del account_id, sync_run_id, cursor, limit
        self.calls.append("positions")
        await asyncio.sleep(0.02)
        raise AssertionError("the attempt deadline should cancel this read")

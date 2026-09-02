"""Fail-closed mapping from the isolated Fosun gateway to broker facts."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Protocol
from uuid import UUID

from pydantic import ValidationError

from nexus.domain.time import require_utc
from nexus.providers.broker.base import (
    BrokerCashFlow,
    BrokerOrderFact,
    BrokerPage,
    BrokerPageQuality,
    BrokerPosition,
    BrokerProviderError,
    CashBalance,
    DateRange,
)
from nexus.providers.broker.fosun_contracts import CashFlowItem, CashItem, OrderItem, PositionItem
from nexus.providers.broker.gateway_client import GatewayClientError, GatewayResponsePage


class GatewayReader(Protocol):
    async def read_cash(self, external_account_id: str) -> GatewayResponsePage: ...

    async def read_holdings(
        self, external_account_id: str, *, cursor: str | None, limit: int
    ) -> GatewayResponsePage: ...

    async def read_orders(
        self, external_account_id: str, *, cursor: str | None, limit: int
    ) -> GatewayResponsePage: ...

    async def read_cash_flows(
        self,
        external_account_id: str,
        *,
        date_from: date,
        date_to: date,
        cursor: str | None,
        limit: int,
    ) -> GatewayResponsePage: ...


_OPEN_ORDER_STATUSES: dict[str, Literal["pending", "submitted", "partially_filled"]] = {
    "10": "pending",
    "20": "pending",
    "21": "pending",
    "22": "pending",
    "40": "submitted",
    "60": "partially_filled",
}
_TERMINAL_ORDER_STATUSES = frozenset({"50", "70", "80", "90", "100"})


class FosunBrokerProvider:
    """Map reviewed gateway contracts without inventing missing provider facts."""

    def __init__(
        self,
        *,
        account_id: UUID,
        external_account_id: str,
        client: GatewayReader,
        instrument_ids: dict[str, UUID],
        cash_flow_kinds: dict[str, str],
    ) -> None:
        if not external_account_id:
            raise ValueError("external account ID is required")
        self._account_id = account_id
        self._external_account_id = external_account_id
        self._client = client
        self._instrument_ids = dict(instrument_ids)
        self._cash_flow_kinds = dict(cash_flow_kinds)

    async def read_cash(self, account_id: UUID, *, sync_run_id: UUID) -> BrokerPage[CashBalance]:
        del sync_run_id
        self._require_account(account_id)
        try:
            response = await self._client.read_cash(self._external_account_id)
            raw_items = tuple(CashItem.model_validate(item) for item in response.items)
            if any(item.external_account_id != self._external_account_id for item in raw_items):
                raise ValueError("account scope mismatch")
            items = tuple(
                CashBalance(
                    currency=item.currency,
                    cash_balance=item.cash_balance,
                    available_cash=item.available_cash,
                    provider_as_of=_optional_utc(item.provider_as_of),
                )
                for item in raw_items
            )
            quality = _quality(response, all(item.provider_as_of is not None for item in items))
            return _page(response, items, quality)
        except _MAPPING_ERRORS:
            raise BrokerProviderError("gateway facts rejected") from None

    async def read_positions_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerPosition]:
        del sync_run_id
        self._require_account(account_id)
        try:
            response = await self._client.read_holdings(
                self._external_account_id, cursor=cursor, limit=limit
            )
            raw_items = tuple(PositionItem.model_validate(item) for item in response.items)
            if any(item.external_account_id != self._external_account_id for item in raw_items):
                raise ValueError("account scope mismatch")
            items = tuple(
                BrokerPosition(
                    instrument_id=self._instrument(item.instrument_code),
                    currency=item.currency,
                    quantity=item.quantity,
                    available_quantity=item.available_quantity,
                    frozen_quantity=item.frozen_quantity,
                    avg_cost=item.average_cost,
                    diluted_cost=item.diluted_cost,
                    current_price=item.current_price,
                    provider_as_of=_optional_utc(item.provider_as_of),
                )
                for item in raw_items
            )
            complete = all(
                item.current_price is not None and item.provider_as_of is not None for item in items
            )
            return _page(response, items, _quality(response, complete))
        except _MAPPING_ERRORS:
            raise BrokerProviderError("gateway facts rejected") from None

    async def read_open_orders_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerOrderFact]:
        del sync_run_id
        self._require_account(account_id)
        try:
            response = await self._client.read_orders(
                self._external_account_id, cursor=cursor, limit=limit
            )
            raw_items = tuple(OrderItem.model_validate(item) for item in response.items)
            items: list[BrokerOrderFact] = []
            for item in raw_items:
                if item.status in _TERMINAL_ORDER_STATUSES:
                    continue
                status = _OPEN_ORDER_STATUSES.get(item.status)
                if status is None:
                    raise ValueError("unreviewed order status")
                items.append(
                    BrokerOrderFact(
                        provider_order_id=item.external_order_id,
                        instrument_id=self._instrument(item.instrument_code),
                        side=item.side,
                        quantity=item.quantity,
                        filled_quantity=item.filled_quantity,
                        price=item.price,
                        status=status,
                        updated_at=_optional_utc(item.updated_at),
                    )
                )
            complete = all(item.updated_at is not None for item in items)
            return _page(response, tuple(items), _quality(response, complete))
        except _MAPPING_ERRORS:
            raise BrokerProviderError("gateway facts rejected") from None

    async def read_cash_flows(
        self,
        account_id: UUID,
        window: DateRange,
        cursor: str | None,
        *,
        sync_run_id: UUID,
        limit: int,
    ) -> BrokerPage[BrokerCashFlow]:
        del sync_run_id
        self._require_account(account_id)
        try:
            response = await self._client.read_cash_flows(
                self._external_account_id,
                date_from=window.start,
                date_to=window.end,
                cursor=cursor,
                limit=limit,
            )
            raw_items = tuple(CashFlowItem.model_validate(item) for item in response.items)
            items = tuple(
                BrokerCashFlow(
                    provider_cash_flow_id=item.external_cash_flow_id,
                    currency=item.currency,
                    amount=item.amount,
                    occurred_at=require_utc(item.occurred_at),
                    kind=self._cash_flow_kind(item.flow_type),
                )
                for item in raw_items
            )
            return _page(response, items, response.quality)
        except _MAPPING_ERRORS:
            raise BrokerProviderError("gateway facts rejected") from None

    def _require_account(self, account_id: UUID) -> None:
        if account_id != self._account_id:
            raise BrokerProviderError("broker account scope rejected")

    def _instrument(self, code: str) -> UUID:
        try:
            return self._instrument_ids[code]
        except KeyError:
            raise ValueError("unmapped instrument") from None

    def _cash_flow_kind(self, value: str) -> str:
        try:
            return self._cash_flow_kinds[value]
        except KeyError:
            raise ValueError("unmapped cash flow kind") from None


_MAPPING_ERRORS = (GatewayClientError, ValidationError, TypeError, ValueError)


def _optional_utc(value: datetime | None) -> datetime | None:
    return None if value is None else require_utc(value)


def _quality(response: GatewayResponsePage, complete: bool) -> BrokerPageQuality:
    return "PASS" if response.quality == "PASS" and complete else "PARTIAL"


def _page[ItemT](
    response: GatewayResponsePage,
    items: tuple[ItemT, ...],
    quality: BrokerPageQuality,
) -> BrokerPage[ItemT]:
    return BrokerPage(
        items=items,
        has_more=response.has_more,
        next_cursor=response.next_cursor,
        source_request_id=response.source_request_id,
        as_of=response.as_of,
        quality=quality,
    )

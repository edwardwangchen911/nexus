"""Synthetic Fosun account-state responses with no provider update timestamps."""

from datetime import UTC, datetime, timedelta

from nexus.providers.broker.gateway_contracts import GatewayResponsePage

NOW = datetime(2026, 9, 2, tzinfo=UTC)


class ObservedGateway:
    def __init__(
        self, *, mismatch: bool = False, missing_price: bool = False, order_code: str = "usAAPL"
    ) -> None:
        self.calls = 0
        self.mismatch = mismatch
        self.missing_price = missing_price
        self.order_code = order_code

    def page(self, item: dict[str, object]) -> GatewayResponsePage:
        self.calls += 1
        return GatewayResponsePage(
            items=(item,),
            source_request_id=f"synthetic-{self.calls}",
            has_more=False,
            next_cursor=None,
            as_of=NOW + timedelta(seconds=self.calls),
            quality="PARTIAL",
        )

    async def read_cash(self, _account: str) -> GatewayResponsePage:
        return self.page(
            {
                "kind": "cash",
                "external_account_id": "synthetic-account",
                "currency": "USD",
                "cash_balance": "1000",
                "available_cash": "1000",
                "provider_as_of": None,
            }
        )

    async def read_holdings(self, _account: str, **_kwargs: object) -> GatewayResponsePage:
        return self.page(
            {
                "kind": "position",
                "external_account_id": "synthetic-account",
                "instrument_code": "usAAPL",
                "currency": "USD",
                "quantity": "2" if self.mismatch and self.calls > 2 else "1",
                "available_quantity": "1",
                "frozen_quantity": "0",
                "average_cost": "90",
                "diluted_cost": None,
                "current_price": None if self.missing_price else "100",
                "provider_as_of": None,
            }
        )

    async def read_orders(self, _account: str, **_kwargs: object) -> GatewayResponsePage:
        return self.page(
            {
                "kind": "order",
                "external_order_id": "synthetic-order",
                "instrument_code": self.order_code,
                "side": "buy",
                "quantity": "1",
                "filled_quantity": "0",
                "price": "95",
                "status": "22",
                "submitted_at": (NOW - timedelta(days=10)).isoformat(),
                "updated_at": None,
            }
        )

    async def read_cash_flows(self, _account: str, **_kwargs: object) -> GatewayResponsePage:
        assert self.calls == 6
        page = self.page(
            {
                "kind": "cash_flow",
                "external_cash_flow_id": "synthetic-flow",
                "currency": "USD",
                "amount": "1000",
                "occurred_at": None,
                "provider_business_date": NOW.date().isoformat(),
                "flow_type": "100",
                "direction": 1,
            }
        )
        return page.model_copy(update={"quality": "PASS"})

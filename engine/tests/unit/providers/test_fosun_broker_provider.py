from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.providers.broker.base import BrokerProviderError, DateRange
from nexus.providers.broker.fosun import FosunBrokerProvider
from nexus.providers.broker.gateway_client import GatewayResponsePage

ACCOUNT_ID = UUID("0198d400-4000-7000-8000-000000000001")
INSTRUMENT_ID = UUID("0198d400-4000-7000-8000-000000000002")
SYNC_RUN_ID = UUID("0198d400-4000-7000-8000-000000000003")
NOW = datetime(2026, 8, 23, 4, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "direction,amount,want", [(1, "12.5", "12.5"), (2, "12.5", "-12.5"), (2, "-12.5", "-12.5")]
)
async def test_provider_business_date_direction_and_opaque_type_are_preserved(
    direction, amount, want
):
    service = provider(
        {
            "cash_flows": page(
                {
                    "kind": "cash_flow",
                    "external_cash_flow_id": "synthetic-flow",
                    "currency": "USD",
                    "amount": amount,
                    "occurred_at": None,
                    "provider_business_date": "2026-08-21",
                    "direction": direction,
                    "flow_type": "703",
                    "business_type": 987654,
                    "product_code": "usSYNTH.A",
                }
            )
        }
    )
    result = await service.read_cash_flows(
        ACCOUNT_ID,
        DateRange(start=date(2026, 8, 20), end=date(2026, 8, 23)),
        None,
        sync_run_id=SYNC_RUN_ID,
        limit=100,
    )
    assert result.quality == "PASS"
    assert result.items[0].amount == Decimal(want)
    assert result.items[0].kind == "fosun:703"
    assert result.items[0].business_type == 987654
    assert result.items[0].product_code == "usSYNTH.A"
    assert result.items[0].occurred_at is None
    assert result.items[0].provider_business_date == date(2026, 8, 21)


def page(*items: dict[str, object], quality: str = "PASS") -> GatewayResponsePage:
    return GatewayResponsePage(
        items=items,
        source_request_id="synthetic-gateway-request",
        has_more=False,
        next_cursor=None,
        as_of=NOW,
        quality=quality,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("direction,amount", [(1, "-1"), (0, "1"), (True, "1"), (2, "NaN")])
async def test_invalid_direction_or_amount_cannot_become_decision_cash_flow(direction, amount):
    service = provider(
        {
            "cash_flows": page(
                {
                    "kind": "cash_flow",
                    "external_cash_flow_id": "synthetic-flow",
                    "currency": "USD",
                    "amount": amount,
                    "occurred_at": None,
                    "provider_business_date": "2026-08-21",
                    "direction": direction,
                    "flow_type": "100",
                }
            )
        }
    )
    with pytest.raises(BrokerProviderError):
        await service.read_cash_flows(
            ACCOUNT_ID,
            DateRange(start=date(2026, 8, 20), end=date(2026, 8, 23)),
            None,
            sync_run_id=SYNC_RUN_ID,
            limit=100,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "context",
    [
        {"business_type": True},
        {"business_type": "123"},
        {"business_type": 1.0},
        {"business_type": 2**63},
        {"product_code": 123},
        {"product_code": "A" * 129},
        {"product_code": "synthetic private memo"},
        {"remark": "synthetic private memo"},
    ],
)
async def test_engine_rejects_invalid_cash_flow_context_at_gateway_boundary(context):
    service = provider(
        {
            "cash_flows": page(
                {
                    "kind": "cash_flow",
                    "external_cash_flow_id": "synthetic-flow",
                    "currency": "USD",
                    "amount": "12.5",
                    "occurred_at": None,
                    "provider_business_date": "2026-08-21",
                    "direction": 2,
                    "flow_type": "703",
                    **context,
                }
            )
        }
    )
    with pytest.raises(BrokerProviderError) as error:
        await service.read_cash_flows(
            ACCOUNT_ID,
            DateRange(start=date(2026, 8, 20), end=date(2026, 8, 23)),
            None,
            sync_run_id=SYNC_RUN_ID,
            limit=100,
        )
    assert str(error.value) == "gateway facts rejected"


class FakeGatewayClient:
    def __init__(self, responses: dict[str, GatewayResponsePage]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, object]] = []

    async def read_cash(self, external_account_id: str) -> GatewayResponsePage:
        self.calls.append(("cash", external_account_id))
        return self.responses["cash"]

    async def read_holdings(
        self, external_account_id: str, *, cursor: str | None, limit: int
    ) -> GatewayResponsePage:
        self.calls.append(("holdings", (external_account_id, cursor, limit)))
        return self.responses["holdings"]

    async def read_orders(
        self, external_account_id: str, *, cursor: str | None, limit: int
    ) -> GatewayResponsePage:
        self.calls.append(("orders", (external_account_id, cursor, limit)))
        return self.responses["orders"]

    async def read_cash_flows(
        self, external_account_id: str, **kwargs: object
    ) -> GatewayResponsePage:
        self.calls.append(("cash_flows", (external_account_id, kwargs)))
        return self.responses["cash_flows"]


def provider(responses: dict[str, GatewayResponsePage]) -> FosunBrokerProvider:
    return FosunBrokerProvider(
        account_id=ACCOUNT_ID,
        external_account_id="synthetic-account-001",
        client=FakeGatewayClient(responses),
        instrument_ids={"usAAPL": INSTRUMENT_ID},
        cash_flow_kinds={"1": "deposit"},
    )


@pytest.mark.asyncio
async def test_cash_and_positions_preserve_nullable_provider_fields_and_force_partial() -> None:
    service = provider(
        {
            "cash": page(
                {
                    "kind": "cash",
                    "external_account_id": "synthetic-account-001",
                    "currency": "USD",
                    "cash_balance": "1000",
                    "available_cash": "900",
                    "provider_as_of": None,
                }
            ),
            "holdings": page(
                {
                    "kind": "position",
                    "external_account_id": "synthetic-account-001",
                    "instrument_code": "usAAPL",
                    "currency": "USD",
                    "quantity": "10",
                    "available_quantity": "9",
                    "frozen_quantity": "1",
                    "average_cost": "100",
                    "diluted_cost": "99",
                    "current_price": "120",
                    "provider_as_of": None,
                }
            ),
        }
    )

    cash = await service.read_cash(ACCOUNT_ID, sync_run_id=SYNC_RUN_ID)
    positions = await service.read_positions_page(
        ACCOUNT_ID,
        sync_run_id=SYNC_RUN_ID,
        cursor=None,
        limit=100,
    )

    assert cash.quality == "PARTIAL"
    assert cash.items[0].provider_as_of is None
    assert positions.quality == "PARTIAL"
    assert positions.items[0].avg_cost != positions.items[0].diluted_cost
    assert positions.items[0].current_price == 120
    assert positions.items[0].provider_as_of is None


@pytest.mark.asyncio
async def test_unknown_instrument_status_or_cash_flow_kind_fails_closed() -> None:
    unknown_instrument = provider(
        {
            "holdings": page(
                {
                    "kind": "position",
                    "external_account_id": "synthetic-account-001",
                    "instrument_code": "usUNKNOWN",
                    "currency": "USD",
                    "quantity": "1",
                    "available_quantity": "1",
                    "frozen_quantity": "0",
                    "average_cost": None,
                    "diluted_cost": None,
                    "current_price": None,
                    "provider_as_of": None,
                }
            )
        }
    )
    with pytest.raises(BrokerProviderError, match="gateway facts rejected"):
        await unknown_instrument.read_positions_page(
            ACCOUNT_ID,
            sync_run_id=SYNC_RUN_ID,
            cursor=None,
            limit=100,
        )

    unknown_order = provider(
        {
            "orders": page(
                {
                    "kind": "order",
                    "external_order_id": "synthetic-order-1",
                    "instrument_code": "usAAPL",
                    "side": "buy",
                    "quantity": "1",
                    "filled_quantity": "0",
                    "price": "100",
                    "status": "999",
                    "submitted_at": NOW.isoformat(),
                    "updated_at": None,
                }
            )
        }
    )
    with pytest.raises(BrokerProviderError, match="gateway facts rejected"):
        await unknown_order.read_open_orders_page(
            ACCOUNT_ID,
            sync_run_id=SYNC_RUN_ID,
            cursor=None,
            limit=100,
        )

    unknown_flow = provider(
        {
            "cash_flows": page(
                {
                    "kind": "cash_flow",
                    "external_cash_flow_id": "synthetic-flow-1",
                    "currency": "USD",
                    "amount": "10",
                    "occurred_at": NOW.isoformat(),
                    "flow_type": "999",
                }
            )
        }
    )
    with pytest.raises(BrokerProviderError, match="gateway facts rejected"):
        await unknown_flow.read_cash_flows(
            ACCOUNT_ID,
            DateRange(start=NOW.date(), end=NOW.date()),
            None,
            sync_run_id=SYNC_RUN_ID,
            limit=100,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "normalized_status"),
    (("20", "pending"), ("22", "pending"), ("40", "submitted"), ("60", "partially_filled")),
)
async def test_only_reviewed_open_order_statuses_survive_and_missing_update_is_partial(
    status: str, normalized_status: str
) -> None:
    service = provider(
        {
            "orders": page(
                {
                    "kind": "order",
                    "external_order_id": "synthetic-open",
                    "instrument_code": "usAAPL",
                    "side": "buy",
                    "quantity": "2",
                    "filled_quantity": "1",
                    "price": "100",
                    "status": status,
                    "submitted_at": NOW.isoformat(),
                    "updated_at": None,
                },
                {
                    "kind": "order",
                    "external_order_id": "synthetic-filled",
                    "instrument_code": "usAAPL",
                    "side": "sell",
                    "quantity": "1",
                    "filled_quantity": "1",
                    "price": "110",
                    "status": "50",
                    "submitted_at": NOW.isoformat(),
                    "updated_at": None,
                },
            )
        }
    )

    result = await service.read_open_orders_page(
        ACCOUNT_ID,
        sync_run_id=SYNC_RUN_ID,
        cursor=None,
        limit=100,
    )

    assert [item.provider_order_id for item in result.items] == ["synthetic-open"]
    assert result.items[0].status == normalized_status
    assert result.items[0].updated_at is None
    assert result.quality == "PARTIAL"

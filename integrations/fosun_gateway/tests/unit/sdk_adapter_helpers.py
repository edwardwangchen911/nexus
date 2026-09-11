"""Shared SDK adapter fixtures and deterministic test doubles."""

from types import SimpleNamespace

import pytest


class RecordingMethod:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def __call__(self, *args: object, **kwargs: object) -> object:
        self.calls.append((args, kwargs))
        return self.response


def _raw(data: object, request_id: str) -> dict[str, object]:
    return {
        "code": 0,
        "requestId": request_id,
        "data": data,
    }


@pytest.fixture
def client() -> SimpleNamespace:
    return SimpleNamespace(
        account=SimpleNamespace(
            list_accounts=RecordingMethod(
                _raw(
                    {
                        "clientId": 100000,
                        "clientStatus": 1,
                        "clientType": 1,
                        "subAccounts": [
                            {
                                "subAccountId": "synthetic-account-001",
                                "status": 1,
                                "subAccountType": 0,
                            }
                        ],
                    },
                    "synthetic-request-accounts",
                )
            )
        ),
        portfolio=SimpleNamespace(
            get_assets_summary=RecordingMethod(
                _raw(
                    {
                        "breakdown": [
                            {
                                "currency": "USD",
                                "ledgerBalance": "10000.00",
                                "cashPurchasingPower": "9000.00",
                            }
                        ],
                        "summary": {},
                    },
                    "synthetic-request-assets",
                )
            ),
            get_holdings=RecordingMethod(
                _raw(
                    {
                        "count": 1,
                        "list": [
                            {
                                "marketCode": "us",
                                "stockCode": "AAPL",
                                "name": "Apple Inc.",
                                "symbol": "usAAPL",
                                "productType": 6,
                                "currency": "USD",
                                "quantity": "10",
                                "quantityAvail": "10",
                                "qtyOnHold": "0",
                                "avgCost": "100.00",
                                "dilutedCost": "98.50",
                                "price": "123.45",
                            }
                        ],
                        "start": 20,
                        "total": 21,
                    },
                    "synthetic-request-holdings",
                )
            ),
        ),
        trade=SimpleNamespace(
            list_orders=RecordingMethod(
                _raw(
                    {
                        "count": 1,
                        "list": [
                            {
                                "orderId": "synthetic-order-001",
                                "marketCode": "us",
                                "stockCode": "AAPL",
                                "direction": 1,
                                "quantity": "1",
                                "filledQuantity": "0",
                                "price": "10000",
                                "power": 2,
                                "orderStatus": 40,
                                "submittedTimestamp": 1787371200,
                            }
                        ],
                        "start": 40,
                        "total": 41,
                    },
                    "synthetic-request-orders",
                )
            ),
            get_cash_flows=RecordingMethod(
                _raw(
                    {
                        "count": 1,
                        "list": [
                            {
                                "flowId": "synthetic-cash-flow-001",
                                "currency": "USD",
                                "amount": "100.00",
                                "createdAt": "2026-08-22T04:00:00Z",
                                "flowType": 1,
                            }
                        ],
                        "start": 0,
                        "total": 1,
                    },
                    "synthetic-request-flows",
                )
            ),
        ),
        market=SimpleNamespace(
            quote=RecordingMethod(
                _raw(
                    [
                        {
                            "mkt": "us",
                            "rawSymbol": "AAPL",
                            "price": 12345,
                            "power": 2,
                            "qtDate": 20260822,
                            "qtTime": 120000000,
                        }
                    ],
                    "synthetic-request-quotes",
                )
            ),
            kline=RecordingMethod(
                _raw(
                    {
                        "data": [
                            {
                                "time": 20260821,
                                "open": 10000,
                                "high": 11000,
                                "low": 9000,
                                "close": 10500,
                                "vol": 1000000,
                            }
                        ],
                        "power": 2,
                        "total": 1,
                        "delay": True,
                    },
                    "synthetic-request-bars",
                )
            ),
        ),
    )


class NoopRateLimiter:
    async def acquire(self) -> None:
        return None

    async def try_acquire(self) -> float | None:
        return None

    def try_acquire_at_call_edge(self) -> float | None:
        return None


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.now += delay

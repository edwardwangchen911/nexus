import asyncio
import logging
import threading
import time
from types import SimpleNamespace

import nexus_fosun_gateway.adapter.sdk as sdk_module
import pytest
from nexus_fosun_gateway.adapter.sdk import (
    DualTokenBucket,
    SdkFosunTransport,
)
from nexus_fosun_gateway.domain import GatewayError


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
                                "currency": "USD",
                                "quantity": "10",
                                "quantityAvail": "10",
                                "qtyOnHold": "0",
                                "avgCost": "100.00",
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
                    {
                        "usAAPL": {
                            "price": 12345,
                            "power": 2,
                            "qtDate": 20260822,
                            "qtTime": 120000000,
                        }
                    },
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


@pytest.mark.asyncio
async def test_sdk_adapter_uses_only_frozen_v1_2_read_method_surface(
    client: SimpleNamespace,
) -> None:
    """Changing an SDK method or its typed arguments breaks the reviewed seam."""
    adapter = SdkFosunTransport(client, rate_limiter=NoopRateLimiter())

    accounts = await adapter.execute("fosun_sync_accounts", {})
    portfolio = await adapter.execute(
        "fosun_sync_portfolio",
        {"account_id": "synthetic-account-001", "cursor": "20", "limit": 20},
    )
    orders = await adapter.execute(
        "fosun_list_orders",
        {"account_id": "synthetic-account-001", "cursor": "40", "limit": 20},
    )
    flows = await adapter.execute(
        "fosun_list_cash_flows",
        {
            "account_id": "synthetic-account-001",
            "date_from": "2026-08-01",
            "date_to": "2026-08-22",
            "cursor": None,
            "limit": 20,
        },
    )
    quotes = await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    bars = await adapter.execute(
        "fosun_get_bars",
        {
            "code": "usAAPL",
            "ktype": "day",
            "start_time": "2026-08-01T00:00:00Z",
            "end_time": "2026-08-22T00:00:00Z",
            "limit": 100,
        },
    )

    assert accounts.items[0].external_account_id == "synthetic-account-001"
    assert len(portfolio.items) == 2
    assert orders.items[0].external_order_id == "synthetic-order-001"
    assert flows.items[0].external_cash_flow_id == "synthetic-cash-flow-001"
    assert str(quotes.items[0].price) == "123.45"
    assert str(bars.items[0].close) == "105"
    assert client.account.list_accounts.calls == [((), {})]
    assert client.portfolio.get_assets_summary.calls == [
        ((), {"sub_account_id": "synthetic-account-001"})
    ]
    assert client.portfolio.get_holdings.calls == [
        (
            (),
            {
                "sub_account_id": "synthetic-account-001",
                "start": 20,
                "count": 20,
            },
        )
    ]
    assert client.trade.list_orders.calls == [
        (
            (),
            {
                "sub_account_id": "synthetic-account-001",
                "start": 40,
                "count": 20,
            },
        )
    ]
    assert client.trade.get_cash_flows.calls == [
        (
            (),
            {
                "sub_account_id": "synthetic-account-001",
                "trade_date_from": "2026-08-01",
                "trade_date_to": "2026-08-22",
            },
        )
    ]
    assert client.market.quote.calls == [((), {"codes": ["usAAPL"]})]
    assert client.market.kline.calls == [
        (
            (),
            {
                "code": "usAAPL",
                "ktype": "day",
                "start_time": 1785542400000,
                "end_time": 1787356800000,
                "num": 100,
            },
        )
    ]


@pytest.mark.asyncio
async def test_sdk_adapter_serializes_calls_with_concurrency_one() -> None:
    """Removing the semaphore could overlap provider SDK calls."""
    state = {"active": 0, "maximum": 0}
    lock = threading.Lock()

    def list_accounts() -> object:
        with lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        time.sleep(0.03)
        with lock:
            state["active"] -= 1
        return _raw({"subAccounts": []}, "synthetic-request-concurrency")

    fake_client = SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts))
    adapter = SdkFosunTransport(fake_client, rate_limiter=NoopRateLimiter())

    await asyncio.gather(
        adapter.execute("fosun_sync_accounts", {}),
        adapter.execute("fosun_sync_accounts", {}),
    )

    assert state["maximum"] == 1


@pytest.mark.asyncio
async def test_provider_timeout_never_retries_while_blocking_call_is_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timed-out worker must not overlap a retry or a later provider call."""
    monkeypatch.setattr(sdk_module, "_DEADLINE_SECONDS", 0.03)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    state = {"active": 0, "maximum": 0, "calls": 0}
    lock = threading.Lock()

    def list_accounts() -> object:
        with lock:
            state["calls"] += 1
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        started.set()
        release.wait(timeout=2)
        with lock:
            state["active"] -= 1
        finished.set()
        return _raw({"subAccounts": []}, "synthetic-timeout-request")

    adapter = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        rate_limiter=NoopRateLimiter(),
    )

    try:
        first = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))
        assert await asyncio.to_thread(started.wait, 1)
        with pytest.raises(GatewayError, match="bounded retry policy"):
            await first

        second = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))
        with pytest.raises(GatewayError, match="bounded retry policy"):
            await second

        assert state == {"active": 1, "maximum": 1, "calls": 1}
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 1)


@pytest.mark.asyncio
async def test_caller_cancellation_keeps_actual_provider_calls_at_concurrency_one() -> None:
    """Cancelling the waiter must not let another SDK thread overlap the live call."""
    first_started = threading.Event()
    first_release = threading.Event()
    second_finished = threading.Event()
    state = {"active": 0, "maximum": 0, "calls": 0}
    lock = threading.Lock()

    def list_accounts() -> object:
        with lock:
            state["calls"] += 1
            call_number = state["calls"]
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        if call_number == 1:
            first_started.set()
            first_release.wait(timeout=2)
        with lock:
            state["active"] -= 1
        if call_number == 2:
            second_finished.set()
        return _raw({"subAccounts": []}, f"synthetic-cancel-request-{call_number}")

    adapter = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        rate_limiter=NoopRateLimiter(),
    )
    first = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))

    try:
        assert await asyncio.to_thread(first_started.wait, 1)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        second = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))
        await asyncio.sleep(0.03)
        assert state == {"active": 1, "maximum": 1, "calls": 1}

        first_release.set()
        page = await second
        assert page.source_request_id == "synthetic-cancel-request-2"
        assert await asyncio.to_thread(second_finished.wait, 1)
    finally:
        first_release.set()


@pytest.mark.asyncio
async def test_cash_flow_cursor_and_limit_are_enforced_inside_gateway() -> None:
    """The frozen cash-flow SDK method has no page args, so gateway paging must still be bounded."""
    response = _raw(
        {
            "count": 2,
            "list": [
                {
                    "flowId": "synthetic-cash-flow-001",
                    "currency": "USD",
                    "amount": "100.00",
                    "createdAt": "2026-08-21T04:00:00Z",
                    "flowType": 1,
                },
                {
                    "flowId": "synthetic-cash-flow-002",
                    "currency": "USD",
                    "amount": "200.00",
                    "createdAt": "2026-08-22T04:00:00Z",
                    "flowType": 1,
                },
            ],
            "start": 0,
            "total": 2,
        },
        "synthetic-request-flow-page",
    )
    adapter = SdkFosunTransport(
        SimpleNamespace(
            trade=SimpleNamespace(get_cash_flows=RecordingMethod(response))
        ),
        rate_limiter=NoopRateLimiter(),
    )

    page = await adapter.execute(
        "fosun_list_cash_flows",
        {
            "account_id": "synthetic-account-001",
            "date_from": "2026-08-01",
            "date_to": "2026-08-22",
            "cursor": "1",
            "limit": 1,
        },
    )

    assert [item.external_cash_flow_id for item in page.items] == [
        "synthetic-cash-flow-002"
    ]
    assert page.has_more is False
    assert page.next_cursor is None


@pytest.mark.asyncio
async def test_sdk_adapter_normalizes_provider_epoch_seconds_and_milliseconds_to_utc() -> None:
    """Reviewed integer epoch shapes must not leak or become naive timestamps."""
    response = _raw(
        {
            "count": 2,
            "list": [
                {
                    "flowId": "synthetic-cash-flow-seconds",
                    "currency": "USD",
                    "amount": "100.00",
                    "createdAt": 1787356800,
                    "flowType": 1,
                },
                {
                    "flowId": "synthetic-cash-flow-milliseconds",
                    "currency": "USD",
                    "amount": "200.00",
                    "createdAt": 1787356800000,
                    "flowType": 1,
                },
            ],
            "start": 0,
            "total": 2,
        },
        "synthetic-request-epoch-shapes",
    )
    adapter = SdkFosunTransport(
        SimpleNamespace(
            trade=SimpleNamespace(get_cash_flows=RecordingMethod(response))
        ),
        rate_limiter=NoopRateLimiter(),
    )

    page = await adapter.execute(
        "fosun_list_cash_flows",
        {
            "account_id": "synthetic-account-001",
            "date_from": "2026-08-01",
            "date_to": "2026-08-22",
            "cursor": None,
            "limit": 20,
        },
    )

    assert [item.occurred_at.isoformat() for item in page.items] == [
        "2026-08-22T00:00:00+00:00",
        "2026-08-22T00:00:00+00:00",
    ]


@pytest.mark.asyncio
async def test_retry_after_is_honored_with_bounded_retries_and_safe_logging(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Dropping Retry-After or retry bounds could hammer the provider and leak its exception."""

    class RateLimited(Exception):
        data = {"retryAfter": "2"}

    attempts = 0

    def quote(*, codes: list[str]) -> object:
        nonlocal attempts
        del codes
        attempts += 1
        if attempts < 3:
            raise RateLimited(
                "synthetic-provider-secret endpoint=/private external-account=synthetic-account-001"
            )
        return _raw({}, "synthetic-request-retried")

    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        rate_limiter=NoopRateLimiter(),
        sleeper=record_sleep,
    )

    with caplog.at_level(logging.INFO, logger="nexus_fosun_gateway.provider"):
        page = await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})

    assert attempts == 3
    assert sleeps == [2.0, 2.0]
    assert page.source_request_id == "synthetic-request-retried"
    assert "synthetic-provider-secret" not in caplog.text
    assert "/private" not in caplog.text
    assert "synthetic-account-001" not in caplog.text


@pytest.mark.asyncio
async def test_retry_deadline_is_bounded() -> None:
    """A provider retry loop must stop when its ten-second absolute deadline is exhausted."""

    class RateLimited(Exception):
        retry_after = "9"

    clock = FakeClock()

    def quote(*, codes: list[str]) -> object:
        del codes
        raise RateLimited

    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        rate_limiter=NoopRateLimiter(),
        sleeper=clock.sleep,
        clock=clock,
    )

    with pytest.raises(GatewayError) as exc_info:
        await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})

    assert exc_info.value.code == "PROVIDER_UNAVAILABLE"
    assert clock.now <= 10.0


@pytest.mark.asyncio
async def test_dual_token_bucket_applies_two_qps_burst_limit() -> None:
    """A third immediate call must wait for a 2 QPS token to refill."""
    clock = FakeClock()
    limiter = DualTokenBucket(clock=clock, sleeper=clock.sleep)

    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    assert clock.now == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_dual_token_bucket_applies_thirty_per_minute_budget() -> None:
    """Call 31 must wait until the earliest call leaves the rolling minute window."""
    clock = FakeClock()
    limiter = DualTokenBucket(clock=clock, sleeper=clock.sleep)

    for _ in range(30):
        await limiter.acquire()
    assert clock.now == pytest.approx(14.0)

    await limiter.acquire()

    assert clock.now == pytest.approx(60.0)


class NoopRateLimiter:
    async def acquire(self) -> None:
        return None


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.now += delay

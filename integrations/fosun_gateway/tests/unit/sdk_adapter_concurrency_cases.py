"""SDK adapter cancellation, retry, and rate-limit cases."""

import asyncio
import gc
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import fsopenapi.exceptions as fsopenapi_exceptions
import nexus_fosun_gateway.adapter.sdk as sdk_module
import pytest
import requests
from fsopenapi.exceptions import AuthenticationError, RateLimitError
from fsopenapi.exceptions import PermissionError as SdkPermissionError
from nexus_fosun_gateway.adapter.probe_sdk import probe_sdk_error_mapper
from nexus_fosun_gateway.adapter.sdk import (
    DualTokenBucket,
    SdkFosunTransport,
)
from nexus_fosun_gateway.domain import GatewayError
from sdk_adapter_helpers import FakeClock, NoopRateLimiter, _raw


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker_error", "expected_code"),
    [
        (requests.ConnectionError("synthetic completed transport failure"), "PROVIDER_UNAVAILABLE"),
        (
            AuthenticationError("code", "message", "request-id", {"data": "value"}),
            "PROVIDER_AUTHENTICATION_FAILED",
        ),
        (
            SdkPermissionError("code", "message", "request-id", {"data": "value"}),
            "PROVIDER_PERMISSION_DENIED",
        ),
        (
            RateLimitError("code", "message", "request-id", {"data": "value"}),
            "PROVIDER_RATE_LIMITED",
        ),
    ],
)
async def test_local_deadline_worker_category(
    monkeypatch: pytest.MonkeyPatch,
    worker_error: Exception,
    expected_code: str,
) -> None:
    """A blocked loop can observe the local deadline before Future wrapping catches up."""
    monkeypatch.setattr(sdk_module, "_DEADLINE_SECONDS", 0.01)
    started = threading.Event()
    release_worker = threading.Event()
    worker_finished = threading.Event()

    def quote(*, codes: list[str]) -> object:
        del codes
        started.set()
        release_worker.wait(timeout=1)
        try:
            raise worker_error
        finally:
            worker_finished.set()

    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        rate_limiter=NoopRateLimiter(),
        sdk_error_mapper=probe_sdk_error_mapper(fsopenapi_exceptions),
    )
    request = asyncio.create_task(adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)}))
    assert await asyncio.to_thread(started.wait, 1)

    def release_while_loop_is_blocked() -> None:
        release_worker.set()
        assert worker_finished.wait(timeout=1)
        time.sleep(0.03)

    asyncio.get_running_loop().call_soon(release_while_loop_is_blocked)
    with pytest.raises(GatewayError) as exc_info:
        await request

    assert exc_info.value.code == expected_code, exc_info.value.code
    assert adapter.is_ready()
    await asyncio.wait_for(adapter._sdk_concurrency.acquire(), timeout=1)
    adapter._sdk_concurrency.release()


@pytest.mark.asyncio
async def test_deadline_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sdk_module, "_DEADLINE_SECONDS", 0.01)
    release_worker = threading.Event()
    worker_finished = threading.Event()

    class SnapshotFuture(ConcurrentFuture[object]):
        def __init__(self) -> None:
            super().__init__()
            self._first_done = True

        def done(self) -> bool:
            state = super().done()
            if self._first_done:
                self._first_done = False
                release_worker.set()
                assert worker_finished.wait(timeout=1)
            return state

    def quote(*, codes: list[str]) -> object:
        del codes
        assert release_worker.wait(timeout=1)
        try:
            raise AuthenticationError("code", "message", "request-id", {"data": "value"})
        finally:
            worker_finished.set()

    monkeypatch.setattr(sdk_module, "Future", SnapshotFuture)
    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        rate_limiter=NoopRateLimiter(),
        sdk_error_mapper=probe_sdk_error_mapper(fsopenapi_exceptions),
    )

    with pytest.raises(GatewayError) as exc_info:
        await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})

    assert exc_info.value.code == "PROVIDER_TRANSPORT_POISONED"
    assert not adapter.is_ready()
    await asyncio.wait_for(adapter._sdk_concurrency.acquire(), timeout=1)
    adapter._sdk_concurrency.release()


@pytest.mark.asyncio
async def test_worker_timeout_mapping() -> None:
    def quote(*, codes: list[str]) -> object:
        del codes
        raise TimeoutError("synthetic worker timeout")

    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        rate_limiter=NoopRateLimiter(),
        sdk_error_mapper=lambda _error: None,
    )

    with pytest.raises(GatewayError) as exc_info:
        await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})

    assert exc_info.value.code == "PROVIDER_APPLICATION_ERROR"


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
async def test_market_backlog_cannot_queue_ahead_of_broker_work() -> None:
    """Per-principal admission must keep a Provider flood out of Broker's SDK queue."""
    first_market_started = threading.Event()
    release_first_market = threading.Event()
    order: list[str] = []
    market_calls = 0
    lock = threading.Lock()

    def quote(*, codes: list[str]) -> object:
        nonlocal market_calls
        del codes
        with lock:
            market_calls += 1
            call_number = market_calls
            order.append(f"market-{call_number}")
        if call_number == 1:
            first_market_started.set()
            release_first_market.wait(timeout=2)
        return _raw({}, f"synthetic-market-{call_number}")

    def list_orders(**kwargs: object) -> object:
        del kwargs
        with lock:
            order.append("broker")
        return _raw(
            {"count": 0, "list": [], "start": 0, "total": 0},
            "synthetic-broker",
        )

    adapter = SdkFosunTransport(
        SimpleNamespace(
            market=SimpleNamespace(quote=quote),
            trade=SimpleNamespace(list_orders=list_orders),
        ),
        rate_limiter=NoopRateLimiter(),
    )
    first_market = asyncio.create_task(adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)}))
    try:
        assert await asyncio.to_thread(first_market_started.wait, 1)
        second_market = asyncio.create_task(
            adapter.execute("fosun_get_quotes", {"codes": ("usMSFT",)})
        )
        await asyncio.sleep(0)
        broker = asyncio.create_task(
            adapter.execute(
                "fosun_list_orders",
                {"account_id": "synthetic-account", "cursor": None, "limit": 100},
            )
        )
        await asyncio.sleep(0)
        release_first_market.set()
        await asyncio.gather(first_market, second_market, broker)
    finally:
        release_first_market.set()

    assert order == ["market-1", "broker", "market-2"]


@pytest.mark.asyncio
async def test_capability_expiry_while_cross_lane_waiting_never_invokes_sdk_method() -> None:
    """Broker must recheck capability after waiting behind a market SDK call."""
    first_market_started = threading.Event()
    release_first_market = threading.Event()
    queued_precheck_complete = asyncio.Event()
    broker_calls = 0
    broker_started = False
    broker_checks = 0
    now = [datetime(2026, 8, 30, tzinfo=UTC)]
    expires_at = now[0] + timedelta(seconds=1)

    def utc_clock() -> datetime:
        nonlocal broker_checks
        if broker_started:
            broker_checks += 1
            if broker_checks == 2:
                queued_precheck_complete.set()
        return now[0]

    def quote(*, codes: list[str]) -> object:
        del codes
        first_market_started.set()
        release_first_market.wait(timeout=2)
        return _raw({}, "synthetic-market-capability-queue")

    def list_orders(**kwargs: object) -> object:
        nonlocal broker_calls
        del kwargs
        broker_calls += 1
        return _raw(
            {"count": 0, "list": [], "start": 0, "total": 0},
            "synthetic-broker-capability-queue",
        )

    adapter = SdkFosunTransport(
        SimpleNamespace(
            market=SimpleNamespace(quote=quote),
            trade=SimpleNamespace(list_orders=list_orders),
        ),
        rate_limiter=NoopRateLimiter(),
        capability_expires_at=expires_at,
        activation_checked_at=now[0],
        activation_monotonic_anchor=time.monotonic(),
        utc_clock=utc_clock,
    )
    market = asyncio.create_task(adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)}))
    try:
        assert await asyncio.to_thread(first_market_started.wait, 1)
        broker_started = True
        broker = asyncio.create_task(
            adapter.execute(
                "fosun_list_orders",
                {"account_id": "synthetic-account", "cursor": None, "limit": 100},
            )
        )
        await asyncio.wait_for(queued_precheck_complete.wait(), timeout=1)
        now[0] = expires_at
        release_first_market.set()
        results = await asyncio.gather(market, broker, return_exceptions=True)
    finally:
        release_first_market.set()

    assert broker_calls == 0
    assert all(isinstance(result, GatewayError) for result in results)
    assert all(result.code == "ACTIVATION_EXPIRED" for result in results)


@pytest.mark.asyncio
async def test_market_rate_budget_cannot_delay_broker_budget() -> None:
    """Exhausting the market minute bucket must leave Broker capacity available."""
    clock = FakeClock()
    market_starts: list[float] = []

    def quote(*, codes: list[str]) -> object:
        del codes
        market_starts.append(clock.now)
        return _raw({}, "synthetic-market-budget")

    def list_orders(**kwargs: object) -> object:
        del kwargs
        return _raw(
            {"count": 0, "list": [], "start": 0, "total": 0},
            "synthetic-broker-budget",
        )

    adapter = SdkFosunTransport(
        SimpleNamespace(
            market=SimpleNamespace(quote=quote),
            trade=SimpleNamespace(list_orders=list_orders),
        ),
        clock=clock,
        sleeper=clock.sleep,
    )
    for _ in range(24):
        await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    before_broker = clock.now

    await adapter.execute(
        "fosun_list_orders",
        {"account_id": "synthetic-account", "cursor": None, "limit": 100},
    )

    assert clock.now == before_broker
    with pytest.raises(GatewayError) as exc_info:
        await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    assert exc_info.value.code == "PROVIDER_UNAVAILABLE"
    assert clock.now == before_broker
    assert len(market_starts) == 24
    assert all(
        later - earlier >= 1.0
        for earlier, later in zip(market_starts, market_starts[1:])
    )
    assert all(
        sum(start > window_end - 60.0 for start in market_starts if start <= window_end) <= 24
        for window_end in market_starts
    )


@pytest.mark.asyncio
async def test_market_rate_wait_releases_shared_sdk_slot_for_broker() -> None:
    """A market QPS wait must not hold the cross-lane SDK semaphore."""
    clock = FakeClock()
    sleep_entered = asyncio.Event()
    release_sleep = asyncio.Event()
    starts: list[tuple[str, float]] = []

    async def controlled_sleep(delay: float) -> None:
        assert delay == 1.0
        sleep_entered.set()
        await release_sleep.wait()
        clock.now += delay

    def quote(*, codes: list[str]) -> object:
        del codes
        starts.append(("market", clock.now))
        return _raw({}, "synthetic-market-rate-wait")

    def list_orders(**kwargs: object) -> object:
        del kwargs
        starts.append(("broker", clock.now))
        return _raw(
            {"count": 0, "list": [], "start": 0, "total": 0},
            "synthetic-broker-during-rate-wait",
        )

    adapter = SdkFosunTransport(
        SimpleNamespace(
            market=SimpleNamespace(quote=quote),
            trade=SimpleNamespace(list_orders=list_orders),
        ),
        clock=clock,
        sleeper=controlled_sleep,
    )
    await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    waiting_market = asyncio.create_task(
        adapter.execute("fosun_get_quotes", {"codes": ("usMSFT",)})
    )
    try:
        await asyncio.wait_for(sleep_entered.wait(), timeout=1)
        await asyncio.wait_for(
            adapter.execute(
                "fosun_list_orders",
                {"account_id": "synthetic-account", "cursor": None, "limit": 100},
            ),
            timeout=0.2,
        )
    finally:
        release_sleep.set()
        await waiting_market

    assert starts == [("market", 0.0), ("broker", 0.0), ("market", 1.0)]


@pytest.mark.asyncio
async def test_rate_token_is_consumed_after_sdk_slot_wait_at_actual_call_time() -> None:
    """A queued call cannot spend an early token and then burst at method start."""
    clock = FakeClock()
    sdk_waiting = asyncio.Event()
    starts: list[float] = []

    class _SignalingSemaphore(asyncio.Semaphore):
        async def acquire(self) -> bool:
            sdk_waiting.set()
            return await super().acquire()

    def quote(*, codes: list[str]) -> object:
        del codes
        starts.append(clock.now)
        return _raw({}, f"synthetic-rate-start-{len(starts)}")

    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        clock=clock,
        sleeper=clock.sleep,
    )
    blocked_sdk = _SignalingSemaphore(0)
    adapter._sdk_concurrency = blocked_sdk
    first = asyncio.create_task(
        adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    )
    await asyncio.wait_for(sdk_waiting.wait(), timeout=1)
    clock.now = 9.0
    blocked_sdk.release()
    await first

    await adapter.execute("fosun_get_quotes", {"codes": ("usMSFT",)})

    assert starts == [9.0, 10.0]


@pytest.mark.asyncio
async def test_rate_token_is_consumed_inside_deferred_sdk_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OS thread scheduling cannot move a spent token away from method start."""
    clock = FakeClock()
    first_thread_created = asyncio.Event()
    deferred_target: list[Callable[[], None]] = []
    thread_starts = 0
    method_starts: list[float] = []

    class _DeferredFirstThread:
        def __init__(
            self,
            *,
            target: Callable[..., None],
            args: tuple[object, ...],
            name: str,
            daemon: bool,
        ) -> None:
            del name, daemon
            self._target = target
            self._args = args

        def start(self) -> None:
            nonlocal thread_starts
            thread_starts += 1
            if thread_starts == 1:
                deferred_target.append(lambda: self._target(*self._args))
                first_thread_created.set()
                return
            self._target(*self._args)

    def quote(*, codes: list[str]) -> object:
        del codes
        method_starts.append(clock.now)
        return _raw({}, f"synthetic-deferred-start-{len(method_starts)}")

    monkeypatch.setattr(sdk_module, "Thread", _DeferredFirstThread)
    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        clock=clock,
        sleeper=clock.sleep,
    )
    first = asyncio.create_task(
        adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    )
    await asyncio.wait_for(first_thread_created.wait(), timeout=1)
    clock.now = 9.0
    deferred_target.pop()()
    await first

    await adapter.execute("fosun_get_quotes", {"codes": ("usMSFT",)})

    assert method_starts == [9.0, 10.0]


@pytest.mark.asyncio
async def test_blocking_log_runs_before_the_rate_call_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow logging handler cannot age a consumed token before SDK entry."""
    clock = FakeClock()
    method_starts: list[float] = []
    delayed = False

    def delayed_info(message: str, *args: object, **kwargs: object) -> None:
        nonlocal delayed
        del message, args
        extra = kwargs.get("extra")
        if isinstance(extra, dict) and extra.get("outcome") == "dispatching" and not delayed:
            delayed = True
            clock.now = 9.0

    def quote(*, codes: list[str]) -> object:
        del codes
        method_starts.append(clock.now)
        return _raw({}, f"synthetic-logging-start-{len(method_starts)}")

    monkeypatch.setattr(sdk_module._LOGGER, "info", delayed_info)
    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(quote=quote)),
        clock=clock,
        sleeper=clock.sleep,
    )

    await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    await adapter.execute("fosun_get_quotes", {"codes": ("usMSFT",)})

    assert method_starts == [9.0, 10.0]


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
    worker_daemon = False
    poison_notifications = 0
    lock = threading.Lock()

    def record_poison() -> None:
        nonlocal poison_notifications
        poison_notifications += 1

    def list_accounts() -> object:
        nonlocal worker_daemon
        worker_daemon = threading.current_thread().daemon
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
        on_poison=record_poison,
    )

    try:
        first = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))
        assert await asyncio.to_thread(started.wait, 1)
        with pytest.raises(GatewayError) as first_error:
            await first
        assert first_error.value.code == "PROVIDER_TRANSPORT_POISONED"
        assert first_error.value.next_action == "RESTART_GATEWAY_AFTER_REVIEW"
        assert not adapter.is_ready()
        assert worker_daemon is True
        assert poison_notifications == 1

        second = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))
        with pytest.raises(GatewayError) as second_error:
            await second
        assert second_error.value.code == "PROVIDER_TRANSPORT_POISONED"
        assert poison_notifications == 1

        assert state == {"active": 1, "maximum": 1, "calls": 1}
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 1)


@pytest.mark.asyncio
async def test_late_sdk_exception_after_timeout_is_consumed_without_logging(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A discarded daemon result must not leak Provider exception text via asyncio."""
    monkeypatch.setattr(sdk_module, "_DEADLINE_SECONDS", 0.03)
    canary = "SENSITIVE_PROVIDER_CANARY"
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    leaked_contexts: list[dict[str, object]] = []

    def capture_loop_error(
        _loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        leaked_contexts.append(context)

    def list_accounts() -> object:
        started.set()
        release.wait(timeout=2)
        finished.set()
        raise RuntimeError(canary)

    adapter = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        rate_limiter=NoopRateLimiter(),
    )
    loop.set_exception_handler(capture_loop_error)
    try:
        with caplog.at_level(logging.INFO, logger="nexus_fosun_gateway.provider"):
            request = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))
            assert await asyncio.to_thread(started.wait, 1)
            with pytest.raises(GatewayError) as error:
                await request
            assert error.value.code == "PROVIDER_TRANSPORT_POISONED"
            release.set()
            assert await asyncio.to_thread(finished.wait, 1)
            await asyncio.wait_for(adapter._sdk_concurrency.acquire(), timeout=1)
            adapter._sdk_concurrency.release()
            del error
            del request
            for _ in range(3):
                await asyncio.sleep(0)
                gc.collect()
    finally:
        release.set()
        loop.set_exception_handler(previous_handler)

    rendered_contexts = " ".join(
        str(value) for context in leaked_contexts for value in context.values()
    )
    assert canary not in rendered_contexts
    assert canary not in caplog.text


@pytest.mark.asyncio
async def test_rate_and_lane_waits_are_inside_the_request_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sdk_module, "_DEADLINE_SECONDS", 0.03)

    class BlockingRateLimiter:
        async def acquire(self) -> None:
            await asyncio.Event().wait()

        async def try_acquire(self) -> float | None:
            await asyncio.Event().wait()
            return None

        def try_acquire_at_call_edge(self) -> float | None:
            return 0.01

    method_calls = 0

    def list_accounts() -> object:
        nonlocal method_calls
        method_calls += 1
        return _raw({"subAccounts": []}, "synthetic-deadline-request")

    adapter = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        rate_limiter=BlockingRateLimiter(),
    )
    with pytest.raises(GatewayError) as rate_error:
        await asyncio.wait_for(adapter.execute("fosun_sync_accounts", {}), timeout=0.2)
    assert rate_error.value.code == "PROVIDER_UNAVAILABLE"
    assert method_calls == 0

    lane_adapter = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        rate_limiter=NoopRateLimiter(),
    )
    await lane_adapter._broker_lane.acquire()
    try:
        with pytest.raises(GatewayError) as lane_error:
            await asyncio.wait_for(
                lane_adapter.execute("fosun_sync_accounts", {}),
                timeout=0.2,
            )
    finally:
        lane_adapter._broker_lane.release()
    assert lane_error.value.code == "PROVIDER_UNAVAILABLE"
    assert method_calls == 0


@pytest.mark.asyncio
async def test_poisoned_transport_close_is_idempotent_and_never_waits_for_sdk_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sdk_module, "_DEADLINE_SECONDS", 0.03)
    started = threading.Event()
    release = threading.Event()

    def list_accounts() -> object:
        started.set()
        release.wait()
        return _raw({"subAccounts": []}, "synthetic-permanent-block")

    adapter = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        rate_limiter=NoopRateLimiter(),
    )
    try:
        request = asyncio.create_task(adapter.execute("fosun_sync_accounts", {}))
        assert await asyncio.to_thread(started.wait, 1)
        with pytest.raises(GatewayError):
            await request

        await asyncio.wait_for(adapter.aclose(), timeout=0.05)
        await asyncio.wait_for(adapter.aclose(), timeout=0.05)
        assert not adapter.is_ready()
    finally:
        release.set()


@pytest.mark.asyncio
async def test_timed_out_sdk_call_poisons_cross_lane_requests_until_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timed-out market call must make a cross-lane broker call fail fast."""
    monkeypatch.setattr(sdk_module, "_DEADLINE_SECONDS", 0.1)
    first_started = threading.Event()
    release_first = threading.Event()
    first_finished = threading.Event()
    now = [datetime(2026, 8, 30, tzinfo=UTC)]
    expires_at = now[0] + timedelta(seconds=1)
    second_method_calls = 0

    def utc_clock() -> datetime:
        return now[0]

    def quote(*, codes: list[str]) -> object:
        del codes
        first_started.set()
        release_first.wait(timeout=2)
        first_finished.set()
        return _raw({}, "synthetic-timeout-market")

    def list_orders(**kwargs: object) -> object:
        nonlocal second_method_calls
        del kwargs
        second_method_calls += 1
        return _raw(
            {"count": 0, "list": [], "start": 0, "total": 0},
            "synthetic-timeout-broker",
        )

    adapter = SdkFosunTransport(
        SimpleNamespace(
            market=SimpleNamespace(quote=quote),
            trade=SimpleNamespace(list_orders=list_orders),
        ),
        rate_limiter=NoopRateLimiter(),
        capability_expires_at=expires_at,
        activation_checked_at=now[0],
        activation_monotonic_anchor=time.monotonic(),
        utc_clock=utc_clock,
    )

    try:
        first = asyncio.create_task(adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)}))
        assert await asyncio.to_thread(first_started.wait, 1)
        with pytest.raises(GatewayError) as first_error:
            await first
        assert first_error.value.code == "PROVIDER_TRANSPORT_POISONED"
        assert first_error.value.next_action == "RESTART_GATEWAY_AFTER_REVIEW"
        assert not adapter.is_ready()

        with pytest.raises(GatewayError) as second_error:
            await adapter.execute(
                "fosun_list_orders",
                {"account_id": "synthetic-account", "cursor": None, "limit": 100},
            )
        assert second_error.value.code == "PROVIDER_TRANSPORT_POISONED"
        release_first.set()
    finally:
        release_first.set()
        assert await asyncio.to_thread(first_finished.wait, 1)

    assert second_method_calls == 0


@pytest.mark.asyncio
async def test_caller_cancellation_poisons_transport_without_overlapping_sdk_calls() -> None:
    """Cancelling a live waiter makes the discarded provider result non-admissible."""
    first_started = threading.Event()
    first_release = threading.Event()
    state = {"active": 0, "maximum": 0, "calls": 0}
    lock = threading.Lock()

    def list_accounts() -> object:
        with lock:
            state["calls"] += 1
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        first_started.set()
        first_release.wait(timeout=2)
        with lock:
            state["active"] -= 1
        return _raw({"subAccounts": []}, "synthetic-cancel-request")

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
        assert not adapter.is_ready()

        with pytest.raises(GatewayError) as second_error:
            await adapter.execute("fosun_sync_accounts", {})
        assert second_error.value.code == "PROVIDER_TRANSPORT_POISONED"
        assert state == {"active": 1, "maximum": 1, "calls": 1}
    finally:
        first_release.set()


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
async def test_bars_sdk_failure_is_terminal_after_one_actual_call() -> None:
    """Retrying kline inside one gateway POST would violate daily child one-shot safety."""

    class RateLimited(Exception):
        data = {"retryAfter": "2"}

    attempts = 0

    def kline(**kwargs: object) -> object:
        nonlocal attempts
        del kwargs
        attempts += 1
        raise RateLimited("synthetic bars failure")

    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    adapter = SdkFosunTransport(
        SimpleNamespace(market=SimpleNamespace(kline=kline)),
        rate_limiter=NoopRateLimiter(),
        sleeper=record_sleep,
    )

    with pytest.raises(GatewayError) as exc_info:
        await adapter.execute(
            "fosun_get_bars",
            {
                "code": "usAAPL",
                "ktype": "day",
                "start_time": "2026-08-01T00:00:00Z",
                "end_time": "2026-08-22T00:00:00Z",
                "limit": 100,
            },
        )

    assert exc_info.value.code == "PROVIDER_UNAVAILABLE"
    assert attempts == 1
    assert sleeps == []


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

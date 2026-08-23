"""SDK adapter cancellation, retry, and rate-limit cases."""
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
from sdk_adapter_helpers import FakeClock, NoopRateLimiter, _raw


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


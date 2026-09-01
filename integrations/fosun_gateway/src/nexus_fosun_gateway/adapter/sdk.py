"""Anti-corruption adapter for the frozen Fosun SDK v1.2.0 read surface."""

import asyncio
import logging
import ssl
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event, Lock, Thread, Timer
from typing import Protocol, cast

import requests

from nexus_fosun_gateway.adapter.normalization import (
    normalize_cash_summary_response,
    normalize_holdings_response,
    normalize_provider_response,
)
from nexus_fosun_gateway.adapter.refusal import NonRetryableTransportRefusal
from nexus_fosun_gateway.domain import CapabilityItem, GatewayError, GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import (
    MARKET_OPERATIONS,
    READ_ONLY_OPERATIONS,
    GatewayOperation,
)
from nexus_fosun_gateway.security.dependency_logging import (
    enforce_sdk_dependency_log_boundary,
)

_LOGGER = logging.getLogger("nexus_fosun_gateway.provider")
_MAX_ATTEMPTS = 3
_DEADLINE_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class _RateLimitWait:
    delay: float


class _ProviderDeadlineExceeded(Exception):
    """Private sentinel for locally enforced provider-call deadlines."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


class RateLimiter(Protocol):
    async def acquire(self) -> None: ...

    async def try_acquire(self) -> float | None: ...

    def try_acquire_at_call_edge(self) -> float | None: ...


class DualTokenBucket:
    """Enforce a two-request burst and thirty-request minute budget."""

    def __init__(
        self,
        *,
        qps_capacity: float = 2.0,
        qps_refill_per_second: float = 2.0,
        requests_per_minute: int = 30,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if qps_capacity <= 0 or qps_refill_per_second <= 0 or not 1 <= requests_per_minute <= 30:
            raise ValueError("provider rate-limit budget is invalid")
        self._qps_capacity = qps_capacity
        self._qps_refill_per_second = qps_refill_per_second
        self._qps_tokens = qps_capacity
        self._updated_at = clock()
        self._minute_limit = requests_per_minute
        self._window_seconds = 60.0
        self._timestamps: deque[float] = deque()
        self._clock = clock
        self._sleeper = sleeper
        self._lock = Lock()

    async def acquire(self) -> None:
        while True:
            wait_for = await self.try_acquire()
            if wait_for is None:
                return
            await self._sleeper(wait_for)

    async def try_acquire(self) -> float | None:
        """Atomically consume both budgets or report when to retry."""
        return self.try_acquire_at_call_edge()

    def try_acquire_at_call_edge(self) -> float | None:
        """Synchronously consume both budgets beside the blocking SDK method."""
        with self._lock:
            now = self._clock()
            elapsed = max(now - self._updated_at, 0.0)
            self._qps_tokens = min(
                self._qps_capacity,
                self._qps_tokens + elapsed * self._qps_refill_per_second,
            )
            self._updated_at = now
            cutoff = now - self._window_seconds
            while self._timestamps and self._timestamps[0] <= cutoff:
                self._timestamps.popleft()
            qps_wait = (
                0.0
                if self._qps_tokens >= 1.0
                else (1.0 - self._qps_tokens) / self._qps_refill_per_second
            )
            minute_wait = (
                0.0
                if len(self._timestamps) < self._minute_limit
                else self._timestamps[0] + self._window_seconds - now
            )
            wait_for = max(qps_wait, minute_wait)
            if wait_for > 0:
                return wait_for
            self._qps_tokens -= 1.0
            self._timestamps.append(now)
            return None


class SdkFosunTransport:
    """Call only seven reviewed operations and return gateway-owned Pydantic models."""

    def __init__(
        self,
        client: object,
        *,
        rate_limiter: RateLimiter | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        capability_expires_at: datetime | None = None,
        activation_checked_at: datetime | None = None,
        activation_monotonic_anchor: float | None = None,
        utc_clock: Callable[[], datetime] = _utc_now,
        on_poison: Callable[[], None] | None = None,
        on_activation_expired: Callable[[datetime], None] | None = None,
        sdk_error_mapper: Callable[[Exception], GatewayError | None] | None = None,
        pre_dispatch_validator: Callable[[], None] | None = None,
        max_attempts: int = _MAX_ATTEMPTS,
    ) -> None:
        enforce_sdk_dependency_log_boundary()
        if (
            not isinstance(max_attempts, int)
            or isinstance(max_attempts, bool)
            or not 1 <= max_attempts <= _MAX_ATTEMPTS
        ):
            raise ValueError("provider attempt budget is invalid")
        if capability_expires_at is not None and (
            capability_expires_at.tzinfo is None or capability_expires_at.utcoffset() is None
        ):
            raise ValueError("activation expiry must be timezone-aware")
        if (activation_checked_at is None) != (activation_monotonic_anchor is None):
            raise ValueError(
                "activation checked time and monotonic anchor must be supplied together"
            )
        if activation_checked_at is not None and (
            activation_checked_at.tzinfo is None or activation_checked_at.utcoffset() is None
        ):
            raise ValueError("activation checked time must be timezone-aware")
        if capability_expires_at is None and activation_checked_at is not None:
            raise ValueError("activation timing requires an expiry")
        if capability_expires_at is not None and activation_checked_at is None:
            # Sample monotonic first: any delay before the wall-clock sample can
            # only shorten, never extend, the projected authorization lifetime.
            activation_monotonic_anchor = clock()
            try:
                activation_checked_at = utc_clock()
            except Exception as error:
                raise ValueError("activation checked time is unavailable") from error
            if activation_checked_at.tzinfo is None or activation_checked_at.utcoffset() is None:
                raise ValueError("activation checked time must be timezone-aware")
        self._client = client
        if rate_limiter is None:
            self._broker_rate_limiter: RateLimiter = DualTokenBucket(
                qps_capacity=1.0,
                qps_refill_per_second=1.0,
                requests_per_minute=6,
                clock=clock,
                sleeper=sleeper,
            )
            self._market_rate_limiter: RateLimiter = DualTokenBucket(
                qps_capacity=1.0,
                qps_refill_per_second=1.0,
                requests_per_minute=24,
                clock=clock,
                sleeper=sleeper,
            )
        else:
            self._broker_rate_limiter = rate_limiter
            self._market_rate_limiter = rate_limiter
        self._sleeper = sleeper
        self._clock = clock
        self._capability_expires_at = capability_expires_at
        self._utc_clock = utc_clock
        self._activation_expired = Event()
        self._activation_expiry_lock = Lock()
        self._on_activation_expired = on_activation_expired
        self._activation_deadline: float | None = None
        self._activation_timer: Timer | None = None
        if capability_expires_at is not None:
            assert activation_checked_at is not None
            assert activation_monotonic_anchor is not None
            self._activation_deadline = activation_monotonic_anchor + max(
                (capability_expires_at - activation_checked_at).total_seconds(),
                0.0,
            )
        self._broker_lane = asyncio.Semaphore(1)
        self._market_lane = asyncio.Semaphore(1)
        self._sdk_concurrency = asyncio.Semaphore(1)
        self._poisoned = Event()
        self._closed = Event()
        self._on_poison = on_poison
        self._sdk_error_mapper = sdk_error_mapper
        self._pre_dispatch_validator = pre_dispatch_validator
        self._max_attempts = max_attempts
        if self._activation_deadline is not None:
            timer = Timer(
                max(self._activation_deadline - self._clock(), 0.0),
                self._expire_activation,
            )
            timer.name = "nexus-fosun-activation-expiry"
            timer.daemon = True
            self._activation_timer = timer
            timer.start()

    async def execute(
        self,
        operation: GatewayOperation,
        payload: Mapping[str, object],
    ) -> GatewayPage[GatewayItem]:
        validator = self._pre_dispatch_validator
        if validator is not None:
            validator()
        self._require_transport_ready()
        self._require_active_capability()
        if operation not in READ_ONLY_OPERATIONS:
            raise GatewayError(
                "OPERATION_NOT_ALLOWED",
                "Gateway operation is not allowed.",
                status_code=404,
                next_action="USE_READ_ONLY_OPERATION",
            )
        if operation == "fosun_capabilities":
            return GatewayPage[GatewayItem](
                items=(
                    CapabilityItem(
                        mode="read_only",
                        operations=tuple(sorted(READ_ONLY_OPERATIONS)),
                    ),
                ),
                source_request_id="gateway-local-capabilities",
                has_more=False,
                next_cursor=None,
                as_of=datetime.now(UTC),
            )
        deadline = self._clock() + _DEADLINE_SECONDS
        lane = self._lane(operation)
        try:
            await asyncio.wait_for(
                lane.acquire(),
                timeout=max(deadline - self._clock(), 0.0),
            )
        except TimeoutError:
            raise _provider_unavailable() from None
        try:
            self._require_transport_ready()
            if operation == "fosun_sync_accounts":
                raw = await self._invoke(
                    operation,
                    self._method("account", "list_accounts"),
                    deadline=deadline,
                )
            elif operation == "fosun_sync_portfolio":
                return await self._portfolio(payload, deadline=deadline)
            elif operation == "fosun_list_orders":
                raw = await self._invoke(
                    operation,
                    self._method("trade", "list_orders"),
                    deadline=deadline,
                    sub_account_id=_required_str(payload, "account_id"),
                    start=_cursor(payload),
                    count=_required_int(payload, "limit"),
                )
            elif operation == "fosun_list_cash_flows":
                raw = await self._invoke(
                    operation,
                    self._method("trade", "get_cash_flows"),
                    deadline=deadline,
                    sub_account_id=_required_str(payload, "account_id"),
                    trade_date_from=_required_str(payload, "date_from"),
                    trade_date_to=_required_str(payload, "date_to"),
                )
            elif operation == "fosun_get_quotes":
                raw = await self._invoke(
                    operation,
                    self._method("market", "quote"),
                    deadline=deadline,
                    codes=list(_required_strings(payload, "codes")),
                )
            elif operation == "fosun_get_bars":
                raw = await self._invoke(
                    operation,
                    self._method("market", "kline"),
                    deadline=deadline,
                    **_bar_sdk_arguments(payload),
                )
            else:
                raise AssertionError("unreachable operation")
        finally:
            lane.release()
        return normalize_provider_response(
            operation,
            raw,
            received_at=datetime.now(UTC),
            context=payload,
        )

    def is_ready(self) -> bool:
        if self._poisoned.is_set() or self._closed.is_set():
            return False
        return self._active_capability()

    def readiness_expires_at(self) -> datetime | None:
        return self._capability_expires_at

    async def aclose(self) -> None:
        """Stop admission without waiting for a possibly stuck daemon SDK call."""
        self._closed.set()
        timer = self._activation_timer
        if timer is not None:
            timer.cancel()

    def _require_transport_ready(self) -> None:
        if not self._poisoned.is_set() and not self._closed.is_set():
            return
        raise GatewayError(
            "PROVIDER_TRANSPORT_POISONED",
            "Provider transport is unavailable and requires a gateway restart.",
            status_code=503,
            next_action="RESTART_GATEWAY_AFTER_REVIEW",
        )

    def _poison_transport(self) -> None:
        if self._poisoned.is_set():
            return
        self._poisoned.set()
        if self._on_poison is not None:
            try:
                self._on_poison()
            except Exception:
                _LOGGER.exception("transport_readiness_invalidation_failed")

    def _require_active_capability(self) -> None:
        if self._active_capability():
            return
        raise GatewayError(
            "ACTIVATION_EXPIRED",
            "Provider read-only activation has expired.",
            status_code=503,
            next_action="REFRESH_GUARDED_READ_ONLY_PROBE",
        )

    def _active_capability(self) -> bool:
        expires_at = self._capability_expires_at
        deadline = self._activation_deadline
        if expires_at is None:
            return True
        if self._activation_expired.is_set():
            return False
        if deadline is None:
            expired = True
        else:
            try:
                expired = self._clock() >= deadline
            except Exception:
                expired = True
        if not expired:
            try:
                wall_now = self._utc_clock()
                expired = (
                    wall_now.tzinfo is None
                    or wall_now.utcoffset() is None
                    or wall_now >= expires_at
                )
            except Exception:
                expired = True
        if expired:
            self._expire_activation()
            return False
        return True

    def _expire_activation(self) -> None:
        expires_at = self._capability_expires_at
        if expires_at is None or self._activation_expired.is_set():
            return
        callback: Callable[[datetime], None] | None = None
        with self._activation_expiry_lock:
            if self._activation_expired.is_set():
                return
            self._activation_expired.set()
            callback = self._on_activation_expired
        if callback is None:
            return
        try:
            callback(expires_at)
        except Exception:
            # The in-memory authorization latch is already permanent. Keep the
            # response fail-closed and force process-level restart review if
            # durable readiness invalidation fails.
            self._poison_transport()
            _LOGGER.exception("activation_expiry_persistence_failed")

    async def _portfolio(
        self,
        payload: Mapping[str, object],
        *,
        deadline: float,
    ) -> GatewayPage[GatewayItem]:
        account_id = _required_str(payload, "account_id")
        read_kind = _required_str(payload, "read_kind")
        if read_kind == "cash":
            raw = await self._invoke(
                "fosun_sync_portfolio",
                self._method("portfolio", "get_assets_summary"),
                deadline=deadline,
                sub_account_id=account_id,
            )
            return normalize_cash_summary_response(
                raw,
                account_id=account_id,
                received_at=datetime.now(UTC),
            )
        if read_kind == "holdings":
            raw = await self._invoke(
                "fosun_sync_portfolio",
                self._method("portfolio", "get_holdings"),
                deadline=deadline,
                sub_account_id=account_id,
                start=_cursor(payload),
                count=_required_int(payload, "limit"),
            )
            return normalize_holdings_response(
                raw,
                account_id=account_id,
                received_at=datetime.now(UTC),
                context=payload,
            )
        raise GatewayError(
            "VALIDATION_ERROR",
            "Request payload is invalid.",
            status_code=422,
            next_action="FIX_REQUEST",
        )

    def _method(self, section_name: str, method_name: str) -> Callable[..., object]:
        section = getattr(self._client, section_name, None)
        method = getattr(section, method_name, None)
        if not callable(method):
            raise GatewayError(
                "SDK_CONTRACT_MISMATCH",
                "Provider SDK does not match the reviewed method surface.",
                status_code=503,
                next_action="REVIEW_SDK_SOURCE",
            )
        return cast("Callable[..., object]", method)

    async def _invoke(
        self,
        operation: GatewayOperation,
        method: Callable[..., object],
        *,
        deadline: float,
        **kwargs: object,
    ) -> object:
        max_attempts = 1 if operation == "fosun_get_bars" else self._max_attempts
        for attempt in range(1, max_attempts + 1):
            self._require_transport_ready()
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise _provider_unavailable()
            try:
                result = await self._invoke_attempt(
                    operation,
                    method,
                    attempt=attempt,
                    deadline=deadline,
                    kwargs=kwargs,
                )
            except _ProviderDeadlineExceeded:
                # A running Python thread cannot be cancelled safely. It is daemonized
                # so process shutdown remains possible, while poison prevents any new
                # work from entering this transport until a reviewed restart.
                if self._poisoned.is_set():
                    self._require_transport_ready()
                raise _provider_unavailable() from None
            except GatewayError:
                raise
            except NonRetryableTransportRefusal:
                raise
            except Exception as error:
                mapper = self._sdk_error_mapper
                mapped = None if mapper is None else mapper(error)
                if mapped is not None:
                    raise mapped from None
                if mapper is None and type(error).__name__ == "PermissionError":
                    raise GatewayError(
                        "PROVIDER_PERMISSION_DENIED",
                        "Provider permission denied.",
                        status_code=502,
                        next_action="REVIEW_PROVIDER_PERMISSION",
                    ) from None
                if mapper is None and isinstance(error, TimeoutError):
                    raise _provider_unavailable() from None
                if mapper is not None and not _is_transport_error(error):
                    raise GatewayError(
                        "PROVIDER_APPLICATION_ERROR",
                        "Provider SDK application failure.",
                        status_code=502,
                        next_action="REVIEW_PROVIDER_RESPONSE",
                    ) from None
                delay = _retry_after(error)
                remaining = deadline - self._clock()
                if attempt == max_attempts or delay > remaining:
                    raise _provider_unavailable() from None
                _LOGGER.info(
                    "provider_retry",
                    extra={
                        "operation": operation,
                        "attempt": attempt,
                        "outcome": "retry",
                    },
                )
                await self._sleeper(delay)
                continue
            self._require_active_capability()
            _LOGGER.info(
                "provider_call",
                extra={"operation": operation, "attempt": attempt, "outcome": "success"},
            )
            return result
        raise AssertionError("bounded retry loop exhausted")

    async def _invoke_attempt(
        self,
        operation: GatewayOperation,
        method: Callable[..., object],
        *,
        attempt: int,
        deadline: float,
        kwargs: Mapping[str, object],
    ) -> object:
        """Consume the rate token only while holding the next real SDK slot."""
        loop = asyncio.get_running_loop()
        limiter = self._rate_limiter(operation)
        while True:
            self._require_transport_ready()
            self._require_active_capability()
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise _ProviderDeadlineExceeded
            try:
                await asyncio.wait_for(
                    self._sdk_concurrency.acquire(),
                    timeout=remaining,
                )
            except TimeoutError:
                raise _ProviderDeadlineExceeded from None
            release_sdk_slot = True
            rate_delay: float | None = None
            try:
                self._require_transport_ready()
                self._require_active_capability()
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise _ProviderDeadlineExceeded
                thread_future: Future[object] = Future()
                sdk_thread = Thread(
                    target=self._run_sdk_call,
                    args=(
                        thread_future,
                        method,
                        kwargs,
                        limiter,
                        operation,
                        attempt,
                    ),
                    name="nexus-fosun-sdk",
                    daemon=True,
                )
                sdk_thread.start()
                wrapped = asyncio.wrap_future(thread_future, loop=loop)
                # A timed-out/cancelled waiter intentionally leaves the daemon
                # call running. Consume any late exception without rendering it;
                # asyncio's orphan-Future logger could expose Provider text.
                wrapped.add_done_callback(_consume_future_exception)
                timeout_scope = asyncio.timeout(remaining)
                try:
                    async with timeout_scope:
                        result = await asyncio.shield(wrapped)
                except asyncio.CancelledError:
                    if not thread_future.done():
                        self._poison_transport()
                        release_sdk_slot = False

                        def release_slot(_: Future[object]) -> None:
                            try:
                                loop.call_soon_threadsafe(self._sdk_concurrency.release)
                            except RuntimeError:
                                pass

                        thread_future.add_done_callback(release_slot)
                    raise
                except TimeoutError:
                    if not timeout_scope.expired():
                        raise
                    worker_done = thread_future.done()
                    if worker_done:
                        worker_error = (
                            None
                            if thread_future.cancelled()
                            else thread_future.exception()
                        )
                        if worker_error is not None:
                            raise worker_error
                    else:
                        self._poison_transport()
                        release_sdk_slot = False

                        def release_slot(_: Future[object]) -> None:
                            try:
                                loop.call_soon_threadsafe(self._sdk_concurrency.release)
                            except RuntimeError:
                                pass

                        thread_future.add_done_callback(release_slot)
                    raise _ProviderDeadlineExceeded from None
                if isinstance(result, _RateLimitWait):
                    rate_delay = result.delay
                else:
                    return result
            finally:
                if release_sdk_slot:
                    self._sdk_concurrency.release()
            assert rate_delay is not None
            remaining = deadline - self._clock()
            if rate_delay > remaining:
                raise _ProviderDeadlineExceeded
            await self._sleeper(rate_delay)

    def _run_sdk_call(
        self,
        future: Future[object],
        method: Callable[..., object],
        kwargs: Mapping[str, object],
        limiter: RateLimiter,
        operation: GatewayOperation,
        attempt: int,
    ) -> None:
        try:
            enforce_sdk_dependency_log_boundary()
            _LOGGER.info(
                "provider_call",
                extra={
                    "operation": operation,
                    "attempt": attempt,
                    "outcome": "dispatching",
                },
            )
            self._require_transport_ready()
            self._require_active_capability()
            rate_delay = limiter.try_acquire_at_call_edge()
            if rate_delay is not None:
                future.set_result(_RateLimitWait(rate_delay))
                return
            # Keep the successful token consume and blocking method invocation
            # adjacent in this same daemon thread. Any potentially blocking log
            # or capability check must remain above the rate linearization point.
            result = method(**kwargs)
        except BaseException as error:
            future.set_exception(error)
        else:
            future.set_result(result)

    def _lane(self, operation: GatewayOperation) -> asyncio.Semaphore:
        return self._market_lane if operation in MARKET_OPERATIONS else self._broker_lane

    def _rate_limiter(self, operation: GatewayOperation) -> RateLimiter:
        return (
            self._market_rate_limiter
            if operation in MARKET_OPERATIONS
            else self._broker_rate_limiter
        )


def _retry_after(error: Exception) -> float:
    value = getattr(error, "retry_after", None)
    if value is None:
        data = getattr(error, "data", None)
        value = data.get("retryAfter") if isinstance(data, Mapping) else None
    if value is None:
        return 0.5
    try:
        return min(max(float(str(value)), 0.0), _DEADLINE_SECONDS)
    except ValueError:
        return 0.5


def _provider_unavailable() -> GatewayError:
    return GatewayError(
        "PROVIDER_UNAVAILABLE",
        "Provider request failed within the bounded retry policy.",
        status_code=503,
        next_action="RETRY_LATER",
    )


def _is_transport_error(error: Exception) -> bool:
    """Classify only concrete local network/transport failures as transport."""
    return isinstance(
        error,
        (
            requests.exceptions.ConnectionError
            | requests.exceptions.Timeout
            | requests.exceptions.SSLError
            | requests.exceptions.ChunkedEncodingError
            | ssl.SSLError
        ),
    )


def _consume_future_exception(future: asyncio.Future[object]) -> None:
    """Mark a late SDK exception retrieved without logging or formatting it."""
    if not future.cancelled():
        future.exception()


def _required_str(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise _invalid_payload()
    return value


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise _invalid_payload()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise _invalid_payload() from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _invalid_payload()
    return parsed


def _bar_sdk_arguments(payload: Mapping[str, object]) -> dict[str, object]:
    code = _required_str(payload, "code")
    ktype = _required_str(payload, "ktype")
    limit = _required_int(payload, "limit")
    start = _optional_datetime(payload.get("start_time"))
    end = _optional_datetime(payload.get("end_time"))
    if (start is None) != (end is None) or (
        start is not None and end is not None and start > end
    ):
        raise _invalid_payload()
    arguments: dict[str, object] = {"code": code, "ktype": ktype, "num": limit}
    if ktype == "day" and end is not None:
        exclusive_date = end.astimezone(UTC).date() + timedelta(days=1)
        arguments["time"] = int(exclusive_date.strftime("%Y%m%d"))
    else:
        arguments["start_time"] = (
            None if start is None else int(start.timestamp() * 1000)
        )
        arguments["end_time"] = None if end is None else int(end.timestamp() * 1000)
    return arguments


def _required_int(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int):
        raise _invalid_payload()
    return value


def _required_strings(payload: Mapping[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, tuple | list) or not value:
        raise _invalid_payload()
    if not all(isinstance(item, str) and item for item in value):
        raise _invalid_payload()
    return tuple(cast("list[str] | tuple[str, ...]", value))


def _cursor(payload: Mapping[str, object]) -> int:
    value = payload.get("cursor")
    if value is None:
        return 0
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        raise _invalid_payload()
    return int(value)


def _invalid_payload() -> GatewayError:
    return GatewayError(
        "VALIDATION_ERROR",
        "Request payload is invalid.",
        status_code=422,
        next_action="FIX_REQUEST",
    )

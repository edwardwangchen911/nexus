"""Anti-corruption adapter for the frozen Fosun SDK v1.2.0 read surface."""

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from functools import partial
from typing import Protocol, cast

from nexus_fosun_gateway.adapter.normalization import (
    normalize_cash_summary_response,
    normalize_holdings_response,
    normalize_provider_response,
)
from nexus_fosun_gateway.domain import CapabilityItem, GatewayError, GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import READ_ONLY_OPERATIONS, GatewayOperation

_LOGGER = logging.getLogger("nexus_fosun_gateway.provider")
_MAX_ATTEMPTS = 3
_DEADLINE_SECONDS = 10.0


def _utc_now() -> datetime:
    return datetime.now(UTC)


class RateLimiter(Protocol):
    async def acquire(self) -> None: ...


class _TokenBucket:
    def __init__(
        self,
        capacity: float,
        refill_per_second: float,
        *,
        clock: Callable[[], float],
        sleeper: Callable[[float], Awaitable[None]],
    ) -> None:
        self._capacity = capacity
        self._refill_per_second = refill_per_second
        self._tokens = capacity
        self._updated_at = clock()
        self._clock = clock
        self._sleeper = sleeper
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = self._clock()
                elapsed = max(now - self._updated_at, 0.0)
                self._tokens = min(
                    self._capacity,
                    self._tokens + elapsed * self._refill_per_second,
                )
                self._updated_at = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait_for = (1.0 - self._tokens) / self._refill_per_second
            await self._sleeper(wait_for)


class DualTokenBucket:
    """Enforce a two-request burst and thirty-request minute budget."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._qps = _TokenBucket(2.0, 2.0, clock=clock, sleeper=sleeper)
        self._minute = _RollingWindowLimiter(
            30,
            60.0,
            clock=clock,
            sleeper=sleeper,
        )

    async def acquire(self) -> None:
        await self._qps.acquire()
        await self._minute.acquire()


class _RollingWindowLimiter:
    def __init__(
        self,
        limit: int,
        window_seconds: float,
        *,
        clock: Callable[[], float],
        sleeper: Callable[[float], Awaitable[None]],
    ) -> None:
        self._limit = limit
        self._window_seconds = window_seconds
        self._clock = clock
        self._sleeper = sleeper
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = self._clock()
                cutoff = now - self._window_seconds
                while self._timestamps and self._timestamps[0] <= cutoff:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._limit:
                    self._timestamps.append(now)
                    return
                wait_for = self._timestamps[0] + self._window_seconds - now
            await self._sleeper(wait_for)


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
        utc_clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if capability_expires_at is not None and (
            capability_expires_at.tzinfo is None or capability_expires_at.utcoffset() is None
        ):
            raise ValueError("activation expiry must be timezone-aware")
        self._client = client
        self._rate_limiter = rate_limiter or DualTokenBucket(clock=clock, sleeper=sleeper)
        self._sleeper = sleeper
        self._clock = clock
        self._capability_expires_at = capability_expires_at
        self._utc_clock = utc_clock
        self._concurrency = asyncio.Semaphore(1)
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="nexus-fosun-sdk",
        )

    async def execute(
        self,
        operation: GatewayOperation,
        payload: Mapping[str, object],
    ) -> GatewayPage[GatewayItem]:
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
        async with self._concurrency:
            if operation == "fosun_sync_accounts":
                raw = await self._invoke(
                    operation,
                    self._method("account", "list_accounts"),
                )
            elif operation == "fosun_sync_portfolio":
                return await self._portfolio(payload)
            elif operation == "fosun_list_orders":
                raw = await self._invoke(
                    operation,
                    self._method("trade", "list_orders"),
                    sub_account_id=_required_str(payload, "account_id"),
                    start=_cursor(payload),
                    count=_required_int(payload, "limit"),
                )
            elif operation == "fosun_list_cash_flows":
                raw = await self._invoke(
                    operation,
                    self._method("trade", "get_cash_flows"),
                    sub_account_id=_required_str(payload, "account_id"),
                    trade_date_from=_required_str(payload, "date_from"),
                    trade_date_to=_required_str(payload, "date_to"),
                )
            elif operation == "fosun_get_quotes":
                raw = await self._invoke(
                    operation,
                    self._method("market", "quote"),
                    codes=list(_required_strings(payload, "codes")),
                )
            elif operation == "fosun_get_bars":
                raw = await self._invoke(
                    operation,
                    self._method("market", "kline"),
                    code=_required_str(payload, "code"),
                    ktype=_required_str(payload, "ktype"),
                    start_time=_optional_timestamp_ms(payload.get("start_time")),
                    end_time=_optional_timestamp_ms(payload.get("end_time")),
                    num=_required_int(payload, "limit"),
                )
            else:
                raise AssertionError("unreachable operation")
        return normalize_provider_response(
            operation,
            raw,
            received_at=datetime.now(UTC),
            context=payload,
        )

    def _require_active_capability(self) -> None:
        if self._capability_expires_at is None:
            return
        now = self._utc_clock()
        if now.tzinfo is None or now.utcoffset() is None or now >= self._capability_expires_at:
            raise GatewayError(
                "ACTIVATION_EXPIRED",
                "Provider read-only activation has expired.",
                status_code=503,
                next_action="REFRESH_GUARDED_READ_ONLY_PROBE",
            )

    async def _portfolio(
        self,
        payload: Mapping[str, object],
    ) -> GatewayPage[GatewayItem]:
        account_id = _required_str(payload, "account_id")
        read_kind = _required_str(payload, "read_kind")
        if read_kind == "cash":
            raw = await self._invoke(
                "fosun_sync_portfolio",
                self._method("portfolio", "get_assets_summary"),
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
        **kwargs: object,
    ) -> object:
        deadline = self._clock() + _DEADLINE_SECONDS
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            await self._rate_limiter.acquire()
            # Queueing, rate-limit waits, or retry backoff can cross the approved
            # capability boundary. Recheck immediately before every provider call.
            self._require_active_capability()
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise GatewayError(
                    "PROVIDER_UNAVAILABLE",
                    "Provider request failed within the bounded retry policy.",
                    status_code=503,
                    next_action="RETRY_LATER",
                )
            _LOGGER.info(
                "provider_call",
                extra={"operation": operation, "attempt": attempt, "outcome": "started"},
            )
            try:
                loop = asyncio.get_running_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(self._executor, partial(method, **kwargs)),
                    timeout=remaining,
                )
            except TimeoutError:
                # A running Python thread cannot be cancelled safely. The dedicated
                # single-worker executor remains the execution boundary, and timeout
                # is terminal so no retry is queued behind a still-active SDK call.
                raise GatewayError(
                    "PROVIDER_UNAVAILABLE",
                    "Provider request failed within the bounded retry policy.",
                    status_code=503,
                    next_action="RETRY_LATER",
                ) from None
            except Exception as error:
                if type(error).__name__ == "PermissionError":
                    raise GatewayError(
                        "PROVIDER_PERMISSION_DENIED",
                        "Provider permission denied.",
                        status_code=502,
                        next_action="REVIEW_PROVIDER_PERMISSION",
                    ) from None
                delay = _retry_after(error)
                remaining = deadline - self._clock()
                if attempt == _MAX_ATTEMPTS or delay > remaining:
                    raise GatewayError(
                        "PROVIDER_UNAVAILABLE",
                        "Provider request failed within the bounded retry policy.",
                        status_code=503,
                        next_action="RETRY_LATER",
                    ) from None
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


def _required_str(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise _invalid_payload()
    return value


def _optional_timestamp_ms(value: object) -> int | None:
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
    return int(parsed.timestamp() * 1000)


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

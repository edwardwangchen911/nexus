"""Fixed-origin, bounded asynchronous HTTP byte transport."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from threading import RLock
from time import monotonic
from typing import Literal

import httpx

logger = logging.getLogger(__name__)


class _DropHttpDependencyRecord(logging.Filter):
    """Block dependency records whose URL/header formatting is not secret-safe."""

    def filter(self, record: logging.LogRecord) -> bool:
        del record
        return False


_HTTP_DEPENDENCY_NAMESPACES = ("httpx", "httpcore")
_HTTP_DEPENDENCY_FILTER = _DropHttpDependencyRecord()
_HTTP_DEPENDENCY_LOG_LOCK = RLock()

_HOST = re.compile(
    r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
_ENCODED_SEPARATOR = re.compile(r"%(?:2f|5c)", re.IGNORECASE)


class HttpPolicyError(RuntimeError):
    """A safe code for a rejected or exhausted provider request."""


@dataclass(frozen=True)
class AllowedOrigin:
    scheme: Literal["https"]
    host: str
    port: int = 443

    def __post_init__(self) -> None:
        if (
            self.scheme != "https"
            or self.host != self.host.lower()
            or _HOST.fullmatch(self.host) is None
            or self.port != 443
        ):
            raise ValueError("ORIGIN_INVALID")


@dataclass(frozen=True)
class HttpRequestPolicy:
    deadline_seconds: float = 30
    max_attempts: int = 3
    max_response_bytes: int = 8 * 1024 * 1024
    retry_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})
    base_backoff_seconds: float = 0.05

    def __post_init__(self) -> None:
        if (
            not 0 < self.deadline_seconds <= 60
            or not 1 <= self.max_attempts <= 3
            or not 1 <= self.max_response_bytes <= 8 * 1024 * 1024
            or not 0 <= self.base_backoff_seconds <= 1
            or any(status < 400 or status > 599 for status in self.retry_statuses)
        ):
            raise ValueError("HTTP_POLICY_INVALID")


class FixedOriginHttpClient:
    """Issue GETs to one immutable HTTPS origin with a single retry budget."""

    def __init__(
        self,
        *,
        provider_name: str,
        origin: AllowedOrigin,
        policy: HttpRequestPolicy,
        client: httpx.AsyncClient,
        monotonic_clock: Callable[[], float] = monotonic,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", provider_name) is None:
            raise ValueError("PROVIDER_NAME_INVALID")
        if client.follow_redirects:
            raise ValueError("REDIRECTS_MUST_BE_DISABLED")
        _enforce_http_dependency_log_boundary()
        self._provider_name = provider_name
        self._origin = origin
        self._policy = policy
        self._client = client
        self._clock = monotonic_clock
        self._sleeper = sleeper

    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes:
        """Return bounded bytes or raise one sanitized policy code."""
        # Common root/httpx DEBUG configuration must never expose a provider
        # query credential or signed header. Repeat at the request boundary in
        # case logging was reconfigured after client construction.
        _enforce_http_dependency_log_boundary()
        path = _canonical_path(path_segments)
        url = httpx.URL(
            scheme=self._origin.scheme,
            host=self._origin.host,
            port=self._origin.port,
            path=path,
        )
        started_at = self._clock()
        deadline = started_at + self._policy.deadline_seconds
        for attempt in range(1, self._policy.max_attempts + 1):
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise HttpPolicyError("DEADLINE_EXCEEDED")
            try:
                async with asyncio.timeout(remaining):
                    async with self._client.stream(
                        "GET",
                        url,
                        params=query,
                        headers=headers,
                    ) as response:
                        status = response.status_code
                        if 300 <= status <= 399:
                            self._log(attempt, status, "redirect_rejected", started_at)
                            raise HttpPolicyError("REDIRECT_REJECTED")
                        if status in self._policy.retry_statuses:
                            self._log(attempt, status, "retryable_status", started_at)
                            if attempt == self._policy.max_attempts:
                                raise HttpPolicyError("RETRY_BUDGET_EXHAUSTED")
                            await self._wait_before_retry(response, attempt, deadline)
                            continue
                        if not 200 <= status <= 299:
                            self._log(attempt, status, "status_rejected", started_at)
                            raise HttpPolicyError("STATUS_REJECTED")
                        payload = await self._read_bounded(response)
                        self._log(attempt, status, "ok", started_at)
                        return payload
            except HttpPolicyError:
                raise
            except TimeoutError:
                raise HttpPolicyError("DEADLINE_EXCEEDED") from None
            except httpx.TransportError:
                self._log(attempt, None, "transport_error", started_at)
                if attempt == self._policy.max_attempts:
                    raise HttpPolicyError("RETRY_BUDGET_EXHAUSTED") from None
                await self._sleep_bounded(
                    self._policy.base_backoff_seconds * (2 ** (attempt - 1)),
                    deadline,
                )
        raise HttpPolicyError("RETRY_BUDGET_EXHAUSTED")

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _read_bounded(self, response: httpx.Response) -> bytes:
        content_length = response.headers.get("content-length")
        if content_length is not None and (
            not content_length.isascii()
            or not content_length.isdecimal()
            or int(content_length) > self._policy.max_response_bytes
        ):
            raise HttpPolicyError("RESPONSE_TOO_LARGE")
        payload = bytearray()
        async for chunk in response.aiter_bytes():
            payload.extend(chunk)
            if len(payload) > self._policy.max_response_bytes:
                raise HttpPolicyError("RESPONSE_TOO_LARGE")
        return bytes(payload)

    async def _wait_before_retry(
        self,
        response: httpx.Response,
        attempt: int,
        deadline: float,
    ) -> None:
        retry_after = response.headers.get("retry-after")
        if retry_after is not None and retry_after.isascii() and retry_after.isdecimal():
            parsed = int(retry_after)
            delay = (
                float(parsed)
                if 0 <= parsed <= 5
                else self._policy.base_backoff_seconds * (2 ** (attempt - 1))
            )
        else:
            delay = self._policy.base_backoff_seconds * (2 ** (attempt - 1))
        await self._sleep_bounded(delay, deadline)

    async def _sleep_bounded(self, delay: float, deadline: float) -> None:
        remaining = deadline - self._clock()
        if remaining <= 0 or delay >= remaining:
            raise HttpPolicyError("DEADLINE_EXCEEDED")
        if delay > 0:
            await self._sleeper(delay)

    def _log(
        self,
        attempt: int,
        status: int | None,
        result: str,
        started_at: float,
    ) -> None:
        status_class = "transport" if status is None else f"{status // 100}xx"
        elapsed = max(0.0, self._clock() - started_at)
        elapsed_bucket = "lt_1s" if elapsed < 1 else "lt_5s" if elapsed < 5 else "gte_5s"
        logger.info(
            "provider_http_result provider=%s attempt=%d status_class=%s "
            "elapsed_bucket=%s result=%s",
            self._provider_name,
            attempt,
            status_class,
            elapsed_bucket,
            result,
        )


def _enforce_http_dependency_log_boundary() -> None:
    """Route httpx/httpcore diagnostics to a closed sink; use sanitized logs above."""
    with _HTTP_DEPENDENCY_LOG_LOCK:
        names = set(_HTTP_DEPENDENCY_NAMESPACES)
        names.update(
            name
            for name in logging.Logger.manager.loggerDict
            if isinstance(name, str)
            and any(
                name.startswith(f"{namespace}.")
                for namespace in _HTTP_DEPENDENCY_NAMESPACES
            )
        )
        for name in names:
            dependency_logger = logging.getLogger(name)
            if _HTTP_DEPENDENCY_FILTER not in dependency_logger.filters:
                dependency_logger.addFilter(_HTTP_DEPENDENCY_FILTER)
            if name in _HTTP_DEPENDENCY_NAMESPACES:
                if not any(
                    isinstance(handler, logging.NullHandler)
                    for handler in dependency_logger.handlers
                ):
                    dependency_logger.addHandler(logging.NullHandler())
                dependency_logger.propagate = False


def _canonical_path(segments: tuple[str, ...]) -> str:
    if not segments:
        raise HttpPolicyError("PATH_SEGMENT_INVALID")
    for segment in segments:
        if (
            not isinstance(segment, str)
            or not segment
            or not segment.isascii()
            or segment in {".", ".."}
            or any(character in segment for character in "/\\?#")
            or "://" in segment
            or _ENCODED_SEPARATOR.search(segment) is not None
            or any(ord(character) < 32 or ord(character) == 127 for character in segment)
        ):
            raise HttpPolicyError("PATH_SEGMENT_INVALID")
    return "/" + "/".join(segments)

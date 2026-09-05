"""Fixed-origin HTTP policy behavior contracts."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

import httpx
import pytest
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)


def _client(
    responder: httpx.MockTransport,
    *,
    max_attempts: int = 3,
    max_response_bytes: int = 64,
    deadline_seconds: float = 3,
    base_backoff_seconds: float = 0.05,
    monotonic_clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], Awaitable[None]] | None = None,
) -> FixedOriginHttpClient:
    return FixedOriginHttpClient(
        provider_name="fixture-provider",
        origin=AllowedOrigin(scheme="https", host="data.example.test", port=443),
        policy=HttpRequestPolicy(
            deadline_seconds=deadline_seconds,
            max_attempts=max_attempts,
            max_response_bytes=max_response_bytes,
            base_backoff_seconds=base_backoff_seconds,
        ),
        client=httpx.AsyncClient(transport=responder, follow_redirects=False),
        **({} if monotonic_clock is None else {"monotonic_clock": monotonic_clock}),
        **({} if sleeper is None else {"sleeper": sleeper}),
    )


def test_policy_error_metadata_accepts_only_nonnegative_integers() -> None:
    """Permissive metadata could carry sensitive exception text into an archive."""
    error = HttpPolicyError(
        "RESPONSE_TOO_LARGE",
        content_length="sensitive-header",  # type: ignore[arg-type]
        bytes_read_boundary=-1,
    )

    assert str(error) == "RESPONSE_TOO_LARGE"
    assert error.content_length is None
    assert error.bytes_read_boundary is None


@pytest.mark.asyncio
async def test_url_shaped_path_is_rejected_before_transport() -> None:
    """Removing segment validation would allow a caller-controlled origin/path."""
    transport = httpx.MockTransport(lambda request: pytest.fail(f"network called: {request}"))
    client = _client(transport)

    with pytest.raises(HttpPolicyError, match="PATH_SEGMENT_INVALID"):
        await client.get_bytes(("https://evil.test",), (), ())

    await client.aclose()


@pytest.mark.asyncio
async def test_redirect_is_never_followed_or_retried() -> None:
    """Treating redirects as retryable could send credentials to another origin."""
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(302, headers={"location": "https://evil.test"})

    client = _client(httpx.MockTransport(respond))

    with pytest.raises(HttpPolicyError, match="REDIRECT_REJECTED"):
        await client.get_bytes(("v1", "bars"), (), ())

    assert calls == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_retryable_status_uses_one_bounded_attempt_budget() -> None:
    """A broken retry counter must not exceed the configured provider budget."""
    statuses = iter((503, 503, 200))
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        status = next(statuses)
        return httpx.Response(status, content=b"ok" if status == 200 else b"unavailable")

    client = _client(httpx.MockTransport(respond), max_attempts=3)

    result = await client.get_bytes(("v1", "bars"), (("symbol", "AAPL"),), ())

    assert result == b"ok"
    assert calls == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_response_larger_than_policy_is_rejected() -> None:
    """Dropping the streaming byte cap could admit an unbounded provider body."""
    client = _client(
        httpx.MockTransport(lambda _: httpx.Response(200, content=b"12345")),
        max_response_bytes=4,
    )

    with pytest.raises(HttpPolicyError, match="RESPONSE_TOO_LARGE"):
        await client.get_bytes(("v1", "bars"), (), ())

    await client.aclose()


@pytest.mark.asyncio
async def test_logs_exclude_request_and_response_material(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Adding URL or body logging would expose secrets and account data."""
    client = _client(
        httpx.MockTransport(lambda _: httpx.Response(200, content=b"fixture-secret-response-body")),
        max_response_bytes=128,
    )
    caplog.set_level(logging.INFO, logger="nexus.providers.http")

    await client.get_bytes(
        ("private", "account"),
        (("api_key", "fixture-secret-query"),),
        (("Authorization", "fixture-secret-header"),),
    )

    rendered = caplog.text
    assert "provider=fixture-provider" in rendered
    assert "elapsed_bucket=lt_1s" in rendered
    assert "data.example.test" not in rendered
    assert "private" not in rendered
    assert "fixture-secret" not in rendered
    assert "Authorization" not in rendered
    await client.aclose()


@pytest.mark.parametrize(
    "segments",
    (
        (),
        ("",),
        (".",),
        ("..",),
        ("a/b",),
        ("a\\b",),
        ("a%2Fb",),
        ("a%5cb",),
        ("a?b",),
        ("a#b",),
        ("\x00",),
        ("中文",),
    ),
)
@pytest.mark.asyncio
async def test_ambiguous_path_segments_never_reach_transport(
    segments: tuple[str, ...],
) -> None:
    """Any path escape or ambiguous encoding must fail before an HTTP call."""
    client = _client(httpx.MockTransport(lambda request: pytest.fail(f"network called: {request}")))

    with pytest.raises(HttpPolicyError, match="PATH_SEGMENT_INVALID"):
        await client.get_bytes(segments, (), ())

    await client.aclose()


@pytest.mark.asyncio
async def test_non_retryable_status_is_attempted_once() -> None:
    """Retrying a definite client rejection would waste quota and obscure failure."""
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, content=b"sensitive-provider-error")

    client = _client(httpx.MockTransport(respond), max_attempts=3)

    with pytest.raises(HttpPolicyError, match="STATUS_REJECTED"):
        await client.get_bytes(("v1",), (), ())

    assert calls == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_retry_backoff_cannot_cross_absolute_deadline() -> None:
    """Resetting a deadline per attempt could keep a provider call alive indefinitely."""
    clock_value = 0.0
    sleeps: list[float] = []
    calls = 0

    def clock() -> float:
        return clock_value

    async def sleeper(seconds: float) -> None:
        nonlocal clock_value
        sleeps.append(seconds)
        clock_value += seconds

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    client = _client(
        httpx.MockTransport(respond),
        deadline_seconds=1.5,
        base_backoff_seconds=1,
        monotonic_clock=clock,
        sleeper=sleeper,
    )

    with pytest.raises(HttpPolicyError, match="DEADLINE_EXCEEDED"):
        await client.get_bytes(("v1",), (), ())

    assert calls == 2
    assert sleeps == [1]
    await client.aclose()


@pytest.mark.asyncio
async def test_out_of_policy_retry_after_uses_local_bounded_backoff() -> None:
    """An excessive Provider Retry-After must not bypass the local wait policy."""
    clock_value = 0.0
    sleeps: list[float] = []
    calls = 0

    def clock() -> float:
        return clock_value

    async def sleeper(seconds: float) -> None:
        nonlocal clock_value
        sleeps.append(seconds)
        clock_value += seconds

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"retry-after": "999"})
        return httpx.Response(200, content=b"ok")

    client = _client(
        httpx.MockTransport(respond),
        base_backoff_seconds=0.25,
        monotonic_clock=clock,
        sleeper=sleeper,
    )

    assert await client.get_bytes(("v1",), (), ()) == b"ok"
    assert sleeps == [0.25]
    await client.aclose()

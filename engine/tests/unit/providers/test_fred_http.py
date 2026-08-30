"""Exact FRED/ALFRED fixed-series transport contracts."""

from __future__ import annotations

import logging

import httpx
import pytest
from nexus.providers.fred import FredValidationError
from nexus.providers.fred_http import FredHttpTransport
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from pydantic import SecretStr

from tests.support.provider_fakes import RecordingFixedOriginClient


@pytest.mark.asyncio
async def test_real_fred_request_never_reaches_dependency_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "fred-log-secret-canary"
    observed_urls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        observed_urls.append(str(request.url))
        return httpx.Response(200, content=b"{}")

    fixed = FixedOriginHttpClient(
        provider_name="fred",
        origin=AllowedOrigin(scheme="https", host="api.stlouisfed.org"),
        policy=HttpRequestPolicy(max_attempts=1),
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(respond),
            follow_redirects=False,
        ),
    )
    transport = FredHttpTransport(fixed, SecretStr(secret))
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    assert await transport.get("/fred/series/observations?series_id=DFF") == b"{}"
    assert observed_urls and secret in observed_urls[0]
    rendered = caplog.text
    assert secret not in rendered
    assert "api_key=" not in rendered
    assert "api.stlouisfed.org" not in rendered
    assert "Authorization" not in rendered
    await fixed.aclose()


@pytest.mark.asyncio
async def test_fred_maps_one_approved_series_to_exact_external_query() -> None:
    """A changed FRED endpoint/query authority must fail before production use."""
    recorder = RecordingFixedOriginClient(payload=b"{}")
    transport = FredHttpTransport(recorder, SecretStr("fred-key-canary"))

    assert await transport.get("/fred/series/observations?series_id=DFF") == b"{}"

    assert recorder.calls[0].path_segments == ("fred", "series", "observations")
    assert recorder.calls[0].query == (
        ("api_key", "fred-key-canary"),
        ("file_type", "json"),
        ("series_id", "DFF"),
    )
    assert recorder.calls[0].header_names == ()


@pytest.mark.asyncio
async def test_alfred_requires_and_preserves_exact_vintage_window() -> None:
    """Dropping either vintage bound would permit ambiguous revised macro data."""
    recorder = RecordingFixedOriginClient(payload=b"{}")
    transport = FredHttpTransport(recorder, SecretStr("fred-key-canary"))

    await transport.get(
        "/alfred/series/observations?"
        "series_id=DGS10&realtime_start=2026-08-01&realtime_end=2026-08-31"
    )

    assert recorder.calls[0].query == (
        ("api_key", "fred-key-canary"),
        ("file_type", "json"),
        ("realtime_end", "2026-08-31"),
        ("realtime_start", "2026-08-01"),
        ("series_id", "DGS10"),
    )


@pytest.mark.parametrize(
    "path",
    (
        "/fred/series/observations?series_id=SP500",
        "/fred/series/observations?series_id=DFF&extra=x",
        "/fred/series/observations?series_id=DFF&series_id=DGS10",
        "/alfred/series/observations?series_id=DFF&realtime_start=2026-08-01",
        "/alfred/series/observations?series_id=DFF&realtime_start=2026-09-01&realtime_end=2026-08-01",
        "https://evil.test/fred/series/observations?series_id=DFF",
        "/fred/series/observations?series_id=DFF#fragment",
    ),
)
@pytest.mark.asyncio
async def test_fred_rejects_unapproved_or_ambiguous_queries_before_http(path: str) -> None:
    """A generic URL/query seam would bypass the frozen macro series authority."""
    recorder = RecordingFixedOriginClient(payload=b"{}")
    transport = FredHttpTransport(recorder, SecretStr("fred-key-canary"))

    with pytest.raises(FredValidationError):
        await transport.get(path)

    assert recorder.calls == []

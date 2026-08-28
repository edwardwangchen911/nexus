"""Exact SEC fixed-path transport contracts."""

from __future__ import annotations

import pytest
from nexus.providers.sec import SecProviderError
from nexus.providers.sec_http import SecHttpTransport

from tests.support.provider_fakes import RecordingFixedOriginClient

HEADERS = {
    "User-Agent": "NEXUS owner@example.com",
    "Accept-Encoding": "gzip, deflate",
}


@pytest.mark.asyncio
async def test_sec_maps_only_companyfacts_and_current_submissions_paths() -> None:
    """Changing SEC path segmentation could widen the transport beyond public data."""
    recorder = RecordingFixedOriginClient(payload=b"{}")
    transport = SecHttpTransport(recorder)

    await transport.get("/api/xbrl/companyfacts/CIK0000320193.json", HEADERS)
    await transport.get("/submissions/CIK0000320193.json", HEADERS)

    assert [call.path_segments for call in recorder.calls] == [
        ("api", "xbrl", "companyfacts", "CIK0000320193.json"),
        ("submissions", "CIK0000320193.json"),
    ]
    assert all(call.query == () for call in recorder.calls)
    assert recorder.calls[0].header_names == ("Accept-Encoding", "User-Agent")


@pytest.mark.asyncio
async def test_sec_allows_only_validated_submission_archive_filename() -> None:
    """Archive discovery must not become a generic data.sec.gov path proxy."""
    recorder = RecordingFixedOriginClient(payload=b"{}")
    transport = SecHttpTransport(recorder)

    await transport.get(
        "/submissions/CIK0000320193-submissions-001.json",
        HEADERS,
    )

    assert recorder.calls[0].path_segments == (
        "submissions",
        "CIK0000320193-submissions-001.json",
    )


@pytest.mark.parametrize(
    ("path", "headers"),
    (
        ("https://evil.test/a", HEADERS),
        ("/api/xbrl/frames/us-gaap.json", HEADERS),
        ("/submissions/../secret.json", HEADERS),
        ("/submissions/CIK0000320193-extra.json", HEADERS),
        ("/submissions/CIK0000320193.json", {"User-Agent": "NEXUS owner@example.com"}),
        (
            "/submissions/CIK0000320193.json",
            {**HEADERS, "Authorization": "secret"},
        ),
        (
            "/submissions/CIK0000320193.json",
            {**HEADERS, "User-Agent": "other client"},
        ),
    ),
)
@pytest.mark.asyncio
async def test_sec_rejects_unapproved_paths_and_header_shapes_before_http(
    path: str,
    headers: dict[str, str],
) -> None:
    """Caller-controlled path or headers must be rejected before credential-free GET."""
    recorder = RecordingFixedOriginClient(payload=b"{}")
    transport = SecHttpTransport(recorder)

    with pytest.raises(SecProviderError, match="SEC_PATH_NOT_ALLOWED"):
        await transport.get(path, headers)

    assert recorder.calls == []

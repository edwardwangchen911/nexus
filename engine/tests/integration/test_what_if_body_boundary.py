"""Raw request boundary tests for the Engine-only what-if endpoint."""

from uuid import UUID

import pytest
from nexus.api.errors import NexusError
from nexus.api.routes.optimization import _bounded_what_if_request
from starlette.requests import Request

_BODY = (
    b'{"proposal_group_id":"0198d500-1000-7000-8000-000000000111",'
    b'"proposal_id":"0198d500-1000-7000-8000-000000000112",'
    b'"policy_version":"optimizer_v1",'
    b'"positions":[{"instrument_id":"0198d500-1000-7000-8000-000000000001",'
    b'"weight":"0.2"}]}'
)


def _request(chunks: tuple[bytes, ...], headers: dict[str, str]) -> Request:
    iterator = iter((*chunks, b""))

    async def receive() -> dict[str, object]:
        chunk = next(iterator)
        return {"type": "http.request", "body": chunk, "more_body": bool(chunk)}

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/internal/v1/what-if/evaluate",
            "headers": [(key.lower().encode(), value.encode()) for key, value in headers.items()],
        },
        receive=receive,
    )


@pytest.mark.asyncio
async def test_bounded_reader_rejects_noncanonical_lengths_before_parsing() -> None:
    for content_length in ("-1", "+1", "01", " 1", "65537"):
        request = _request(
            (_BODY,),
            {"Content-Type": "application/json", "Content-Length": content_length},
        )
        with pytest.raises(NexusError) as error:
            await _bounded_what_if_request(request)
        assert error.value.status_code == 422
        assert not hasattr(request, "_body")


@pytest.mark.asyncio
async def test_bounded_reader_rejects_invalid_media_types_before_parsing() -> None:
    for media_type in ("text/plain", "application/problem+json", ""):
        request = _request((_BODY,), {"Content-Type": media_type})
        with pytest.raises(NexusError) as error:
            await _bounded_what_if_request(request)
        assert error.value.status_code == 422
        assert not hasattr(request, "_body")


@pytest.mark.asyncio
async def test_bounded_reader_rejects_one_large_or_cumulative_chunks_without_caching() -> None:
    oversized = b"x" * (64 * 1024 + 1)
    chunks: tuple[tuple[bytes, ...], ...] = (
        (oversized,),
        (b"x" * (64 * 1024), b"x"),
    )
    for stream in chunks:
        request = _request(stream, {"Content-Type": "application/json"})
        with pytest.raises(NexusError) as error:
            await _bounded_what_if_request(request)
        assert error.value.status_code == 422
        assert not hasattr(request, "_body")


@pytest.mark.asyncio
async def test_bounded_reader_accepts_json_charset_and_preserves_exact_raw_bytes() -> None:
    request = _request(
        (_BODY,),
        {"Content-Type": "application/json; charset=UTF-8", "Content-Length": str(len(_BODY))},
    )
    payload = await _bounded_what_if_request(request)
    assert payload.proposal_group_id == UUID("0198d500-1000-7000-8000-000000000111")
    assert await request.body() == _BODY

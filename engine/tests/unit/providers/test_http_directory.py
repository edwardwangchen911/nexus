import httpx
import pytest
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)


@pytest.mark.asyncio
async def test_explicit_directory_path_preserves_fixed_origin_and_slash():
    paths = []

    def handle(request):
        paths.append(str(request.url))
        return httpx.Response(200, content=b"bounded")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        bounded = FixedOriginHttpClient(
            provider_name="test",
            origin=AllowedOrigin(scheme="https", host="example.com"),
            policy=HttpRequestPolicy(max_attempts=1),
            client=client,
        )
        assert await bounded.get_bytes(("directory",), (), (), trailing_slash=True) == b"bounded"
        assert await bounded.get_bytes(("directory",), (), ()) == b"bounded"
    assert paths == ["https://example.com/directory/", "https://example.com/directory"]


@pytest.mark.asyncio
@pytest.mark.parametrize("segments", [("directory", ""), ("..",), ("https://other.com",)])
async def test_directory_flag_does_not_allow_invalid_path_segments(segments):
    def forbidden(request):
        pytest.fail("invalid path must not be contacted")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        bounded = FixedOriginHttpClient(
            provider_name="test",
            origin=AllowedOrigin(scheme="https", host="example.com"),
            policy=HttpRequestPolicy(max_attempts=1),
            client=client,
        )
        with pytest.raises(HttpPolicyError, match="PATH_SEGMENT_INVALID"):
            await bounded.get_bytes(segments, (), (), trailing_slash=True)

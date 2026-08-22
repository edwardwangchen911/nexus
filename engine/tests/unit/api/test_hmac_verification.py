from collections.abc import Mapping

import pytest
from fastapi import Request
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.api.auth import verify_hmac_v1
from nexus.api.errors import ErrorCode, NexusError
from nexus.settings import EngineSettings


class NoDatabaseAccess:
    """Raises if a rejected request reaches nonce storage."""

    def begin_nested(self) -> None:
        raise AssertionError("invalid authentication reached the database")


def make_request(
    method: str,
    path_and_query: str,
    headers: Mapping[str, str],
    body: bytes,
    *,
    raw_path: bytes | None = None,
    decoded_path: str | None = None,
) -> Request:
    path, _, query = path_and_query.partition("?")
    return Request(
        {
            "type": "http",
            "method": method,
            "path": decoded_path or path,
            "raw_path": raw_path or path.encode("ascii"),
            "query_string": query.encode("ascii"),
            "headers": [(key.lower().encode(), value.encode()) for key, value in headers.items()],
        }
    )


def hmac_settings() -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers, body",
    [
        (
            build_test_hmac_headers(
                "POST", "/internal/v1/test", b"{}", signature_override="invalid"
            ),
            b"{}",
        ),
        (build_test_hmac_headers("POST", "/internal/v1/test", b"{}", key_id="unknown"), b"{}"),
        (build_test_hmac_headers("POST", "/internal/v1/test", b"{}", timestamp=0), b"{}"),
        (build_test_hmac_headers("POST", "/internal/v1/test", b"{}"), b'{"modified":true}'),
    ],
)
async def test_rejected_hmac_never_reaches_nonce_storage(
    headers: dict[str, str], body: bytes
) -> None:
    """A bad key, time, body, or signature must not consume a nonce."""
    request = make_request("POST", "/internal/v1/test", headers, body)

    with pytest.raises(NexusError) as exc_info:
        await verify_hmac_v1(request, body, NoDatabaseAccess(), hmac_settings())  # type: ignore[arg-type]

    assert exc_info.value.code is ErrorCode.UNAUTHENTICATED
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_percent_encoded_raw_path_is_verified_without_framework_decoding() -> None:
    """Changing `%2F` to `/` before signing verification would authenticate a different target."""
    body = b""
    path_and_query = "/internal%2Fv1/system-status?space=+&space=%20&tag=%E4%B8%AD"
    headers = build_test_hmac_headers("GET", path_and_query, body)
    request = make_request(
        "GET",
        path_and_query,
        headers,
        body,
        raw_path=b"/internal%2Fv1/system-status",
        decoded_path="/internal/v1/system-status",
    )

    with pytest.raises(AssertionError, match="database"):
        await verify_hmac_v1(request, body, NoDatabaseAccess(), hmac_settings())  # type: ignore[arg-type]

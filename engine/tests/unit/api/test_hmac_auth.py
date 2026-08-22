import pytest
from nexus.api.auth import canonical_hmac_v1, sign_hmac_v1

HMAC_KEY = "nexus-test-hmac-key-32-bytes!!!!"


@pytest.mark.parametrize(
    ("method", "path_and_query", "body", "nonce", "expected_signature"),
    [
        (
            "POST",
            "/internal/v1/research-runs?instrument_id=abc&tag=a&tag=z",
            b'{"instrument_id":"abc"}',
            "0198f7a0-1111-7777-8111-111111111111",
            "AcCgtVoeHFg46x1y6ggnVthTrQwuBH1GoyulelJ-lYo",
        ),
        (
            "GET",
            "/internal/v1/system-status?tag=z&tag=a&instrument_id=abc",
            b"",
            "0198f7a0-1111-7777-8111-111111111112",
            "wtOOjfEZFK97l-HA0wHlkyZ9p0-rM7fL3S0NLk7t7js",
        ),
        (
            "POST",
            "/internal/v1/research-runs?z=%E4%B8%AD&a=%2F&a=%20",
            '{"note":"中文"}'.encode(),
            "0198f7a0-1111-7777-8111-111111111113",
            "FTX4MTbJapN28aKxVkVhDAfEVYuVRQKQl2F9wNKeQLE",
        ),
    ],
)
def test_hmac_v1_golden_vectors_are_byte_exact(
    method: str,
    path_and_query: str,
    body: bytes,
    nonce: str,
    expected_signature: str,
) -> None:
    """Changing query normalization, UTF-8 bytes, or empty-body hashing breaks interop."""
    canonical = canonical_hmac_v1(method, path_and_query, 1787371200, nonce, body)

    assert sign_hmac_v1(HMAC_KEY, canonical) == expected_signature
    assert canonical.endswith(
        {
            b"": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            b'{"instrument_id":"abc"}': (
                "5d2235c37b076326bed7b94bb8315b5141c2edc8b2a74a5e737146e2c20fc117"
            ),
            '{"note":"中文"}'.encode(): (
                "c7935850b16d3e9a0d211b542fcd1e181e13209ab365bb7d7e022d64f8738a8b"
            ),
        }[body]
    )

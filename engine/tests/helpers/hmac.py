"""Independent test fixtures for the public HMAC v1 vector."""

import base64
import hashlib
import hmac
import time
from urllib.parse import parse_qsl, quote, urlsplit

TEST_HMAC_KEY_ID = "test-current"
TEST_HMAC_SECRET = "nexus-test-hmac-key-32-bytes!!!!"
TEST_ACTOR_ID = "nexus-test-user"
TEST_REQUEST_ID = "0198f7a0-1111-7777-8111-111111111114"
TEST_NONCE = "0198f7a0-1111-7777-8111-111111111115"


def build_test_hmac_headers(
    method: str,
    path_and_query: str,
    body: bytes,
    *,
    key_id: str = TEST_HMAC_KEY_ID,
    nonce: str = TEST_NONCE,
    timestamp: int | None = None,
    signature_override: str | None = None,
) -> dict[str, str]:
    """Build an independently signed request; tests assert production behavior, not this helper."""
    issued_at = int(time.time()) if timestamp is None else timestamp
    parsed = urlsplit(path_and_query)
    pairs = [
        (quote(key, safe="~-._"), quote(value, safe="~-._"))
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    pairs.sort(key=lambda pair: (pair[0].encode("ascii"), pair[1].encode("ascii")))
    query = "&".join(f"{key}={value}" for key, value in pairs)
    normalized = parsed.path if not query else f"{parsed.path}?{query}"
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join((method.upper(), normalized, str(issued_at), nonce, body_hash))
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(TEST_HMAC_SECRET.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    return {
        "X-Nexus-Key-Id": key_id,
        "X-Nexus-Timestamp": str(issued_at),
        "X-Nexus-Nonce": nonce,
        "X-Nexus-Body-SHA256": body_hash,
        "X-Nexus-Signature": signature_override or signature,
        "X-Nexus-Actor-Id": TEST_ACTOR_ID,
        "X-Request-Id": TEST_REQUEST_ID,
    }

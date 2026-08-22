"""Gateway-local HMAC v1 verification with bounded in-process replay protection."""

import asyncio
import base64
import hashlib
import hmac
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import parse_qsl, quote, urlsplit
from uuid import UUID

from fastapi import Request

from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.settings import GatewaySettings

_SIGNATURE_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_MAX_HMAC_AGE_SECONDS = 60


def canonical_hmac_v1(
    method: str,
    path_and_query: str,
    timestamp: int,
    nonce: str,
    body: bytes,
) -> str:
    parsed = urlsplit(path_and_query)
    if not parsed.path.startswith("/") or not _valid_percent_escapes(path_and_query):
        raise ValueError("path_and_query must contain an absolute path")
    pairs = [
        (quote(key, safe="~-._"), quote(value, safe="~-._"))
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    pairs.sort(key=lambda pair: (pair[0].encode("ascii"), pair[1].encode("ascii")))
    query = "&".join(f"{key}={value}" for key, value in pairs)
    normalized = parsed.path if not query else f"{parsed.path}?{query}"
    return "\n".join(
        (method.upper(), normalized, str(timestamp), nonce, hashlib.sha256(body).hexdigest())
    )


@dataclass(frozen=True)
class VerifiedGatewayActor:
    actor_id: str
    request_id: UUID
    key_id: str


class ReplayCache:
    """Bound replay state for the single-process gateway deployment."""

    def __init__(self, max_entries: int = 4096, ttl_seconds: int = 60) -> None:
        self._entries: OrderedDict[tuple[str, str], float] = OrderedDict()
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._lock = asyncio.Lock()

    async def reserve(self, key_id: str, nonce: str, now: float) -> bool:
        async with self._lock:
            cutoff = now - self._ttl_seconds
            while self._entries and next(iter(self._entries.values())) < cutoff:
                self._entries.popitem(last=False)
            identity = (key_id, nonce)
            if identity in self._entries:
                return False
            self._entries[identity] = now
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
            return True


class GatewayHmacVerifier:
    def __init__(self, settings: GatewaySettings, replay_cache: ReplayCache | None = None) -> None:
        self._settings = settings
        self._replay_cache = replay_cache or ReplayCache()

    async def verify(self, request: Request, body: bytes) -> VerifiedGatewayActor:
        required = (
            "X-Nexus-Key-Id",
            "X-Nexus-Timestamp",
            "X-Nexus-Nonce",
            "X-Nexus-Body-SHA256",
            "X-Nexus-Signature",
            "X-Nexus-Actor-Id",
            "X-Request-Id",
        )
        values = {name: request.headers.get(name) for name in required}
        if any(value is None or value == "" for value in values.values()):
            raise _unauthenticated()
        timestamp = _parse_timestamp(values["X-Nexus-Timestamp"])
        now = time.time()
        if timestamp is None or abs(int(now) - timestamp) > _MAX_HMAC_AGE_SECONDS:
            raise _unauthenticated()
        nonce = _parse_uuid7(values["X-Nexus-Nonce"])
        request_id = _parse_uuid7(values["X-Request-Id"])
        if nonce is None or request_id is None:
            raise _unauthenticated()
        key_id = values["X-Nexus-Key-Id"]
        secret = self._verification_secret(key_id)
        if key_id is None or secret is None:
            raise _unauthenticated()
        supplied_hash = values["X-Nexus-Body-SHA256"]
        body_hash = hashlib.sha256(body).hexdigest()
        if supplied_hash is None or not hmac.compare_digest(supplied_hash, body_hash):
            raise _unauthenticated()
        signature = _decode_signature(values["X-Nexus-Signature"])
        if signature is None:
            raise _unauthenticated()
        path_and_query = _raw_path_and_query(request)
        if path_and_query is None:
            raise _unauthenticated()
        try:
            canonical = canonical_hmac_v1(
                request.method,
                path_and_query,
                timestamp,
                str(nonce),
                body,
            )
        except ValueError:
            raise _unauthenticated() from None
        expected = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise _unauthenticated()
        if not await self._replay_cache.reserve(key_id, str(nonce), now):
            raise _unauthenticated()
        actor_id = values["X-Nexus-Actor-Id"]
        if actor_id is None:
            raise _unauthenticated()
        return VerifiedGatewayActor(actor_id=actor_id, request_id=request_id, key_id=key_id)

    def _verification_secret(self, key_id: str | None) -> str | None:
        settings = self._settings
        current_secret = settings.gateway_hmac_current_secret
        if key_id == settings.gateway_hmac_current_key_id and current_secret is not None:
            return current_secret.get_secret_value()
        if (
            key_id == settings.gateway_hmac_previous_key_id
            and settings.gateway_hmac_previous_secret is not None
        ):
            return settings.gateway_hmac_previous_secret.get_secret_value()
        return None


def _parse_timestamp(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _parse_uuid7(value: str | None) -> UUID | None:
    try:
        parsed = UUID(value) if value is not None else None
    except ValueError:
        return None
    return parsed if parsed is not None and parsed.version == 7 else None


def _decode_signature(value: str | None) -> bytes | None:
    if value is None or _SIGNATURE_PATTERN.fullmatch(value) is None:
        return None
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except ValueError:
        return None


def _raw_path_and_query(request: Request) -> str | None:
    raw_path = request.scope.get("raw_path")
    raw_query = request.scope.get("query_string")
    if not isinstance(raw_path, bytes) or not isinstance(raw_query, bytes):
        return None
    try:
        path = raw_path.decode("ascii")
        query = raw_query.decode("ascii")
    except UnicodeDecodeError:
        return None
    if not path.startswith("/") or not _valid_percent_escapes(path + query):
        return None
    return path if not query else f"{path}?{query}"


def _valid_percent_escapes(value: str) -> bool:
    index = 0
    while index < len(value):
        if value[index] == "%":
            if index + 2 >= len(value) or any(
                char not in "0123456789abcdefABCDEF" for char in value[index + 1 : index + 3]
            ):
                return False
            index += 3
        else:
            index += 1
    return True


def _unauthenticated() -> GatewayError:
    return GatewayError(
        "UNAUTHENTICATED",
        "Service authentication failed.",
        status_code=401,
        next_action="REAUTHENTICATE",
    )

"""HMAC v1 verification for calls accepted from the same-origin BFF."""

import base64
import hashlib
import hmac
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import parse_qsl, quote, urlsplit
from uuid import UUID

from fastapi import Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.errors import ErrorCode, NexusError
from nexus.persistence.models.runtime import UsedNonce
from nexus.settings import EngineSettings

_SIGNATURE_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_MAX_HMAC_AGE_SECONDS = 60


@dataclass(frozen=True)
class VerifiedServiceActor:
    """Authenticated BFF identity bound to a verified service request."""

    actor_id: str
    request_id: UUID
    key_id: str


def canonical_hmac_v1(
    method: str,
    path_and_query: str,
    timestamp: int,
    nonce: str,
    body: bytes,
) -> str:
    """Return the UTF-8 HMAC v1 canonical form before it is signed."""
    parsed = urlsplit(path_and_query)
    if not parsed.path.startswith("/") or not _valid_percent_escapes(path_and_query):
        raise ValueError("path_and_query must contain an absolute path")
    query_pairs = [
        (quote(key, safe="~-._"), quote(value, safe="~-._"))
        for key, value in parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=False)
    ]
    query_pairs.sort(key=lambda pair: (pair[0].encode("ascii"), pair[1].encode("ascii")))
    normalized_query = "&".join(f"{key}={value}" for key, value in query_pairs)
    normalized_path_and_query = parsed.path
    if normalized_query:
        normalized_path_and_query = f"{normalized_path_and_query}?{normalized_query}"
    body_sha256 = hashlib.sha256(body).hexdigest()
    return "\n".join(
        (method.upper(), normalized_path_and_query, str(timestamp), nonce, body_sha256)
    )


def sign_hmac_v1(secret: str, canonical: str) -> str:
    """Sign a canonical HMAC v1 value as unpadded base64url."""
    digest = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


async def verify_hmac_v1(
    request: Request,
    body: bytes,
    session: AsyncSession,
    settings: EngineSettings,
) -> VerifiedServiceActor:
    """Validate a BFF call and reserve its nonce in the caller's transaction."""
    actor, nonce = verify_hmac_signature_v1(request, body, settings)
    await reserve_hmac_nonce(session, actor=actor, nonce=nonce)
    return actor


def verify_hmac_signature_v1(
    request: Request,
    body: bytes,
    settings: EngineSettings,
) -> tuple[VerifiedServiceActor, UUID]:
    """Validate HMAC headers without requiring a dependency connection."""
    headers = request.headers
    required = (
        "X-Nexus-Key-Id",
        "X-Nexus-Timestamp",
        "X-Nexus-Nonce",
        "X-Nexus-Body-SHA256",
        "X-Nexus-Signature",
        "X-Nexus-Actor-Id",
        "X-Request-Id",
    )
    values = {name: headers.get(name) for name in required}
    if any(value is None or value == "" for value in values.values()):
        raise _unauthenticated()

    timestamp = _parse_timestamp(values["X-Nexus-Timestamp"])
    if timestamp is None or abs(int(time.time()) - timestamp) > _MAX_HMAC_AGE_SECONDS:
        raise _unauthenticated()
    nonce = _parse_uuid7(values["X-Nexus-Nonce"])
    request_id = _parse_uuid7(values["X-Request-Id"])
    if nonce is None or request_id is None:
        raise _unauthenticated()
    actor_id = values["X-Nexus-Actor-Id"]
    if actor_id is None:
        raise _unauthenticated()

    key_id = values["X-Nexus-Key-Id"]
    secret = _verification_secret(key_id, settings)
    if key_id is None or secret is None:
        raise _unauthenticated()

    body_hash = hashlib.sha256(body).hexdigest()
    supplied_body_hash = values["X-Nexus-Body-SHA256"]
    if supplied_body_hash is None or not hmac.compare_digest(supplied_body_hash, body_hash):
        raise _unauthenticated()
    signature = _decode_signature(values["X-Nexus-Signature"])
    if signature is None:
        raise _unauthenticated()
    path_and_query = _raw_path_and_query(request)
    if path_and_query is None:
        raise _unauthenticated()
    try:
        canonical = canonical_hmac_v1(request.method, path_and_query, timestamp, str(nonce), body)
    except ValueError:
        raise _unauthenticated() from None
    expected = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).digest()
    if not hmac.compare_digest(signature, expected):
        raise _unauthenticated()

    return VerifiedServiceActor(actor_id=actor_id, request_id=request_id, key_id=key_id), nonce


async def reserve_hmac_nonce(
    session: AsyncSession,
    *,
    actor: VerifiedServiceActor,
    nonce: UUID,
) -> None:
    """Persist replay protection after signature verification succeeds."""
    try:
        async with session.begin_nested():
            session.add(UsedNonce(key_id=actor.key_id, nonce=str(nonce), used_at=datetime.now(UTC)))
            await session.flush()
    except IntegrityError as error:
        raise _unauthenticated() from error


def _verification_secret(key_id: str | None, settings: EngineSettings) -> str | None:
    if key_id == settings.engine_hmac_current_key_id and settings.engine_hmac_current_secret:
        return settings.engine_hmac_current_secret.get_secret_value()
    if key_id == settings.engine_hmac_previous_key_id and settings.engine_hmac_previous_secret:
        return settings.engine_hmac_previous_secret.get_secret_value()
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
    if (
        not path.startswith("/")
        or not _valid_percent_escapes(path)
        or not _valid_percent_escapes(query)
    ):
        return None
    return path if not query else f"{path}?{query}"


def _valid_percent_escapes(value: str) -> bool:
    index = 0
    while index < len(value):
        if value[index] == "%":
            if index + 2 >= len(value) or any(
                character not in "0123456789abcdefABCDEF"
                for character in value[index + 1 : index + 3]
            ):
                return False
            index += 3
        else:
            index += 1
    return True


def _unauthenticated() -> NexusError:
    return NexusError(
        ErrorCode.UNAUTHENTICATED,
        "Service authentication failed.",
        status_code=401,
    )

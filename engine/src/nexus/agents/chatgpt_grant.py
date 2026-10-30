"""Existing NEXUS app grant: protected storage, signed identity and one-shot refresh.

No registration, reconsent, model calls, paid fallback or implicit refresh.
The caller must be authorized to rotate credentials before invoking refresh_once.
"""

from __future__ import annotations

import asyncio
import base64
import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

ISSUER = "https://auth.openai.com"
RESOURCE = "https://api.openai.com/v1"
JWKS_URL = ISSUER + "/.well-known/jwks.json"
TOKEN_URL = ISSUER + "/api/accounts/oauth/token"
_REQUIRED_SCOPES = frozenset({"resource.invoke", "chatgpt.tokens.use.direct", "offline_access"})
_LIMIT = 128 * 1024
_ERRORS = frozenset(
    {
        "invalid_grant",
        "invalid_client",
        "invalid_refresh_token",
        "token_expired",
        "refresh_token_expired",
        "refresh_token_invalidated",
        "refresh_token_reused",
    }
)


class GrantError(ValueError):
    """Fixed machine codes only; never retains responses, request bodies or secrets."""

    def __init__(self, code: str, *, http_status: int | None = None, phase: str | None = None):
        super().__init__(code)
        self.http_status = http_status
        self.phase = phase


def _retained_error(error: BaseException) -> GrantError | None:
    """Resource __aexit__ failures must not erase an earlier safe rejection."""
    seen: set[int] = set()
    current: BaseException | None = error
    for _ in range(8):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        if isinstance(current, GrantError):
            return current
        current = current.__cause__ or current.__context__
    return None


def _json(raw: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise GrantError("CHATGPT_JSON_INVALID")
            result[key] = value
        return result

    try:
        if len(raw) > _LIMIT:
            raise ValueError
        value = json.loads(raw, object_pairs_hook=unique)
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, RecursionError):
        raise GrantError("CHATGPT_JSON_INVALID") from None


def _text(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > _LIMIT or not value.isascii():
        raise GrantError("CHATGPT_CREDENTIAL_INVALID")
    if any(ord(c) < 33 or ord(c) > 126 for c in value):
        raise GrantError("CHATGPT_CREDENTIAL_INVALID")
    return value


def _number(value: Any) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise GrantError("CHATGPT_TIME_INVALID")
    return float(value)


def _decode(part: str) -> bytes:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", part):
        raise GrantError("CHATGPT_SIGNATURE_INVALID")
    try:
        return base64.b64decode(part + "=" * (-len(part) % 4), altchars=b"-_", validate=True)
    except ValueError:
        raise GrantError("CHATGPT_SIGNATURE_INVALID") from None


def _signed(token: str, jwks: Mapping[str, Any]) -> dict[str, Any]:
    try:
        head, body, sig = token.split(".")
        header = _json(_decode(head))
        if header.get("alg") != "RS256" or "crit" in header:
            raise ValueError
        kid = _text(header.get("kid"))
        keys = jwks.get("keys")
        if not isinstance(keys, list):
            raise ValueError
        matches = [k for k in keys if isinstance(k, dict) and k.get("kid") == kid]
        if len(matches) != 1:
            raise ValueError
        key = matches[0]
        if key.get("kty") != "RSA" or key.get("alg", "RS256") != "RS256":
            raise ValueError
        if key.get("use", "sig") != "sig" or key.get("key_ops", ["verify"]) != ["verify"]:
            raise ValueError
        public = rsa.RSAPublicNumbers(
            int.from_bytes(_decode(key["e"])), int.from_bytes(_decode(key["n"]))
        ).public_key()
        if not 2048 <= public.key_size <= 8192:
            raise ValueError
        public.verify(
            _decode(sig), (head + "." + body).encode(), padding.PKCS1v15(), hashes.SHA256()
        )
        return _json(_decode(body))
    except (KeyError, TypeError, ValueError, InvalidSignature):
        raise GrantError("CHATGPT_SIGNATURE_INVALID") from None


@dataclass(frozen=True, repr=False)
class Grant:
    _record: dict[str, Any]
    expires_at: float
    not_before: float
    identity_verified: bool = True

    def expired(self, now: datetime) -> bool:
        return now.timestamp() >= self.expires_at

    def bearer(self, now: datetime) -> str:
        if now.timestamp() < self.not_before:
            raise GrantError("CHATGPT_TIME_INVALID")
        if self.expired(now):
            raise GrantError("CHATGPT_ACCESS_EXPIRED")
        return "Bearer " + _text(self._record["access_token"])


def _validate(data: dict[str, Any], jwks: Mapping[str, Any], now: datetime) -> Grant:
    client = _text(data.get("client_id"))
    subject = _text(data.get("subject"))
    if not client.startswith("oaiapp_") or data.get("issuer") != ISSUER:
        raise GrantError("CHATGPT_REGISTRATION_INVALID")
    _text(data.get("ext_agent_host_id"))
    _text(data.get("refresh_token"))
    if data.get("token_type") != "Bearer":
        raise GrantError("CHATGPT_CREDENTIAL_INVALID")
    scopes = data.get("scopes")
    if not isinstance(scopes, list) or not all(isinstance(s, str) for s in scopes):
        raise GrantError("CHATGPT_SCOPE_REQUIRED")
    if not _REQUIRED_SCOPES.issubset(scopes):
        raise GrantError("CHATGPT_SCOPE_REQUIRED")
    access = _signed(_text(data.get("access_token")), jwks)
    identity = _signed(_text(data.get("id_token")), jwks)
    for claims, audience in ((access, RESOURCE), (identity, client)):
        aud = claims.get("aud")
        if aud != audience and aud != [audience]:
            raise GrantError("CHATGPT_IDENTITY_MISMATCH")
        if claims.get("iss") != ISSUER or claims.get("sub") != subject:
            raise GrantError("CHATGPT_IDENTITY_MISMATCH")
        iat, exp = _number(claims.get("iat")), _number(claims.get("exp"))
        if (
            not iat < exp
            or iat > now.timestamp()
            or _number(claims.get("nbf", iat)) > now.timestamp()
        ):
            raise GrantError("CHATGPT_TIME_INVALID")
    # A retained expired ID token proves the signed historical identity, not a
    # current login. Current inference additionally requires unexpired access.
    if access.get("client_id") != client or set(str(access.get("scope", "")).split()) != set(
        scopes
    ):
        raise GrantError("CHATGPT_SCOPE_OR_CLIENT_MISMATCH")
    saved = _number(data.get("saved_at_unix"))
    duration = _number(data.get("expires_in"))
    if saved > now.timestamp() or duration <= 0:
        raise GrantError("CHATGPT_TIME_INVALID")
    _number(data.get("earliest_refresh_at", 0))
    return Grant(
        data,
        min(_number(access["exp"]), saved + duration),
        max(_number(access["iat"]), _number(access.get("nbf", access["iat"]))),
    )


def _secure(fd: int, *, directory: bool = False) -> None:
    info = os.fstat(fd)
    valid = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if (
        not valid
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != (0o700 if directory else 0o600)
    ):
        raise GrantError("CHATGPT_STORAGE_UNSAFE")
    if not directory and (info.st_nlink != 1 or info.st_size > _LIMIT):
        raise GrantError("CHATGPT_STORAGE_UNSAFE")


def _read(fd: int, name: str) -> bytes:
    try:
        with os.fdopen(
            os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd), "rb"
        ) as file:
            _secure(file.fileno())
            return file.read(_LIMIT + 1)
    except OSError:
        raise GrantError("CHATGPT_STORAGE_UNAVAILABLE") from None


def _marker(data: Mapping[str, Any]) -> str:
    return (
        ".refresh-attempt-" + hashlib.sha256(_text(data.get("refresh_token")).encode()).hexdigest()
    )


def _unused(fd: int, data: Mapping[str, Any]) -> None:
    try:
        os.stat(_marker(data), dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise GrantError("CHATGPT_REFRESH_ALREADY_ATTEMPTED")


def _receipt(
    fd: int, name: str, reason: str, phase: str, status: int | None, committed: bool
) -> None:
    # Best-effort diagnostic; the already-fsynced intent remains the fence even
    # if this write fails. Never overwrite an earlier outcome or store prose.
    try:
        out_fd = os.open(
            name + ".outcome.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd
        )
        with os.fdopen(out_fd, "w") as out:
            json.dump(
                {
                    "reason": reason,
                    "phase": phase,
                    "http_status": status,
                    "credential_commit_confirmed": committed,
                },
                out,
            )
            out.flush()
            os.fsync(out.fileno())
        os.fsync(fd)
    except OSError:
        pass


async def _response(
    client: httpx.AsyncClient, method: str, url: str, data: dict[str, str] | None = None
) -> dict[str, Any]:
    async with client.stream(method, url, data=data) as response:
        raw = bytearray()
        async for part in response.aiter_bytes():
            if len(raw) + len(part) > _LIMIT:
                raise GrantError("CHATGPT_RESPONSE_TOO_LARGE")
            raw.extend(part)
        if response.status_code != 200:
            code = None
            try:
                code = _json(bytes(raw)).get("error")
            except GrantError:
                pass
            if method == "POST" and isinstance(code, str) and code in _ERRORS:
                raise GrantError(
                    "CHATGPT_REFRESH_" + code.upper(),
                    http_status=response.status_code,
                    phase="jwks" if method == "GET" else "token",
                )
            raise GrantError(
                "CHATGPT_HTTP_REJECTED",
                http_status=response.status_code,
                phase="jwks" if method == "GET" else "token",
            )
        return _json(bytes(raw))


class GrantStore:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    @contextmanager
    def _directory(self) -> Iterator[int]:
        try:
            fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError:
            raise GrantError("CHATGPT_STORAGE_UNAVAILABLE") from None
        try:
            _secure(fd, directory=True)
            yield fd
        finally:
            os.close(fd)

    def _load(self, fd: int, *, jwks: Mapping[str, Any], now: datetime) -> tuple[Grant, bytes]:
        raw = _read(fd, "credentials.json")
        data = _json(raw)
        host = _json(_read(fd, "host.json"))
        if data.get("ext_agent_host_id") != host.get("ext_agent_host_id"):
            raise GrantError("CHATGPT_HOST_MISMATCH")
        registrations = [
            _json(_read(fd, name))
            for name in os.listdir(fd)
            if re.fullmatch(r"registration-\d+\.json", name)
        ]
        if not any(
            all(r.get(k) == data.get(k) for k in ("client_id", "ext_agent_host_id"))
            for r in registrations
        ):
            raise GrantError("CHATGPT_REGISTRATION_REQUIRED")
        _unused(fd, data)
        return _validate(data, jwks, now), raw

    def read(self, *, jwks: Mapping[str, Any], now: datetime) -> Grant:
        with self._directory() as fd:
            return self._load(fd, jwks=jwks, now=now)[0]

    async def inspect(
        self, *, clock: Callable[[], datetime], transport: httpx.AsyncBaseTransport | None = None
    ) -> dict[str, Any]:
        grant = await self.load_verified(clock=clock, transport=transport)
        return {
            "status": "refresh_required" if grant.expired(clock()) else "access_ready",
            "signature_verified": True,
            "inspection_only": True,
            "dispatch_authority": False,
            "model_calls": 0,
            "credential_writes": 0,
        }

    async def load_verified(
        self, *, clock: Callable[[], datetime], transport: httpx.AsyncBaseTransport | None = None
    ) -> Grant:
        """Read the existing signed grant; never register, rotate or refresh it."""
        try:
            async with (
                asyncio.timeout(10),
                httpx.AsyncClient(
                    transport=transport,
                    timeout=10,
                    follow_redirects=False,
                    trust_env=True,
                    auth=None,
                ) as client,
            ):
                jwks = await _response(client, "GET", JWKS_URL)
                grant = self.read(jwks=jwks, now=clock())
            return grant
        except GrantError:
            raise
        except Exception as error:
            retained = _retained_error(error)
            if retained is not None:
                raise retained from None
            raise GrantError("CHATGPT_INSPECTION_UNAVAILABLE") from None

    async def refresh_once(
        self, *, clock: Callable[[], datetime], transport: httpx.AsyncBaseTransport | None = None
    ) -> Grant:
        """Explicit rotation only. Persist intent before POST; ambiguous attempts never replay."""
        with self._directory() as fd:
            lock = os.open(
                "credentials.refresh-lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=fd
            )
            attempt = None
            phase = "preflight"
            reason = "CHATGPT_REFRESH_RESULT_UNKNOWN"
            status = None
            committed = False
            try:
                _secure(lock)
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise GrantError("CHATGPT_REFRESH_BUSY") from None
                # Check spent identity before network work. Legacy attempt markers
                # and the original app lock namespace remain authoritative.
                _unused(fd, _json(_read(fd, "credentials.json")))
                async with (
                    asyncio.timeout(10),
                    httpx.AsyncClient(
                        transport=transport,
                        timeout=10,
                        follow_redirects=False,
                        trust_env=True,
                        auth=None,
                    ) as client,
                ):
                    phase = "jwks"
                    jwks = await _response(client, "GET", JWKS_URL)
                    phase = "validate_existing"
                    old, raw = self._load(fd, jwks=jwks, now=clock())
                    if clock().timestamp() < _number(old._record.get("earliest_refresh_at", 0)):
                        raise GrantError("CHATGPT_REFRESH_TOO_EARLY")
                    if not old.expired(clock()):
                        raise GrantError("CHATGPT_REFRESH_NOT_REQUIRED")
                    marker = os.open(
                        _marker(old._record), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd
                    )
                    attempt = _marker(old._record)
                    with os.fdopen(marker, "wb") as out:
                        out.write(b'{"phase":"dispatch_intent","automatic_replay":false}\n')
                        out.flush()
                        os.fsync(out.fileno())
                    os.fsync(fd)
                    phase = "token"
                    response = await _response(
                        client,
                        "POST",
                        TOKEN_URL,
                        {
                            "grant_type": "refresh_token",
                            "client_id": old._record["client_id"],
                            "refresh_token": old._record["refresh_token"],
                            "resource": RESOURCE,
                        },
                    )
                    phase = "validate_replacement"
                    new = dict(old._record)
                    if not isinstance(response.get("scope"), str):
                        raise GrantError("CHATGPT_SCOPE_REQUIRED")
                    new.update(
                        access_token=response.get("access_token"),
                        refresh_token=response.get("refresh_token"),
                        token_type=response.get("token_type"),
                        expires_in=response.get("expires_in"),
                        scopes=response["scope"].split(),
                        saved_at_unix=clock().timestamp(),
                        earliest_refresh_at=response.get("earliest_refresh_at", 0),
                    )
                    if "id_token" in response:
                        new["id_token"] = response["id_token"]
                    if not set(new["scopes"]).issubset(old._record["scopes"]):
                        raise GrantError("CHATGPT_SCOPE_CHANGED")
                    if new["refresh_token"] == old._record["refresh_token"]:
                        raise GrantError("CHATGPT_ROTATION_INVALID")
                    grant = _validate(new, jwks, clock())
                    if grant.expired(clock()):
                        raise GrantError("CHATGPT_ACCESS_EXPIRED")
                    if (
                        "id_token" in response
                        and _number(_signed(new["id_token"], jwks)["exp"]) <= clock().timestamp()
                    ):
                        raise GrantError("CHATGPT_ID_TOKEN_EXPIRED")
                    phase = "persist"
                    self._replace(fd, raw, new)
                    committed = True
                    phase = "cleanup"
                phase = "complete"
                reason = "CHATGPT_REFRESH_COMPLETED"
                return grant
            except asyncio.CancelledError as error:
                retained = _retained_error(error)
                reason = "CHATGPT_REFRESH_CANCELLED" if retained is None else str(retained)
                if retained is not None:
                    status, phase = retained.http_status, retained.phase or phase
                raise
            except Exception as error:
                retained = _retained_error(error)
                if retained is None:
                    reason = (
                        "CHATGPT_STORAGE_WRITE_UNCONFIRMED"
                        if isinstance(error, OSError) and not isinstance(error, TimeoutError)
                        else reason
                    )
                    retained = GrantError(reason, phase=phase)
                reason, status = str(retained), retained.http_status
                phase = retained.phase or phase
                retained.phase = phase
                raise retained from None
            finally:
                if attempt is not None:
                    _receipt(fd, attempt, reason, phase, status, committed)
                os.close(lock)

    def _replace(self, fd: int, original: bytes, record: dict[str, Any]) -> None:
        if _read(fd, "credentials.json") != original:
            raise GrantError("CHATGPT_CREDENTIAL_CHANGED")
        name = ".credentials-" + secrets.token_hex(12)
        temp = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd)
        try:
            with os.fdopen(temp, "wb") as out:
                out.write(json.dumps(record, sort_keys=True, allow_nan=False).encode())
                out.flush()
                os.fsync(out.fileno())
            if _read(fd, "credentials.json") != original:
                raise GrantError("CHATGPT_CREDENTIAL_CHANGED")
            os.replace(name, "credentials.json", src_dir_fd=fd, dst_dir_fd=fd)
            os.fsync(fd)
        finally:
            try:
                os.unlink(name, dir_fd=fd)
            except FileNotFoundError:
                pass

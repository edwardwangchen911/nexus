"""Protected local configuration and OAuth primitives for Schwab market streaming."""

from __future__ import annotations

import json
import os
import secrets
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx
from dotenv import dotenv_values

from nexus.providers.market.schwab_stream import (
    SchwabStreamCredentials,
    SchwabStreamError,
    validate_streamer_url,
)

_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
_USER_PREFERENCE_URL = "https://api.schwabapi.com/trader/v1/userPreference"
_MAX_TOKEN_RESPONSE_BYTES = 256 * 1024
_TOKEN_SCHEMA = "nexus_schwab_oauth_token_v1"
_MAX_TOKEN_FILE_BYTES = 32 * 1024


class SchwabOAuthError(ValueError):
    """Stable, value-free Schwab OAuth error."""


@dataclass(frozen=True)
class SchwabAppConfig:
    app_key: str = field(repr=False)
    app_secret: str = field(repr=False)
    callback_url: str

    def __post_init__(self) -> None:
        if (
            not _printable(self.app_key, 512)
            or not _printable(self.app_secret, 512)
            or self.callback_url != "https://127.0.0.1"
        ):
            raise SchwabOAuthError("SCHWAB_APP_CONFIG_INVALID")


@dataclass(frozen=True)
class SchwabOAuthToken:
    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    obtained_at: datetime
    expires_at: datetime


def load_schwab_app_config(
    repo_root: Path, process_environment: Mapping[str, str]
) -> SchwabAppConfig:
    """Load static app credentials from the ignored project configuration."""
    values = {
        name: value
        for name, value in dotenv_values(
            repo_root.resolve() / "infra" / ".env.local"
        ).items()
        if isinstance(value, str) and value
    }
    values.update({name: value for name, value in process_environment.items() if value})
    try:
        return SchwabAppConfig(
            app_key=values["NEXUS_SCHWAB_APP_KEY"],
            app_secret=values["NEXUS_SCHWAB_APP_SECRET"],
            callback_url=values["NEXUS_SCHWAB_CALLBACK_URL"],
        )
    except KeyError:
        raise SchwabOAuthError("SCHWAB_APP_CONFIG_REQUIRED") from None


def build_schwab_authorization_url(config: SchwabAppConfig) -> str:
    query = urlencode(
        {"client_id": config.app_key, "redirect_uri": config.callback_url}
    )
    return f"https://api.schwabapi.com/v1/oauth/authorize?{query}"


def parse_schwab_callback(config: SchwabAppConfig, callback: str) -> str:
    try:
        parsed = urlsplit(callback)
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except (TypeError, ValueError):
        raise SchwabOAuthError("SCHWAB_OAUTH_CALLBACK_INVALID") from None
    if (
        parsed.scheme != "https"
        or parsed.hostname != "127.0.0.1"
        or parsed.port not in {None, 443}
        or parsed.path not in {"", "/"}
        or parsed.fragment
        or parsed.username
        or parsed.password
        or config.callback_url != "https://127.0.0.1"
        or len([value for name, value in pairs if name == "code" and value]) != 1
        or any(name not in {"code", "session"} for name, _value in pairs)
    ):
        raise SchwabOAuthError("SCHWAB_OAUTH_CALLBACK_INVALID")
    return next(value for name, value in pairs if name == "code")


async def exchange_schwab_authorization_code(
    config: SchwabAppConfig,
    code: str,
    client: httpx.AsyncClient,
    *,
    now: datetime | None = None,
) -> SchwabOAuthToken:
    """Exchange one authorization code once; never expose provider error bodies."""
    if not _printable(code, 8192):
        raise SchwabOAuthError("SCHWAB_OAUTH_CODE_INVALID")
    obtained_at = now or datetime.now(UTC)
    if obtained_at.tzinfo is None or obtained_at.utcoffset() is None:
        raise SchwabOAuthError("SCHWAB_OAUTH_TIME_INVALID")
    try:
        response = await client.post(
            _TOKEN_URL,
            auth=httpx.BasicAuth(config.app_key, config.app_secret),
            headers={"Accept": "application/json"},
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": config.callback_url,
            },
            follow_redirects=False,
            timeout=10,
        )
    except httpx.HTTPError:
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_REQUEST_FAILED") from None
    if response.status_code != 200:
        raise SchwabOAuthError(f"SCHWAB_OAUTH_TOKEN_HTTP_{response.status_code}")
    if len(response.content) > _MAX_TOKEN_RESPONSE_BYTES:
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_RESPONSE_INVALID")
    try:
        payload = json.loads(response.content, object_pairs_hook=_unique_json)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_RESPONSE_INVALID") from None
    if not isinstance(payload, dict):
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_RESPONSE_INVALID")
    access_token = payload.get("access_token")
    refresh_token = payload.get("refresh_token")
    expires_in = payload.get("expires_in")
    if (
        not isinstance(access_token, str)
        or not _printable(access_token, 8192)
        or not isinstance(refresh_token, str)
        or not _printable(refresh_token, 8192)
        or type(expires_in) is not int
        or not 1 <= expires_in <= 86_400
        or payload.get("token_type") != "Bearer"
    ):
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_RESPONSE_INVALID")
    return SchwabOAuthToken(
        access_token=access_token,
        refresh_token=refresh_token,
        obtained_at=obtained_at,
        expires_at=obtained_at + timedelta(seconds=expires_in),
    )


def save_schwab_token(repo_root: Path, token: SchwabOAuthToken) -> Path:
    directory = repo_root.resolve() / ".secrets" / "nexus-schwab-local"
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    if (
        directory.is_symlink()
        or not directory.is_dir()
        or stat.S_IMODE(directory.stat().st_mode) != 0o700
    ):
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_STORE_INVALID")
    path = directory / "tokens.json"
    payload = json.dumps(
        {
            "schema": _TOKEN_SCHEMA,
            "access_token": token.access_token,
            "refresh_token": token.refresh_token,
            "obtained_at": token.obtained_at.isoformat(),
            "expires_at": token.expires_at.isoformat(),
        },
        sort_keys=True,
    ).encode()
    temporary = directory / f".tokens-{os.getpid()}-{secrets.token_hex(8)}.tmp"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        with os.fdopen(descriptor, "wb") as output:
            descriptor = None
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except OSError:
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_STORE_INVALID") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return path


def load_schwab_token(repo_root: Path) -> SchwabOAuthToken:
    directory = repo_root.resolve() / ".secrets" / "nexus-schwab-local"
    path = directory / "tokens.json"
    if (
        directory.is_symlink()
        or not directory.is_dir()
        or stat.S_IMODE(directory.stat().st_mode) != 0o700
        or path.is_symlink()
        or not path.is_file()
        or stat.S_IMODE(path.stat().st_mode) != 0o600
        or path.stat().st_size > _MAX_TOKEN_FILE_BYTES
    ):
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_STORE_INVALID")
    try:
        payload = json.loads(path.read_bytes(), object_pairs_hook=_unique_json)
        if not isinstance(payload, dict) or set(payload) != {
            "schema",
            "access_token",
            "refresh_token",
            "obtained_at",
            "expires_at",
        }:
            raise ValueError
        token = SchwabOAuthToken(
            access_token=payload["access_token"],
            refresh_token=payload["refresh_token"],
            obtained_at=datetime.fromisoformat(payload["obtained_at"]),
            expires_at=datetime.fromisoformat(payload["expires_at"]),
        )
    except (KeyError, TypeError, ValueError, UnicodeDecodeError):
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_STORE_INVALID") from None
    if (
        payload["schema"] != _TOKEN_SCHEMA
        or not _printable(token.access_token, 8192)
        or not _printable(token.refresh_token, 8192)
        or token.obtained_at.tzinfo is None
        or token.obtained_at.utcoffset() is None
        or token.expires_at.tzinfo is None
        or token.expires_at.utcoffset() is None
        or token.expires_at <= token.obtained_at
    ):
        raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_STORE_INVALID")
    return token


async def fetch_schwab_stream_preferences(
    token: SchwabOAuthToken,
    client: httpx.AsyncClient,
    *,
    now: datetime | None = None,
) -> tuple[str, SchwabStreamCredentials]:
    """Fetch only the short-lived fields needed for a market-data stream login."""
    checked_at = now or datetime.now(UTC)
    if (
        checked_at.tzinfo is None
        or checked_at.utcoffset() is None
        or checked_at >= token.expires_at
    ):
        raise SchwabOAuthError("SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED")
    try:
        response = await client.get(
            _USER_PREFERENCE_URL,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token.access_token}",
            },
            follow_redirects=False,
            timeout=10,
        )
    except httpx.HTTPError:
        raise SchwabOAuthError("SCHWAB_USER_PREFERENCE_REQUEST_FAILED") from None
    if response.status_code != 200 or len(response.content) > _MAX_TOKEN_RESPONSE_BYTES:
        raise SchwabOAuthError("SCHWAB_USER_PREFERENCE_REQUEST_FAILED")
    try:
        payload = json.loads(response.content, object_pairs_hook=_unique_json)
        streamer_info = payload["streamerInfo"]
        if not isinstance(streamer_info, list) or len(streamer_info) != 1:
            raise ValueError
        entry = streamer_info[0]
        if not isinstance(entry, dict):
            raise ValueError
        streamer_url = validate_streamer_url(entry["streamerSocketUrl"])
        credentials = SchwabStreamCredentials(
            access_token=token.access_token,
            customer_id=entry["schwabClientCustomerId"],
            correlation_id=entry["schwabClientCorrelId"],
            channel=entry["schwabClientChannel"],
            function_id=entry["schwabClientFunctionId"],
        )
    except (KeyError, TypeError, ValueError, SchwabStreamError):
        raise SchwabOAuthError("SCHWAB_USER_PREFERENCE_RESPONSE_INVALID") from None
    return streamer_url, credentials


def _unique_json(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SchwabOAuthError("SCHWAB_OAUTH_TOKEN_RESPONSE_INVALID")
        result[key] = value
    return result


def _printable(value: object, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value.encode()) <= maximum
        and value.isascii()
        and all(33 <= ord(character) <= 126 for character in value)
    )

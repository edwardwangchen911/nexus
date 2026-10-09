"""Load fixed host-local read-only Gateway settings without a diagnostic launcher."""

import argparse
import json
import os
import re
import stat
import subprocess
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from cryptography import x509
from pydantic import ValidationError

from nexus_fosun_gateway.settings import (
    FOSUN_LOCAL_READ_ONLY_HTTPS_PROXY_URL,
    FOSUN_REST_BASE_URL,
    GatewaySettings,
)

_HMAC_NAMES = frozenset(
    {
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
    }
)
_MAX_PRIVATE_FILE_BYTES = 1024 * 1024
_RELEASE_PATHS = (
    "integrations/fosun_gateway",
    "contracts/providers/fosun/manifest.json",
    "pyproject.toml",
    "uv.lock",
)
_LOCAL_CERTIFICATES = (
    "server.pem",
    "server-ca.pem",
    "broker-ca.pem",
    "market-ca.pem",
    "broker-client.pem",
    "market-client.pem",
)
_TLS_CERTIFICATE_CODES = frozenset(
    {
        "LOCAL_GATEWAY_TLS_CERTIFICATE_INVALID",
        "LOCAL_GATEWAY_TLS_CERTIFICATE_EXPIRED",
        "LOCAL_GATEWAY_TLS_CERTIFICATE_NOT_YET_VALID",
    }
)


def _certificate_now() -> datetime:
    return datetime.now(UTC)


def _validate_local_certificate_times(local: Path) -> None:
    now = _certificate_now()
    for name in _LOCAL_CERTIFICATES:
        raw = _private_file(local / name).encode("utf-8")
        try:
            certificates = x509.load_pem_x509_certificates(raw)
            if len(certificates) != 1:
                raise ValueError
        except ValueError:
            raise ValueError("LOCAL_GATEWAY_TLS_CERTIFICATE_INVALID") from None
        certificate = certificates[0]
        if now < certificate.not_valid_before_utc:
            raise ValueError("LOCAL_GATEWAY_TLS_CERTIFICATE_NOT_YET_VALID")
        if now >= certificate.not_valid_after_utc:
            raise ValueError("LOCAL_GATEWAY_TLS_CERTIFICATE_EXPIRED")


def _private_directory(path: Path) -> None:
    try:
        info = path.lstat()
    except OSError:
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE") from None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")


def _private_file(path: Path) -> str:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & 0o077
                or info.st_nlink != 1
                or not 0 < info.st_size <= _MAX_PRIVATE_FILE_BYTES
            ):
                raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
            raw = os.read(descriptor, _MAX_PRIVATE_FILE_BYTES + 1)
        finally:
            os.close(descriptor)
        if not raw or len(raw) > _MAX_PRIVATE_FILE_BYTES:
            raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
        return raw.decode("utf-8")
    except (OSError, UnicodeError):
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE") from None


def _private_existing_file(path: Path) -> None:
    try:
        info = path.lstat()
    except OSError:
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE") from None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
        or info.st_nlink != 1
    ):
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")


def _hmac_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in _private_file(path).splitlines():
        if "=" not in line:
            raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
        name, value = line.split("=", 1)
        if name not in _HMAC_NAMES or name in values or not value:
            raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
        values[name] = value
    if set(values) != _HMAC_NAMES:
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
    if (
        len(values["FOSUN_GATEWAY_HMAC_CURRENT_SECRET"]) < 32
        or len(values["FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET"]) < 32
        or values["FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID"]
        == values["FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID"]
        or values["FOSUN_GATEWAY_HMAC_CURRENT_SECRET"]
        == values["FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET"]
    ):
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
    return values


def load_local_gateway_settings(repo_root: Path) -> GatewaySettings:
    """Use only the fixed local paths and preserve the existing replay journal."""
    root = repo_root.resolve()
    local = root / ".secrets" / "nexus-fosun-local"
    provider = root / ".secrets" / "fosun-openapi"
    for directory in (root / ".secrets", local, provider):
        _private_directory(directory)
    replay = local / "replay.sqlite3"
    _private_existing_file(replay)
    for name in (
        "server.pem",
        "server.key",
        "broker-ca.pem",
        "market-ca.pem",
        "server-ca.pem",
        "broker-client.pem",
        "broker-client.key",
        "market-client.pem",
        "market-client.key",
    ):
        _private_existing_file(local / name)
    _validate_local_certificate_times(local)
    manifest = root / "contracts" / "providers" / "fosun" / "manifest.json"
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
    provider_values = {
        "FSOPENAPI_API_KEY": _private_file(provider / "api-key").strip(),
        "FSOPENAPI_CLIENT_PRIVATE_KEY": _private_file(provider / "private.pem"),
        "FSOPENAPI_SERVER_PUBLIC_KEY": _private_file(provider / "server-public.pem"),
    }
    if any(not value.strip() for value in provider_values.values()):
        raise ValueError("LOCAL_GATEWAY_MATERIAL_UNSAFE")
    values = {
        **_hmac_values(local / "hmac.env"),
        "NEXUS_ENV": "production",
        "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
        "FOSUN_GATEWAY_TRANSPORT_MODE": "local_read_only",
        "FOSUN_GATEWAY_REPLAY_STORE_PATH": str(replay),
        "FOSUN_SDK_ADMITTED": "true",
        "FOSUN_PROVIDER_MANIFEST": str(manifest),
        "FSOPENAPI_BASE_URL": FOSUN_REST_BASE_URL,
        "FSOPENAPI_HTTPS_PROXY_URL": FOSUN_LOCAL_READ_ONLY_HTTPS_PROXY_URL,
        "FOSUN_GATEWAY_SERVER_CERT": str(local / "server.pem"),
        "FOSUN_GATEWAY_SERVER_KEY": str(local / "server.key"),
        "FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA": str(local / "broker-ca.pem"),
        "FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA": str(local / "market-ca.pem"),
        **provider_values,
    }
    try:
        return GatewaySettings.model_validate(values)
    except ValidationError:
        raise ValueError("LOCAL_GATEWAY_SETTINGS_INVALID") from None


def checkout_release(repo_root: Path) -> str | None:
    root = repo_root.resolve()
    try:
        revision = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "HEAD"],
            check=False,
            capture_output=True,
        )
    except OSError:
        return None
    value = revision.stdout.decode("ascii", errors="ignore").strip()
    if revision.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        return None
    try:
        status = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--",
                *_RELEASE_PATHS,
            ],
            check=False,
            capture_output=True,
        )
    except OSError:
        return None
    if status.returncode != 0 or status.stdout:
        return None
    return value


def _runtime_matches_repo(repo_root: Path) -> bool:
    from nexus_fosun_gateway import main as gateway_main

    expected = repo_root.resolve() / "integrations/fosun_gateway/src/nexus_fosun_gateway"
    return (
        Path(__file__).resolve().parent == expected
        and Path(gateway_main.__file__).resolve().parent == expected
        and checkout_release(repo_root) is not None
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus-fosun-gateway-local")
    parser.add_argument("action", choices=("check", "run"))
    parser.add_argument("--repo-root", type=Path, required=True)
    arguments = parser.parse_args(argv)
    if not _runtime_matches_repo(arguments.repo_root):
        print(json.dumps({"status": "blocked", "code": "LOCAL_GATEWAY_RELEASE_UNVERIFIED"}))
        return 1
    if arguments.action == "run" and (
        checkout_release(arguments.repo_root) != os.environ.get("NEXUS_RELEASE_REVISION")
    ):
        print(json.dumps({"status": "blocked", "code": "LOCAL_GATEWAY_RELEASE_UNVERIFIED"}))
        return 1
    try:
        settings = load_local_gateway_settings(arguments.repo_root)
    except ValueError as error:
        code = str(error)
        if code not in {
            "LOCAL_GATEWAY_MATERIAL_UNSAFE",
            "LOCAL_GATEWAY_SETTINGS_INVALID",
            *_TLS_CERTIFICATE_CODES,
        }:
            code = "LOCAL_GATEWAY_SETTINGS_INVALID"
        print(json.dumps({"status": "blocked", "code": code}))
        return 1
    if arguments.action == "check":
        print(
            json.dumps(
                {
                    "status": "ready",
                    "mode": settings.transport_mode,
                    "capability": settings.capability,
                    "network_checked": False,
                }
            )
        )
        return 0
    from nexus_fosun_gateway import main as gateway_main

    # The pinned SDK reads these keys from process environment, even when its
    # GatewaySettings caller has already loaded the fixed private material.
    assert settings.fsopenapi_client_private_key is not None
    assert settings.fsopenapi_server_public_key is not None
    sdk_environment = {
        "FSOPENAPI_CLIENT_PRIVATE_KEY": settings.fsopenapi_client_private_key.get_secret_value(),
        "FSOPENAPI_SERVER_PUBLIC_KEY": settings.fsopenapi_server_public_key.get_secret_value(),
    }
    previous = {name: os.environ.get(name) for name in sdk_environment}
    try:
        os.environ.update(sdk_environment)
        gateway_main.run(settings)
    except Exception:
        print(json.dumps({"status": "failed", "code": "LOCAL_GATEWAY_START_FAILED"}))
        return 1
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    return 0

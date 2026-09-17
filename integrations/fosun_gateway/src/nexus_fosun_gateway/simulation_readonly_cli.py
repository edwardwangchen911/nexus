"""Isolated, bounded simulation reads; default preflight never opens the SDK."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import stat
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from importlib.metadata import distribution
from pathlib import Path
from threading import Lock
from typing import Literal
from uuid import uuid4

from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from nexus_fosun_gateway.adapter.simulation_readonly import SimulationReadOnlyTransport, refusal
from nexus_fosun_gateway.admission import probe_ready_sdk_subject
from nexus_fosun_gateway.security.dependency_logging import enforce_sdk_dependency_log_boundary
from nexus_fosun_gateway.security.provider_environment import reject_unsafe_provider_environment
from nexus_fosun_gateway.security.provider_tls import (
    HttpSurface,
    enforce_probe_tls_controls,
)
from nexus_fosun_gateway.settings import FOSUN_REST_BASE_URL

_SDK_COMMIT = "891d95fb4967efaa682387c1b35966ceea38fb21"
_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_CONFIG = Path.home() / ".config/nexus/fosun-simulation/provider.env"
_READ_PATHS = frozenset(
    {
        "/v1/portfolio/CashSummary",
        "/v1/portfolio/Holdings",
        "/v1/trade/OrderList",
        "/v1/trade/CashFlows",
    }
)


class SimulationReadSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True, frozen=True)

    api_key: SecretStr = Field(alias="FOSUN_SIMULATION_API_KEY")
    client_private_key: SecretStr = Field(alias="FOSUN_SIMULATION_CLIENT_PRIVATE_KEY")
    server_public_key: SecretStr = Field(alias="FOSUN_SIMULATION_SERVER_PUBLIC_KEY")
    account_id: SecretStr = Field(alias="FOSUN_SIMULATION_ACCOUNT_ID")
    simulation_write_enabled: Literal[False] = Field(
        default=False,
        alias="FOSUN_SIMULATION_WRITE_ENABLED",
    )
    execution_locked: Literal[True] = Field(
        default=True,
        alias="FOSUN_SIMULATION_EXECUTION_LOCKED",
    )

    @field_validator("simulation_write_enabled", mode="before")
    @classmethod
    def writes_disabled(cls, value: object) -> bool:
        if value is False or value == "false":
            return False
        raise ValueError("simulation writes must remain disabled")

    @field_validator("execution_locked", mode="before")
    @classmethod
    def execution_remains_locked(cls, value: object) -> bool:
        if value is True or value == "true":
            return True
        raise ValueError("execution must remain locked")

    @field_validator("api_key", "client_private_key", "server_public_key", "account_id")
    @classmethod
    def material_present(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip() or len(value.get_secret_value()) > 65536:
            raise ValueError("simulation configuration material is invalid")
        return value

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return [("simulation_write_enabled", False), ("execution_locked", True)]


def _private_path(path: Path, *, directory: bool) -> None:
    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        not expected(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise ValueError("simulation storage must be private and owned by this user")
    if path.resolve() != path.absolute():
        raise ValueError("simulation storage must not use symlinks")


def load_settings(path: Path) -> SimulationReadSettings:
    if not path.is_absolute() or any((parent / ".git").exists() for parent in path.parents):
        raise ValueError("simulation configuration must be outside a Git worktree")
    _private_path(path.parent, directory=True)
    _private_path(path, directory=False)
    if path.stat().st_size > 256 * 1024:
        raise ValueError("simulation configuration exceeds size limit")
    # No ambient FSOPENAPI values, dotenv expansion, or live-account fallback.
    return SimulationReadSettings.model_validate(dotenv_values(path, interpolate=False))


def restrict_sdk_requests(client: object, account_id: str) -> None:
    original = getattr(client, "_request")

    def guarded(method: str, path: str, data: object = None, params: object = None) -> object:
        account_query = path == "/v1/account/Accounts" and data == {}
        selected_read = (
            path in _READ_PATHS
            and isinstance(data, dict)
            and data.get("subAccountId") == account_id
            and "clientId" not in data
            and "applyAccountId" not in data
        )
        if method != "POST" or params is not None or not (account_query or selected_read):
            raise refusal("SIMULATION_OPERATION_REFUSED")
        return original(method, path, data=data, params=params)  # type: ignore[no-any-return]

    setattr(client, "_request", guarded)


class _HttpBudget:
    """At most one Session plus nine business reads, no auth replay or retries."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = Lock()
        self._deadline = time.monotonic() + 120

    def before_dispatch(self, surface: HttpSurface) -> None:
        with self._lock:
            count = self._counts.get(surface, 0)
            if time.monotonic() > self._deadline or count >= (1 if surface == "auth" else 9):
                raise refusal("SIMULATION_HTTP_BUDGET_EXCEEDED")
            self._counts[surface] = count + 1

    def after_response(self, surface: HttpSurface, status_code: int) -> None:
        if status_code != 200:
            raise refusal("SIMULATION_PROVIDER_HTTP_ERROR")


@contextmanager
def build_reader(settings: SimulationReadSettings) -> Iterator[SimulationReadOnlyTransport]:
    reject_unsafe_provider_environment()
    manifest = json.loads((_ROOT / "contracts/providers/fosun/manifest.json").read_text())
    subject = probe_ready_sdk_subject(manifest)
    metadata = json.loads(distribution("fsopenapi").read_text("direct_url.json") or "{}")
    if (
        subject is None
        or subject.sdk_commit != _SDK_COMMIT
        or metadata.get("vcs_info", {}).get("commit_id") != _SDK_COMMIT
    ):
        raise refusal("SIMULATION_SDK_CONTRACT_MISMATCH")
    enforce_sdk_dependency_log_boundary()
    sdk = importlib.import_module("fsopenapi")
    auth = importlib.import_module("fsopenapi.auth")
    # The pinned SDK consumes keys during construction. This CLI owns its process;
    # restore the prior environment immediately, before any provider call or thread.
    values = {
        "FSOPENAPI_CLIENT_PRIVATE_KEY": settings.client_private_key.get_secret_value(),
        "FSOPENAPI_SERVER_PUBLIC_KEY": settings.server_public_key.get_secret_value(),
        "SDK_TYPE": "",
    }
    previous = {key: os.environ.get(key) for key in values}
    try:
        os.environ.update(values)
        client = sdk.SDKClient(
            base_url=FOSUN_REST_BASE_URL,
            api_key=settings.api_key.get_secret_value(),
            logging_enable=False,
            log_body=False,
        )
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    controls = enforce_probe_tls_controls(client, auth, observer=_HttpBudget())
    try:
        account_id = settings.account_id.get_secret_value()
        restrict_sdk_requests(client, account_id)
        yield SimulationReadOnlyTransport(client, account_id=account_id)
    finally:
        controls.close()


def _write_private(path: Path, payload: dict[str, object]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as file:
        json.dump(payload, file, ensure_ascii=True)
        file.flush()
        os.fsync(file.fileno())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=_DEFAULT_CONFIG)
    parser.add_argument("--read", action="store_true")
    parser.add_argument("--date-from", type=date.fromisoformat)
    parser.add_argument("--date-to", type=date.fromisoformat)
    args = parser.parse_args(argv)
    result: dict[str, object] = {
        "status": "configuration_blocked",
        "network_attempted": False,
        "simulation_write_enabled": False,
        "execution_locked": True,
    }
    run_dir: Path | None = None
    try:
        settings = load_settings(args.config)
        result["status"] = "configuration_ready"
        if args.read:
            if (
                args.date_from is None
                or args.date_to is None
                or args.date_from > args.date_to
                or (args.date_to - args.date_from).days > 31
            ):
                raise ValueError("bounded dates required")
            reads = args.config.parent / "reads"
            reads.mkdir(mode=0o700, exist_ok=True)
            _private_path(reads, directory=True)
            result["run_id"] = run_id = str(uuid4())
            run_dir = reads / run_id
            run_dir.mkdir(mode=0o700)
            _write_private(run_dir / "started.json", {**result, "status": "started"})
            with build_reader(settings) as reader:
                result["network_attempted"] = True
                # Persist the conservative dispatch boundary before the first SDK call.
                # A crash with this marker and no terminal status is an unknown result.
                _write_private(run_dir / "dispatching.json", {**result, "status": "dispatching"})
                snapshot = asyncio.run(reader.collect(args.date_from, args.date_to))
            _write_private(run_dir / "snapshot.json", snapshot)
            result["status"] = "read_completed"
            _write_private(run_dir / "status.json", result)
    except Exception:
        result["status"] = "read_blocked" if args.read else "configuration_blocked"
        if run_dir is not None:
            try:
                _write_private(run_dir / "status.json", result)
            except OSError:
                result["status"] = "storage_failed"
        print(json.dumps(result))
        return 2
    print(json.dumps(result))
    return 0


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()

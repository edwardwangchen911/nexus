"""One safe local entry point for Fosun readiness, persisted status, and refresh."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import dotenv_values
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import desc, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.api.schemas.portfolio import derive_snapshot_readiness, snapshot_time_basis
from nexus.ops.fosun_local_refresh import (
    LocalFosunRefreshSettings,
    LocalOrderReadError,
    _gateway_failure_category,
    _safe_failure_code,
    refresh_local_fosun,
)
from nexus.persistence.models.portfolio import (
    BrokerOrderSnapshot,
    BrokerSyncRun,
    CashSnapshot,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.providers.broker.gateway_contracts import GatewayClientError

_REQUIRED_ENVIRONMENT = (
    "DATABASE_URL",
    "OBJECT_STORE_ENDPOINT",
    "OBJECT_STORE_REGION",
    "OBJECT_STORE_BUCKET",
    "OBJECT_STORE_ACCESS_KEY_ID",
    "OBJECT_STORE_SECRET_ACCESS_KEY",
    "FOSUN_GATEWAY_BASE_URL",
    "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
    "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
    "FOSUN_GATEWAY_CLIENT_CERT",
    "FOSUN_GATEWAY_CLIENT_KEY",
    "FOSUN_GATEWAY_CLIENT_CA",
    "FOSUN_MARKET_GATEWAY_BASE_URL",
    "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
    "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
    "FOSUN_MARKET_GATEWAY_CLIENT_CERT",
    "FOSUN_MARKET_GATEWAY_CLIENT_KEY",
    "FOSUN_MARKET_GATEWAY_CLIENT_CA",
)
_TLS_ENVIRONMENT = (
    "FOSUN_GATEWAY_CLIENT_CERT",
    "FOSUN_GATEWAY_CLIENT_KEY",
    "FOSUN_GATEWAY_CLIENT_CA",
    "FOSUN_MARKET_GATEWAY_CLIENT_CERT",
    "FOSUN_MARKET_GATEWAY_CLIENT_KEY",
    "FOSUN_MARKET_GATEWAY_CLIENT_CA",
)
_LOCAL_GATEWAY_ENVIRONMENT = {
    "FOSUN_GATEWAY_BASE_URL": "https://127.0.0.1:8081",
    "FOSUN_MARKET_GATEWAY_BASE_URL": "https://127.0.0.1:8082",
}
_LOCAL_TLS_FILES = {
    "FOSUN_GATEWAY_CLIENT_CERT": "broker-client.pem",
    "FOSUN_GATEWAY_CLIENT_KEY": "broker-client.key",
    "FOSUN_GATEWAY_CLIENT_CA": "server-ca.pem",
    "FOSUN_MARKET_GATEWAY_CLIENT_CERT": "market-client.pem",
    "FOSUN_MARKET_GATEWAY_CLIENT_KEY": "market-client.key",
    "FOSUN_MARKET_GATEWAY_CLIENT_CA": "server-ca.pem",
}
_LOCAL_HMAC_ENVIRONMENT = (
    "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
    "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
    "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
    "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
)


class _LocalStatusSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
    )

    database_url: str = Field(validation_alias="DATABASE_URL")


class LocalDatabaseError(RuntimeError):
    """Value-free failure at the local persisted-status boundary."""


def load_local_environment(
    repo_root: Path,
    process_environment: Mapping[str, str],
) -> dict[str, str]:
    """Compose ignored local settings and fixed loopback paths without mutating the process."""
    root = repo_root.resolve()
    file_values = dotenv_values(root / "infra" / ".env.local")
    environment = {
        name: value for name, value in file_values.items() if isinstance(value, str) and value
    }
    hmac_values = dotenv_values(root / ".secrets" / "nexus-fosun-local" / "hmac.env")
    environment.update(
        {
            name: value
            for name in _LOCAL_HMAC_ENVIRONMENT
            if isinstance((value := hmac_values.get(name)), str) and value
        }
    )
    environment.update(_LOCAL_GATEWAY_ENVIRONMENT)
    secret_root = root / ".secrets" / "nexus-fosun-local"
    environment.update(
        {name: str(secret_root / filename) for name, filename in _LOCAL_TLS_FILES.items()}
    )
    _localize_compose_endpoints(environment)
    environment.update({name: value for name, value in process_environment.items() if value})
    return environment


def _localize_compose_endpoints(environment: dict[str, str]) -> None:
    """Translate Compose-only service names to their loopback host ports."""
    database_url = environment.get("DATABASE_URL")
    if database_url:
        parsed_database = make_url(database_url)
        if parsed_database.host == "postgres":
            environment["DATABASE_URL"] = parsed_database.set(
                host="127.0.0.1",
                port=_host_port(environment, "NEXUS_POSTGRES_HOST_PORT", 5432),
            ).render_as_string(hide_password=False)

    object_store_endpoint = environment.get("OBJECT_STORE_ENDPOINT")
    if object_store_endpoint:
        parsed_object_store = urlsplit(object_store_endpoint)
        if parsed_object_store.hostname == "minio":
            environment["OBJECT_STORE_ENDPOINT"] = urlunsplit(
                parsed_object_store._replace(
                    netloc=(f"127.0.0.1:{_host_port(environment, 'NEXUS_MINIO_HOST_PORT', 9000)}")
                )
            )


def _host_port(environment: Mapping[str, str], name: str, default: int) -> int:
    raw = environment.get(name, str(default))
    try:
        port = int(raw)
    except ValueError:
        raise ValueError("LOCAL_HOST_PORT_INVALID") from None
    if not 1 <= port <= 65535:
        raise ValueError("LOCAL_HOST_PORT_INVALID")
    return port


def initialize_local_hmac(repo_root: Path) -> dict[str, object]:
    """Create two local-only HMAC identities once without returning their values."""
    secret_root = repo_root.resolve() / ".secrets" / "nexus-fosun-local"
    if secret_root.is_symlink():
        raise RuntimeError("LOCAL_HMAC_IDENTITY_INVALID")
    secret_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    identity_file = secret_root / "hmac.env"
    if identity_file.exists():
        _validate_local_hmac_file(identity_file)
        return {"status": "exists", "hmac_identities": 2, "secrets_printed": False}

    values = {
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "broker-local-v1",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": secrets.token_urlsafe(32),
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID": "market-local-v1",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET": secrets.token_urlsafe(32),
    }
    payload = "".join(f"{name}={values[name]}\n" for name in _LOCAL_HMAC_ENVIRONMENT)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(identity_file, flags, 0o600)
    except FileExistsError:
        _validate_local_hmac_file(identity_file)
        return {"status": "exists", "hmac_identities": 2, "secrets_printed": False}
    try:
        os.write(descriptor, payload.encode("ascii"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {"status": "created", "hmac_identities": 2, "secrets_printed": False}


def _validate_local_hmac_file(identity_file: Path) -> None:
    if identity_file.is_symlink() or not identity_file.is_file():
        raise RuntimeError("LOCAL_HMAC_IDENTITY_INVALID")
    if identity_file.stat().st_mode & 0o077:
        raise RuntimeError("LOCAL_HMAC_IDENTITY_INVALID")
    values = dotenv_values(identity_file)
    selected = tuple(values.get(name) for name in _LOCAL_HMAC_ENVIRONMENT)
    if (
        any(not isinstance(value, str) or not value for value in selected)
        or len(str(selected[1])) < 32
        or len(str(selected[3])) < 32
        or selected[0] == selected[2]
        or selected[1] == selected[3]
    ):
        raise RuntimeError("LOCAL_HMAC_IDENTITY_INVALID")


def doctor_local_environment(
    environment: Mapping[str, str],
    *,
    is_file: Callable[[Path], bool] = Path.is_file,
) -> dict[str, object]:
    """Inspect only local names and file presence; never connect or return values."""
    configured = tuple(name for name in _REQUIRED_ENVIRONMENT if environment.get(name))
    missing = tuple(name for name in _REQUIRED_ENVIRONMENT if not environment.get(name))
    tls_files = {
        name: "present" if is_file(Path(environment[name])) else "missing"
        for name in _TLS_ENVIRONMENT
        if environment.get(name)
    }
    ready = not missing and all(state == "present" for state in tls_files.values())
    return {
        "status": "ready" if ready else "configuration_incomplete",
        "configured_environment_count": len(configured),
        "missing_environment": missing,
        "tls_files": tls_files,
        "network_checked": False,
        "database_checked": False,
    }


async def read_local_status(database_url: str) -> dict[str, object]:
    """Read a redacted summary of the latest locally persisted broker snapshot."""
    engine = create_async_engine(database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            snapshot = (
                await session.execute(
                    select(PortfolioSnapshotRecord)
                    .order_by(
                        desc(PortfolioSnapshotRecord.created_at),
                        desc(PortfolioSnapshotRecord.id),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            run = (
                await session.get(BrokerSyncRun, snapshot.sync_run_id)
                if snapshot
                else (
                    await session.execute(
                        select(BrokerSyncRun)
                        .order_by(desc(BrokerSyncRun.created_at), desc(BrokerSyncRun.id))
                        .limit(1)
                    )
                ).scalar_one_or_none()
            )
            if snapshot is None:
                return {
                    "status": "no_snapshot",
                    "last_sync_run_status": run.status if run else None,
                    "network_checked": False,
                }
            counts: dict[str, int] = {}
            for name, model in (
                ("cash", CashSnapshot),
                ("positions", PositionSnapshot),
                ("open_orders", BrokerOrderSnapshot),
            ):
                count = await session.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.portfolio_snapshot_id == snapshot.id)
                )
                counts[name] = int(count or 0)
            display, decision = derive_snapshot_readiness(
                snapshot.status,
                has_display_facts=bool(counts["cash"] or counts["positions"]),
                cash_flow_quality=run.cash_flow_quality if run else "NOT_RUN",
            )
            return {
                "status": "ok",
                "snapshot_status": snapshot.status,
                "display_readiness": display,
                "decision_readiness": decision,
                "as_of": snapshot.as_of.isoformat() if snapshot.as_of else None,
                "time_basis": snapshot_time_basis(snapshot.source_observations),
                "last_sync_run_status": run.status if run else None,
                "cash_flow_quality": run.cash_flow_quality if run else None,
                "counts": counts,
                "network_checked": False,
            }
    finally:
        await engine.dispose()


async def read_local_status_safely(
    database_url: str,
    *,
    reader: Callable[[str], Awaitable[dict[str, object]]] | None = None,
) -> dict[str, object]:
    selected_reader = reader or read_local_status
    try:
        return await selected_reader(database_url)
    except (KeyboardInterrupt, SystemExit):
        raise
    except (OSError, SQLAlchemyError):
        raise LocalDatabaseError("LOCAL_DATABASE_UNAVAILABLE") from None


def safe_failure(error: Exception) -> dict[str, str]:
    if isinstance(error, LocalOrderReadError):
        return error.safe_output()
    if isinstance(error, GatewayClientError):
        category = _gateway_failure_category(str(error))
    elif isinstance(error, LocalDatabaseError):
        category = "database"
    elif isinstance(error, ValidationError):
        category = "configuration"
    elif isinstance(error, SQLAlchemyError):
        category = "database"
    elif isinstance(error, TimeoutError):
        category = "timeout"
    else:
        category = "local"
    return {
        "status": "failed",
        "category": category,
        "code": _safe_failure_code(error),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nexus-fosun-local")
    parser.add_argument("command", choices=("init", "doctor", "status", "refresh"))
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    command = _parser().parse_args(argv).command
    try:
        local_environment = load_local_environment(Path.cwd(), os.environ)
        if command == "init":
            result = initialize_local_hmac(Path.cwd())
        elif command == "doctor":
            result = doctor_local_environment(local_environment)
        elif command == "status":
            status_settings = _LocalStatusSettings.model_validate(local_environment)
            result = asyncio.run(read_local_status_safely(status_settings.database_url))
        else:
            refresh_settings = LocalFosunRefreshSettings.model_validate(local_environment)
            result = asyncio.run(refresh_local_fosun(refresh_settings))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as error:
        sys.stderr.write(json.dumps(safe_failure(error), sort_keys=True) + "\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()

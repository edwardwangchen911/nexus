"""One safe local entry point for Fosun readiness, persisted status, and refresh."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path

from dotenv import dotenv_values
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import desc, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.api.schemas.portfolio import derive_snapshot_readiness
from nexus.ops.fosun_local_refresh import (
    LocalFosunRefreshSettings,
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
        name: value
        for name, value in file_values.items()
        if isinstance(value, str) and value
    }
    environment.update(_LOCAL_GATEWAY_ENVIRONMENT)
    secret_root = root / ".secrets" / "nexus-fosun-local"
    environment.update(
        {
            name: str(secret_root / filename)
            for name, filename in _LOCAL_TLS_FILES.items()
        }
    )
    environment.update(
        {name: value for name, value in process_environment.items() if value}
    )
    return environment


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
                await session.execute(
                    select(BrokerSyncRun)
                    .order_by(desc(BrokerSyncRun.created_at), desc(BrokerSyncRun.id))
                    .limit(1)
                )
            ).scalar_one_or_none()
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
            )
            return {
                "status": "ok",
                "snapshot_status": snapshot.status,
                "display_readiness": display,
                "decision_readiness": decision,
                "as_of": snapshot.as_of.isoformat() if snapshot.as_of else None,
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


def _gateway_failure_category(message: str) -> str:
    categories = {
        "gateway rejected request: PROVIDER_AUTHENTICATION_FAILED": "auth",
        "gateway rejected request: PROVIDER_PERMISSION_DENIED": "permission",
        "gateway rejected request: PROVIDER_RATE_LIMITED": "rate_limit",
        "gateway rejected request: PROVIDER_APPLICATION_ERROR": "provider",
        "gateway rejected request: PROVIDER_ERROR": "provider",
        "gateway rejected request: PROVIDER_UNAVAILABLE": "provider",
        "gateway rejected request: PROVIDER_MALFORMED_RESPONSE": "contract",
        "gateway rejected request: PROVIDER_WINDOW_MISMATCH": "contract",
        "gateway request failed": "transport",
        "gateway response contract rejected": "contract",
        "gateway response rejected": "contract",
    }
    return categories.get(message, "gateway")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nexus-fosun-local")
    parser.add_argument("command", choices=("doctor", "status", "refresh"))
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    command = _parser().parse_args(argv).command
    local_environment = load_local_environment(Path.cwd(), os.environ)
    try:
        if command == "doctor":
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

"""One safe local entry point for Fosun readiness, persisted status, and refresh."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from pydantic import ValidationError
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


class _LocalStatusSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
    )

    database_url: str


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


def safe_failure(error: Exception) -> dict[str, str]:
    if isinstance(error, GatewayClientError):
        category = _gateway_failure_category(str(error))
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
    try:
        if command == "doctor":
            result = doctor_local_environment(os.environ)
        elif command == "status":
            status_settings = _LocalStatusSettings()  # type: ignore[call-arg]
            result = asyncio.run(read_local_status(status_settings.database_url))
        else:
            refresh_settings = LocalFosunRefreshSettings()  # type: ignore[call-arg]
            result = asyncio.run(refresh_local_fosun(refresh_settings))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as error:
        sys.stderr.write(json.dumps(safe_failure(error), sort_keys=True) + "\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()

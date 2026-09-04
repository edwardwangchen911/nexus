"""Collect one explicitly selected Shadow day's returned raw-price set."""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Never
from urllib.parse import urlsplit

import httpx
from pydantic import Field, SecretStr
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.daily_once import DailyOnceSettings
from nexus.ops.fosun_local import load_local_environment
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.shadow.price_collection import ShadowPriceQueryRequest, collect_shadow_prices


class ShadowPriceSettings(DailyOnceSettings):
    alpaca_key: SecretStr = Field(validation_alias="ALPACA_API_KEY_ID")
    alpaca_secret: SecretStr = Field(validation_alias="ALPACA_API_SECRET_KEY")


def validate_local_config(settings: ShadowPriceSettings) -> None:
    database = make_url(settings.database_url or "")
    endpoint = urlsplit(settings.object_store_endpoint or "")
    local = {"localhost", "127.0.0.1", "::1"}
    if (
        not settings.explicit_artifact_config
        or database.drivername != "postgresql+asyncpg"
        or database.host not in local
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in local
        or endpoint.username is not None
        or endpoint.password is not None
        or endpoint.path not in {"", "/"}
        or endpoint.query
        or endpoint.fragment
        or not settings.alpaca_key.get_secret_value().strip()
        or not settings.alpaca_secret.get_secret_value().strip()
    ):
        raise ValueError("SHADOW_PRICE_CONFIG_INVALID")


async def run(settings: ShadowPriceSettings, request: ShadowPriceQueryRequest) -> dict[str, object]:
    validate_local_config(settings)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        async with httpx.AsyncClient(follow_redirects=False, timeout=30) as http:
            client = FixedOriginHttpClient(
                provider_name="alpaca",
                origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
                policy=HttpRequestPolicy(max_attempts=1),
                client=http,
            )
            return await collect_shadow_prices(
                factory,
                request,
                client=client,
                store=store,
                key_id=settings.alpaca_key,
                secret=settings.alpaca_secret,
            )
    finally:
        await engine.dispose()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise ValueError("SHADOW_PRICE_REQUEST_INVALID")


def main() -> None:
    parser = _Parser(description="只采集明确已有 Shadow 根的一日原始价格；不启动/推进/下单。")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    for name in ("shadow-group-id", "session-date", "idempotency-key"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--feed", choices=("iex", "sip"), required=True)
    parser.add_argument("--delivery-mode", choices=("real_time", "delayed"), required=True)
    try:
        args = vars(parser.parse_args())
        repo_root = args.pop("repo_root")
        request = ShadowPriceQueryRequest.model_validate(args)
    except (ValueError, OSError, RecursionError):
        print('{"status":"rejected","code":"SHADOW_PRICE_REQUEST_INVALID"}', file=sys.stderr)
        raise SystemExit(2) from None
    try:
        settings = ShadowPriceSettings.model_validate(load_local_environment(repo_root, {}))
        validate_local_config(settings)
    except Exception:
        print('{"status":"rejected","code":"SHADOW_PRICE_CONFIG_INVALID"}', file=sys.stderr)
        raise SystemExit(2) from None
    try:
        result = asyncio.run(run(settings, request))
    except (Exception, KeyboardInterrupt):
        # Commit acknowledgement can be uncertain; never claim rollback.
        print('{"status":"unknown","code":"SHADOW_PRICE_RESULT_UNKNOWN"}', file=sys.stderr)
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

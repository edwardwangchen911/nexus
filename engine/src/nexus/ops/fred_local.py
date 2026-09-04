"""Explicit FRED originals collection or local import; no model or account actions."""

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Never
from urllib.parse import urlsplit

import httpx
from pydantic import SecretStr
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.daily_once import DailyOnceSettings
from nexus.ops.fosun_local import load_local_environment
from nexus.providers.fred_http import FredHttpTransport
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.research.fred_collection import collect_fred_vintage, load_fred_collection
from nexus.research.fred_observations import FredVintageCollection, FredVintageRequest
from nexus.research.fred_storage import import_fred_vintage, read_fred_observations


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise ValueError("FRED_REQUEST_INVALID")


def validate_local_import(settings: DailyOnceSettings) -> None:
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
    ):
        raise ValueError("FRED_CONFIG_INVALID")


async def collect(
    request: FredVintageRequest, *, key: SecretStr, directory: Path
) -> dict[str, object]:
    if not key.get_secret_value().strip() or request.vintage > datetime.now(UTC).date():
        raise ValueError("FRED_CONFIG_INVALID")
    async with httpx.AsyncClient(follow_redirects=False, timeout=30) as client:
        transport = FredHttpTransport(
            FixedOriginHttpClient(
                provider_name="fred",
                origin=AllowedOrigin(scheme="https", host="api.stlouisfed.org"),
                policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=1024 * 1024),
                client=client,
            ),
            key,
        )
        result = await collect_fred_vintage(request, transport=transport, output_dir=directory)
    return {
        "status": "collected",
        "observation_count": len(result.observations),
        "missing_value_count": len(result.missing_dates),
        "execution_locked": True,
    }


async def import_collection(
    settings: DailyOnceSettings,
    *,
    collection: FredVintageCollection,
    store: S3ProviderEvidenceStore | None = None,
) -> dict[str, object]:
    validate_local_import(settings)
    if collection.received_at > datetime.now(UTC):
        raise ValueError("FRED_ARCHIVE_INVALID")
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session, session.begin():
            await assert_daily_pipeline_schema_ready(session)
            actual_store = store or S3ProviderEvidenceStore(
                settings, disable_environment_proxies=True
            )
            ids = await import_fred_vintage(session, collection=collection, store=actual_store)
            visible = await read_fred_observations(session, as_of=datetime.now(UTC))
            visible_ids = {row.observation_id for row in visible.values()}
        return {
            "status": "imported",
            "observation_ids": [str(value) for value in ids],
            "visible_observation_count": len(set(ids) & visible_ids),
            "execution_locked": True,
        }
    finally:
        await engine.dispose()


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("collect")
    for name in ("series", "start", "end", "vintage"):
        capture.add_argument(f"--{name}", required=True)
    capture.add_argument("--output-dir", type=Path, required=True)
    importing = commands.add_parser("import")
    importing.add_argument("--collection-dir", type=Path, required=True)
    try:
        args = parser.parse_args()
        request = (
            FredVintageRequest.model_validate(
                {
                    "series": args.series,
                    "observation_start": args.start,
                    "observation_end": args.end,
                    "vintage": args.vintage,
                }
            )
            if args.command == "collect"
            else None
        )
        collection = load_fred_collection(args.collection_dir) if args.command == "import" else None
    except (ValueError, OSError, RecursionError):
        print('{"status":"rejected","code":"FRED_REQUEST_INVALID"}', file=sys.stderr)
        raise SystemExit(2) from None
    try:
        environment = load_local_environment(args.repo_root, os.environ)
        settings = None
        key = SecretStr("")
        if request is not None:
            key = SecretStr(environment.get("FRED_API_KEY", ""))
            if not key.get_secret_value().strip() or request.vintage > datetime.now(UTC).date():
                raise ValueError
            args.output_dir.mkdir(mode=0o700)
        else:
            settings = DailyOnceSettings.model_validate(environment)
            validate_local_import(settings)
    except Exception:
        print('{"status":"rejected","code":"FRED_CONFIG_INVALID"}', file=sys.stderr)
        raise SystemExit(2) from None
    try:
        if request is not None:
            result = asyncio.run(collect(request, key=key, directory=args.output_dir))
        else:
            assert collection is not None and settings is not None
            result = asyncio.run(import_collection(settings, collection=collection))
    except (Exception, KeyboardInterrupt):
        # Originals or a commit may already exist. Do not promise rollback or retry.
        print('{"status":"unknown","code":"FRED_RESULT_UNKNOWN"}', file=sys.stderr)
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

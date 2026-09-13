"""Explicit, default-off local entrypoint for one Agent paper-cycle step."""

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.paper.agent_cycle import AgentPaperCycleResult, advance_agent_paper_cycle
from nexus.paper.agent_market import PaperMarketWindowResult, collect_agent_paper_window
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.market.alpaca import AlpacaMarketDataProvider
from nexus.providers.market.alpaca_http import AlpacaHttpTransport, SqlInstrumentSymbolResolver
from nexus.settings import EngineSettings

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


class AgentPaperRunRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["run"]
    account_id: UUID
    decision_time: datetime
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")

    @field_validator("decision_time")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("explicit UTC time is required")
        return value


class AgentPaperCollectWindowRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["collect-window"]
    cycle_id: UUID
    instrument_id: UUID
    window_start: datetime

    @field_validator("window_start")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("explicit UTC time is required")
        return value


class AgentPaperMarketSettings(EngineSettings):
    alpaca_key: SecretStr = Field(validation_alias="ALPACA_API_KEY_ID")
    alpaca_secret: SecretStr = Field(validation_alias="ALPACA_API_SECRET_KEY")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="nexus-engine-agent-paper-once")
    commands = root.add_subparsers(dest="action", required=True)
    run = commands.add_parser("run")
    run.add_argument("--account-id", required=True)
    run.add_argument("--decision-time", required=True)
    run.add_argument("--idempotency-key", required=True)
    collect = commands.add_parser("collect-window")
    collect.add_argument("--cycle-id", required=True)
    collect.add_argument("--instrument-id", required=True)
    collect.add_argument("--window-start", required=True)
    return root


def _emit_error(code: str) -> int:
    print(json.dumps({"error_code": code}, separators=(",", ":"), sort_keys=True))
    return 1


async def run_once(
    settings: EngineSettings, request: AgentPaperRunRequest
) -> AgentPaperCycleResult:
    if not settings.database_url:
        raise ValueError("AGENT_PAPER_DATABASE_UNCONFIGURED")
    url = make_url(settings.database_url)
    if (
        url.drivername != "postgresql+asyncpg"
        or url.host not in _LOCAL_HOSTS
        or bool(set(url.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise ValueError("AGENT_PAPER_DATABASE_NOT_LOCAL")
    if request.decision_time > datetime.now(UTC):
        raise ValueError("AGENT_PAPER_TIME_IN_FUTURE")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session, session.begin():
            return await advance_agent_paper_cycle(
                session,
                account_id=request.account_id,
                decision_time=request.decision_time,
                idempotency_key=request.idempotency_key,
                daily_limit_usd=settings.agent_daily_budget_usd,
            )
    finally:
        await engine.dispose()


async def collect_window_once(
    settings: AgentPaperMarketSettings,
    request: AgentPaperCollectWindowRequest,
) -> PaperMarketWindowResult:
    if not settings.database_url:
        raise ValueError("AGENT_PAPER_DATABASE_UNCONFIGURED")
    database = make_url(settings.database_url)
    endpoint = urlsplit(settings.object_store_endpoint or "")
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in _LOCAL_HOSTS
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in _LOCAL_HOSTS
        or endpoint.username is not None
        or endpoint.password is not None
        or endpoint.path not in {"", "/"}
        or endpoint.query
        or endpoint.fragment
        or not settings.alpaca_key.get_secret_value().strip()
        or not settings.alpaca_secret.get_secret_value().strip()
    ):
        raise ValueError("AGENT_PAPER_MARKET_CONFIG_INVALID")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        async with (
            factory() as resolver_session,
            httpx.AsyncClient(follow_redirects=False, timeout=30) as http,
        ):
            client = FixedOriginHttpClient(
                provider_name="alpaca",
                origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
                policy=HttpRequestPolicy(max_attempts=1),
                client=http,
            )
            provider = AlpacaMarketDataProvider(
                AlpacaHttpTransport(
                    client,
                    key_id=settings.alpaca_key,
                    secret=settings.alpaca_secret,
                    feed="iex",
                    symbol_resolver=SqlInstrumentSymbolResolver(resolver_session),
                ),
                feed="iex",
                delivery_mode="real_time",
            )
            return await collect_agent_paper_window(
                factory,
                cycle_id=request.cycle_id,
                instrument_id=request.instrument_id,
                window_start=request.window_start,
                provider=provider,
                object_store=store,
            )
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        values = vars(args)
        action = values.get("action")
        request = (
            AgentPaperRunRequest.model_validate(values)
            if action == "run"
            else AgentPaperCollectWindowRequest.model_validate(values)
        )
        environment = load_local_environment(Path.cwd(), {})
        settings = EngineSettings.model_validate(environment)
        if not settings.agent_paper_cycle_enabled:
            return _emit_error("AGENT_PAPER_CYCLE_DISABLED")
        result: AgentPaperCycleResult | PaperMarketWindowResult
        if isinstance(request, AgentPaperRunRequest):
            result = asyncio.run(run_once(settings, request))
        else:
            market_settings = AgentPaperMarketSettings.model_validate(
                environment
            )
            result = asyncio.run(collect_window_once(market_settings, request))
    except Exception as error:
        code = str(error)
        if code not in {
            "AGENT_PAPER_ACCOUNT_INVALID",
            "AGENT_PAPER_DATABASE_UNCONFIGURED",
            "AGENT_PAPER_DATABASE_NOT_LOCAL",
            "AGENT_PAPER_TIME_IN_FUTURE",
            "AGENT_PAPER_MARKET_CONFIG_INVALID",
        }:
            code = "AGENT_PAPER_RESULT_UNKNOWN"
        return _emit_error(code)
    print(json.dumps(result.model_dump(mode="json"), separators=(",", ":"), sort_keys=True))
    return 0

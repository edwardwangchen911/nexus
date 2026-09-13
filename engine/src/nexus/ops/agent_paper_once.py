"""Explicit, default-off local entrypoint for one Agent paper-cycle step."""

import argparse
import asyncio
import json
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.paper.agent_cycle import AgentPaperCycleResult, advance_agent_paper_cycle
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


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="nexus-engine-agent-paper-once")
    commands = root.add_subparsers(dest="action", required=True)
    run = commands.add_parser("run")
    run.add_argument("--account-id", required=True)
    run.add_argument("--decision-time", required=True)
    run.add_argument("--idempotency-key", required=True)
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


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        request = AgentPaperRunRequest.model_validate(vars(args))
        settings = EngineSettings()
        if not settings.agent_paper_cycle_enabled:
            return _emit_error("AGENT_PAPER_CYCLE_DISABLED")
        result = asyncio.run(run_once(settings, request))
    except Exception as error:
        code = str(error)
        if code not in {
            "AGENT_PAPER_DATABASE_UNCONFIGURED",
            "AGENT_PAPER_DATABASE_NOT_LOCAL",
            "AGENT_PAPER_TIME_IN_FUTURE",
        }:
            code = "AGENT_PAPER_RESULT_UNKNOWN"
        return _emit_error(code)
    print(json.dumps(result.model_dump(mode="json"), separators=(",", ":"), sort_keys=True))
    return 0

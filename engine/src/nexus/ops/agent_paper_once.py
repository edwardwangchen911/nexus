"""Explicit, default-off local entrypoint for one Agent paper-cycle step."""

import argparse
import asyncio
import json
import re
from collections.abc import Sequence
from datetime import UTC, datetime, time, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, NoReturn
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.jobs.models import JobType
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.agent_risk_authority import collect_source_proofs
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.ops.openai_local import local_settings
from nexus.ops.schwab_market_stream import (
    ProviderMarketStreamError,
    SchwabMarketStreamImportResult,
    capture_and_import_schwab_level_one_once,
)
from nexus.paper.agent_cycle import (
    AgentPaperCycleResult,
    advance_agent_paper_cycle,
    finish_schwab_agent_paper_trigger,
    reserve_schwab_agent_paper_trigger,
    resume_schwab_agent_paper_cycle,
    start_schwab_agent_paper_cycle,
)
from nexus.paper.agent_cycle_authority import (
    load_quasi_realtime_cycle_authority,
    select_agent_paper_service_calendar_authority,
)
from nexus.paper.agent_cycle_models import AgentPaperTriggerReservation
from nexus.paper.agent_market import (
    PaperMarketWindowResult,
    collect_agent_paper_window,
    resolve_agent_paper_market_scope,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.providers.base import MarketDataProvider
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.market.alpaca import AlpacaMarketDataProvider
from nexus.providers.market.alpaca_http import AlpacaHttpTransport, SqlInstrumentSymbolResolver
from nexus.providers.market.schwab_oauth import SchwabOAuthError
from nexus.providers.market.schwab_price_history import SchwabPriceHistoryProvider
from nexus.providers.market.schwab_stream import SchwabStreamError
from nexus.risk.query_authority_producer import (
    inspect_query_authority_scope,
    persist_query_authorities,
)
from nexus.settings import AgentRuntimeMode, EngineSettings
from nexus.worker import run_agent_worker_once

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_SAFE_SCHWAB_FAILURE_CODES = frozenset(
    {
        "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID",
        "PROVIDER_MARKET_STREAM_BYTE_LIMIT",
        "PROVIDER_MARKET_STREAM_FRAME_INVALID",
        "PROVIDER_MARKET_STREAM_INCOMPLETE",
        "PROVIDER_MARKET_STREAM_PROTOCOL_INVALID",
        "PROVIDER_MARKET_STREAM_PROVIDER_REJECTED",
        "PROVIDER_MARKET_STREAM_SECRET_REFLECTION",
        "SCHWAB_MARKET_STREAM_ARTIFACT_REPLAY_CONFLICT",
        "SCHWAB_MARKET_STREAM_BINDINGS_INVALID",
        "SCHWAB_MARKET_STREAM_EMPTY",
        "SCHWAB_MARKET_STREAM_EVIDENCE_INVALID",
        "SCHWAB_MARKET_STREAM_EVIDENCE_STALE",
        "SCHWAB_MARKET_STREAM_IDENTITY_REQUIRED",
        "SCHWAB_MARKET_STREAM_IMPORT_TIME_INVALID",
        "SCHWAB_APP_CONFIG_INVALID",
        "SCHWAB_APP_CONFIG_REQUIRED",
        "SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED",
        "SCHWAB_OAUTH_CALLBACK_INVALID",
        "SCHWAB_OAUTH_CODE_INVALID",
        "SCHWAB_OAUTH_REFRESH_REQUEST_FAILED",
        "SCHWAB_OAUTH_REFRESH_RESPONSE_INVALID",
        "SCHWAB_OAUTH_TIME_INVALID",
        "SCHWAB_OAUTH_TOKEN_REQUEST_FAILED",
        "SCHWAB_OAUTH_TOKEN_RESPONSE_INVALID",
        "SCHWAB_OAUTH_TOKEN_STORE_INVALID",
        "SCHWAB_STREAM_CREDENTIALS_INVALID",
        "SCHWAB_STREAM_FRAME_INVALID",
        "SCHWAB_STREAM_QUOTE_INVALID",
        "SCHWAB_STREAM_REQUEST_INVALID",
        "SCHWAB_STREAM_SYMBOLS_INVALID",
        "SCHWAB_STREAM_TIME_INVALID",
        "SCHWAB_STREAM_URL_INVALID",
        "SCHWAB_USER_PREFERENCE_REQUEST_FAILED",
        "SCHWAB_USER_PREFERENCE_RESPONSE_INVALID",
    }
)
_SAFE_SCHWAB_HTTP_FAILURE_CODE = re.compile(
    r"SCHWAB_(?:OAUTH_(?:TOKEN|REFRESH)|USER_PREFERENCE)_HTTP_[1-5][0-9]{2}"
)


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


class AgentPaperTriggerRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["trigger"]
    account_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")


class AgentPaperResumeRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["resume"]
    cycle_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")


class AgentPaperCollectWindowRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["collect-window"]
    cycle_id: UUID
    instrument_id: UUID | None = None


class AgentPaperRunResearchRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["run-research"]
    cycle_id: UUID


class AgentPaperRunResearchResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    job_id: UUID
    job_processed: Literal[True] = True
    execution_locked: Literal[True] = True


class AgentPaperMarketSettings(EngineSettings):
    alpaca_key: SecretStr | None = Field(default=None, validation_alias="ALPACA_API_KEY_ID")
    alpaca_secret: SecretStr | None = Field(default=None, validation_alias="ALPACA_API_SECRET_KEY")


class AgentPaperTriggerSettings(EngineSettings):
    alpaca_key: SecretStr = Field(validation_alias="ALPACA_API_KEY_ID")
    alpaca_secret: SecretStr = Field(validation_alias="ALPACA_API_SECRET_KEY")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="nexus-engine-agent-paper-once")
    commands = root.add_subparsers(dest="action", required=True)
    run = commands.add_parser("run")
    run.add_argument("--account-id", required=True)
    run.add_argument("--decision-time", required=True)
    run.add_argument("--idempotency-key", required=True)
    trigger = commands.add_parser("trigger")
    trigger.add_argument("--account-id", required=True)
    trigger.add_argument("--idempotency-key", required=True)
    resume = commands.add_parser("resume")
    resume.add_argument("--cycle-id", required=True)
    resume.add_argument("--idempotency-key", required=True)
    run_research = commands.add_parser("run-research")
    run_research.add_argument("--cycle-id", required=True)
    collect = commands.add_parser("collect-window")
    collect.add_argument("--cycle-id", required=True)
    collect.add_argument("--instrument-id")
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


def _validate_local_database(database_url: str | None) -> None:
    if not database_url:
        raise ValueError("AGENT_PAPER_DATABASE_UNCONFIGURED")
    url = make_url(database_url)
    if (
        url.drivername != "postgresql+asyncpg"
        or url.host not in _LOCAL_HOSTS
        or bool(set(url.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise ValueError("AGENT_PAPER_DATABASE_NOT_LOCAL")


def _validate_trigger_quote(
    quote: LatestQuote | None,
    artifact: DataArtifact | None,
    result: SchwabMarketStreamImportResult,
    *,
    decision_time: datetime,
) -> None:
    if (
        quote is None
        or artifact is None
        or result.quote_ids.get("AAPL") != quote.id
        or result.artifact_id != artifact.id
        or quote.data_artifact_id != artifact.id
        or quote.source != "schwab"
        or quote.feed != "l1"
        or quote.delivery_mode != "real_time"
        or quote.event_kind != "quote"
        or quote.evidence_status != "verified"
        or quote.bid is None
        or quote.ask is None
        or quote.bid <= 0
        or quote.ask <= 0
        or quote.bid > quote.ask
        or artifact.source != "schwab"
        or artifact.schema_version != "schwab_level_one_stream_v1"
        or quote.created_at > decision_time
        or artifact.created_at > decision_time
        or quote.received_at != artifact.received_at
        or quote.observed_at > quote.received_at
        or quote.received_at > decision_time
    ):
        raise ValueError("SCHWAB_L1_INCOMPLETE")
    if decision_time - quote.received_at > timedelta(
        seconds=60
    ) or quote.received_at - quote.observed_at > timedelta(seconds=60):
        raise ValueError("SCHWAB_L1_FRESHNESS_UNPROVEN")


def _raise_schwab_l1_incomplete(error: Exception, *, diagnostic_path: Path) -> NoReturn:
    provider_error_code = str(error)
    if (
        provider_error_code not in _SAFE_SCHWAB_FAILURE_CODES
        and _SAFE_SCHWAB_HTTP_FAILURE_CODE.fullmatch(provider_error_code) is None
    ):
        provider_error_code = "SCHWAB_MARKET_STREAM_FAILURE_UNCLASSIFIED"
    payload = {
        "schema": "nexus_schwab_l1_failure_v1",
        "stage": "capture_or_import",
        "provider_error_code": provider_error_code,
    }
    try:
        diagnostic_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        write_private(
            diagnostic_path,
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(),
        )
    except OSError:
        pass
    raise ValueError("SCHWAB_L1_INCOMPLETE") from error


async def trigger_in_session(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    settings: AgentPaperTriggerSettings,
    request: AgentPaperTriggerRequest,
    *,
    reservation: AgentPaperTriggerReservation,
    repo_root: Path,
    client: httpx.AsyncClient,
    expected_calendar_authority_id: UUID | None = None,
) -> AgentPaperCycleResult:
    """Run one bounded provider pass, freeze DB time, and create one research Job."""
    initial_time = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(initial_time, datetime):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    local_time = initial_time.astimezone(ZoneInfo("America/New_York"))
    if not time(7) <= local_time.time().replace(tzinfo=None) < time(16):
        raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
    decision_session = reservation.decision_session
    decision_slot_at = reservation.decision_slot_at
    if local_time.date() != decision_session:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")

    selected = (
        await select_agent_paper_service_calendar_authority(session, now=initial_time)
        if expected_calendar_authority_id is None
        else None
    )
    calendar_authority_id = (
        selected.calendar_authority_id
        if selected is not None
        else expected_calendar_authority_id
    )
    assert calendar_authority_id is not None

    initial_authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=request.account_id,
        now=initial_time,
        expected_calendar_authority_id=calendar_authority_id,
    )
    scope = await inspect_query_authority_scope(
        session,
        decision_session=decision_session,
        symbol="AAPL",
        as_of=initial_time,
        universe_snapshot_id=initial_authority.universe_snapshot_id,
        source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    )
    digest = sha256(request.idempotency_key.encode()).hexdigest()[:16]
    output_parent = repo_root / ".secrets" / "nexus-agent-paper"
    output_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    attempt_directory = output_parent / f"{decision_slot_at.strftime('%Y%m%dT%H%MZ')}-{digest}"
    try:
        proofs = await collect_source_proofs(
            symbol="AAPL",
            instrument_id=scope.instrument_id,
            decision_session=decision_session,
            key_id=settings.alpaca_key,
            secret=settings.alpaca_secret,
            output_dir=attempt_directory,
            cutoff_at=None,
        )
    except ValueError as error:
        raise ValueError("RISK_INPUT_REQUIRED") from error

    try:
        stream = await capture_and_import_schwab_level_one_once(
            session,
            store,
            repo_root=repo_root,
            client=client,
            instrument_id=scope.instrument_id,
        )
    except (ProviderMarketStreamError, SchwabOAuthError, SchwabStreamError) as error:
        _raise_schwab_l1_incomplete(
            error,
            diagnostic_path=attempt_directory / "schwab-failure.json",
        )
    decision_time = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(decision_time, datetime):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    final_local = decision_time.astimezone(ZoneInfo("America/New_York"))
    if final_local.date() != decision_session or not time(7) <= final_local.time().replace(
        tzinfo=None
    ) < time(16):
        raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
    quote_id = stream.quote_ids.get("AAPL")
    quote = None if quote_id is None else await session.get(LatestQuote, quote_id)
    artifact = await session.get(DataArtifact, stream.artifact_id)
    _validate_trigger_quote(quote, artifact, stream, decision_time=decision_time)

    try:
        await persist_query_authorities(
            session,
            universe_snapshot_id=scope.universe_snapshot_id,
            decision_session=decision_session,
            symbol="AAPL",
            proofs=proofs,
            source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        )
    except ValueError as error:
        raise ValueError("RISK_INPUT_REQUIRED") from error
    authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=request.account_id,
        now=decision_time,
        expected_calendar_authority_id=calendar_authority_id,
    )
    return await start_schwab_agent_paper_cycle(
        session,
        authority=authority,
        decision_slot_at=decision_slot_at,
        idempotency_key=request.idempotency_key,
        daily_limit_usd=settings.agent_daily_budget_usd,
    )


async def trigger_once(
    settings: AgentPaperTriggerSettings,
    request: AgentPaperTriggerRequest,
    *,
    repo_root: Path,
    expected_calendar_authority_id: UUID | None = None,
) -> AgentPaperCycleResult:
    _validate_local_database(settings.database_url)
    endpoint = urlsplit(settings.object_store_endpoint or "")
    if (
        endpoint.scheme not in {"http", "https"}
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
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as claim_session, claim_session.begin():
            now = await claim_session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
            reservation = await reserve_schwab_agent_paper_trigger(
                claim_session,
                account_id=request.account_id,
                client_idempotency_key=request.idempotency_key,
                now=now,
            )
        if reservation.replay is not None:
            return reservation.replay
        store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        try:
            async with (
                factory() as session,
                session.begin(),
                httpx.AsyncClient(follow_redirects=False, timeout=30) as client,
            ):
                result = await trigger_in_session(
                    session,
                    store,
                    settings,
                    request,
                    reservation=reservation,
                    repo_root=repo_root,
                    client=client,
                    expected_calendar_authority_id=expected_calendar_authority_id,
                )
                await finish_schwab_agent_paper_trigger(
                    session, reservation=reservation, result=result
                )
                return result
        except Exception as error:
            try:
                async with factory() as failure_session, failure_session.begin():
                    await finish_schwab_agent_paper_trigger(
                        failure_session,
                        reservation=reservation,
                        error_code=str(error),
                    )
            except Exception:
                pass
            raise
    finally:
        await engine.dispose()


async def resume_once(
    settings: EngineSettings, request: AgentPaperResumeRequest
) -> AgentPaperCycleResult:
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session, session.begin():
            return await resume_schwab_agent_paper_cycle(
                session, cycle_id=request.cycle_id, idempotency_key=request.idempotency_key
            )
    finally:
        await engine.dispose()


def resolve_queued_cycle_research_job(
    *,
    events: Sequence[object],
    jobs: Sequence[object],
    output_run_ids: Sequence[UUID],
) -> UUID:
    """Resolve one untouched paper-research Job from immutable cycle facts."""
    if not events or getattr(events[-1], "to_status", None) != "researching":
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    raw_run_ids: list[str] = []
    for event in events:
        fact_ids = getattr(event, "fact_ids", None)
        raw = fact_ids.get("research_run_ids") if isinstance(fact_ids, dict) else None
        if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
            raw_run_ids.extend(raw)
    if len(raw_run_ids) != 1:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    try:
        run_id = UUID(raw_run_ids[0])
    except ValueError as error:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN") from error
    if output_run_ids or len(jobs) != 1:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    job = jobs[0]
    payload = getattr(job, "payload", None)
    job_id = getattr(job, "id", None)
    if (
        getattr(job, "job_type", None) != JobType.AGENT_PAPER_RESEARCH.value
        or getattr(job, "status", None) != "queued"
        or getattr(job, "attempt_count", None) != 0
        or getattr(job, "manual_retry_count", None) != 0
        or getattr(job, "max_attempts", None) != 1
        or getattr(job, "started_at", None) is not None
        or getattr(job, "finished_at", None) is not None
        or not isinstance(payload, dict)
        or payload.get("run_id") != str(run_id)
        or not isinstance(job_id, UUID)
    ):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    return job_id


async def _resolve_queued_research_job_id(database_url: str, *, requested_cycle_id: UUID) -> UUID:
    engine = create_async_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            events = tuple(
                await session.scalars(
                    select(AgentPaperCycleEventRecord)
                    .where(AgentPaperCycleEventRecord.cycle_id == requested_cycle_id)
                    .order_by(AgentPaperCycleEventRecord.sequence)
                )
            )
            raw_run_ids: list[str] = []
            for event in events:
                raw = event.fact_ids.get("research_run_ids")
                if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
                    raw_run_ids.extend(raw)
            jobs: tuple[object, ...] = ()
            output_run_ids: tuple[UUID, ...] = ()
            if raw_run_ids:
                jobs = tuple(
                    await session.scalars(
                        select(Job).where(Job.payload["run_id"].as_string().in_(tuple(raw_run_ids)))
                    )
                )
                output_run_ids = tuple(
                    await session.scalars(
                        select(AgentResearchOutputRecord.run_id).where(
                            AgentResearchOutputRecord.run_id.in_(
                                tuple(UUID(value) for value in raw_run_ids)
                            )
                        )
                    )
                )
            return resolve_queued_cycle_research_job(
                events=events, jobs=jobs, output_run_ids=output_run_ids
            )
    except (TypeError, ValueError) as error:
        if str(error) == "AGENT_PAPER_RESULT_UNKNOWN":
            raise
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN") from error
    finally:
        await engine.dispose()


async def run_research_once(
    settings: EngineSettings,
    request: AgentPaperRunResearchRequest,
    *,
    repo_root: Path,
) -> AgentPaperRunResearchResult:
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    job_id = await _resolve_queued_research_job_id(
        settings.database_url, requested_cycle_id=request.cycle_id
    )
    backend = (
        "responses" if settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES else "codex_sdk"
    )
    worker_settings = local_settings(repo_root, backend=backend)
    if worker_settings.database_url != settings.database_url:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    worked = await run_agent_worker_once(worker_settings, job_ids=(job_id,))
    if not worked:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    return AgentPaperRunResearchResult(cycle_id=request.cycle_id, job_id=job_id)


async def collect_window_once(
    settings: AgentPaperMarketSettings,
    request: AgentPaperCollectWindowRequest,
    *,
    repo_root: Path,
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
            scope = await resolve_agent_paper_market_scope(
                resolver_session,
                cycle_id=request.cycle_id,
                instrument_id=request.instrument_id,
            )
            resolver = SqlInstrumentSymbolResolver(resolver_session)
            provider: MarketDataProvider
            if scope.execution_source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID:
                provider = SchwabPriceHistoryProvider(repo_root, http, symbol_resolver=resolver)
            else:
                if (
                    settings.alpaca_key is None
                    or settings.alpaca_secret is None
                    or not settings.alpaca_key.get_secret_value().strip()
                    or not settings.alpaca_secret.get_secret_value().strip()
                ):
                    raise ValueError("AGENT_PAPER_MARKET_CONFIG_INVALID")
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
                        symbol_resolver=resolver,
                    ),
                    feed="iex",
                    delivery_mode="real_time",
                )
            return await collect_agent_paper_window(
                factory,
                cycle_id=request.cycle_id,
                instrument_id=scope.instrument_id,
                window_start=scope.window_start,
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
        request: (
            AgentPaperRunRequest
            | AgentPaperTriggerRequest
            | AgentPaperResumeRequest
            | AgentPaperRunResearchRequest
            | AgentPaperCollectWindowRequest
        )
        if action == "run":
            request = AgentPaperRunRequest.model_validate(values)
        elif action == "trigger":
            request = AgentPaperTriggerRequest.model_validate(values)
        elif action == "resume":
            request = AgentPaperResumeRequest.model_validate(values)
        elif action == "run-research":
            request = AgentPaperRunResearchRequest.model_validate(values)
        else:
            request = AgentPaperCollectWindowRequest.model_validate(values)
        environment = load_local_environment(Path.cwd(), {})
        settings = EngineSettings.model_validate(environment)
        if not settings.agent_paper_cycle_enabled:
            return _emit_error("AGENT_PAPER_CYCLE_DISABLED")
        result: AgentPaperCycleResult | PaperMarketWindowResult | AgentPaperRunResearchResult
        if isinstance(request, AgentPaperRunRequest):
            result = asyncio.run(run_once(settings, request))
        elif isinstance(request, AgentPaperTriggerRequest):
            trigger_settings = AgentPaperTriggerSettings.model_validate(environment)
            result = asyncio.run(trigger_once(trigger_settings, request, repo_root=Path.cwd()))
        elif isinstance(request, AgentPaperResumeRequest):
            result = asyncio.run(resume_once(settings, request))
        elif isinstance(request, AgentPaperRunResearchRequest):
            result = asyncio.run(run_research_once(settings, request, repo_root=Path.cwd()))
        else:
            market_settings = AgentPaperMarketSettings.model_validate(environment)
            result = asyncio.run(
                collect_window_once(market_settings, request, repo_root=Path.cwd())
            )
    except Exception as error:
        code = str(error)
        if code not in {
            "AGENT_PAPER_ACCOUNT_INVALID",
            "AGENT_PAPER_DATABASE_UNCONFIGURED",
            "AGENT_PAPER_DATABASE_NOT_LOCAL",
            "AGENT_PAPER_TIME_IN_FUTURE",
            "AGENT_PAPER_MARKET_CONFIG_INVALID",
            "AGENT_PAPER_OUTSIDE_WINDOW",
            "RISK_INPUT_REQUIRED",
            "SCHWAB_L1_FRESHNESS_UNPROVEN",
            "SCHWAB_L1_INCOMPLETE",
            "RESEARCH_ADMISSION_UNAVAILABLE",
            "PAPER_MARKET_WINDOW_UNAVAILABLE",
        }:
            code = "AGENT_PAPER_RESULT_UNKNOWN"
        return _emit_error(code)
    print(json.dumps(result.model_dump(mode="json"), separators=(",", ":"), sort_keys=True))
    return 0

"""Fail-closed foreground scheduler for Agent internal-paper cycles."""

import argparse
import asyncio
import errno
import fcntl
import json
import os
import signal
import stat
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from time import monotonic as monotonic_clock
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from pydantic import SecretStr
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from sqlalchemy import and_, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.agents.codex_sdk import sdk_status
from nexus.agents.persistence import select_authoritative_snapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.repository import upsert_worker_heartbeat
from nexus.ops.agent_paper_once import (
    AgentPaperCollectWindowRequest,
    AgentPaperMarketSettings,
    AgentPaperResumeRequest,
    AgentPaperTriggerRequest,
    AgentPaperTriggerSettings,
    collect_window_once,
    resume_once,
    trigger_once,
)
from nexus.ops.alpaca_adjusted_authority_import import import_adjusted_authority
from nexus.ops.alpaca_prices_import import import_trends
from nexus.ops.alpaca_prices_local import collect_prices
from nexus.ops.daily_once import DailyOnceSettings
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.fred_local import collect as collect_fred
from nexus.ops.fred_local import import_collection as import_fred_collection
from nexus.ops.fred_local import validate_local_import
from nexus.ops.local_stack import LocalStackConfigurationError, _validate_local_env
from nexus.ops.openai_local import local_settings
from nexus.ops.paper_once import PaperAdvanceRequest
from nexus.ops.paper_once import run_once as run_paper_once
from nexus.paper.agent_cycle_authority import (
    ServiceCalendarAuthority,
    load_agent_paper_service_calendar_authority,
    load_quasi_realtime_cycle_authority,
    select_agent_paper_research_target,
    select_agent_paper_service_calendar_authority,
)
from nexus.paper.agent_cycle_models import floor_decision_slot
from nexus.paper.agent_market import resolve_agent_paper_market_scope
from nexus.paper.errors import PaperPerformanceError
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.paper.read_service import PaperReadService
from nexus.persistence.migrations import (
    AGENT_PAPER_LOOP_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperMarketWindowRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperFillRecord,
    PaperOrderEventRecord,
    PaperPerformanceSnapshotRecord,
    PaperRequestIdempotencyRecord,
)
from nexus.persistence.models.runtime import Job, WorkerHeartbeat
from nexus.providers.http import HttpPolicyError
from nexus.providers.market.schwab_oauth import (
    SchwabOAuthError,
    load_schwab_app_config,
    load_schwab_token,
    require_live_schwab_access_token,
)
from nexus.research.alpaca_prices_archive import collection_window
from nexus.research.current_price_trends import SOURCE as CURRENT_TRENDS_SOURCE
from nexus.research.current_price_trends_storage import read_current_price_trends
from nexus.research.fred_collection import load_fred_collection
from nexus.research.fred_observations import FredVintageRequest
from nexus.research.fred_storage import FredResearchObservation, read_fred_observations
from nexus.risk.query_authority_producer import inspect_query_authority_scope
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once


class AgentPaperLoopAction(StrEnum):
    TRIGGER = "trigger"
    RUN_RESEARCH_JOB = "run_research_job"
    RESUME = "resume"
    COLLECT_WINDOW = "collect_window"
    ADVANCE_ORDER = "advance_order"
    PUBLISH_PERFORMANCE = "publish_performance"
    WAIT_FOR_SLOT = "wait_for_slot"
    WAIT_FOR_MARKET = "wait_for_market"
    WAIT_FOR_SESSION = "wait_for_session"
    STOP_RESULT_UNKNOWN = "stop_result_unknown"
    STOP_CYCLE_UNAVAILABLE = "stop_cycle_unavailable"
    STOP_ORDER_UNAVAILABLE = "stop_order_unavailable"
    STOP_PERFORMANCE_UNAVAILABLE = "stop_performance_unavailable"
    STOP_PREREQUISITE_UNAVAILABLE = "stop_prerequisite_unavailable"
    STOP_CALENDAR_UNAVAILABLE = "stop_calendar_unavailable"
    STOP_CALENDAR_AUTHORITY_MISMATCH = "stop_calendar_authority_mismatch"
    STOP_DISABLED = "stop_disabled"
    STOP_SESSION_CLOSED = "stop_session_closed"
    STOP_REQUESTED = "stop_requested"
    STOP_UNCONFIRMED = "stop_unconfirmed"


@dataclass(frozen=True)
class AgentPaperLoopSnapshot:
    now: datetime
    enabled: bool = True
    calendar_authority_id: UUID | None = None
    decision_window_start_at: datetime | None = None
    decision_window_end_at: datetime | None = None
    next_decision_window_start_at: datetime | None = None
    cycle_id: UUID | None = None
    cycle_status: str | None = None
    cycle_result_code: str | None = None
    cycle_slot_at: datetime | None = None
    research_job_status: str | None = None
    research_job_id: UUID | str | None = None
    market_window_ready: bool = False
    market_window_start_at: datetime | None = None
    market_window_end_at: datetime | None = None
    market_window_id: UUID | None = None
    has_open_order: bool = False
    order_id: UUID | None = None
    performance_due_at: datetime | None = None


@dataclass(frozen=True)
class PriorDailyScope:
    instrument_id: UUID
    prior_session: date
    authority_ready: bool
    decision_session: date | None = None
    symbol: str = "AAPL"


@dataclass(frozen=True)
class CurrentTrendsScope:
    instrument_id: UUID
    start_session: date
    prior_session: date
    evidence_ready: bool
    snapshot_ready: bool
    symbol: str = "AAPL"


@dataclass(frozen=True)
class MacroScope:
    instrument_id: UUID
    vintage: date
    ready_series: frozenset[str]
    snapshot_ready: bool
    attempt_session: date | None = None


_TERMINAL_JOB_STATUSES = frozenset({"succeeded", "failed", "dead_letter", "skipped"})
_CANDIDATE_RETRY_CODES = frozenset(
    {
        "CANDIDATE_EVIDENCE_UNAVAILABLE",
        "RESEARCH_OUTPUT_NOT_ACTIONABLE",
        "RESEARCH_BLOCKED",
        "AGENT_DECISION_CONFLICT",
    }
)
_SAFE_TRIGGER_FAILURE_CODES = frozenset(
    {
        "AGENT_PAPER_ACCOUNT_INVALID",
        "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
        "AGENT_PAPER_CALENDAR_UNAVAILABLE",
        "AGENT_PAPER_CODEX_STATUS_UNAVAILABLE",
        "AGENT_PAPER_LOOP_ALREADY_RUNNING",
        "AGENT_PAPER_MARKET_WINDOW_CLOSED",
        "AGENT_PAPER_OUTSIDE_WINDOW",
        "RISK_INPUT_REQUIRED",
        "SCHWAB_L1_FRESHNESS_UNPROVEN",
        "SCHWAB_L1_INCOMPLETE",
        "CODEX_CHATGPT_LOGIN_REQUIRED",
    }
)
_MARKET_PUBLICATION_LAG = timedelta(seconds=60)
_PAPER_MACRO_SERIES = frozenset({"DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "VIXCLS"})
_PAPER_MONTHLY_MACRO_SERIES = frozenset({"CPIAUCSL", "UNRATE"})
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_SAFE_SCHWAB_PREFLIGHT_CODES = frozenset(
    {
        "SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED",
        "SCHWAB_OAUTH_TIME_INVALID",
        "SCHWAB_OAUTH_TOKEN_STORE_INVALID",
    }
)


def acquire_agent_paper_lock(*, runtime_root: Path | None = None) -> AbstractContextManager[None]:
    """Hold the single per-user Agent paper process lock for one runner lifetime."""

    @contextmanager
    def locked() -> Iterator[None]:
        root = (
            runtime_root
            if runtime_root is not None
            else Path.home() / "Library" / "Application Support" / "NEXUS" / "runtime"
        )
        try:
            root.mkdir(mode=0o700, parents=True)
        except FileExistsError:
            pass
        except OSError:
            raise RuntimeError("AGENT_PAPER_LOOP_RUNTIME_INVALID") from None

        directory_fd = -1
        lock_fd = -1
        locked_fd = False
        try:
            directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            directory_fd = os.open(root, directory_flags)
            directory = os.fstat(directory_fd)
            if (
                not stat.S_ISDIR(directory.st_mode)
                or directory.st_uid != os.getuid()
                or stat.S_IMODE(directory.st_mode) != 0o700
            ):
                raise RuntimeError("AGENT_PAPER_LOOP_RUNTIME_INVALID")
            lock_fd = os.open(
                "agent-paper.lock",
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_fd,
            )
            lock_file = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(lock_file.st_mode)
                or lock_file.st_uid != os.getuid()
                or stat.S_IMODE(lock_file.st_mode) != 0o600
                or lock_file.st_nlink != 1
            ):
                raise RuntimeError("AGENT_PAPER_LOOP_RUNTIME_INVALID")
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno in {errno.EACCES, errno.EAGAIN}:
                    raise RuntimeError("AGENT_PAPER_LOOP_ALREADY_RUNNING") from None
                raise RuntimeError("AGENT_PAPER_LOOP_RUNTIME_INVALID") from None
            locked_fd = True
            yield
        except RuntimeError:
            raise
        except OSError:
            raise RuntimeError("AGENT_PAPER_LOOP_RUNTIME_INVALID") from None
        finally:
            if locked_fd:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            if lock_fd >= 0:
                os.close(lock_fd)
            if directory_fd >= 0:
                os.close(directory_fd)

    return locked()


def _nonempty_environment_values(environment: Mapping[str, str], names: Sequence[str]) -> bool:
    return all(
        isinstance(environment.get(name), str) and bool(environment[name].strip()) for name in names
    )


def _require_explicit_schwab_config(environment: Mapping[str, str]) -> None:
    if not _nonempty_environment_values(
        environment,
        ("NEXUS_SCHWAB_APP_KEY", "NEXUS_SCHWAB_APP_SECRET", "NEXUS_SCHWAB_CALLBACK_URL"),
    ):
        raise ValueError("AGENT_PAPER_SCHWAB_APP_CONFIG_INVALID")


def _agent_paper_host_port(environment: Mapping[str, str], name: str, default: int) -> int:
    try:
        port = int(environment.get(name, str(default)))
    except ValueError:
        raise ValueError("AGENT_PAPER_LOCAL_HOST_PORT_INVALID") from None
    if not 1 <= port <= 65535:
        raise ValueError("AGENT_PAPER_LOCAL_HOST_PORT_INVALID")
    return port


class _FileOnlySources:
    """Explicit service settings never supplement missing values from process configuration."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


class _FileEngineSettings(_FileOnlySources, EngineSettings):
    pass


class _FileTriggerSettings(_FileOnlySources, AgentPaperTriggerSettings):
    pass


class _FileMarketSettings(_FileOnlySources, AgentPaperMarketSettings):
    pass


class _FileDailySettings(_FileOnlySources, DailyOnceSettings):
    pass


def load_agent_paper_service_environment(
    repo_root: Path, process_environment: Mapping[str, str], *, env_file: Path | None = None
) -> dict[str, str]:
    """Load configuration and restore loopback endpoints required by host networking."""
    if env_file is None:
        environment = load_local_environment(repo_root, process_environment)
    else:
        environment = {
            name: value
            for name, value in dotenv_values(env_file, interpolate=False).items()
            if isinstance(value, str) and value
        }
        environment.update({name: value for name, value in process_environment.items() if value})
    database_url = environment.get("DATABASE_URL")
    if database_url:
        database = make_url(database_url)
        if database.host == "postgres":
            environment["DATABASE_URL"] = database.set(
                host="127.0.0.1",
                port=_agent_paper_host_port(environment, "NEXUS_POSTGRES_HOST_PORT", 5432),
            ).render_as_string(hide_password=False)
    object_store_endpoint = environment.get("OBJECT_STORE_ENDPOINT")
    if object_store_endpoint:
        object_store = urlsplit(object_store_endpoint)
        if object_store.hostname == "minio":
            host_port = _agent_paper_host_port(environment, "NEXUS_MINIO_HOST_PORT", 9000)
            environment["OBJECT_STORE_ENDPOINT"] = urlunsplit(
                object_store._replace(netloc=f"127.0.0.1:{host_port}")
            )
    return environment


def _agent_paper_engine_config_valid(environment: Mapping[str, str]) -> bool:
    required = (
        "DATABASE_URL",
        "OBJECT_STORE_ENDPOINT",
        "OBJECT_STORE_REGION",
        "OBJECT_STORE_BUCKET",
        "OBJECT_STORE_ACCESS_KEY_ID",
        "OBJECT_STORE_SECRET_ACCESS_KEY",
        "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID",
        "NEXUS_ENGINE_HMAC_CURRENT_SECRET",
    )
    if not _nonempty_environment_values(environment, required):
        return False
    try:
        database = make_url(environment["DATABASE_URL"])
        object_store = urlsplit(environment["OBJECT_STORE_ENDPOINT"])
    except (TypeError, ValueError):
        return False
    return (
        database.drivername == "postgresql+asyncpg"
        and database.host in _LOOPBACK_HOSTS
        and bool(database.database)
        and not database.query
        and object_store.scheme in {"http", "https"}
        and object_store.hostname in _LOOPBACK_HOSTS
        and object_store.username is None
        and object_store.password is None
        and object_store.path in {"", "/"}
        and not object_store.query
        and not object_store.fragment
    )


def _agent_paper_output_directory_valid(repo_root: Path) -> bool:
    directory = repo_root.resolve() / ".secrets" / "nexus-agent-paper"
    try:
        return (
            not directory.is_symlink()
            and directory.is_dir()
            and stat.S_IMODE(directory.stat().st_mode) == 0o700
            and os.access(directory, os.R_OK | os.W_OK | os.X_OK)
        )
    except OSError:
        return False


def _schwab_token_store_writable(repo_root: Path) -> bool:
    directory = repo_root.resolve() / ".secrets" / "nexus-schwab-local"
    try:
        filesystem = os.statvfs(directory)
        return (
            directory.is_dir()
            and not directory.is_symlink()
            and not filesystem.f_flag & os.ST_RDONLY
            and os.access(directory, os.W_OK | os.X_OK)
        )
    except OSError:
        return False


async def _codex_sdk_preflight_blocker_async() -> str | None:
    """Read local ChatGPT login state without refreshing auth or invoking a model."""
    try:
        result = await sdk_status()
    except Exception:
        return "AGENT_PAPER_CODEX_STATUS_UNAVAILABLE"
    if result.get("ready_for_model_requests") is True:
        return None
    if result.get("blocker") == "CODEX_CHATGPT_LOGIN_REQUIRED":
        return "CODEX_CHATGPT_LOGIN_REQUIRED"
    return "AGENT_PAPER_CODEX_STATUS_UNAVAILABLE"


def _codex_sdk_preflight_blocker() -> str | None:
    return asyncio.run(_codex_sdk_preflight_blocker_async())


def agent_paper_service_preflight(
    environment: Mapping[str, str], *, repo_root: Path, now: datetime | None = None
) -> dict[str, object]:
    """Validate local service prerequisites without network or persistence access."""
    blocking_codes: set[str] = set()
    optional_codes: list[str] = []
    requested = environment.get("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false")
    requested_normalized = requested.strip().lower()
    if requested_normalized in {"", "0", "false", "no", "off"}:
        blocking_codes.add("AGENT_PAPER_CYCLE_DISABLED")
    try:
        UUID(environment.get("NEXUS_AGENT_PAPER_ACCOUNT_ID", ""))
    except (AttributeError, TypeError, ValueError):
        blocking_codes.add("AGENT_PAPER_ACCOUNT_INVALID")
    engine_config_valid = _agent_paper_engine_config_valid(environment)
    if not engine_config_valid:
        blocking_codes.add("AGENT_PAPER_ENGINE_CONFIG_INVALID")
    runtime_mode = environment.get("AGENT_RUNTIME_MODE", "").strip().lower()
    model_config_valid = runtime_mode == "codex_sdk"
    if not runtime_mode:
        blocking_codes.add("AGENT_PAPER_MODEL_CONFIG_REQUIRED")
    elif not model_config_valid:
        blocking_codes.add("AGENT_PAPER_MODEL_RUNTIME_MISMATCH")
    if engine_config_valid and model_config_valid:
        try:
            _FileEngineSettings.model_validate(environment)
        except Exception:
            blocking_codes.add("AGENT_PAPER_RUNTIME_CONFIG_INVALID")
        else:
            if model_blocker := _codex_sdk_preflight_blocker():
                blocking_codes.add(model_blocker)
    if not _nonempty_environment_values(
        environment, ("ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY")
    ):
        blocking_codes.add("AGENT_PAPER_MARKET_SOURCE_CONFIG_REQUIRED")
    try:
        _require_explicit_schwab_config(environment)
        load_schwab_app_config(repo_root, environment)
    except (OSError, SchwabOAuthError, ValueError):
        blocking_codes.add("AGENT_PAPER_SCHWAB_APP_CONFIG_INVALID")
    try:
        token = load_schwab_token(repo_root)
        if not _schwab_token_store_writable(repo_root):
            blocking_codes.add("AGENT_PAPER_SCHWAB_TOKEN_STORE_READ_ONLY")
        require_live_schwab_access_token(token, now=now)
    except SchwabOAuthError as error:
        code = str(error)
        if code == "SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED":
            optional_codes.append("SCHWAB_OAUTH_ACCESS_TOKEN_REFRESH_REQUIRED")
        else:
            blocking_codes.add(
                code if code in _SAFE_SCHWAB_PREFLIGHT_CODES else "AGENT_PAPER_SCHWAB_TOKEN_INVALID"
            )
    except OSError:
        blocking_codes.add("AGENT_PAPER_SCHWAB_TOKEN_INVALID")
    if not _agent_paper_output_directory_valid(repo_root):
        blocking_codes.add("AGENT_PAPER_OUTPUT_DIRECTORY_INVALID")
    if not _nonempty_environment_values(environment, ("FRED_API_KEY",)):
        optional_codes.append("AGENT_PAPER_FRED_OPTIONAL_UNCONFIGURED")
    return {
        "status": "blocked" if blocking_codes else "ready",
        "execution_locked": True,
        "external_requests": 0,
        "blocking_codes": sorted(blocking_codes),
        "optional_codes": optional_codes,
    }


def _macro_snapshot_contains_required_observations(
    observations: Sequence[FredResearchObservation], *, bound_ids: set[UUID]
) -> bool:
    return all(
        any(
            observation.series == series
            and observation.evidence_ref_id in bound_ids
            and observation.calendar_evidence_ref_id in bound_ids
            for observation in observations
        )
        for series in _PAPER_MACRO_SERIES
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


async def latest_visible_macro_vintage(
    session: AsyncSession,
    *,
    now: datetime,
    expected_calendar_authority_id: UUID,
) -> date:
    """Return the newest conservative vintage from the bound persisted calendar."""
    current = require_utc(now)
    opened_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == expected_calendar_authority_id,
            ExchangeCalendarSessionRecord.market_open_at <= current,
            ExchangeCalendarSessionRecord.created_at <= current,
        )
        .order_by(ExchangeCalendarSessionRecord.session.desc())
        .limit(1)
    )
    if not isinstance(opened_session, date):
        raise ValueError("AGENT_PAPER_CALENDAR_UNAVAILABLE")
    return opened_session - timedelta(days=1)


async def heartbeat_agent_paper_loop(
    settings: EngineSettings,
    *,
    worker_id: str,
    started_at: datetime,
    seen_at: datetime,
) -> None:
    """Persist one Agent paper-loop liveness fact without changing business state."""
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            await upsert_worker_heartbeat(
                session,
                queue=QueueName.AGENT_PAPER,
                worker_id=worker_id,
                process_version=AGENT_PAPER_LOOP_PROCESS_VERSION,
                migration_head=CURRENT_MIGRATION_HEAD,
                started_at=started_at,
                seen_at=seen_at,
            )
    finally:
        await engine.dispose()


async def _select_initial_service_calendar_authority_id(
    settings: EngineSettings,
) -> UUID:
    """Select one service calendar using the database clock in the same transaction."""
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            selected = await select_agent_paper_service_calendar_authority(session, now=now)
            return selected.calendar_authority_id
    finally:
        await engine.dispose()


async def _inspect_prior_daily_scope(
    settings: EngineSettings,
    *,
    account_id: UUID,
    now: datetime,
    expected_calendar_authority_id: UUID,
) -> PriorDailyScope:
    require_utc(now)
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            database_now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(database_now, datetime):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            service_calendar = await load_agent_paper_service_calendar_authority(
                session,
                now=database_now,
                expected_calendar_authority_id=expected_calendar_authority_id,
            )
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=account_id,
                now=database_now,
                expected_calendar_authority_id=expected_calendar_authority_id,
            )
            if (
                service_calendar.calendar_authority_id != expected_calendar_authority_id
                or authority.calendar_authority_id != expected_calendar_authority_id
            ):
                raise ValueError("AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH")
            target = await select_agent_paper_research_target(
                session,
                account_id=account_id,
                before_slot_at=floor_decision_slot(database_now),
            )
            scope = await inspect_query_authority_scope(
                session,
                decision_session=authority.decision_session,
                symbol=target.symbol,
                as_of=database_now,
                universe_snapshot_id=authority.universe_snapshot_id,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            )
            prior_session = await session.scalar(
                select(ExchangeCalendarSessionRecord.session)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == authority.calendar_authority_id,
                    ExchangeCalendarSessionRecord.session < authority.decision_session,
                )
                .order_by(ExchangeCalendarSessionRecord.session.desc())
                .limit(1)
            )
            if not isinstance(prior_session, date):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            existing = await session.scalar(
                select(PriceBarSetAuthority.id)
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                )
                .join(
                    PriceBarRecord,
                    PriceBarRecord.price_series_manifest_id == PriceSeriesManifest.id,
                )
                .where(
                    PriceBarSetAuthority.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
                    PriceBarSetAuthority.available_at <= database_now,
                    PriceBarSetAuthority.created_at <= database_now,
                    PriceBarRecord.instrument_id == scope.instrument_id,
                    PriceBarRecord.session == prior_session,
                    PriceBarRecord.available_at <= database_now,
                    PriceBarRecord.created_at <= database_now,
                    PriceSeriesManifest.created_at <= database_now,
                    PriceSeriesManifest.source == "alpaca",
                    PriceSeriesManifest.feed == "sip",
                    PriceSeriesManifest.delivery_mode == "delayed",
                    PriceSeriesManifest.adjustment == "all",
                    PriceSeriesManifest.interval == "1Day",
                )
                .order_by(PriceBarSetAuthority.available_at.desc(), PriceBarSetAuthority.id)
                .limit(1)
            )
            return PriorDailyScope(
                instrument_id=scope.instrument_id,
                prior_session=prior_session,
                authority_ready=existing is not None,
                decision_session=authority.decision_session,
                symbol=target.symbol,
            )
    finally:
        await engine.dispose()


async def prepare_prior_daily_once(
    settings: AgentPaperTriggerSettings,
    *,
    account_id: UUID,
    repo_root: Path,
    now: datetime,
    expected_calendar_authority_id: UUID,
) -> str:
    """Create at most one exact prior-session daily authority before a cycle."""
    scope = await _inspect_prior_daily_scope(
        settings,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    symbol = getattr(scope, "symbol", "AAPL")
    if scope.authority_ready:
        return "ready"
    parent = repo_root / ".secrets" / "nexus-agent-paper" / "daily"
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    output_parent = parent if symbol == "AAPL" else parent / symbol
    output_parent.mkdir(mode=0o700, exist_ok=True)
    output_dir = output_parent / scope.prior_session.isoformat()
    try:
        output_dir.mkdir(mode=0o700)
    except FileExistsError:
        return "unavailable"
    try:
        request, calendar = collection_window(
            scope.instrument_id,
            scope.prior_session,
            scope.prior_session,
            "all",
            now=now,
        )
        collected = await collect_prices(
            request,
            symbol=symbol,
            calendar=calendar,
            resolved_at=now,
            key_id=settings.alpaca_key,
            secret=settings.alpaca_secret,
            output_dir=output_dir,
            max_pages=10,
            identity_asof=scope.prior_session,
        )
        if collected.get("status") != "collected" or collected.get("readback_verified") is not True:
            return "unavailable"
        imported = await import_adjusted_authority(
            settings,
            instrument_id=scope.instrument_id,
            collection_dir=output_dir,
            purpose="agent-paper-decision",
        )
        if imported.get("status") != "stored_authority":
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    except ValueError:
        return "unavailable"
    verified = await _inspect_prior_daily_scope(
        settings,
        account_id=account_id,
        now=datetime.now(UTC),
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    if (
        not verified.authority_ready
        or verified.instrument_id != scope.instrument_id
        or verified.prior_session != scope.prior_session
        or getattr(verified, "symbol", "AAPL") != symbol
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    return "prepared"


async def _inspect_macro_scope(
    settings: EngineSettings,
    *,
    account_id: UUID,
    now: datetime,
    expected_calendar_authority_id: UUID,
) -> MacroScope:
    require_utc(now)
    prior = await _inspect_prior_daily_scope(
        settings,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            database_now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(database_now, datetime):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            await load_agent_paper_service_calendar_authority(
                session,
                now=database_now,
                expected_calendar_authority_id=expected_calendar_authority_id,
            )
            vintage = await latest_visible_macro_vintage(
                session,
                now=database_now,
                expected_calendar_authority_id=expected_calendar_authority_id,
            )
            observations = await read_fred_observations(
                session,
                as_of=database_now,
                series=_PAPER_MACRO_SERIES,
                vintage=vintage,
            )
            matching = tuple(
                observation
                for observation in observations.values()
                if observation.vintage == vintage and observation.series in _PAPER_MACRO_SERIES
            )
            ready_series = frozenset(observation.series for observation in matching)
            snapshot_ready = False
            if ready_series == _PAPER_MACRO_SERIES:
                snapshot = await select_authoritative_snapshot(
                    session,
                    instrument_id=prior.instrument_id,
                    as_of=database_now,
                )
                if snapshot is not None:
                    bound_ids = set(
                        await session.scalars(
                            select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                                AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id,
                                AgentDataSnapshotEvidenceRecord.instrument_id
                                == prior.instrument_id,
                                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                            )
                        )
                    )
                    snapshot_ready = _macro_snapshot_contains_required_observations(
                        matching,
                        bound_ids=bound_ids,
                    )
            return MacroScope(
                instrument_id=prior.instrument_id,
                vintage=vintage,
                ready_series=ready_series,
                snapshot_ready=snapshot_ready,
                attempt_session=prior.decision_session,
            )
    finally:
        await engine.dispose()


def _macro_request(series: str, vintage: date) -> FredVintageRequest:
    if series in _PAPER_MONTHLY_MACRO_SERIES:
        observation_end = vintage.replace(day=1) - timedelta(days=1)
        observation_start = observation_end.replace(day=1)
    else:
        observation_start = vintage - timedelta(days=31)
        observation_end = vintage
    return FredVintageRequest(
        series=series,
        observation_start=observation_start,
        observation_end=observation_end,
        vintage=vintage,
    )


async def prepare_macro_once(
    settings: DailyOnceSettings,
    *,
    fred_key: SecretStr,
    account_id: UUID,
    repo_root: Path,
    now: datetime,
    expected_calendar_authority_id: UUID,
) -> str:
    """Best-effort daily macro preparation; unavailable macro does not block a cycle."""
    scope = await _inspect_macro_scope(
        settings,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    if scope.ready_series == _PAPER_MACRO_SERIES:
        if scope.snapshot_ready:
            return "ready"
        await _prepare_current_trends_research_snapshot(
            settings,
            instrument_id=scope.instrument_id,
            now=datetime.now(UTC),
            macro_vintage=scope.vintage,
        )
        verified = await _inspect_macro_scope(
            settings,
            account_id=account_id,
            now=datetime.now(UTC),
            expected_calendar_authority_id=expected_calendar_authority_id,
        )
        if (
            verified.instrument_id != scope.instrument_id
            or verified.vintage != scope.vintage
            or verified.ready_series != _PAPER_MACRO_SERIES
            or not verified.snapshot_ready
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        return "prepared"
    try:
        validate_local_import(settings)
    except ValueError:
        return "unavailable"
    parent = repo_root / ".secrets" / "nexus-agent-paper" / "macro"
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    attempt_session = getattr(scope, "attempt_session", None)
    if not isinstance(attempt_session, date):
        attempt_session = now.astimezone(ZoneInfo("America/New_York")).date()
    output_dir = parent / attempt_session.isoformat()
    try:
        output_dir.mkdir(mode=0o700)
    except FileExistsError:
        return "unavailable"
    missing = _PAPER_MACRO_SERIES - scope.ready_series
    for series in sorted(missing):
        series_dir = output_dir / series
        series_dir.mkdir(mode=0o700)
        try:
            collected = await collect_fred(
                _macro_request(series, scope.vintage),
                key=fred_key,
                directory=series_dir,
            )
        except (HttpPolicyError, ValueError):
            return "unavailable"
        if collected.get("status") != "collected" or not collected.get("observation_count"):
            return "unavailable"
        imported = await import_fred_collection(
            settings,
            collection=load_fred_collection(series_dir),
        )
        if imported.get("status") != "imported" or not imported.get("visible_observation_count"):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    await _prepare_current_trends_research_snapshot(
        settings,
        instrument_id=scope.instrument_id,
        now=datetime.now(UTC),
        macro_vintage=scope.vintage,
    )
    verified = await _inspect_macro_scope(
        settings,
        account_id=account_id,
        now=datetime.now(UTC),
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    if (
        verified.instrument_id != scope.instrument_id
        or verified.vintage != scope.vintage
        or verified.ready_series != _PAPER_MACRO_SERIES
        or not verified.snapshot_ready
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    return "prepared"


async def _inspect_current_trends_scope(
    settings: EngineSettings,
    *,
    account_id: UUID,
    now: datetime,
    expected_calendar_authority_id: UUID,
) -> CurrentTrendsScope:
    require_utc(now)
    prior = await _inspect_prior_daily_scope(
        settings,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            database_now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(database_now, datetime):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            await load_agent_paper_service_calendar_authority(
                session,
                now=database_now,
                expected_calendar_authority_id=expected_calendar_authority_id,
            )
            required_start = prior.prior_session - timedelta(days=366)
            authority_range_start = await session.scalar(
                select(ExchangeCalendarAuthorityRecord.range_start).where(
                    ExchangeCalendarAuthorityRecord.id == expected_calendar_authority_id,
                    ExchangeCalendarAuthorityRecord.range_start <= required_start,
                )
            )
            if not isinstance(authority_range_start, date):
                raise ValueError("AGENT_PAPER_CALENDAR_UNAVAILABLE")
            start_session = await session.scalar(
                select(ExchangeCalendarSessionRecord.session)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == expected_calendar_authority_id,
                    ExchangeCalendarSessionRecord.session >= required_start,
                    ExchangeCalendarSessionRecord.session <= prior.prior_session,
                    ExchangeCalendarSessionRecord.created_at <= database_now,
                )
                .order_by(ExchangeCalendarSessionRecord.session.asc())
                .limit(1)
            )
            if not isinstance(start_session, date):
                raise ValueError("AGENT_PAPER_CALENDAR_UNAVAILABLE")
            documents = await read_current_price_trends(
                session,
                instrument_id=prior.instrument_id,
                as_of=database_now,
            )
            evidence_ready = len(documents) == 1 and all(
                document.last_observed_on == prior.prior_session
                and all(
                    document.features[name] is not None
                    for name in ("sma_20", "sma_200", "rsi_14", "volatility_20")
                )
                for document in documents.values()
            )
            snapshot_ready = False
            if evidence_ready:
                evidence_ref_id = next(iter(documents))
                snapshot = await select_authoritative_snapshot(
                    session,
                    instrument_id=prior.instrument_id,
                    as_of=database_now,
                )
                snapshot_ready = snapshot is not None and bool(
                    await session.scalar(
                        select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                            AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id,
                            AgentDataSnapshotEvidenceRecord.instrument_id == prior.instrument_id,
                            AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                            AgentDataSnapshotEvidenceRecord.source == CURRENT_TRENDS_SOURCE,
                            AgentDataSnapshotEvidenceRecord.evidence_ref_id == evidence_ref_id,
                        )
                    )
                )
            return CurrentTrendsScope(
                instrument_id=prior.instrument_id,
                start_session=start_session,
                prior_session=prior.prior_session,
                evidence_ready=evidence_ready,
                snapshot_ready=snapshot_ready,
                symbol=prior.symbol,
            )
    finally:
        await engine.dispose()


async def _prepare_current_trends_research_snapshot(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    now: datetime,
    macro_vintage: date | None = None,
) -> None:
    """Bind already-persisted trends into the normal authoritative research snapshot."""
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            snapshot = await prepare_research_snapshot(
                session,
                instrument_id,
                as_of=now,
                macro_series=(_PAPER_MACRO_SERIES if macro_vintage is not None else frozenset()),
                macro_vintage=macro_vintage,
            )
            if snapshot is None:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    finally:
        await engine.dispose()


async def prepare_current_trends_once(
    settings: AgentPaperTriggerSettings,
    *,
    account_id: UUID,
    repo_root: Path,
    now: datetime,
    expected_calendar_authority_id: UUID,
) -> str:
    """Create at most one current-trends evidence record for the prior session."""
    scope = await _inspect_current_trends_scope(
        settings,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    symbol = getattr(scope, "symbol", "AAPL")
    if scope.snapshot_ready:
        return "ready"
    if scope.evidence_ready:
        await _prepare_current_trends_research_snapshot(
            settings,
            instrument_id=scope.instrument_id,
            now=datetime.now(UTC),
        )
        verified = await _inspect_current_trends_scope(
            settings,
            account_id=account_id,
            now=datetime.now(UTC),
            expected_calendar_authority_id=expected_calendar_authority_id,
        )
        if (
            not verified.evidence_ready
            or not verified.snapshot_ready
            or verified.instrument_id != scope.instrument_id
            or verified.prior_session != scope.prior_session
            or getattr(verified, "symbol", "AAPL") != symbol
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        return "prepared"
    parent = repo_root / ".secrets" / "nexus-agent-paper" / "trends"
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    output_parent = parent if symbol == "AAPL" else parent / symbol
    output_parent.mkdir(mode=0o700, exist_ok=True)
    output_dir = output_parent / scope.prior_session.isoformat()
    try:
        output_dir.mkdir(mode=0o700)
    except FileExistsError:
        return "unavailable"
    try:
        request, calendar = collection_window(
            scope.instrument_id,
            scope.start_session,
            scope.prior_session,
            "split",
            now=now,
        )
    except ValueError:
        return "unavailable"
    collected = await collect_prices(
        request,
        symbol=symbol,
        calendar=calendar,
        resolved_at=now,
        key_id=settings.alpaca_key,
        secret=settings.alpaca_secret,
        output_dir=output_dir,
        max_pages=10,
        identity_asof=scope.prior_session,
    )
    if collected.get("status") != "collected" or collected.get("readback_verified") is not True:
        return "unavailable"
    imported = await import_trends(
        settings,
        instrument_id=scope.instrument_id,
        collection_dir=output_dir,
    )
    if imported.get("status") != "stored_context":
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    if imported.get("last_observed_on") != scope.prior_session.isoformat():
        return "unavailable"
    await _prepare_current_trends_research_snapshot(
        settings,
        instrument_id=scope.instrument_id,
        now=datetime.now(UTC),
    )
    verified = await _inspect_current_trends_scope(
        settings,
        account_id=account_id,
        now=datetime.now(UTC),
        expected_calendar_authority_id=expected_calendar_authority_id,
    )
    if (
        not verified.evidence_ready
        or not verified.snapshot_ready
        or verified.instrument_id != scope.instrument_id
        or verified.prior_session != scope.prior_session
        or getattr(verified, "symbol", "AAPL") != symbol
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    return "prepared"


def resolve_cycle_research_job(
    *, events: Sequence[object], jobs: Sequence[object]
) -> tuple[UUID, str] | None:
    """Resolve exactly one Job whose persisted run ID belongs to the cycle."""
    run_ids: list[str] = []
    for event in events:
        fact_ids = getattr(event, "fact_ids", None)
        raw = fact_ids.get("research_run_ids") if isinstance(fact_ids, dict) else None
        if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
            run_ids.extend(raw)
    if len(run_ids) != 1 or len(jobs) != 1:
        return None
    job = jobs[0]
    payload = getattr(job, "payload", None)
    job_id = getattr(job, "id", None)
    status = getattr(job, "status", None)
    if (
        not isinstance(payload, dict)
        or payload.get("run_id") != run_ids[0]
        or not isinstance(job_id, UUID)
        or not isinstance(status, str)
    ):
        return None
    return job_id, status


def snapshot_from_cycle_view(
    *,
    now: datetime,
    view: object,
    research_job: tuple[UUID, str] | None,
    market_scope: object | None,
    calendar: ServiceCalendarAuthority,
    market_window: object | None = None,
    performance_due_at: datetime | None = None,
) -> AgentPaperLoopSnapshot:
    """Map the bounded read projection into the scheduler's fail-closed contract."""
    cycle_id = getattr(view, "cycle_id", None)
    cycle_status = getattr(view, "status", None)
    cycle_result_code = getattr(view, "result_code", None)
    cycle_slot_at = getattr(view, "decision_slot_at", None)
    order_ids = getattr(view, "order_ids", ())
    execution_market = getattr(view, "execution_market", None)
    market_status = getattr(execution_market, "status", None)
    if (
        not isinstance(cycle_id, UUID)
        or not isinstance(cycle_status, str)
        or not isinstance(cycle_slot_at, datetime)
        or not isinstance(order_ids, tuple)
        or any(not isinstance(value, UUID) for value in order_ids)
        or len(order_ids) > 1
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    order_id = order_ids[0] if order_ids else None
    has_open_order = cycle_status == "awaiting_market" and order_id is not None
    market_window_ready = has_open_order and market_status == "qualified"
    market_window_start_at = None
    market_window_end_at = None
    market_window_id = None
    if has_open_order and not market_window_ready:
        scope_cycle_id = getattr(market_scope, "cycle_id", None)
        market_window_start_at = getattr(market_scope, "window_start", None)
        market_window_end_at = getattr(market_scope, "window_end", None)
        if (
            scope_cycle_id != cycle_id
            or not isinstance(market_window_start_at, datetime)
            or not isinstance(market_window_end_at, datetime)
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        market_window_end_at += _MARKET_PUBLICATION_LAG
    elif has_open_order:
        market_window_id = getattr(market_window, "id", None)
        market_cycle_id = getattr(market_window, "cycle_id", None)
        market_order_id = getattr(market_window, "order_id", None)
        market_window_start_at = getattr(market_window, "window_start_at", None)
        market_window_end_at = getattr(market_window, "window_end_at", None)
        if (
            not isinstance(market_window_id, UUID)
            or market_cycle_id != cycle_id
            or market_order_id != order_id
            or not isinstance(market_window_start_at, datetime)
            or not isinstance(market_window_end_at, datetime)
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    return AgentPaperLoopSnapshot(
        now=now,
        calendar_authority_id=calendar.calendar_authority_id,
        decision_window_start_at=calendar.decision_window_start_at,
        decision_window_end_at=calendar.market_close_at,
        next_decision_window_start_at=calendar.next_decision_window_start_at,
        cycle_id=cycle_id,
        cycle_status=cycle_status,
        cycle_result_code=(cycle_result_code if isinstance(cycle_result_code, str) else None),
        cycle_slot_at=cycle_slot_at,
        research_job_status=None if research_job is None else research_job[1],
        research_job_id=None if research_job is None else research_job[0],
        market_window_ready=market_window_ready,
        market_window_start_at=market_window_start_at,
        market_window_end_at=market_window_end_at,
        market_window_id=market_window_id,
        has_open_order=has_open_order,
        order_id=order_id,
        performance_due_at=performance_due_at,
    )


def choose_loop_action(snapshot: AgentPaperLoopSnapshot) -> AgentPaperLoopAction:
    """Choose one safe next action without performing I/O or retrying unknown work."""
    if not snapshot.enabled:
        return AgentPaperLoopAction.STOP_DISABLED

    if snapshot.cycle_status == "researching":
        if snapshot.research_job_status == "queued":
            return AgentPaperLoopAction.RUN_RESEARCH_JOB
        if snapshot.research_job_status in _TERMINAL_JOB_STATUSES:
            return AgentPaperLoopAction.RESUME
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    if (
        snapshot.cycle_status == "awaiting_market"
        and snapshot.has_open_order
        and snapshot.market_window_ready
    ):
        return AgentPaperLoopAction.ADVANCE_ORDER

    if snapshot.cycle_status == "awaiting_market" and snapshot.has_open_order:
        if snapshot.market_window_end_at is None:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        if snapshot.now < snapshot.market_window_end_at:
            return AgentPaperLoopAction.WAIT_FOR_MARKET
        return AgentPaperLoopAction.COLLECT_WINDOW

    if snapshot.cycle_status in {"decision_recorded", "risk_evaluated"}:
        return AgentPaperLoopAction.RESUME

    if (
        snapshot.calendar_authority_id is None
        or snapshot.decision_window_start_at is None
        or snapshot.decision_window_end_at is None
        or snapshot.next_decision_window_start_at is None
    ):
        return AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE
    if snapshot.now < snapshot.decision_window_start_at:
        return AgentPaperLoopAction.WAIT_FOR_SESSION
    if snapshot.now >= snapshot.decision_window_end_at:
        return AgentPaperLoopAction.WAIT_FOR_SESSION

    if snapshot.performance_due_at is not None:
        return AgentPaperLoopAction.PUBLISH_PERFORMANCE

    current_slot = floor_decision_slot(snapshot.now)
    if snapshot.cycle_status in {"unavailable", "failed"} and (
        snapshot.cycle_slot_at is None or snapshot.cycle_slot_at == current_slot
    ):
        if snapshot.cycle_result_code in _CANDIDATE_RETRY_CODES:
            return AgentPaperLoopAction.WAIT_FOR_SLOT
        return AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE

    if snapshot.cycle_status not in {None, "completed", "unavailable", "failed"}:
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    if snapshot.cycle_slot_at == current_slot:
        return AgentPaperLoopAction.WAIT_FOR_SLOT
    return AgentPaperLoopAction.TRIGGER


async def run_foreground_loop(
    *,
    inspect: Callable[[], Awaitable[AgentPaperLoopSnapshot]],
    execute: Callable[
        [AgentPaperLoopAction, AgentPaperLoopSnapshot],
        Awaitable[AgentPaperLoopAction | None],
    ],
    sleep: Callable[[float], Awaitable[None]],
    heartbeat: Callable[[datetime], Awaitable[None]],
    shutdown_requested: Callable[[], bool] = lambda: False,
    confirm_shutdown: Callable[
        [AgentPaperLoopAction, AgentPaperLoopSnapshot, AgentPaperLoopAction | None],
        Awaitable[bool],
    ] = lambda _action, _snapshot, _result: asyncio.sleep(0, result=False),
    heartbeat_interval: timedelta = timedelta(seconds=15),
    clock: Callable[[], datetime] = _utc_now,
    monotonic: Callable[[], float] = monotonic_clock,
) -> AgentPaperLoopAction:
    """Run persisted one-step actions until a fail-closed stop condition is reached."""
    interval_seconds = heartbeat_interval.total_seconds()
    if interval_seconds <= 0:
        raise ValueError("heartbeat_interval must be positive")

    async def wait_for_heartbeat_interval(
        delay_seconds: float,
    ) -> AgentPaperLoopAction | None:
        await sleep(delay_seconds)
        return None

    async def persist_heartbeat() -> None:
        async with asyncio.timeout(interval_seconds):
            await heartbeat(clock())

    if shutdown_requested():
        return AgentPaperLoopAction.STOP_REQUESTED
    try:
        await persist_heartbeat()
    except Exception:
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    while True:
        if shutdown_requested():
            return AgentPaperLoopAction.STOP_REQUESTED
        snapshot = await inspect()
        if shutdown_requested():
            return AgentPaperLoopAction.STOP_REQUESTED
        action = choose_loop_action(snapshot)
        if action in {
            AgentPaperLoopAction.STOP_RESULT_UNKNOWN,
            AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE,
            AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE,
            AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH,
            AgentPaperLoopAction.STOP_DISABLED,
            AgentPaperLoopAction.STOP_SESSION_CLOSED,
        }:
            return action
        if action == AgentPaperLoopAction.WAIT_FOR_SLOT:
            next_slot = floor_decision_slot(snapshot.now) + timedelta(minutes=5)
            remaining = (next_slot - snapshot.now).total_seconds()
            while remaining > 0:
                duration = min(remaining, interval_seconds)
                await sleep(duration)
                if shutdown_requested():
                    return AgentPaperLoopAction.STOP_REQUESTED
                try:
                    await persist_heartbeat()
                except Exception:
                    return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
                remaining -= duration
            continue
        if action == AgentPaperLoopAction.WAIT_FOR_SESSION:
            assert snapshot.decision_window_start_at is not None
            assert snapshot.next_decision_window_start_at is not None
            if snapshot.now < snapshot.decision_window_start_at:
                wait_until = snapshot.decision_window_start_at
            else:
                wait_until = snapshot.next_decision_window_start_at
            deadline = monotonic() + (wait_until - snapshot.now).total_seconds()
            while (remaining := deadline - monotonic()) > 0:
                duration = min(remaining, interval_seconds)
                await sleep(duration)
                if shutdown_requested():
                    return AgentPaperLoopAction.STOP_REQUESTED
                try:
                    await persist_heartbeat()
                except Exception:
                    return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
            continue
        if action == AgentPaperLoopAction.WAIT_FOR_MARKET:
            assert snapshot.market_window_end_at is not None
            remaining = (snapshot.market_window_end_at - snapshot.now).total_seconds()
            while remaining > 0:
                duration = min(remaining, interval_seconds)
                await sleep(duration)
                if shutdown_requested():
                    return AgentPaperLoopAction.STOP_REQUESTED
                try:
                    await persist_heartbeat()
                except Exception:
                    return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
                remaining -= duration
            continue
        action_task: asyncio.Future[AgentPaperLoopAction | None] = asyncio.ensure_future(
            execute(action, snapshot)
        )
        heartbeat_task: asyncio.Future[AgentPaperLoopAction | None] | None = None
        heartbeat_failed = False
        action_error: Exception | None = None
        try:
            await asyncio.sleep(0)
            next_heartbeat_at = monotonic() + interval_seconds
            while not action_task.done():
                heartbeat_task = asyncio.ensure_future(
                    wait_for_heartbeat_interval(max(0.0, next_heartbeat_at - monotonic()))
                )
                done, _ = await asyncio.wait(
                    {action_task, heartbeat_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if action_task in done:
                    break
                await heartbeat_task
                heartbeat_task = None
                try:
                    await persist_heartbeat()
                except Exception:
                    heartbeat_failed = True
                    break
                next_heartbeat_at += interval_seconds
            try:
                stop = await action_task
            except Exception as error:
                action_error = error
                stop = AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        finally:
            if heartbeat_task is not None:
                if not heartbeat_task.done() and action_task.done():
                    heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
        if shutdown_requested():
            try:
                confirmed = await confirm_shutdown(action, snapshot, stop)
            except Exception:
                confirmed = False
            if action_error is not None or heartbeat_failed or not confirmed:
                return AgentPaperLoopAction.STOP_UNCONFIRMED
            return AgentPaperLoopAction.STOP_REQUESTED
        if action_error is not None:
            raise action_error
        if heartbeat_failed:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        try:
            await persist_heartbeat()
        except Exception:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        if stop is not None:
            return stop


def _shutdown_postcondition_matches(
    *,
    action: AgentPaperLoopAction,
    before: AgentPaperLoopSnapshot,
    result: AgentPaperLoopAction | None,
    after: AgentPaperLoopSnapshot,
) -> bool:
    if result is not None or after.calendar_authority_id != before.calendar_authority_id:
        return False
    if action == AgentPaperLoopAction.TRIGGER:
        return (
            after.cycle_id is not None
            and after.cycle_slot_at == floor_decision_slot(before.now)
            and after.cycle_status
            in {
                "researching",
                "decision_recorded",
                "risk_evaluated",
                "awaiting_market",
                "completed",
            }
        )
    if action == AgentPaperLoopAction.RUN_RESEARCH_JOB:
        return (
            before.cycle_id is not None
            and after.cycle_id == before.cycle_id
            and before.research_job_id is not None
            and after.research_job_id == before.research_job_id
            and after.research_job_status in _TERMINAL_JOB_STATUSES
        )
    if action == AgentPaperLoopAction.RESUME:
        return (
            before.cycle_id is not None
            and after.cycle_id == before.cycle_id
            and after.cycle_status
            in {
                "decision_recorded",
                "risk_evaluated",
                "awaiting_market",
                "completed",
                "unavailable",
            }
        )
    return False


async def confirm_agent_paper_shutdown(
    settings: EngineSettings,
    *,
    account_id: UUID,
    expected_calendar_authority_id: UUID,
    action: AgentPaperLoopAction,
    before_snapshot: AgentPaperLoopSnapshot,
    action_result: AgentPaperLoopAction | None,
) -> bool:
    """Read once and verify the exact action's persisted shutdown postcondition."""
    exact_actions = {
        AgentPaperLoopAction.COLLECT_WINDOW,
        AgentPaperLoopAction.ADVANCE_ORDER,
        AgentPaperLoopAction.PUBLISH_PERFORMANCE,
    }
    if action in exact_actions:
        if (
            before_snapshot.calendar_authority_id != expected_calendar_authority_id
            or not settings.database_url
        ):
            return False
        database = make_url(settings.database_url)
        if (
            database.drivername != "postgresql+asyncpg"
            or database.host not in {"localhost", "127.0.0.1", "::1"}
            or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
        ):
            return False
        engine = create_async_engine(settings.database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as session:
                if action == AgentPaperLoopAction.PUBLISH_PERFORMANCE:
                    if before_snapshot.performance_due_at is None:
                        return False
                    persisted = await session.scalar(
                        select(PaperPerformanceSnapshotRecord.id).where(
                            PaperPerformanceSnapshotRecord.account_id == account_id,
                            PaperPerformanceSnapshotRecord.as_of
                            == before_snapshot.performance_due_at,
                        )
                    )
                elif action == AgentPaperLoopAction.COLLECT_WINDOW:
                    if (
                        before_snapshot.cycle_id is None
                        or before_snapshot.order_id is None
                        or before_snapshot.market_window_start_at is None
                        or before_snapshot.market_window_end_at is None
                    ):
                        return False
                    persisted = await session.scalar(
                        select(AgentPaperMarketWindowRecord.id).where(
                            AgentPaperMarketWindowRecord.cycle_id == before_snapshot.cycle_id,
                            AgentPaperMarketWindowRecord.order_id == before_snapshot.order_id,
                            AgentPaperMarketWindowRecord.purpose == "paper_fill",
                            AgentPaperMarketWindowRecord.window_start_at
                            == before_snapshot.market_window_start_at,
                            AgentPaperMarketWindowRecord.window_end_at
                            == before_snapshot.market_window_end_at - _MARKET_PUBLICATION_LAG,
                        )
                    )
                else:
                    if (
                        before_snapshot.cycle_id is None
                        or before_snapshot.order_id is None
                        or before_snapshot.market_window_id is None
                    ):
                        return False
                    persisted = await session.scalar(
                        select(PaperRequestIdempotencyRecord.id)
                        .join(
                            PaperOrderEventRecord,
                            and_(
                                PaperOrderEventRecord.order_id
                                == PaperRequestIdempotencyRecord.order_id,
                                PaperOrderEventRecord.account_id
                                == PaperRequestIdempotencyRecord.account_id,
                                PaperOrderEventRecord.event_seq
                                == PaperRequestIdempotencyRecord.result_event_seq,
                            ),
                        )
                        .join(
                            PaperFillRecord,
                            PaperFillRecord.id == PaperOrderEventRecord.source_fill_id,
                        )
                        .join(
                            PaperExecutionWindowAuthorityRecord,
                            PaperExecutionWindowAuthorityRecord.id
                            == PaperFillRecord.execution_window_authority_id,
                        )
                        .join(
                            AgentPaperMarketWindowRecord,
                            AgentPaperMarketWindowRecord.id == before_snapshot.market_window_id,
                        )
                        .where(
                            PaperRequestIdempotencyRecord.account_id == account_id,
                            PaperRequestIdempotencyRecord.order_id == before_snapshot.order_id,
                            PaperRequestIdempotencyRecord.operation == "advance",
                            PaperRequestIdempotencyRecord.idempotency_key
                            == f"agent-paper-loop-advance:{before_snapshot.order_id}",
                            PaperOrderEventRecord.to_status.in_(("partially_filled", "filled")),
                            AgentPaperMarketWindowRecord.cycle_id == before_snapshot.cycle_id,
                            AgentPaperMarketWindowRecord.order_id == before_snapshot.order_id,
                            AgentPaperMarketWindowRecord.purpose == "paper_fill",
                            PaperExecutionWindowAuthorityRecord.order_id
                            == before_snapshot.order_id,
                            PaperExecutionWindowAuthorityRecord.account_id == account_id,
                            PaperExecutionWindowAuthorityRecord.window_start_at
                            == AgentPaperMarketWindowRecord.window_start_at,
                            PaperExecutionWindowAuthorityRecord.window_end_at
                            == AgentPaperMarketWindowRecord.window_end_at,
                            PaperExecutionWindowAuthorityRecord.source_authority_id
                            == AgentPaperMarketWindowRecord.source_authority_id,
                        )
                    )
                return persisted is not None and action_result is None
        except Exception:
            return False
        finally:
            await engine.dispose()
    try:
        after_snapshot = await inspect_agent_paper_loop(
            settings,
            account_id=account_id,
            expected_calendar_authority_id=expected_calendar_authority_id,
        )
    except Exception:
        return False
    return _shutdown_postcondition_matches(
        action=action,
        before=before_snapshot,
        result=action_result,
        after=after_snapshot,
    )


async def run_local_agent_paper_loop(
    settings: EngineSettings,
    *,
    account_id: UUID,
    repo_root: Path,
    env_file: Path | None = None,
) -> AgentPaperLoopAction:
    """Run the concrete local loop through persisted inspection and scoped actions."""

    if not settings.database_url:
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    worker_id = f"agent-paper-loop-{new_uuid7()}"
    started_at = datetime.now(UTC)
    with acquire_agent_paper_lock():
        shutdown = asyncio.Event()
        event_loop = asyncio.get_running_loop()
        installed_signals: list[signal.Signals] = []
        try:
            for item in (signal.SIGINT, signal.SIGTERM):
                event_loop.add_signal_handler(item, shutdown.set)
                installed_signals.append(item)
            if model_blocker := await _codex_sdk_preflight_blocker_async():
                raise RuntimeError(model_blocker)
            try:
                calendar_authority_id = await _select_initial_service_calendar_authority_id(
                    settings
                )
            except ValueError as error:
                if str(error) == "AGENT_PAPER_CALENDAR_UNAVAILABLE":
                    return AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE
                if str(error) == "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH":
                    return AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH
                raise

            async def inspect() -> AgentPaperLoopSnapshot:
                current = (
                    _FileEngineSettings.model_validate(
                        load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
                    )
                    if env_file is not None
                    else EngineSettings.model_validate(
                        load_agent_paper_service_environment(repo_root, os.environ)
                    )
                )
                if not current.agent_paper_cycle_enabled:
                    return AgentPaperLoopSnapshot(now=datetime.now(UTC), enabled=False)
                return await inspect_agent_paper_loop(
                    current,
                    account_id=account_id,
                    expected_calendar_authority_id=calendar_authority_id,
                )

            async def execute(
                action: AgentPaperLoopAction, snapshot: AgentPaperLoopSnapshot
            ) -> AgentPaperLoopAction | None:
                return await execute_agent_paper_loop_action(
                    settings,
                    account_id=account_id,
                    repo_root=repo_root,
                    action=action,
                    snapshot=snapshot,
                    **({"env_file": env_file} if env_file is not None else {}),
                )

            async def heartbeat(seen_at: datetime) -> None:
                await heartbeat_agent_paper_loop(
                    settings,
                    worker_id=worker_id,
                    started_at=started_at,
                    seen_at=seen_at,
                )

            async def confirm_shutdown(
                action: AgentPaperLoopAction,
                before_snapshot: AgentPaperLoopSnapshot,
                action_result: AgentPaperLoopAction | None,
            ) -> bool:
                return await confirm_agent_paper_shutdown(
                    settings,
                    account_id=account_id,
                    expected_calendar_authority_id=calendar_authority_id,
                    action=action,
                    before_snapshot=before_snapshot,
                    action_result=action_result,
                )

            try:
                return await run_foreground_loop(
                    inspect=inspect,
                    execute=execute,
                    sleep=asyncio.sleep,
                    heartbeat=heartbeat,
                    shutdown_requested=shutdown.is_set,
                    confirm_shutdown=confirm_shutdown,
                )
            except ValueError as error:
                if str(error) == "AGENT_PAPER_CALENDAR_UNAVAILABLE":
                    return AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE
                if str(error) == "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH":
                    return AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH
                raise
        finally:
            for installed_signal in installed_signals:
                event_loop.remove_signal_handler(installed_signal)


async def execute_agent_paper_loop_action(
    settings: EngineSettings,
    *,
    account_id: UUID,
    repo_root: Path,
    action: AgentPaperLoopAction,
    snapshot: AgentPaperLoopSnapshot,
    env_file: Path | None = None,
) -> AgentPaperLoopAction | None:
    """Execute exactly one planner-approved action through existing product services."""
    if action == AgentPaperLoopAction.TRIGGER:
        if snapshot.calendar_authority_id is None:
            return AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE
        expected_calendar_authority_id = snapshot.calendar_authority_id
        environment = (
            load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
            if env_file is not None
            else load_agent_paper_service_environment(repo_root, os.environ)
        )
        trigger_settings = (
            _FileTriggerSettings if env_file is not None else AgentPaperTriggerSettings
        ).model_validate(environment)
        if env_file is not None:
            _require_explicit_schwab_config(environment)
        macro_settings = (
            _FileDailySettings if env_file is not None else DailyOnceSettings
        ).model_validate(environment)
        prior_daily = await prepare_prior_daily_once(
            trigger_settings,
            account_id=account_id,
            repo_root=repo_root,
            now=snapshot.now,
            expected_calendar_authority_id=expected_calendar_authority_id,
        )
        if prior_daily == "prepared":
            return None
        if prior_daily == "unavailable":
            return AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE
        if prior_daily != "ready":
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        macro = await prepare_macro_once(
            macro_settings,
            fred_key=SecretStr(environment.get("FRED_API_KEY", "")),
            account_id=account_id,
            repo_root=repo_root,
            now=snapshot.now,
            expected_calendar_authority_id=expected_calendar_authority_id,
        )
        if macro == "prepared":
            return None
        if macro not in {"ready", "unavailable"}:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        current_trends = await prepare_current_trends_once(
            trigger_settings,
            account_id=account_id,
            repo_root=repo_root,
            now=snapshot.now,
            expected_calendar_authority_id=expected_calendar_authority_id,
        )
        if current_trends == "prepared":
            return None
        if current_trends == "unavailable":
            return AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE
        if current_trends != "ready":
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        await trigger_once(
            trigger_settings,
            AgentPaperTriggerRequest(
                action="trigger",
                account_id=account_id,
                idempotency_key=(
                    f"agent-paper-loop:{floor_decision_slot(snapshot.now).isoformat()}"
                ),
            ),
            repo_root=repo_root,
            expected_calendar_authority_id=expected_calendar_authority_id,
            **({"process_environment": environment} if env_file is not None else {}),
        )
        return None
    if action == AgentPaperLoopAction.RUN_RESEARCH_JOB:
        if not isinstance(snapshot.research_job_id, UUID):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        research_settings = (
            _FileEngineSettings.model_validate(
                {
                    **load_agent_paper_service_environment(repo_root, {}, env_file=env_file),
                    "OPENAI_API_KEY": None,
                    "NEXUS_PROVIDER_JOBS_ENABLED": "false",
                }
            )
            if env_file is not None
            else local_settings(repo_root, backend="codex_sdk")
        )
        worked = await run_agent_worker_once(
            research_settings,
            job_ids=(snapshot.research_job_id,),
        )
        if not worked:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        return None
    if action == AgentPaperLoopAction.PUBLISH_PERFORMANCE:
        if snapshot.performance_due_at is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        published = await publish_due_performance_once(
            settings,
            account_id=account_id,
            as_of=snapshot.performance_due_at,
        )
        if not published:
            return AgentPaperLoopAction.STOP_PERFORMANCE_UNAVAILABLE
        return None
    if snapshot.cycle_id is None:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    if action == AgentPaperLoopAction.RESUME:
        await resume_once(
            settings,
            AgentPaperResumeRequest(
                action="resume",
                cycle_id=snapshot.cycle_id,
                idempotency_key=f"agent-paper-loop-resume:{snapshot.cycle_id}",
            ),
        )
        return None
    if action == AgentPaperLoopAction.COLLECT_WINDOW:
        environment = (
            load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
            if env_file is not None
            else load_agent_paper_service_environment(repo_root, os.environ)
        )
        if env_file is not None:
            _require_explicit_schwab_config(environment)
        await collect_window_once(
            (
                _FileMarketSettings if env_file is not None else AgentPaperMarketSettings
            ).model_validate(environment),
            AgentPaperCollectWindowRequest(action="collect-window", cycle_id=snapshot.cycle_id),
            repo_root=repo_root,
            **({"process_environment": environment} if env_file is not None else {}),
        )
        return None
    if action == AgentPaperLoopAction.ADVANCE_ORDER and snapshot.order_id is not None:
        result = await run_paper_once(
            settings,
            PaperAdvanceRequest(
                action="advance",
                account_id=account_id,
                order_id=snapshot.order_id,
                evaluated_at=snapshot.now,
                idempotency_key=f"agent-paper-loop-advance:{snapshot.order_id}",
            ),
        )
        if result.get("action") in {"no_data", "no_capacity"}:
            return AgentPaperLoopAction.STOP_ORDER_UNAVAILABLE
        return None
    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")


async def publish_due_performance_once(
    settings: EngineSettings,
    *,
    account_id: UUID,
    as_of: datetime,
) -> bool:
    """Publish one exact close, stopping cleanly when its price facts are incomplete."""
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory.begin() as session:
                await publish_paper_performance_snapshot(
                    session,
                    account_id=account_id,
                    as_of=as_of,
                )
        except PaperPerformanceError as error:
            if error.code == "PAPER_PERFORMANCE_PRICE_INCOMPLETE":
                return False
            raise
        return True
    finally:
        await engine.dispose()


async def inspect_agent_paper_loop(
    settings: EngineSettings,
    *,
    account_id: UUID,
    expected_calendar_authority_id: UUID,
) -> AgentPaperLoopSnapshot:
    """Read one bounded scheduler snapshot without starting or replaying work."""
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            calendar = await load_agent_paper_service_calendar_authority(
                session,
                now=now,
                expected_calendar_authority_id=expected_calendar_authority_id,
            )
            service = PaperReadService(session, now)
            account, _ = await service.account()
            if account.id != account_id:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            latest_completed_close = await session.scalar(
                select(func.max(ExchangeCalendarSessionRecord.market_close_at))
                .join(
                    ExchangeCalendarAuthorityRecord,
                    ExchangeCalendarAuthorityRecord.id
                    == ExchangeCalendarSessionRecord.calendar_authority_id,
                )
                .where(
                    ExchangeCalendarAuthorityRecord.id == expected_calendar_authority_id,
                    ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                    ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                    ExchangeCalendarAuthorityRecord.published_at
                    <= ExchangeCalendarSessionRecord.market_close_at,
                    ExchangeCalendarSessionRecord.market_close_at <= now,
                )
            )
            performance_due_at = None
            if isinstance(latest_completed_close, datetime):
                existing_performance = await session.scalar(
                    select(PaperPerformanceSnapshotRecord.id).where(
                        PaperPerformanceSnapshotRecord.account_id == account_id,
                        PaperPerformanceSnapshotRecord.as_of == latest_completed_close,
                    )
                )
                if existing_performance is None:
                    performance_due_at = latest_completed_close
            view = await service.latest_agent_cycle()
            if view is None:
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=calendar.calendar_authority_id,
                    decision_window_start_at=calendar.decision_window_start_at,
                    decision_window_end_at=calendar.market_close_at,
                    next_decision_window_start_at=calendar.next_decision_window_start_at,
                    performance_due_at=performance_due_at,
                )

            events: tuple[object, ...] = ()
            jobs: tuple[object, ...] = ()
            if view.status == "researching":
                events = tuple(
                    await session.scalars(
                        select(AgentPaperCycleEventRecord)
                        .where(AgentPaperCycleEventRecord.cycle_id == view.cycle_id)
                        .order_by(AgentPaperCycleEventRecord.sequence)
                    )
                )
                run_ids: list[str] = []
                for event in events:
                    fact_ids = getattr(event, "fact_ids", None)
                    raw = fact_ids.get("research_run_ids") if isinstance(fact_ids, dict) else None
                    if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
                        run_ids.extend(raw)
                if run_ids:
                    jobs = tuple(
                        await session.scalars(
                            select(Job).where(
                                Job.job_type.in_(
                                    (
                                        JobType.AGENT_RESEARCH.value,
                                        JobType.AGENT_PAPER_RESEARCH.value,
                                    )
                                ),
                                Job.payload["run_id"].as_string().in_(tuple(run_ids)),
                            )
                        )
                    )
            research_job = resolve_cycle_research_job(events=events, jobs=jobs)
            market_scope = None
            market_window = None
            if (
                view.status == "awaiting_market"
                and len(view.order_ids) == 1
                and view.execution_market.status != "qualified"
            ):
                market_scope = await resolve_agent_paper_market_scope(
                    session, cycle_id=view.cycle_id
                )
            elif (
                view.status == "awaiting_market"
                and len(view.order_ids) == 1
                and view.execution_market.status == "qualified"
            ):
                unused_windows = tuple(
                    await session.scalars(
                        select(AgentPaperMarketWindowRecord)
                        .outerjoin(
                            PaperExecutionWindowAuthorityRecord,
                            and_(
                                PaperExecutionWindowAuthorityRecord.order_id
                                == AgentPaperMarketWindowRecord.order_id,
                                PaperExecutionWindowAuthorityRecord.window_start_at
                                == AgentPaperMarketWindowRecord.window_start_at,
                                PaperExecutionWindowAuthorityRecord.window_end_at
                                == AgentPaperMarketWindowRecord.window_end_at,
                                PaperExecutionWindowAuthorityRecord.source_authority_id
                                == AgentPaperMarketWindowRecord.source_authority_id,
                            ),
                        )
                        .where(
                            AgentPaperMarketWindowRecord.cycle_id == view.cycle_id,
                            AgentPaperMarketWindowRecord.order_id == view.order_ids[0],
                            AgentPaperMarketWindowRecord.purpose == "paper_fill",
                            PaperExecutionWindowAuthorityRecord.id.is_(None),
                        )
                        .order_by(AgentPaperMarketWindowRecord.window_start_at)
                        .limit(2)
                    )
                )
                if len(unused_windows) != 1:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                market_window = unused_windows[0]
            return snapshot_from_cycle_view(
                now=now,
                view=view,
                research_job=research_job,
                market_scope=market_scope,
                calendar=calendar,
                market_window=market_window,
                performance_due_at=performance_due_at,
            )
    finally:
        await engine.dispose()


def _safe_agent_paper_worker_id(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("agent-paper-loop-"):
        return False
    suffix = value.removeprefix("agent-paper-loop-")
    try:
        return str(UUID(suffix)) == suffix
    except (AttributeError, TypeError, ValueError):
        return False


def _classify_agent_paper_service_heartbeat(
    now: object,
    heartbeat: object,
    *,
    freshness: timedelta = timedelta(seconds=30),
) -> dict[str, object]:
    if freshness <= timedelta(0) or not _safe_agent_paper_worker_id(
        getattr(heartbeat, "worker_id", None)
    ):
        return {"status": "unavailable"}
    seen_at = getattr(heartbeat, "seen_at", None)
    started_at = getattr(heartbeat, "started_at", None)
    if (
        not isinstance(now, datetime)
        or now.tzinfo is None
        or now.utcoffset() is None
        or not isinstance(seen_at, datetime)
        or seen_at.tzinfo is None
        or seen_at.utcoffset() is None
        or not isinstance(started_at, datetime)
        or started_at.tzinfo is None
        or started_at.utcoffset() is None
        or seen_at < started_at
    ):
        return {"status": "unavailable"}
    process_version = getattr(heartbeat, "process_version", None)
    migration_head = getattr(heartbeat, "migration_head", None)
    if (
        process_version != AGENT_PAPER_LOOP_PROCESS_VERSION
        or migration_head != CURRENT_MIGRATION_HEAD
    ):
        return {"status": "incompatible"}
    if seen_at > now:
        return {"status": "unavailable"}
    if now - seen_at > freshness:
        return {"status": "stale"}
    return {
        "status": "fresh",
        "seen_at": seen_at.isoformat(),
        "started_at": started_at.isoformat(),
        "process_version": process_version,
        "migration_head": migration_head,
    }


async def read_agent_paper_service_heartbeat(database_url: str | None) -> dict[str, object]:
    """Read one persisted liveness fact and classify it against the database clock."""
    if not database_url:
        return {"status": "unavailable"}
    database = make_url(database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in _LOOPBACK_HOSTS
        or database.query
    ):
        return {"status": "unavailable"}
    try:
        engine = create_async_engine(database_url)
        try:
            async with async_sessionmaker(engine).begin() as session:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                row = (
                    await session.execute(
                        select(func.clock_timestamp(), WorkerHeartbeat)
                        .where(WorkerHeartbeat.queue == QueueName.AGENT_PAPER.value)
                        .order_by(WorkerHeartbeat.seen_at.desc(), WorkerHeartbeat.id.desc())
                        .limit(1)
                    )
                ).one_or_none()
                if row is None:
                    return {"status": "missing"}
                now, heartbeat = row
                return _classify_agent_paper_service_heartbeat(now, heartbeat)
        finally:
            await engine.dispose()
    except Exception:
        return {"status": "unavailable"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus-engine-agent-paper-loop")
    parser.add_argument("--account-id")
    diagnostics = parser.add_mutually_exclusive_group()
    diagnostics.add_argument("--service-preflight", action="store_true")
    diagnostics.add_argument("--service-status", action="store_true")
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args(argv)
    root = args.repo_root.resolve() if args.repo_root is not None else Path.cwd()
    env_file = None
    if args.repo_root is not None or args.env_file is not None or args.service_status:
        try:
            if args.repo_root is None or args.env_file is None:
                raise LocalStackConfigurationError("AGENT_PAPER_SERVICE_CONFIG_REQUIRED")
            if not root.is_dir() or not (root / "infra/compose.yaml").is_file():
                raise LocalStackConfigurationError("REPOSITORY_ROOT_INVALID")
            path = args.env_file if args.env_file.is_absolute() else root / args.env_file
            env_file = _validate_local_env(root, path)
        except (LocalStackConfigurationError, OSError) as error:
            code = (
                str(error)
                if isinstance(error, LocalStackConfigurationError)
                else "LOCAL_ENV_UNSAFE"
            )
            print(json.dumps({"error_code": code}))
            return 1
    try:
        environment = (
            load_agent_paper_service_environment(root, {}, env_file=env_file)
            if env_file is not None
            else load_agent_paper_service_environment(root, os.environ)
        )
        if args.service_preflight or args.service_status:
            try:
                result = agent_paper_service_preflight(environment, repo_root=root)
            except Exception:
                result = {
                    "status": "blocked",
                    "execution_locked": True,
                    "external_requests": 0,
                    "blocking_codes": ["AGENT_PAPER_PREFLIGHT_CONFIG_LOAD_FAILED"],
                    "optional_codes": [],
                }
            if args.service_status:
                try:
                    heartbeat = asyncio.run(
                        read_agent_paper_service_heartbeat(environment.get("DATABASE_URL"))
                    )
                except Exception:
                    heartbeat = {"status": "unavailable"}
                print(
                    json.dumps(
                        {"heartbeat": heartbeat, "preflight": result, "execution_locked": True},
                        sort_keys=True,
                    )
                )
                return 0 if result["status"] == "ready" and heartbeat["status"] == "fresh" else 1
            print(json.dumps(result, sort_keys=True))
            return 0 if result["status"] == "ready" else 1
        account_id: UUID | None = None
        if args.account_id is not None:
            try:
                account_id = UUID(args.account_id)
            except (AttributeError, TypeError, ValueError):
                print(json.dumps({"error_code": "AGENT_PAPER_ACCOUNT_INVALID"}))
                return 1
        requested = environment.get("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false")
        if requested.strip().lower() in {"", "0", "false", "no", "off"}:
            print(json.dumps({"error_code": "AGENT_PAPER_CYCLE_DISABLED"}))
            return 1
        if account_id is None:
            try:
                account_id = UUID(environment.get("NEXUS_AGENT_PAPER_ACCOUNT_ID", ""))
            except (AttributeError, TypeError, ValueError):
                print(json.dumps({"error_code": "AGENT_PAPER_ACCOUNT_INVALID"}))
                return 1
        if environment.get("AGENT_RUNTIME_MODE", "").strip().lower() != "codex_sdk":
            print(json.dumps({"error_code": "AGENT_PAPER_MODEL_RUNTIME_MISMATCH"}))
            return 1
        settings = (_FileEngineSettings if env_file is not None else EngineSettings).model_validate(
            environment
        )
        if not settings.agent_paper_cycle_enabled:
            print(json.dumps({"error_code": "AGENT_PAPER_CYCLE_DISABLED"}))
            return 1
        reason = asyncio.run(
            run_local_agent_paper_loop(
                settings,
                account_id=account_id,
                repo_root=root,
                **({"env_file": env_file} if env_file is not None else {}),
            )
        )
        if reason == AgentPaperLoopAction.STOP_UNCONFIRMED:
            print(json.dumps({"error_code": "AGENT_PAPER_LOOP_STOP_UNCONFIRMED"}))
            return 75
        if reason == AgentPaperLoopAction.STOP_RESULT_UNKNOWN:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if reason == AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE:
            print(json.dumps({"error_code": "AGENT_PAPER_LOOP_CYCLE_UNAVAILABLE"}))
            return 1
        if reason == AgentPaperLoopAction.STOP_PERFORMANCE_UNAVAILABLE:
            print(json.dumps({"error_code": "AGENT_PAPER_LOOP_PERFORMANCE_UNAVAILABLE"}))
            return 1
        if reason == AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE:
            print(json.dumps({"error_code": "AGENT_PAPER_LOOP_PREREQUISITE_UNAVAILABLE"}))
            return 1
        if reason == AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE:
            print(json.dumps({"error_code": "AGENT_PAPER_CALENDAR_UNAVAILABLE"}))
            return 1
        if reason == AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH:
            print(json.dumps({"error_code": "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH"}))
            return 1
    except KeyboardInterrupt:
        print(json.dumps({"error_code": "AGENT_PAPER_LOOP_INTERRUPTED"}))
        return 130
    except Exception as error:
        code = str(error)
        if code not in _SAFE_TRIGGER_FAILURE_CODES:
            code = "AGENT_PAPER_LOOP_RESULT_UNKNOWN"
        print(json.dumps({"error_code": code}))
        return 1
    print(json.dumps({"execution_locked": True, "status": "stopped", "reason": reason.value}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Fail-closed foreground scheduler for Agent internal-paper cycles."""

import argparse
import asyncio
import errno
import fcntl
import json
import os
import re
import signal
import stat
import sys
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from time import monotonic as monotonic_clock
from typing import TypedDict
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from pydantic import SecretStr
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from sqlalchemy import and_, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.agents.codex_sdk import sdk_status
from nexus.agents.persistence import select_authoritative_snapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.agents.synthesis_lifecycle import SYNTHESIS_JOB_VERSIONS
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.repository import upsert_worker_heartbeat
from nexus.ops.agent_capacity_rejection import (
    CAPACITY_REJECTION_CODES,
    close_capacity_rejected_cycle_once,
)
from nexus.ops.agent_paper_formal import (
    require_formal_host_configuration_match,
    require_formal_host_trigger_profile,
    resolve_formal_host_cycle_profile,
)
from nexus.ops.agent_paper_identity import prepare_agent_paper_identity_once
from nexus.ops.agent_paper_once import (
    AgentPaperCollectWindowRequest,
    AgentPaperMarketSettings,
    AgentPaperResumeRequest,
    AgentPaperTriggerRequest,
    AgentPaperTriggerSettings,
    collect_window_once,
    load_agent_version_candidate,
    prepare_agent_version_slot_once,
    resume_once,
    run_cycle_job_once,
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
from nexus.ops.paper_once import PaperAdvanceRequest, read_agent_window_advance_result
from nexus.ops.paper_once import run_once as run_paper_once
from nexus.ops.preparation_diagnostics import (
    PHASES,
    exception_code,
    preparation_call,
    preparation_failure_fields,
    preparation_rejected,
    preparation_step,
    preparation_unavailable,
    preparation_unknown,
)
from nexus.paper.agent_cycle_authority import (
    ServiceCalendarAuthority,
    load_agent_paper_service_calendar_authority,
    load_quasi_realtime_cycle_authority,
    select_agent_paper_service_calendar_authority,
)
from nexus.paper.agent_cycle_models import PAPER_MACRO_SERIES as _PAPER_MACRO_SERIES
from nexus.paper.agent_cycle_models import floor_decision_slot
from nexus.paper.agent_market import resolve_agent_paper_market_scope
from nexus.paper.agent_version_evaluation import (
    close_incomplete_agent_version_evaluation,
    close_unrecoverable_agent_version_evaluation,
    finalize_agent_version_evaluation,
    load_oos_session_closes,
)
from nexus.paper.agent_version_runner import (
    resume_agent_version_slot,
)
from nexus.paper.candidate_batch import (
    CandidateInstrumentIdentity,
    validate_agent_paper_candidate_batch,
)
from nexus.paper.candidate_batch import (
    macro_snapshot_contains_required_observations as _macro_snapshot_contains_required_observations,
)
from nexus.paper.errors import PaperPerformanceError
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1
from nexus.paper.portfolio_proposal import KNOWN_PORTFOLIO_PROPOSAL_REJECTIONS
from nexus.paper.read_service import PaperReadService
from nexus.persistence.migrations import (
    AGENT_PAPER_LOOP_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.agent_versions import (
    AgentVersionCyclePairRecord,
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    MacroObservation,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job, JobLease, WorkerHeartbeat
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
from nexus.research.fred_collection import has_archived_timeout_failure, load_fred_collection
from nexus.research.fred_observations import FredVintageRequest
from nexus.research.fred_storage import read_fred_observations
from nexus.risk.query_authority_producer import inspect_query_authority_scope
from nexus.settings import AgentRuntimeMode, EngineSettings
from nexus.worker import run_agent_worker_once


class _RecoveryLoopArguments(TypedDict, total=False):
    recovery_guard: Callable[[EngineSettings], Awaitable[None]]


_RELEASE_REVISION = re.compile(r"^[0-9a-f]{40}$")


def agent_paper_process_version(release_revision: str | None) -> str:
    """Bind the protocol version to one optional canonical checkout revision."""
    if release_revision is None:
        return AGENT_PAPER_LOOP_PROCESS_VERSION
    if _RELEASE_REVISION.fullmatch(release_revision) is None:
        raise ValueError("release_revision must be a canonical Git SHA")
    return f"{AGENT_PAPER_LOOP_PROCESS_VERSION}+{release_revision}"


def agent_paper_host_identity(
    settings: EngineSettings, *, worker_id: str, account_id: UUID | None
) -> tuple[str, str]:
    """Advertise only the enabled scope consumed by this process, never a file revision."""
    from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL, manual_host_scope
    from nexus.settings import AgentRuntimeMode

    if not _safe_agent_paper_worker_id(worker_id) or ":manual:" in worker_id:
        raise ValueError("MANUAL_HOST_NOT_READY")
    legacy = (worker_id, agent_paper_process_version(settings.release_revision))
    if (
        settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
        or not settings.agent_paper_manual_enabled
        or not settings.agent_paper_cycle_enabled
        or settings.formal_budget_protection_enabled
        or settings.release_revision is None
        or account_id is None
        or settings.agent_paper_account_id != account_id
        or settings.agent_paper_candidate_batch_id is None
        or settings.agent_paper_formal_model_config_id is None
    ):
        return legacy
    return (
        f"{worker_id}:manual:{manual_host_scope(settings)}",
        f"{MANUAL_HOST_PROTOCOL}+{settings.release_revision}",
    )


def parse_agent_paper_process_version(value: object) -> tuple[str | None, str | None]:
    """Read current and legacy heartbeat versions without guessing malformed revisions."""
    from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL

    if value == AGENT_PAPER_LOOP_PROCESS_VERSION:
        return AGENT_PAPER_LOOP_PROCESS_VERSION, None
    if not isinstance(value, str):
        return None, None
    for protocol in (AGENT_PAPER_LOOP_PROCESS_VERSION, MANUAL_HOST_PROTOCOL):
        prefix = f"{protocol}+"
        if value.startswith(prefix):
            release_revision = value.removeprefix(prefix)
            if _RELEASE_REVISION.fullmatch(release_revision) is not None:
                return protocol, release_revision
    return None, None


class AgentPaperLoopAction(StrEnum):
    ADVANCE_EVALUATION = "advance_evaluation"
    CLOSE_INCOMPLETE_EVALUATION = "close_incomplete_evaluation"
    FINALIZE_EVALUATION = "finalize_evaluation"
    RUN_EVALUATION_RESEARCH_JOB = "run_evaluation_research_job"
    RESUME_EVALUATION = "resume_evaluation"
    WAIT_FOR_EVALUATION = "wait_for_evaluation"
    WAIT_FOR_EVALUATION_JOB = "wait_for_evaluation_job"
    RUN_MONITOR_INCREMENT = "run_monitor_increment"
    TRIGGER = "trigger"
    FINISH_MANUAL = "finish_manual"
    TRIGGER_MANUAL = "trigger_manual"
    PREPARE_INPUTS = "prepare_inputs"
    RUN_RESEARCH_JOB = "run_research_job"
    RESUME = "resume"
    COLLECT_WINDOW = "collect_window"
    ADVANCE_ORDER = "advance_order"
    PUBLISH_EVALUATION_PERFORMANCE = "publish_evaluation_performance"
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
    market_open_at: datetime | None = None
    decision_window_start_at: datetime | None = None
    decision_window_end_at: datetime | None = None
    next_decision_window_start_at: datetime | None = None
    next_market_open_at: datetime | None = None
    cycle_id: UUID | None = None
    cycle_status: str | None = None
    cycle_result_code: str | None = None
    cycle_slot_at: datetime | None = None
    trigger_attempt_slot_at: datetime | None = None
    trigger_failure_code: str | None = None
    research_job_status: str | None = None
    research_job_id: UUID | str | None = None
    research_job_lease_expires_at: datetime | None = None
    market_window_ready: bool = False
    market_window_start_at: datetime | None = None
    market_window_end_at: datetime | None = None
    market_window_id: UUID | None = None
    has_open_order: bool = False
    order_id: UUID | None = None
    instrument_id: UUID | None = None
    performance_due_at: datetime | None = None
    evaluation_id: UUID | None = None
    evaluation_slot_at: datetime | None = None
    action_account_id: UUID | None = None
    additional_market_windows_allowed: bool = False
    manual_finish_request_id: UUID | None = None
    manual_pending_request_id: UUID | None = None
    manual_pending_status: str | None = None
    manual_research_only: bool = False
    monitor_trigger_id: UUID | None = None
    monitor_trigger_status: str | None = None
    monitor_batch_id: UUID | None = None
    host_prepared_market_open_at: datetime | None = None


@dataclass(frozen=True)
class PriorDailyScope:
    instrument_id: UUID
    prior_session: date
    authority_ready: bool
    decision_session: date | None = None


@dataclass(frozen=True)
class CurrentTrendsScope:
    instrument_id: UUID
    start_session: date | None
    prior_session: date
    evidence_ready: bool
    snapshot_ready: bool


def _next_evaluation_performance(
    *,
    account_ids: tuple[UUID, UUID],
    session_closes: tuple[datetime, ...],
    persisted_snapshot_keys: set[tuple[UUID, datetime]],
    now: datetime,
) -> tuple[UUID | None, datetime | None]:
    """Select one due account or the first future close from persisted facts."""
    for close_at in session_closes:
        missing_accounts = tuple(
            account_id
            for account_id in account_ids
            if (account_id, close_at) not in persisted_snapshot_keys
        )
        if not missing_accounts:
            continue
        return (missing_accounts[0] if close_at <= now else None, close_at)
    return None, None


@dataclass(frozen=True)
class MacroScope:
    instrument_id: UUID
    vintage: date
    ready_series: frozenset[str]
    snapshot_ready: bool
    attempt_session: date | None = None


_TERMINAL_JOB_STATUSES = frozenset({"succeeded", "failed", "dead_letter", "skipped"})
_SAFE_TRIGGER_FAILURE_CODES = frozenset(
    {
        "AGENT_PAPER_ACCOUNT_INVALID",
        "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
        "AGENT_PAPER_CALENDAR_UNAVAILABLE",
        "AGENT_PAPER_CODEX_STATUS_UNAVAILABLE",
        "AGENT_PAPER_LOOP_ALREADY_RUNNING",
        "AGENT_PAPER_MARKET_WINDOW_CLOSED",
        "AGENT_PAPER_OUTSIDE_WINDOW",
        "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
        "AGENT_PAPER_PROVIDER_CAPACITY_INVALID",
        "RISK_INPUT_REQUIRED",
        "SCHWAB_L1_FRESHNESS_UNPROVEN",
        "SCHWAB_L1_INCOMPLETE",
        "CODEX_CHATGPT_LOGIN_REQUIRED",
        "CODEX_FORMAL_HOST_PROFILE_INVALID",
        "CANDIDATE_BATCH_REQUIRED",
        "CANDIDATE_BATCH_INVALID",
        "PORTFOLIO_SYNTHESIS_UNAVAILABLE",
        "PORTFOLIO_PROPOSAL_UNAVAILABLE",
        "POLICY_UNCONFIRMED",
        "TARGET_ACTIVATION_PENDING",
    }
)
_SLOT_TERMINAL_TRIGGER_FAILURE_CODES = frozenset(
    {
        "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
        "RISK_INPUT_REQUIRED",
        "SCHWAB_L1_FRESHNESS_UNPROVEN",
        "SCHWAB_L1_INCOMPLETE",
    }
)
_MARKET_PUBLICATION_LAG = timedelta(seconds=60)
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


def agent_paper_service_startup_preflight(
    environment: Mapping[str, str], *, repo_root: Path
) -> dict[str, object]:
    """Validate only static prerequisites needed to wait and inspect persistence."""
    blocking_codes: set[str] = set()
    if environment.get("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false").strip().lower() in {
        "",
        "0",
        "false",
        "no",
        "off",
    }:
        blocking_codes.add("AGENT_PAPER_CYCLE_DISABLED")
    try:
        UUID(environment.get("NEXUS_AGENT_PAPER_ACCOUNT_ID", ""))
    except (AttributeError, TypeError, ValueError):
        blocking_codes.add("AGENT_PAPER_ACCOUNT_INVALID")
    try:
        database = make_url(environment.get("DATABASE_URL", ""))
    except (TypeError, ValueError):
        database = None
    if (
        database is None
        or database.drivername != "postgresql+asyncpg"
        or database.host not in _LOOPBACK_HOSTS
        or not database.database
        or database.query
    ):
        blocking_codes.add("AGENT_PAPER_DATABASE_CONFIG_INVALID")
    runtime_mode = environment.get("AGENT_RUNTIME_MODE", "").strip().lower()
    if not runtime_mode:
        blocking_codes.add("AGENT_PAPER_MODEL_CONFIG_REQUIRED")
    elif runtime_mode != "codex_sdk":
        blocking_codes.add("AGENT_PAPER_MODEL_RUNTIME_MISMATCH")
    try:
        _FileEngineSettings.model_validate(environment)
    except Exception:
        blocking_codes.add("AGENT_PAPER_RUNTIME_CONFIG_INVALID")
    if not _agent_paper_output_directory_valid(repo_root):
        blocking_codes.add("AGENT_PAPER_OUTPUT_DIRECTORY_INVALID")
    return {
        "status": "blocked" if blocking_codes else "ready",
        "execution_locked": True,
        "external_requests": 0,
        "blocking_codes": sorted(blocking_codes),
        "optional_codes": [],
        **_normal_cycle_config_readiness(environment),
    }


async def _agent_paper_startup_persistence_ready(database_url: str | None) -> bool:
    if not database_url:
        return False
    engine = create_async_engine(database_url)
    try:
        async with asyncio.timeout(5):
            async with engine.connect() as connection:
                value: object = await connection.scalar(text("SELECT 1"))
                return value == 1
    except Exception:
        return False
    finally:
        await engine.dispose()


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
        **_normal_cycle_config_readiness(environment),
    }


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
    account_id: UUID | None = None,
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
    worker_id, process_version = agent_paper_host_identity(
        settings, worker_id=worker_id, account_id=account_id
    )
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            await upsert_worker_heartbeat(
                session,
                queue=QueueName.AGENT_PAPER,
                worker_id=worker_id,
                process_version=process_version,
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
            evaluation_calendars = tuple(
                await session.scalars(
                    select(AgentVersionEvaluationRecord.calendar_authority_id)
                    .outerjoin(
                        AgentVersionEvaluationDecisionRecord,
                        AgentVersionEvaluationDecisionRecord.evaluation_id
                        == AgentVersionEvaluationRecord.id,
                    )
                    .where(AgentVersionEvaluationDecisionRecord.id.is_(None))
                    .limit(2)
                )
            )
            if len(evaluation_calendars) > 1:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            if evaluation_calendars:
                selected = await load_agent_paper_service_calendar_authority(
                    session,
                    now=now,
                    expected_calendar_authority_id=evaluation_calendars[0],
                )
                return selected.calendar_authority_id
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
    candidate: CandidateInstrumentIdentity,
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
                expected_universe_snapshot_id=candidate.universe_snapshot_id,
            )
            if (
                service_calendar.calendar_authority_id != expected_calendar_authority_id
                or authority.calendar_authority_id != expected_calendar_authority_id
            ):
                raise ValueError("AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH")
            scope = await inspect_query_authority_scope(
                session,
                decision_session=authority.decision_session,
                symbol=candidate.symbol,
                as_of=database_now,
                universe_snapshot_id=authority.universe_snapshot_id,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            )
            if scope.instrument_id != candidate.instrument_id:
                raise ValueError("CANDIDATE_BATCH_INVALID")
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
    candidate: CandidateInstrumentIdentity,
) -> str:
    """Create at most one exact prior-session daily authority before a cycle."""
    scope = await preparation_call(
        "prior_daily",
        "scope",
        _inspect_prior_daily_scope(
            settings,
            account_id=account_id,
            now=now,
            expected_calendar_authority_id=expected_calendar_authority_id,
            candidate=candidate,
        ),
    )
    if scope.authority_ready:
        return "ready"
    parent = repo_root / ".secrets" / "nexus-agent-paper" / "daily"
    with preparation_step("prior_daily", "archive"):
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if candidate.symbol == "AAPL" and (parent / scope.prior_session.isoformat()).exists():
        return preparation_unavailable("prior_daily", "archive", "ATTEMPT_EXISTS")
    output_dir = parent / f"{scope.prior_session.isoformat()}-{scope.instrument_id}"
    try:
        with preparation_step("prior_daily", "archive"):
            output_dir.mkdir(mode=0o700)
    except FileExistsError:
        return preparation_unavailable("prior_daily", "archive", "ATTEMPT_EXISTS")
    try:
        with preparation_step("prior_daily", "window"):
            request, calendar = collection_window(
                scope.instrument_id,
                scope.prior_session,
                scope.prior_session,
                "all",
                now=now,
            )
        collected = await preparation_call(
            "prior_daily",
            "collect",
            collect_prices(
                request,
                symbol=candidate.symbol,
                calendar=calendar,
                resolved_at=now,
                key_id=settings.alpaca_key,
                secret=settings.alpaca_secret,
                output_dir=output_dir,
                max_pages=10,
                identity_asof=scope.prior_session,
            ),
        )
        if collected.get("status") != "collected" or collected.get("readback_verified") is not True:
            return preparation_unavailable("prior_daily", "collect", "COLLECTION_UNAVAILABLE")
        imported = await preparation_call(
            "prior_daily",
            "import",
            import_adjusted_authority(
                settings,
                instrument_id=scope.instrument_id,
                collection_dir=output_dir,
                purpose="agent-paper-decision",
            ),
        )
        if imported.get("status") != "stored_authority":
            raise preparation_unknown("prior_daily", "import", "IMPORT_RESULT_INVALID")
    except ValueError as error:
        return preparation_rejected("prior_daily", "window", error)
    verified = await preparation_call(
        "prior_daily",
        "readback",
        _inspect_prior_daily_scope(
            settings,
            account_id=account_id,
            now=datetime.now(UTC),
            expected_calendar_authority_id=expected_calendar_authority_id,
            candidate=candidate,
        ),
    )
    if (
        not verified.authority_ready
        or verified.instrument_id != scope.instrument_id
        or verified.prior_session != scope.prior_session
    ):
        raise preparation_unknown("prior_daily", "readback", "READBACK_MISMATCH")
    return "prepared"


async def _inspect_macro_scope(
    settings: EngineSettings,
    *,
    account_id: UUID,
    now: datetime,
    expected_calendar_authority_id: UUID,
    candidate: CandidateInstrumentIdentity,
) -> MacroScope:
    require_utc(now)
    prior = await _inspect_prior_daily_scope(
        settings,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=expected_calendar_authority_id,
        candidate=candidate,
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
        # Monthly releases can lag into the next month. Include both economic
        # period anchors within the existing 31-day bound; the fixed vintage
        # still excludes unpublished values and preserves each observed month.
        observation_end = (vintage.replace(day=1) - timedelta(days=1)).replace(day=1)
        observation_start = (observation_end - timedelta(days=1)).replace(day=1)
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
    candidate: CandidateInstrumentIdentity,
) -> str:
    """Best-effort daily macro preparation; unavailable macro does not block a cycle."""
    scope = await preparation_call(
        "macro",
        "scope",
        _inspect_macro_scope(
            settings,
            account_id=account_id,
            now=now,
            expected_calendar_authority_id=expected_calendar_authority_id,
            candidate=candidate,
        ),
    )
    if scope.ready_series == _PAPER_MACRO_SERIES:
        if scope.snapshot_ready:
            return "ready"
        await preparation_call(
            "macro",
            "snapshot",
            _prepare_current_trends_research_snapshot(
                settings,
                instrument_id=scope.instrument_id,
                now=datetime.now(UTC),
                macro_vintage=scope.vintage,
            ),
        )
        verified = await preparation_call(
            "macro",
            "readback",
            _inspect_macro_scope(
                settings,
                account_id=account_id,
                now=datetime.now(UTC),
                expected_calendar_authority_id=expected_calendar_authority_id,
                candidate=candidate,
            ),
        )
        if (
            verified.instrument_id != scope.instrument_id
            or verified.vintage != scope.vintage
            or verified.ready_series != _PAPER_MACRO_SERIES
            or not verified.snapshot_ready
        ):
            raise preparation_unknown("macro", "readback", "READBACK_MISMATCH")
        return "prepared"
    try:
        validate_local_import(settings)
    except ValueError:
        return preparation_unavailable("macro", "configuration", "VALIDATION_REJECTED")
    parent = repo_root / ".secrets" / "nexus-agent-paper" / "macro"
    with preparation_step("macro", "archive"):
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    attempt_session = getattr(scope, "attempt_session", None)
    if not isinstance(attempt_session, date):
        attempt_session = now.astimezone(ZoneInfo("America/New_York")).date()
    output_dir = parent / attempt_session.isoformat()
    try:
        with preparation_step("macro", "archive"):
            output_dir.mkdir(mode=0o700)
    except FileExistsError:
        return preparation_unavailable("macro", "archive", "ATTEMPT_EXISTS")
    missing = _PAPER_MACRO_SERIES - scope.ready_series
    for series in sorted(missing):
        series_dir = output_dir / series
        with preparation_step("macro", "archive"):
            series_dir.mkdir(mode=0o700)
        try:
            collected = await preparation_call(
                "macro",
                "collect",
                collect_fred(
                    _macro_request(series, scope.vintage),
                    key=fred_key,
                    directory=series_dir,
                ),
            )
        except TimeoutError:
            if has_archived_timeout_failure(series_dir):
                return preparation_unavailable("macro", "collect", "ARCHIVED_TIMEOUT")
            raise
        except (HttpPolicyError, ValueError) as error:
            return preparation_rejected("macro", "collect", error)
        if collected.get("status") != "collected" or not collected.get("observation_count"):
            return preparation_unavailable("macro", "collect", "COLLECTION_UNAVAILABLE")
        with preparation_step("macro", "archive"):
            collection = load_fred_collection(series_dir)
        imported = await preparation_call(
            "macro",
            "import",
            import_fred_collection(
                settings,
                collection=collection,
            ),
        )
        if imported.get("status") != "imported" or not imported.get("visible_observation_count"):
            raise preparation_unknown("macro", "import", "IMPORT_RESULT_INVALID")
    await preparation_call(
        "macro",
        "snapshot",
        _prepare_current_trends_research_snapshot(
            settings,
            instrument_id=scope.instrument_id,
            now=datetime.now(UTC),
            macro_vintage=scope.vintage,
        ),
    )
    verified = await preparation_call(
        "macro",
        "readback",
        _inspect_macro_scope(
            settings,
            account_id=account_id,
            now=datetime.now(UTC),
            expected_calendar_authority_id=expected_calendar_authority_id,
            candidate=candidate,
        ),
    )
    if (
        verified.instrument_id != scope.instrument_id
        or verified.vintage != scope.vintage
        or verified.ready_series != _PAPER_MACRO_SERIES
        or not verified.snapshot_ready
    ):
        raise preparation_unknown("macro", "readback", "READBACK_MISMATCH")
    return "prepared"


async def _inspect_current_trends_scope(
    settings: EngineSettings,
    *,
    account_id: UUID,
    now: datetime,
    expected_calendar_authority_id: UUID,
    candidate: CandidateInstrumentIdentity,
) -> CurrentTrendsScope:
    require_utc(now)
    prior = await _inspect_prior_daily_scope(
        settings,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=expected_calendar_authority_id,
        candidate=candidate,
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
            start_session = None
            if not evidence_ready:
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
            return CurrentTrendsScope(
                instrument_id=prior.instrument_id,
                start_session=start_session,
                prior_session=prior.prior_session,
                evidence_ready=evidence_ready,
                snapshot_ready=snapshot_ready,
            )
    finally:
        await engine.dispose()


async def _prepare_current_trends_research_snapshot(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    now: datetime,
    macro_vintage: date | None = None,
) -> UUID:
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
        factory = async_sessionmaker(
            engine, expire_on_commit=False, info={"engine_settings": settings}
        )
        async with factory.begin() as session:
            database_now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(database_now, datetime):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            # The next verifier reads on the database clock. Never freeze a
            # future snapshot when the host leads it, or expand the caller's
            # cutoff when the database leads the host. Original receipt and
            # availability timestamps remain unchanged.
            now = min(require_utc(now), require_utc(database_now))
            if macro_vintage is None:
                previous = await select_authoritative_snapshot(
                    session, instrument_id=instrument_id, as_of=now
                )
                if previous is not None:
                    bound_ids = set(
                        await session.scalars(
                            select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                                AgentDataSnapshotEvidenceRecord.data_snapshot_id == previous.id,
                                AgentDataSnapshotEvidenceRecord.instrument_id == instrument_id,
                                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                            )
                        )
                    )
                    macro_rows = (
                        await session.scalars(
                            select(MacroObservation).where(
                                MacroObservation.source == "fred",
                                MacroObservation.evidence_ref_id.in_(bound_ids),
                                MacroObservation.available_at <= now,
                                MacroObservation.created_at <= now,
                            )
                        )
                    ).all()
                    prior_complete = all(
                        any(
                            row.series == series
                            and row.calendar_evidence_ref_id in bound_ids
                            and row.real_time_start == row.real_time_end
                            for row in macro_rows
                        )
                        for series in _PAPER_MACRO_SERIES
                    )
                    vintages = {row.real_time_start for row in macro_rows}
                    if prior_complete:
                        if len(vintages) != 1:
                            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                        candidate_vintage = vintages.pop()
                        current = await read_fred_observations(
                            session,
                            as_of=now,
                            series=_PAPER_MACRO_SERIES,
                            vintage=candidate_vintage,
                        )
                        if {row.series for row in current.values()} != _PAPER_MACRO_SERIES:
                            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                        macro_vintage = candidate_vintage
            snapshot = await prepare_research_snapshot(
                session,
                instrument_id,
                as_of=now,
                macro_series=(_PAPER_MACRO_SERIES if macro_vintage is not None else frozenset()),
                macro_vintage=macro_vintage,
            )
            if snapshot is None:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            return snapshot.id
    finally:
        await engine.dispose()


async def prepare_current_trends_once(
    settings: AgentPaperTriggerSettings,
    *,
    account_id: UUID,
    repo_root: Path,
    now: datetime,
    expected_calendar_authority_id: UUID,
    candidate: CandidateInstrumentIdentity,
) -> str:
    """Create at most one current-trends evidence record for the prior session."""
    scope = await preparation_call(
        "current_trends",
        "scope",
        _inspect_current_trends_scope(
            settings,
            account_id=account_id,
            now=now,
            expected_calendar_authority_id=expected_calendar_authority_id,
            candidate=candidate,
        ),
    )
    if scope.snapshot_ready:
        return "ready"
    if scope.evidence_ready:
        await preparation_call(
            "current_trends",
            "snapshot",
            _prepare_current_trends_research_snapshot(
                settings,
                instrument_id=scope.instrument_id,
                now=datetime.now(UTC),
            ),
        )
        verified = await preparation_call(
            "current_trends",
            "readback",
            _inspect_current_trends_scope(
                settings,
                account_id=account_id,
                now=datetime.now(UTC),
                expected_calendar_authority_id=expected_calendar_authority_id,
                candidate=candidate,
            ),
        )
        if (
            not verified.evidence_ready
            or not verified.snapshot_ready
            or verified.instrument_id != scope.instrument_id
            or verified.prior_session != scope.prior_session
        ):
            raise preparation_unknown("current_trends", "readback", "READBACK_MISMATCH")
        return "prepared"
    if scope.start_session is None:
        raise preparation_unknown("current_trends", "scope", "SCOPE_UNAVAILABLE")
    parent = repo_root / ".secrets" / "nexus-agent-paper" / "trends"
    with preparation_step("current_trends", "archive"):
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if candidate.symbol == "AAPL" and (parent / scope.prior_session.isoformat()).exists():
        return preparation_unavailable("current_trends", "archive", "ATTEMPT_EXISTS")
    output_dir = parent / f"{scope.prior_session.isoformat()}-{scope.instrument_id}"
    try:
        with preparation_step("current_trends", "archive"):
            output_dir.mkdir(mode=0o700)
    except FileExistsError:
        return preparation_unavailable("current_trends", "archive", "ATTEMPT_EXISTS")
    try:
        with preparation_step("current_trends", "window"):
            request, calendar = collection_window(
                scope.instrument_id,
                scope.start_session,
                scope.prior_session,
                "split",
                now=now,
            )
    except ValueError:
        return preparation_unavailable("current_trends", "window", "VALIDATION_REJECTED")
    collected = await preparation_call(
        "current_trends",
        "collect",
        collect_prices(
            request,
            symbol=candidate.symbol,
            calendar=calendar,
            resolved_at=now,
            key_id=settings.alpaca_key,
            secret=settings.alpaca_secret,
            output_dir=output_dir,
            max_pages=10,
            identity_asof=scope.prior_session,
        ),
    )
    if collected.get("status") != "collected" or collected.get("readback_verified") is not True:
        return preparation_unavailable("current_trends", "collect", "COLLECTION_UNAVAILABLE")
    imported = await preparation_call(
        "current_trends",
        "import",
        import_trends(
            settings,
            instrument_id=scope.instrument_id,
            collection_dir=output_dir,
        ),
    )
    if imported.get("status") != "stored_context":
        raise preparation_unknown("current_trends", "import", "IMPORT_RESULT_INVALID")
    if imported.get("last_observed_on") != scope.prior_session.isoformat():
        return preparation_unavailable("current_trends", "import", "OBSERVATION_SESSION_MISMATCH")
    await preparation_call(
        "current_trends",
        "snapshot",
        _prepare_current_trends_research_snapshot(
            settings,
            instrument_id=scope.instrument_id,
            now=datetime.now(UTC),
        ),
    )
    verified = await preparation_call(
        "current_trends",
        "readback",
        _inspect_current_trends_scope(
            settings,
            account_id=account_id,
            now=datetime.now(UTC),
            expected_calendar_authority_id=expected_calendar_authority_id,
            candidate=candidate,
        ),
    )
    if (
        not verified.evidence_ready
        or not verified.snapshot_ready
        or verified.instrument_id != scope.instrument_id
        or verified.prior_session != scope.prior_session
    ):
        raise preparation_unknown("current_trends", "readback", "READBACK_MISMATCH")
    return "prepared"


def resolve_cycle_research_job(
    *, events: Sequence[object], jobs: Sequence[object]
) -> tuple[UUID, str] | None:
    """Select one next Job only after accounting for the complete frozen run set."""
    run_ids: list[str] = []
    for event in events:
        fact_ids = getattr(event, "fact_ids", None)
        raw = fact_ids.get("research_run_ids") if isinstance(fact_ids, dict) else None
        if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
            run_ids.extend(raw)
    if not 1 <= len(run_ids) <= 5 or len(set(run_ids)) != len(run_ids) or len(jobs) != len(run_ids):
        return None
    matched: dict[str, tuple[UUID, str]] = {}
    for job in jobs:
        payload = getattr(job, "payload", None)
        job_id, status = getattr(job, "id", None), getattr(job, "status", None)
        run_id = payload.get("run_id") if isinstance(payload, dict) else None
        if (
            run_id not in run_ids
            or run_id in matched
            or not isinstance(job_id, UUID)
            or not isinstance(status, str)
        ):
            return None
        matched[run_id] = (job_id, status)
    if set(matched) != set(run_ids):
        return None
    ordered = [matched[run_id] for run_id in run_ids]
    # Unknown/running work blocks dispatch; never advance past a still-running job.
    for job in ordered:
        if job[1] not in _TERMINAL_JOB_STATUSES and job[1] != "queued":
            return job
    for job in ordered:
        if job[1] in _TERMINAL_JOB_STATUSES and job[1] != "succeeded":
            return job
    return next((job for job in ordered if job[1] == "queued"), ordered[0])


def _portfolio_slot(window_start: datetime) -> datetime:
    zone = ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone)
    return datetime.combine(
        window_start.astimezone(zone).date(),
        PORTFOLIO_ALLOCATION_V1.decision_local_time,
        tzinfo=zone,
    ).astimezone(UTC)


def select_active_order_identity(
    rows: Sequence[tuple[UUID, UUID, str]],
    *,
    additional_market_windows_allowed: bool,
) -> tuple[UUID, UUID] | None:
    """Select one durable open order, finishing partial work before new work."""
    if len(rows) > 50 or len({row[0] for row in rows}) != len(rows):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    settled_or_executable = {
        "submitted",
        "partially_filled",
        "filled",
        "cancelled",
        "rejected",
        "risk_rejected",
    }
    if any(status not in settled_or_executable for _order_id, _instrument_id, status in rows):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    partial = sorted(
        (
            (order_id, instrument_id)
            for order_id, instrument_id, status in rows
            if status == "partially_filled"
        ),
        key=lambda row: row[0],
    )
    submitted = sorted(
        (
            (order_id, instrument_id)
            for order_id, instrument_id, status in rows
            if status == "submitted"
        ),
        key=lambda row: row[0],
    )
    priority = (partial, submitted) if additional_market_windows_allowed else (submitted, partial)
    return next((rows[0] for rows in priority if rows), None)


def snapshot_from_cycle_view(
    *,
    now: datetime,
    view: object,
    research_job: tuple[UUID, str] | None,
    market_scope: object | None,
    calendar: ServiceCalendarAuthority,
    market_window: object | None = None,
    performance_due_at: datetime | None = None,
    trigger_attempt_slot_at: datetime | None = None,
    trigger_failure_code: str | None = None,
    additional_market_windows_allowed: bool = False,
    active_order_id: UUID | None = None,
    active_instrument_id: UUID | None = None,
    active_order_status: str | None = None,
) -> AgentPaperLoopSnapshot:
    """Map the bounded read projection into the scheduler's fail-closed contract."""
    cycle_id = getattr(view, "cycle_id", None)
    cycle_status = getattr(view, "status", None)
    cycle_slot_at = getattr(view, "decision_slot_at", None)
    order_ids = getattr(view, "order_ids", ())
    if (
        not isinstance(cycle_id, UUID)
        or not isinstance(cycle_status, str)
        or not isinstance(cycle_slot_at, datetime)
        or not isinstance(order_ids, tuple)
        or any(not isinstance(value, UUID) for value in order_ids)
        or len(order_ids) > 50
        or (active_order_id is not None and active_order_id not in order_ids)
        or (active_instrument_id is not None and active_order_id is None)
        or (active_order_status is not None and active_order_id is None)
        or active_order_status not in {None, "submitted", "partially_filled"}
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    order_id = active_order_id
    if order_id is None and len(order_ids) == 1:
        order_id = order_ids[0]
    action_cycle_status = (
        "awaiting_market"
        if active_order_status == "submitted"
        else "partially_filled"
        if active_order_status == "partially_filled"
        else cycle_status
    )
    if action_cycle_status in {"awaiting_market", "partially_filled"} and order_id is None:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    instrument_id = active_instrument_id
    has_open_order = (
        action_cycle_status in {"awaiting_market", "partially_filled"} and order_id is not None
    )
    market_window_ready = has_open_order and market_window is not None
    market_window_start_at = None
    market_window_end_at = None
    market_window_id = None
    if (
        has_open_order
        and not market_window_ready
        and not (
            action_cycle_status == "partially_filled" and not additional_market_windows_allowed
        )
    ):
        scope_cycle_id = getattr(market_scope, "cycle_id", None)
        scope_instrument_id = getattr(market_scope, "instrument_id", None)
        market_window_start_at = getattr(market_scope, "window_start", None)
        market_window_end_at = getattr(market_scope, "window_end", None)
        if (
            scope_cycle_id != cycle_id
            or not isinstance(scope_instrument_id, UUID)
            or (instrument_id is not None and scope_instrument_id != instrument_id)
            or not isinstance(market_window_start_at, datetime)
            or not isinstance(market_window_end_at, datetime)
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        instrument_id = scope_instrument_id
        market_window_end_at += _MARKET_PUBLICATION_LAG
    elif market_window_ready:
        market_window_id = getattr(market_window, "id", None)
        market_cycle_id = getattr(market_window, "cycle_id", None)
        market_order_id = getattr(market_window, "order_id", None)
        market_instrument_id = getattr(market_window, "instrument_id", None)
        market_window_start_at = getattr(market_window, "window_start_at", None)
        market_window_end_at = getattr(market_window, "window_end_at", None)
        if (
            not isinstance(market_window_id, UUID)
            or market_cycle_id != cycle_id
            or market_order_id != order_id
            or not isinstance(market_instrument_id, UUID)
            or (instrument_id is not None and market_instrument_id != instrument_id)
            or not isinstance(market_window_start_at, datetime)
            or not isinstance(market_window_end_at, datetime)
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        instrument_id = market_instrument_id
    return AgentPaperLoopSnapshot(
        now=now,
        calendar_authority_id=calendar.calendar_authority_id,
        market_open_at=calendar.market_open_at,
        decision_window_start_at=calendar.decision_window_start_at,
        decision_window_end_at=calendar.market_close_at,
        next_decision_window_start_at=calendar.next_decision_window_start_at,
        next_market_open_at=calendar.next_market_open_at,
        cycle_id=cycle_id,
        cycle_status=action_cycle_status,
        cycle_slot_at=cycle_slot_at,
        trigger_attempt_slot_at=trigger_attempt_slot_at,
        trigger_failure_code=trigger_failure_code,
        research_job_status=None if research_job is None else research_job[1],
        research_job_id=None if research_job is None else research_job[0],
        market_window_ready=market_window_ready,
        market_window_start_at=market_window_start_at,
        market_window_end_at=market_window_end_at,
        market_window_id=market_window_id,
        has_open_order=has_open_order,
        order_id=order_id,
        instrument_id=instrument_id,
        performance_due_at=performance_due_at,
        additional_market_windows_allowed=additional_market_windows_allowed,
    )


def choose_loop_action(
    snapshot: AgentPaperLoopSnapshot, *, prepared_market_open_at: datetime | None = None
) -> AgentPaperLoopAction:
    """Choose one safe next action without performing I/O or retrying unknown work."""
    if not snapshot.enabled:
        return AgentPaperLoopAction.STOP_DISABLED
    if prepared_market_open_at is None:
        prepared_market_open_at = snapshot.host_prepared_market_open_at
    if snapshot.monitor_trigger_id is not None:
        ordinary = choose_loop_action(
            replace(snapshot, monitor_trigger_id=None, monitor_trigger_status=None),
            prepared_market_open_at=prepared_market_open_at,
        )
        if snapshot.monitor_trigger_status == "unknown":
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        allowed = {AgentPaperLoopAction.WAIT_FOR_SESSION, AgentPaperLoopAction.WAIT_FOR_SLOT}
        if snapshot.monitor_trigger_status == "admitted":
            allowed |= {AgentPaperLoopAction.TRIGGER, AgentPaperLoopAction.PREPARE_INPUTS}
        if ordinary in allowed:
            if snapshot.monitor_trigger_status == "pending":
                from nexus.monitoring.host import (
                    BATCH_PREPARATION_HEADROOM_SECONDS,
                    INCREMENT_ACTION_HEADROOM_SECONDS,
                )

                if snapshot.decision_window_start_at is None or snapshot.market_open_at is None:
                    return ordinary
                # Preserve the daily slot: existing bounded preclaim + Worker plus
                # 90 seconds for admission, persisted closeout and reinspection.
                headroom = timedelta(
                    seconds=INCREMENT_ACTION_HEADROOM_SECONDS
                    + (
                        BATCH_PREPARATION_HEADROOM_SECONDS
                        if snapshot.monitor_batch_id is not None
                        else 0
                    )
                )
                daily_slot = _portfolio_slot(snapshot.decision_window_start_at)
                if (
                    snapshot.now < snapshot.market_open_at
                    or snapshot.now < daily_slot < snapshot.now + headroom
                ):
                    return ordinary
            return AgentPaperLoopAction.RUN_MONITOR_INCREMENT
        return ordinary
    if snapshot.manual_pending_status in {"preparing", "unknown"}:
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    if snapshot.manual_research_only and snapshot.manual_pending_request_id is not None:
        if snapshot.manual_pending_status == "queued":
            return AgentPaperLoopAction.TRIGGER_MANUAL
        if snapshot.manual_pending_status == "active":
            return AgentPaperLoopAction.FINISH_MANUAL
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    if snapshot.manual_finish_request_id is not None:
        if (
            snapshot.evaluation_id is not None
            or snapshot.cycle_status not in {"completed", "failed", "unavailable"}
            or snapshot.cycle_id is None
        ):
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        return AgentPaperLoopAction.FINISH_MANUAL
    if snapshot.manual_pending_request_id is not None:
        if snapshot.manual_pending_status != "queued":
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        ordinary = choose_loop_action(
            replace(snapshot, manual_pending_request_id=None, manual_pending_status=None),
            prepared_market_open_at=prepared_market_open_at,
        )
        if ordinary in {
            AgentPaperLoopAction.WAIT_FOR_SESSION,
            AgentPaperLoopAction.WAIT_FOR_SLOT,
            AgentPaperLoopAction.PREPARE_INPUTS,
            AgentPaperLoopAction.PUBLISH_PERFORMANCE,
        }:
            return AgentPaperLoopAction.TRIGGER_MANUAL
        return ordinary
    if snapshot.evaluation_id is None and snapshot.cycle_result_code == "POLICY_UNCONFIRMED":
        if snapshot.cycle_id is None or snapshot.cycle_status not in {"researching", "unavailable"}:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        if snapshot.performance_due_at is not None:
            return AgentPaperLoopAction.PUBLISH_PERFORMANCE
        return AgentPaperLoopAction.WAIT_FOR_SLOT
    if snapshot.evaluation_id is None and snapshot.cycle_result_code == "TARGET_ACTIVATION_PENDING":
        return AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE

    if snapshot.evaluation_id is not None:
        if snapshot.evaluation_slot_at is None:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        if snapshot.cycle_status == "evaluation_waiting":
            return AgentPaperLoopAction.WAIT_FOR_EVALUATION
        if snapshot.cycle_status == "evaluation_performance_due":
            if snapshot.performance_due_at is None or snapshot.action_account_id is None:
                return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
            return AgentPaperLoopAction.PUBLISH_EVALUATION_PERFORMANCE
        if snapshot.cycle_status == "evaluation_complete":
            return AgentPaperLoopAction.FINALIZE_EVALUATION
        if snapshot.cycle_status == "evaluation_pending":
            if snapshot.now > snapshot.evaluation_slot_at + timedelta(seconds=60):
                return AgentPaperLoopAction.CLOSE_INCOMPLETE_EVALUATION
            return AgentPaperLoopAction.ADVANCE_EVALUATION
        if snapshot.cycle_status == "researching":
            if snapshot.research_job_status == "queued":
                return AgentPaperLoopAction.RUN_EVALUATION_RESEARCH_JOB
            if snapshot.research_job_status == "running_expired":
                return AgentPaperLoopAction.RUN_EVALUATION_RESEARCH_JOB
            if snapshot.research_job_status == "running":
                if snapshot.research_job_lease_expires_at is None:
                    return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
                return AgentPaperLoopAction.WAIT_FOR_EVALUATION_JOB
            if snapshot.research_job_status in _TERMINAL_JOB_STATUSES:
                return AgentPaperLoopAction.RESUME_EVALUATION
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        if snapshot.cycle_status in {
            "completed",
            "decision_recorded",
            "risk_evaluated",
        }:
            return AgentPaperLoopAction.RESUME_EVALUATION
        if snapshot.cycle_status == "partially_filled" and snapshot.has_open_order:
            if not snapshot.additional_market_windows_allowed:
                return AgentPaperLoopAction.STOP_ORDER_UNAVAILABLE
            if snapshot.market_window_ready:
                return AgentPaperLoopAction.ADVANCE_ORDER
            if snapshot.market_window_end_at is None:
                return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
            if snapshot.now < snapshot.market_window_end_at:
                return AgentPaperLoopAction.WAIT_FOR_MARKET
            return AgentPaperLoopAction.COLLECT_WINDOW
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
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    if snapshot.cycle_status == "researching":
        if snapshot.research_job_status == "queued":
            return AgentPaperLoopAction.RUN_RESEARCH_JOB
        if snapshot.research_job_status in _TERMINAL_JOB_STATUSES | {"running_expired"}:
            return AgentPaperLoopAction.RESUME
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    if (
        snapshot.cycle_status == "partially_filled"
        and snapshot.has_open_order
        and not snapshot.additional_market_windows_allowed
    ):
        return AgentPaperLoopAction.STOP_ORDER_UNAVAILABLE

    if (
        snapshot.cycle_status in {"awaiting_market", "partially_filled"}
        and snapshot.has_open_order
        and snapshot.market_window_ready
    ):
        return AgentPaperLoopAction.ADVANCE_ORDER

    if snapshot.cycle_status in {"awaiting_market", "partially_filled"} and snapshot.has_open_order:
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

    if snapshot.performance_due_at is not None:
        return AgentPaperLoopAction.PUBLISH_PERFORMANCE

    decision_slot = _portfolio_slot(snapshot.decision_window_start_at)
    if snapshot.now < decision_slot:
        if (
            snapshot.market_open_at is not None
            and snapshot.market_open_at <= snapshot.now
            and snapshot.market_open_at < decision_slot
            and prepared_market_open_at != snapshot.market_open_at
        ):
            return AgentPaperLoopAction.PREPARE_INPUTS
        return AgentPaperLoopAction.WAIT_FOR_SESSION
    if snapshot.now >= snapshot.decision_window_end_at:
        return AgentPaperLoopAction.WAIT_FOR_SESSION
    if snapshot.now >= decision_slot + timedelta(minutes=5):
        return AgentPaperLoopAction.WAIT_FOR_SESSION

    current_slot = floor_decision_slot(snapshot.now)
    if (snapshot.trigger_attempt_slot_at is None) != (snapshot.trigger_failure_code is None):
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    if snapshot.trigger_failure_code is not None:
        if snapshot.trigger_failure_code not in _SLOT_TERMINAL_TRIGGER_FAILURE_CODES:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        if snapshot.trigger_attempt_slot_at == current_slot:
            return AgentPaperLoopAction.WAIT_FOR_SLOT
    if snapshot.cycle_status not in {None, "completed", "unavailable", "failed"}:
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    if snapshot.cycle_slot_at == current_slot:
        return AgentPaperLoopAction.WAIT_FOR_SLOT
    return AgentPaperLoopAction.TRIGGER


def _normal_wait_until(
    snapshot: AgentPaperLoopSnapshot,
    action: AgentPaperLoopAction,
) -> datetime | None:
    """Return the next normal-flow wakeup without changing its scheduling policy."""
    if action == AgentPaperLoopAction.WAIT_FOR_SLOT:
        return floor_decision_slot(snapshot.now) + timedelta(minutes=5)
    if action == AgentPaperLoopAction.WAIT_FOR_MARKET:
        return snapshot.market_window_end_at
    if action != AgentPaperLoopAction.WAIT_FOR_SESSION:
        return None
    if snapshot.decision_window_start_at is None or snapshot.next_decision_window_start_at is None:
        return None
    if snapshot.now < _portfolio_slot(snapshot.decision_window_start_at):
        wait_until = _portfolio_slot(snapshot.decision_window_start_at)
        if (
            snapshot.market_open_at is not None
            and snapshot.now < snapshot.market_open_at < wait_until
        ):
            wait_until = snapshot.market_open_at
    else:
        wait_until = _portfolio_slot(snapshot.next_decision_window_start_at)
        if (
            snapshot.next_market_open_at is not None
            and snapshot.now < snapshot.next_market_open_at < wait_until
        ):
            wait_until = snapshot.next_market_open_at
    if (
        snapshot.decision_window_end_at is not None
        and snapshot.now < snapshot.decision_window_end_at < wait_until
    ):
        return snapshot.decision_window_end_at
    return wait_until


_SAFE_PREPARATION_FAILURE_PHASES = frozenset(
    {
        "configuration",
        "prior_daily",
        "macro",
        "current_trends",
        "research_identity",
        "prepare_pair",
        "trigger_request",
        "trigger_reservation",
        "trigger_provider",
        "preparation_journal",
        "preparation_effects",
    }
)


def _annotate_agent_paper_failure(error: Exception, *, phase: str) -> None:
    """Attach one allowlisted diagnostic phase without exposing exception details."""
    if phase not in _SAFE_PREPARATION_FAILURE_PHASES:
        return
    try:
        setattr(error, "_nexus_agent_paper_failure_phase", phase)
        if phase in PHASES and not preparation_failure_fields(error):
            setattr(
                error, "_nexus_preparation_diagnostic", (phase, "operation", exception_code(error))
            )
    except Exception:
        return


def _emit_agent_paper_unknown_failure(
    stage: str,
    occurred_at: datetime,
    *,
    action: AgentPaperLoopAction | None = None,
    error: Exception | None = None,
) -> None:
    """Best-effort safe stage evidence for one fail-closed unknown result."""
    if stage not in {"execute", "heartbeat", "inspect", "planner"}:
        stage = "unknown"
    try:
        payload = {
            "error_code": "AGENT_PAPER_LOOP_RESULT_UNKNOWN",
            "event": "agent_paper_loop_failure",
            "failure_stage": stage,
            "occurred_at": occurred_at.isoformat(),
        }
        if action is not None:
            payload["failure_action"] = action.value
        if stage == "heartbeat" and error is not None:
            payload["failure_kind"] = (
                "timeout"
                if isinstance(error, TimeoutError)
                else "database"
                if isinstance(error, SQLAlchemyError)
                else "unexpected_exception"
            )
        phase = getattr(error, "_nexus_agent_paper_failure_phase", None)
        if phase in _SAFE_PREPARATION_FAILURE_PHASES:
            payload["failure_phase"] = phase
        from nexus.ops.manual_paper_host import (
            MANUAL_TRIGGER_CODES,
            MANUAL_TRIGGER_KINDS,
            MANUAL_TRIGGER_PHASES,
        )

        code = getattr(error, "_nexus_agent_paper_failure_code", None)
        if phase in MANUAL_TRIGGER_PHASES and code in MANUAL_TRIGGER_CODES:
            payload["failure_code"] = code
        kind = getattr(error, "_nexus_agent_paper_failure_kind", None)
        if phase in MANUAL_TRIGGER_PHASES and kind in MANUAL_TRIGGER_KINDS:
            payload["failure_kind"] = kind
        from nexus.worker import CapacityPreclaimTimeout

        if (
            stage == "execute"
            and action == AgentPaperLoopAction.RUN_RESEARCH_JOB
            and isinstance(error, CapacityPreclaimTimeout)
        ):
            payload.update(
                failure_phase="research_preclaim",
                failure_code="CODEX_CAPACITY_PRECLAIM_TIMEOUT",
                failure_kind="timeout",
            )
        from nexus.agents.portfolio_synthesis_worker import SynthesisHandlerResultUnknown

        if (
            stage == "execute"
            and action == AgentPaperLoopAction.RUN_RESEARCH_JOB
            and isinstance(error, SynthesisHandlerResultUnknown)
        ):
            payload.update(
                failure_phase="portfolio_synthesis_handler",
                failure_code="PORTFOLIO_SYNTHESIS_HANDLER_RESULT_UNKNOWN",
                failure_kind="timeout",
            )
        payload.update(preparation_failure_fields(error))
        print(
            json.dumps(payload, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
    except Exception:
        # Diagnostics must never replace the original fail-closed outcome.
        return


async def run_foreground_loop(
    *,
    inspect: Callable[[], Awaitable[AgentPaperLoopSnapshot]],
    execute: Callable[
        [AgentPaperLoopAction, AgentPaperLoopSnapshot],
        Awaitable[AgentPaperLoopAction | None],
    ],
    sleep: Callable[[float], Awaitable[None]],
    wait_sleep: Callable[[float], Awaitable[None]] | None = None,
    wake_for_pending_work: Callable[[], Awaitable[bool]] | None = None,
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

    last_heartbeat_at: datetime | None = None
    prepared_market_open_at: datetime | None = None

    async def persist_heartbeat() -> datetime:
        nonlocal last_heartbeat_at
        seen_at = clock()
        async with asyncio.timeout(interval_seconds):
            await heartbeat(seen_at)
        last_heartbeat_at = seen_at
        return seen_at

    def stop_unknown(stage: str, *, error: Exception | None = None) -> AgentPaperLoopAction:
        _emit_agent_paper_unknown_failure(stage, clock(), error=error)
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    async def wait_before_reinspection(
        *,
        snapshot_now: datetime,
        wait_until: datetime,
        elapsed_by_monotonic: bool,
    ) -> AgentPaperLoopAction | None:
        """Wait until due, but break early when wall time jumps across the target."""
        remaining = max(0.0, (wait_until - snapshot_now).total_seconds())
        deadline = monotonic() + remaining
        wall_started_at = last_heartbeat_at
        while remaining > 0:
            duration = min(remaining, interval_seconds)
            await (wait_sleep or sleep)(duration)
            if shutdown_requested():
                return AgentPaperLoopAction.STOP_REQUESTED
            try:
                seen_at = await persist_heartbeat()
            except Exception as error:
                return stop_unknown("heartbeat", error=error)
            if wake_for_pending_work is not None:
                try:
                    async with asyncio.timeout(interval_seconds):
                        if await wake_for_pending_work():
                            break
                except Exception as error:
                    return stop_unknown("inspect", error=error)
            if (
                wall_started_at is not None
                and seen_at >= wall_started_at
                and snapshot_now + (seen_at - wall_started_at) >= wait_until
            ):
                break
            remaining = deadline - monotonic() if elapsed_by_monotonic else remaining - duration
        return None

    if shutdown_requested():
        return AgentPaperLoopAction.STOP_REQUESTED
    try:
        await persist_heartbeat()
    except Exception as error:
        return stop_unknown("heartbeat", error=error)
    while True:
        if shutdown_requested():
            return AgentPaperLoopAction.STOP_REQUESTED
        try:
            snapshot = await inspect()
        except Exception:
            _emit_agent_paper_unknown_failure("inspect", clock())
            raise
        if shutdown_requested():
            return AgentPaperLoopAction.STOP_REQUESTED
        action = choose_loop_action(snapshot, prepared_market_open_at=prepared_market_open_at)
        if action in {
            AgentPaperLoopAction.STOP_RESULT_UNKNOWN,
            AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE,
            AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE,
            AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH,
            AgentPaperLoopAction.STOP_DISABLED,
            AgentPaperLoopAction.STOP_SESSION_CLOSED,
        }:
            if action == AgentPaperLoopAction.STOP_RESULT_UNKNOWN:
                return stop_unknown("planner")
            return action
        if action == AgentPaperLoopAction.WAIT_FOR_SLOT:
            next_slot = floor_decision_slot(snapshot.now) + timedelta(minutes=5)
            stopped = await wait_before_reinspection(
                snapshot_now=snapshot.now,
                wait_until=next_slot,
                elapsed_by_monotonic=False,
            )
            if stopped is not None:
                return stopped
            continue
        if action == AgentPaperLoopAction.WAIT_FOR_EVALUATION:
            assert snapshot.evaluation_slot_at is not None
            stopped = await wait_before_reinspection(
                snapshot_now=snapshot.now,
                wait_until=snapshot.evaluation_slot_at,
                elapsed_by_monotonic=True,
            )
            if stopped is not None:
                return stopped
            continue
        if action == AgentPaperLoopAction.WAIT_FOR_EVALUATION_JOB:
            assert snapshot.research_job_lease_expires_at is not None
            stopped = await wait_before_reinspection(
                snapshot_now=snapshot.now,
                wait_until=snapshot.research_job_lease_expires_at,
                elapsed_by_monotonic=True,
            )
            if stopped is not None:
                return stopped
            continue
        if action == AgentPaperLoopAction.WAIT_FOR_SESSION:
            wait_until = _normal_wait_until(snapshot, action)
            assert wait_until is not None
            if (
                prepared_market_open_at == snapshot.market_open_at
                and snapshot.decision_window_start_at is not None
            ):
                wait_until = max(wait_until, _portfolio_slot(snapshot.decision_window_start_at))
            stopped = await wait_before_reinspection(
                snapshot_now=snapshot.now,
                wait_until=wait_until,
                elapsed_by_monotonic=True,
            )
            if stopped is not None:
                return stopped
            continue
        if action == AgentPaperLoopAction.WAIT_FOR_MARKET:
            assert snapshot.market_window_end_at is not None
            stopped = await wait_before_reinspection(
                snapshot_now=snapshot.now,
                wait_until=snapshot.market_window_end_at,
                elapsed_by_monotonic=False,
            )
            if stopped is not None:
                return stopped
            continue
        if action == AgentPaperLoopAction.RUN_MONITOR_INCREMENT:
            snapshot = replace(snapshot, host_prepared_market_open_at=prepared_market_open_at)
        action_task: asyncio.Future[AgentPaperLoopAction | None] = asyncio.ensure_future(
            execute(action, snapshot)
        )
        heartbeat_task: asyncio.Future[AgentPaperLoopAction | None] | None = None
        heartbeat_error: Exception | None = None
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
                except Exception as error:
                    heartbeat_error = error
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
            if action_error is not None or heartbeat_error is not None or not confirmed:
                return AgentPaperLoopAction.STOP_UNCONFIRMED
            return AgentPaperLoopAction.STOP_REQUESTED
        if action_error is not None:
            _emit_agent_paper_unknown_failure("execute", clock(), action=action, error=action_error)
            raise action_error
        if heartbeat_error is not None:
            return stop_unknown("heartbeat", error=heartbeat_error)
        if action == AgentPaperLoopAction.PREPARE_INPUTS and stop is None:
            prepared_market_open_at = snapshot.market_open_at
        try:
            await persist_heartbeat()
        except Exception as error:
            return stop_unknown("heartbeat", error=error)
        if stop is not None:
            if stop == AgentPaperLoopAction.STOP_RESULT_UNKNOWN:
                return stop_unknown("execute")
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
    if action == AgentPaperLoopAction.RUN_MONITOR_INCREMENT:
        from nexus.ops.monitor_dispatch import confirm_monitor_shutdown

        if (
            before_snapshot.monitor_trigger_id is None
            or before_snapshot.calendar_authority_id != expected_calendar_authority_id
            or action_result is not None
        ):
            return False
        return await confirm_monitor_shutdown(
            settings, account_id=account_id, trigger_id=before_snapshot.monitor_trigger_id
        )
    exact_actions = {
        AgentPaperLoopAction.COLLECT_WINDOW,
        AgentPaperLoopAction.ADVANCE_ORDER,
        AgentPaperLoopAction.PUBLISH_EVALUATION_PERFORMANCE,
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
                if action in {
                    AgentPaperLoopAction.PUBLISH_EVALUATION_PERFORMANCE,
                    AgentPaperLoopAction.PUBLISH_PERFORMANCE,
                }:
                    if before_snapshot.performance_due_at is None:
                        return False
                    performance_account_id = (
                        before_snapshot.action_account_id
                        if action == AgentPaperLoopAction.PUBLISH_EVALUATION_PERFORMANCE
                        else account_id
                    )
                    if performance_account_id is None:
                        return False
                    persisted = await session.scalar(
                        select(PaperPerformanceSnapshotRecord.id).where(
                            PaperPerformanceSnapshotRecord.account_id == performance_account_id,
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
                    advance_account_id = before_snapshot.action_account_id or account_id
                    advance_result = await read_agent_window_advance_result(
                        session,
                        account_id=advance_account_id,
                        cycle_id=before_snapshot.cycle_id,
                        order_id=before_snapshot.order_id,
                        market_window_id=before_snapshot.market_window_id,
                    )
                    if advance_result is None:
                        return False
                    if advance_result.action in {"no_data", "no_capacity"}:
                        return action_result == AgentPaperLoopAction.STOP_ORDER_UNAVAILABLE
                    return action_result is None
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
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], datetime] = _utc_now,
    monotonic: Callable[[], float] = monotonic_clock,
    recovery_guard: Callable[[EngineSettings], Awaitable[None]] | None = None,
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
    if settings.formal_budget_protection_enabled:
        bound_environment, pinned_calendar = await _load_bound_loop_environment(
            settings, repo_root, {} if env_file is not None else os.environ, env_file=env_file
        )
        initial = (
            _FileEngineSettings.model_validate(bound_environment)
            if env_file is not None
            else EngineSettings.model_validate(bound_environment)
        )
        if make_url(initial.database_url or "") != database:
            raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
        settings = initial.model_copy(update={"release_revision": settings.release_revision})
        if pinned_calendar is not None:
            account_id = UUID(bound_environment["NEXUS_AGENT_PAPER_ACCOUNT_ID"])
    worker_id = f"agent-paper-loop-{new_uuid7()}"
    started_at = clock()
    with acquire_agent_paper_lock():
        if recovery_guard is not None:
            await recovery_guard(settings)
        shutdown = asyncio.Event()

        async def wait_sleep(seconds: float) -> None:
            async def wait_for_shutdown() -> None:
                await shutdown.wait()

            sleeper: asyncio.Future[None] = asyncio.ensure_future(sleep(seconds))
            stopper = asyncio.create_task(wait_for_shutdown())
            try:
                done, _ = await asyncio.wait(
                    {sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED
                )
                if sleeper in done:
                    await sleeper
            finally:
                for task in (sleeper, stopper):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(sleeper, stopper, return_exceptions=True)

        event_loop = asyncio.get_running_loop()
        installed_signals: list[signal.Signals] = []
        try:
            for item in (signal.SIGINT, signal.SIGTERM):
                event_loop.add_signal_handler(item, shutdown.set)
                installed_signals.append(item)
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

            from nexus.monitoring.host import PriceMonitorCoordinator
            from nexus.ops.research_monitor import refresh_host_observations, scan_host_once

            # The process lock and initial calendar belong to this database. A
            # later config reload must never silently move the running host.
            host_database = database
            monitor_host_scope: (
                tuple[UUID, UUID, UUID | None, UUID | None, AgentRuntimeMode | None] | None
            ) = (
                (
                    account_id,
                    calendar_authority_id,
                    settings.agent_paper_candidate_batch_id,
                    settings.agent_paper_formal_model_config_id,
                    settings.agent_runtime_mode,
                )
                if settings.agent_paper_formal_model_config_id is not None
                else None
            )

            async def scan_monitor() -> object:
                return await scan_host_once(
                    settings,
                    account_id=account_id,
                    calendar_authority_id=calendar_authority_id,
                    repo_root=repo_root,
                    env_file=env_file,
                )

            async def prepare_monitor(scheduled_at: datetime) -> object:
                return await refresh_host_observations(
                    settings,
                    account_id=account_id,
                    calendar_authority_id=calendar_authority_id,
                    scheduled_at=scheduled_at,
                    repo_root=repo_root,
                    env_file=env_file,
                )

            monitor = PriceMonitorCoordinator(scan_monitor, prepare=prepare_monitor)
            reported_monitor_bucket: datetime | None = None

            def report_monitor_diagnostic() -> None:
                nonlocal reported_monitor_bucket
                if (
                    monitor.busy
                    or monitor.last_diagnostic is None
                    or monitor.diagnostic_bucket is None
                    or monitor.diagnostic_bucket == reported_monitor_bucket
                ):
                    return
                reported_monitor_bucket = monitor.diagnostic_bucket
                # Fixed monitor diagnostics are distinct from research unknowns.
                # Never print the scan result, configuration or exception text.
                try:
                    print(
                        json.dumps(
                            {
                                "event": "research.monitor.scan_failed",
                                "error_code": monitor.last_diagnostic,
                                "scheduled_at": monitor.diagnostic_bucket.isoformat(),
                            },
                            sort_keys=True,
                        ),
                        file=sys.stderr,
                        flush=True,
                    )
                except OSError:
                    pass

            async def inspect() -> AgentPaperLoopSnapshot:
                nonlocal account_id, calendar_authority_id, settings, monitor_host_scope
                environment = load_agent_paper_service_environment(
                    repo_root,
                    {} if env_file is not None else os.environ,
                    **({"env_file": env_file} if env_file is not None else {}),
                )
                if environment.get("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false").lower() in {
                    "",
                    "0",
                    "false",
                    "no",
                    "off",
                }:
                    return AgentPaperLoopSnapshot(now=clock(), enabled=False)
                from nexus.ops.formal_budget_protection import bind_runtime_environment

                if make_url(environment.get("DATABASE_URL", "")) != host_database:
                    raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
                environment, pinned_calendar = await bind_runtime_environment(settings, environment)
                current_account_id = (
                    account_id
                    if pinned_calendar is None
                    else UUID(environment["NEXUS_AGENT_PAPER_ACCOUNT_ID"])
                )
                current_calendar_id = (
                    calendar_authority_id if pinned_calendar is None else UUID(pinned_calendar)
                )
                current = (
                    _FileEngineSettings.model_validate(environment)
                    if env_file is not None
                    else EngineSettings.model_validate(environment)
                )
                if not current.agent_paper_cycle_enabled:
                    return AgentPaperLoopSnapshot(now=clock(), enabled=False)
                current_scope = (
                    current_account_id,
                    current_calendar_id,
                    current.agent_paper_candidate_batch_id,
                    current.agent_paper_formal_model_config_id,
                    current.agent_runtime_mode,
                )
                if (
                    make_url(current.database_url or "") != host_database
                    or (monitor_host_scope is not None and current_scope != monitor_host_scope)
                    or (
                        current.agent_paper_formal_model_config_id is not None
                        and current.agent_paper_account_id != current_account_id
                    )
                ):
                    raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
                if (
                    monitor_host_scope is None
                    and current.agent_paper_formal_model_config_id is not None
                ):
                    monitor_host_scope = current_scope
                account_id, calendar_authority_id = current_account_id, current_calendar_id
                # Release identity belongs to the running process, not the reloaded
                # business configuration or its frozen budget binding.
                current = current.model_copy(update={"release_revision": settings.release_revision})
                settings = current
                evaluation = await inspect_agent_version_evaluation_loop(
                    current,
                    expected_calendar_authority_id=calendar_authority_id,
                )
                if evaluation is not None and evaluation.cycle_status != "evaluation_waiting":
                    return evaluation
                evaluation_wait_at = None if evaluation is None else evaluation.evaluation_slot_at
                if evaluation is not None and evaluation_wait_at is None:
                    return evaluation
                normal = await inspect_agent_paper_loop(
                    current,
                    account_id=account_id,
                    expected_calendar_authority_id=calendar_authority_id,
                )
                from nexus.ops.manual_paper_host import inspect_manual_close, inspect_manual_queue

                normal = await inspect_manual_close(current, account_id=account_id, snapshot=normal)
                normal = await inspect_manual_queue(current, account_id=account_id, snapshot=normal)
                from nexus.monitoring.host import INCREMENT_ACTION_HEADROOM_SECONDS
                from nexus.ops.monitor_dispatch import inspect_monitor_queue

                # Upcoming evaluation work also needs its full execution window.
                if evaluation_wait_at is None or evaluation_wait_at > normal.now + timedelta(
                    seconds=INCREMENT_ACTION_HEADROOM_SECONDS
                ):
                    normal = await inspect_monitor_queue(
                        current, account_id=account_id, snapshot=normal
                    )
                if evaluation is None:
                    return normal
                assert evaluation_wait_at is not None
                normal_wait_until = _normal_wait_until(normal, choose_loop_action(normal))
                if normal_wait_until is not None and evaluation_wait_at <= normal_wait_until:
                    return evaluation
                return normal

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
                    account_id=account_id,
                    started_at=started_at,
                    seen_at=seen_at,
                )
                report_monitor_diagnostic()
                if settings.agent_paper_cycle_enabled and not shutdown.is_set():
                    monitor.kick(seen_at)

            observed_monitor_bucket: datetime | None = None

            async def wake_for_pending_work() -> bool:
                nonlocal observed_monitor_bucket
                from nexus.ops.manual_paper_host import manual_work_pending

                if settings.agent_paper_manual_enabled and await manual_work_pending(
                    settings, account_id=account_id
                ):
                    return True
                if not monitor.busy and monitor.last_bucket != observed_monitor_bucket:
                    observed_monitor_bucket = monitor.last_bucket
                    return True
                return False

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
                    sleep=sleep,
                    wait_sleep=wait_sleep,
                    heartbeat=heartbeat,
                    shutdown_requested=shutdown.is_set,
                    confirm_shutdown=confirm_shutdown,
                    clock=clock,
                    monotonic=monotonic,
                    wake_for_pending_work=wake_for_pending_work,
                )
            except ValueError as error:
                if str(error) == "AGENT_PAPER_CALENDAR_UNAVAILABLE":
                    return AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE
                if str(error) == "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH":
                    return AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH
                raise
            finally:
                try:
                    await monitor.close()
                finally:
                    report_monitor_diagnostic()
        finally:
            for installed_signal in installed_signals:
                event_loop.remove_signal_handler(installed_signal)


async def _load_bound_loop_environment(
    settings: EngineSettings,
    repo_root: Path,
    process_environment: Mapping[str, str],
    *,
    env_file: Path | None = None,
) -> tuple[dict[str, str], str | None]:
    from nexus.ops.formal_budget_protection import bind_runtime_environment

    environment = load_agent_paper_service_environment(
        repo_root, process_environment, **({"env_file": env_file} if env_file is not None else {})
    )
    if environment.get("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false").lower() in {
        "",
        "0",
        "false",
        "no",
        "off",
    }:
        return environment, None
    return await bind_runtime_environment(settings, environment)


def _candidate_batch_setting(environment: Mapping[str, str]) -> UUID:
    raw = environment.get("NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID", "").strip()
    if not raw:
        raise ValueError("CANDIDATE_BATCH_REQUIRED")
    try:
        return UUID(raw)
    except ValueError:
        raise ValueError("CANDIDATE_BATCH_INVALID") from None


def _normal_cycle_config_readiness(environment: Mapping[str, str]) -> dict[str, object]:
    """Host readiness does not authorize starting a new ordinary cycle."""
    try:
        _candidate_batch_setting(environment)
    except ValueError as error:
        return {
            "normal_cycle_status": "blocked",
            "normal_cycle_blocking_codes": [str(error)],
        }
    return {
        "normal_cycle_status": "requires_batch_validation",
        "normal_cycle_blocking_codes": [],
    }


async def _load_loop_candidates(
    settings: EngineSettings,
    *,
    account_id: UUID,
    batch_id: UUID,
    expected_calendar_authority_id: UUID,
) -> tuple[CandidateInstrumentIdentity, ...]:
    if not settings.database_url:
        raise ValueError("AGENT_PAPER_DATABASE_UNCONFIGURED")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            batch, identities = await validate_agent_paper_candidate_batch(
                session,
                batch_id=batch_id,
                account_id=account_id,
                as_of=now,
            )
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=account_id,
                now=now,
                expected_calendar_authority_id=expected_calendar_authority_id,
                expected_universe_snapshot_id=batch.universe_snapshot_id,
            )
            if batch.universe_snapshot_id != authority.universe_snapshot_id:
                raise ValueError("CANDIDATE_BATCH_INVALID")
            return identities
    finally:
        await engine.dispose()


async def _close_unrecoverable_evaluation_once(
    *,
    database_url: str | None,
    evaluation_id: UUID,
    decision_slot_at: datetime,
    reason_code: str,
) -> None:
    if not database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            database_now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(database_now, datetime):
                raise RuntimeError("AGENT_VERSION_TIME_UNAVAILABLE")
            await close_unrecoverable_agent_version_evaluation(
                session,
                evaluation_id=evaluation_id,
                decision_slot_at=decision_slot_at,
                reason_code=reason_code,
                now=database_now,
            )
    finally:
        await engine.dispose()


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
    if action == AgentPaperLoopAction.RUN_MONITOR_INCREMENT:
        from nexus.ops.monitor_dispatch import dispatch_host_once

        if (
            snapshot.monitor_trigger_id is None
            or snapshot.calendar_authority_id is None
            or choose_loop_action(snapshot) != action
        ):
            raise RuntimeError("MONITOR_HOST_SCOPE_CHANGED")
        result = await dispatch_host_once(
            settings,
            account_id=account_id,
            trigger_id=snapshot.monitor_trigger_id,
            calendar_authority_id=snapshot.calendar_authority_id,
            repo_root=repo_root,
            env_file=env_file,
            prepared_market_open_at=snapshot.host_prepared_market_open_at,
        )
        return (
            AgentPaperLoopAction.STOP_RESULT_UNKNOWN
            if result["status"] in {"unknown", "unconfirmed"}
            else None
        )
    if action == AgentPaperLoopAction.TRIGGER_MANUAL:
        from nexus.ops.manual_paper_host import dispatch_manual_request

        if (
            choose_loop_action(snapshot) != action
            or snapshot.manual_pending_request_id is None
            or (not snapshot.manual_research_only and snapshot.calendar_authority_id is None)
            or account_id != settings.agent_paper_account_id
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        environment = load_agent_paper_service_environment(
            repo_root,
            {} if env_file is not None else os.environ,
            **({"env_file": env_file} if env_file is not None else {}),
        )
        trigger_settings = (
            _FileTriggerSettings if env_file is not None else AgentPaperTriggerSettings
        ).model_validate(environment)
        require_formal_host_configuration_match(settings, trigger_settings)
        await require_formal_host_trigger_profile(trigger_settings, now=snapshot.now)
        await dispatch_manual_request(
            settings,
            trigger_settings,
            request_id=snapshot.manual_pending_request_id,
            repo_root=repo_root,
            calendar_authority_id=snapshot.calendar_authority_id,
            process_environment=environment,
        )
        return None
    if action == AgentPaperLoopAction.FINISH_MANUAL:
        from nexus.ops.manual_paper_host import finish_configured_manual_request
        from nexus.persistence.session import session_factory

        if (
            snapshot.manual_finish_request_id is None
            or (not snapshot.manual_research_only and snapshot.cycle_id is None)
            or account_id != settings.agent_paper_account_id
            or choose_loop_action(snapshot) != action
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        factory = session_factory(settings)
        try:
            async with factory.begin() as session:
                view = await finish_configured_manual_request(
                    session,
                    settings,
                    request_id=snapshot.manual_finish_request_id,
                    cancel_unused=True,
                )
                if not snapshot.manual_research_only and view.cycle_id != snapshot.cycle_id:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        finally:
            await factory.kw["bind"].dispose()
        return None
    bound_environment: dict[str, str] = {}
    pinned_calendar = None
    if settings.formal_budget_protection_enabled:
        bound_environment, pinned_calendar = await _load_bound_loop_environment(
            settings,
            repo_root,
            {} if env_file is not None else os.environ,
            env_file=env_file,
        )
        if bound_environment.get("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false").lower() in {
            "",
            "0",
            "false",
            "no",
            "off",
        }:
            return AgentPaperLoopAction.STOP_DISABLED
    if pinned_calendar is not None:
        account_id = UUID(bound_environment["NEXUS_AGENT_PAPER_ACCOUNT_ID"])
        if str(snapshot.calendar_authority_id) != pinned_calendar:
            return AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH
    formal_stage_capacity = False
    if action in {
        AgentPaperLoopAction.RUN_RESEARCH_JOB,
        AgentPaperLoopAction.RESUME,
        AgentPaperLoopAction.COLLECT_WINDOW,
        AgentPaperLoopAction.ADVANCE_ORDER,
    }:
        if snapshot.cycle_id is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        formal_stage_capacity = await resolve_formal_host_cycle_profile(
            settings,
            account_id=account_id,
            cycle_id=snapshot.cycle_id,
            research_job_id=(
                snapshot.research_job_id
                if action == AgentPaperLoopAction.RUN_RESEARCH_JOB
                and isinstance(snapshot.research_job_id, UUID)
                else None
            ),
        )
    if action == AgentPaperLoopAction.PREPARE_INPUTS:
        if (
            snapshot.calendar_authority_id is None
            or snapshot.market_open_at is None
            or snapshot.decision_window_start_at is None
            or not snapshot.market_open_at
            <= snapshot.now
            < _portfolio_slot(snapshot.decision_window_start_at)
        ):
            _emit_agent_paper_unknown_failure(
                "execute",
                snapshot.now,
                action=action,
                error=preparation_unknown("configuration", "window", "SCOPE_UNAVAILABLE"),
            )
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        try:
            environment = (
                (
                    bound_environment
                    if pinned_calendar is not None
                    else load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
                )
                if env_file is not None
                else (
                    bound_environment
                    if pinned_calendar is not None
                    else load_agent_paper_service_environment(repo_root, os.environ)
                )
            )
            trigger_settings = (
                _FileTriggerSettings if env_file is not None else AgentPaperTriggerSettings
            ).model_validate(environment)
            require_formal_host_configuration_match(settings, trigger_settings)
            await require_formal_host_trigger_profile(trigger_settings, now=snapshot.now)
            if env_file is not None:
                _require_explicit_schwab_config(environment)
            macro_settings = (
                _FileDailySettings if env_file is not None else DailyOnceSettings
            ).model_validate(environment)
        except Exception as error:
            _annotate_agent_paper_failure(error, phase="configuration")
            raise
        with preparation_step("configuration", "candidates"):
            candidates = await _load_loop_candidates(
                trigger_settings,
                account_id=account_id,
                batch_id=_candidate_batch_setting(environment),
                expected_calendar_authority_id=snapshot.calendar_authority_id,
            )
        for candidate in candidates:
            try:
                daily = await prepare_prior_daily_once(
                    trigger_settings,
                    account_id=account_id,
                    repo_root=repo_root,
                    now=snapshot.now,
                    expected_calendar_authority_id=snapshot.calendar_authority_id,
                    candidate=candidate,
                )
            except Exception as error:
                _annotate_agent_paper_failure(error, phase="prior_daily")
                raise
            if daily not in {"ready", "prepared", "unavailable"}:
                _emit_agent_paper_unknown_failure(
                    "execute",
                    snapshot.now,
                    action=action,
                    error=preparation_unknown("prior_daily", "readback", "READBACK_MISMATCH"),
                )
                return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        try:
            identity_status = await prepare_agent_paper_identity_once(
                trigger_settings,
                account_id=account_id,
                batch_id=_candidate_batch_setting(environment),
                expected_calendar_authority_id=snapshot.calendar_authority_id,
                repo_root=repo_root,
                contact=environment.get("SEC_CONTACT", ""),
            )
        except Exception as error:
            _annotate_agent_paper_failure(error, phase="research_identity")
            raise
        if identity_status not in {"ready", "prepared", "unavailable"}:
            _emit_agent_paper_unknown_failure(
                "execute",
                snapshot.now,
                action=action,
                error=preparation_unknown("research_identity", "readback", "READBACK_MISMATCH"),
            )
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        for candidate in candidates:
            try:
                macro = await prepare_macro_once(
                    macro_settings,
                    fred_key=SecretStr(environment.get("FRED_API_KEY", "")),
                    account_id=account_id,
                    repo_root=repo_root,
                    now=snapshot.now,
                    expected_calendar_authority_id=snapshot.calendar_authority_id,
                    candidate=candidate,
                )
            except Exception as error:
                _annotate_agent_paper_failure(error, phase="macro")
                raise
            if macro not in {"ready", "prepared", "unavailable"}:
                _emit_agent_paper_unknown_failure(
                    "execute",
                    snapshot.now,
                    action=action,
                    error=preparation_unknown("macro", "readback", "READBACK_MISMATCH"),
                )
                return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        for candidate in candidates:
            try:
                trends = await prepare_current_trends_once(
                    trigger_settings,
                    account_id=account_id,
                    repo_root=repo_root,
                    now=snapshot.now,
                    expected_calendar_authority_id=snapshot.calendar_authority_id,
                    candidate=candidate,
                )
            except Exception as error:
                _annotate_agent_paper_failure(error, phase="current_trends")
                raise
            if trends not in {"ready", "prepared", "unavailable"}:
                _emit_agent_paper_unknown_failure(
                    "execute",
                    snapshot.now,
                    action=action,
                    error=preparation_unknown("current_trends", "readback", "READBACK_MISMATCH"),
                )
                return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        return None
    if action == AgentPaperLoopAction.ADVANCE_EVALUATION:
        if (
            snapshot.evaluation_id is None
            or snapshot.evaluation_slot_at is None
            or snapshot.calendar_authority_id is None
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        try:
            environment = (
                (
                    bound_environment
                    if pinned_calendar is not None
                    else load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
                )
                if env_file is not None
                else (
                    bound_environment
                    if pinned_calendar is not None
                    else load_agent_paper_service_environment(repo_root, os.environ)
                )
            )
            trigger_settings = (
                _FileTriggerSettings if env_file is not None else AgentPaperTriggerSettings
            ).model_validate(environment)
            if env_file is not None:
                _require_explicit_schwab_config(environment)
            macro_settings = (
                _FileDailySettings if env_file is not None else DailyOnceSettings
            ).model_validate(environment)
        except Exception as error:
            _annotate_agent_paper_failure(error, phase="configuration")
            raise
        candidate = await load_agent_version_candidate(
            trigger_settings,
            evaluation_id=snapshot.evaluation_id,
            expected_calendar_authority_id=snapshot.calendar_authority_id,
            now=snapshot.now,
        )
        try:
            prior_daily = await prepare_prior_daily_once(
                trigger_settings,
                account_id=account_id,
                repo_root=repo_root,
                now=snapshot.now,
                expected_calendar_authority_id=snapshot.calendar_authority_id,
                candidate=candidate,
            )
        except Exception as error:
            _annotate_agent_paper_failure(error, phase="prior_daily")
            raise
        if prior_daily == "prepared":
            return None
        if prior_daily == "unavailable":
            await _close_unrecoverable_evaluation_once(
                database_url=trigger_settings.database_url,
                evaluation_id=snapshot.evaluation_id,
                decision_slot_at=snapshot.evaluation_slot_at,
                reason_code="PAIR_PRIOR_DAILY_UNAVAILABLE",
            )
            return None
        if prior_daily != "ready":
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        try:
            macro = await prepare_macro_once(
                macro_settings,
                fred_key=SecretStr(environment.get("FRED_API_KEY", "")),
                account_id=account_id,
                repo_root=repo_root,
                now=snapshot.now,
                expected_calendar_authority_id=snapshot.calendar_authority_id,
                candidate=candidate,
            )
        except Exception as error:
            _annotate_agent_paper_failure(error, phase="macro")
            raise
        if macro == "prepared":
            return None
        if macro not in {"ready", "unavailable"}:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        try:
            current_trends = await prepare_current_trends_once(
                trigger_settings,
                account_id=account_id,
                repo_root=repo_root,
                now=snapshot.now,
                expected_calendar_authority_id=snapshot.calendar_authority_id,
                candidate=candidate,
            )
        except Exception as error:
            _annotate_agent_paper_failure(error, phase="current_trends")
            raise
        if current_trends == "prepared":
            return None
        if current_trends == "unavailable":
            await _close_unrecoverable_evaluation_once(
                database_url=trigger_settings.database_url,
                evaluation_id=snapshot.evaluation_id,
                decision_slot_at=snapshot.evaluation_slot_at,
                reason_code="PAIR_CURRENT_TRENDS_UNAVAILABLE",
            )
            return None
        if current_trends != "ready":
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        try:
            evaluation_result = await prepare_agent_version_slot_once(
                trigger_settings,
                evaluation_id=snapshot.evaluation_id,
                decision_slot_at=snapshot.evaluation_slot_at,
                repo_root=repo_root,
                expected_calendar_authority_id=snapshot.calendar_authority_id,
                **({"process_environment": environment} if env_file is not None else {}),
            )
        except Exception as error:
            _annotate_agent_paper_failure(error, phase="prepare_pair")
            raise
        if evaluation_result.status != "incomparable":
            return None
        await _close_unrecoverable_evaluation_once(
            database_url=trigger_settings.database_url,
            evaluation_id=snapshot.evaluation_id,
            decision_slot_at=snapshot.evaluation_slot_at,
            reason_code=evaluation_result.reason_code,
        )
        return None
    if action in {
        AgentPaperLoopAction.CLOSE_INCOMPLETE_EVALUATION,
        AgentPaperLoopAction.FINALIZE_EVALUATION,
        AgentPaperLoopAction.RESUME_EVALUATION,
    }:
        if snapshot.evaluation_id is None or snapshot.evaluation_slot_at is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if not settings.database_url:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        engine = create_async_engine(settings.database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory.begin() as session:
                if action == AgentPaperLoopAction.CLOSE_INCOMPLETE_EVALUATION:
                    await close_incomplete_agent_version_evaluation(
                        session,
                        evaluation_id=snapshot.evaluation_id,
                        now=snapshot.now,
                    )
                    return None
                if action == AgentPaperLoopAction.FINALIZE_EVALUATION:
                    await finalize_agent_version_evaluation(
                        session,
                        evaluation_id=snapshot.evaluation_id,
                        now=snapshot.now,
                    )
                    return None
                evaluation_result = await resume_agent_version_slot(
                    session,
                    evaluation_id=snapshot.evaluation_id,
                    decision_slot_at=snapshot.evaluation_slot_at,
                    now=snapshot.now,
                )
                if evaluation_result.status == "incomparable":
                    database_now = await session.scalar(select(func.clock_timestamp()))
                    if not isinstance(database_now, datetime):
                        raise RuntimeError("AGENT_VERSION_TIME_UNAVAILABLE")
                    await close_unrecoverable_agent_version_evaluation(
                        session,
                        evaluation_id=snapshot.evaluation_id,
                        decision_slot_at=snapshot.evaluation_slot_at,
                        reason_code=evaluation_result.reason_code,
                        now=database_now,
                    )
                    return None
                return None
        finally:
            await engine.dispose()
    if action == AgentPaperLoopAction.RUN_EVALUATION_RESEARCH_JOB:
        if not isinstance(snapshot.research_job_id, UUID):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if await _codex_sdk_preflight_blocker_async():
            await asyncio.sleep(5)
            return None
        research_settings = (
            _FileEngineSettings.model_validate(
                {
                    **(
                        bound_environment
                        if pinned_calendar is not None
                        else load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
                    ),
                    "OPENAI_API_KEY": None,
                    "NEXUS_PROVIDER_JOBS_ENABLED": "false",
                }
            )
            if pinned_calendar is not None or env_file is not None
            else local_settings(repo_root, backend="codex_sdk")
        )
        worked = await run_agent_worker_once(
            research_settings,
            job_ids=(snapshot.research_job_id,),
        )
        if not worked and snapshot.research_job_status != "running_expired":
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        return None
    if action == AgentPaperLoopAction.TRIGGER:
        if snapshot.calendar_authority_id is None:
            return AgentPaperLoopAction.STOP_CALENDAR_UNAVAILABLE
        expected_calendar_authority_id = snapshot.calendar_authority_id
        environment = (
            (
                bound_environment
                if pinned_calendar is not None
                else load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
            )
            if env_file is not None
            else (
                bound_environment
                if pinned_calendar is not None
                else load_agent_paper_service_environment(repo_root, os.environ)
            )
        )
        trigger_settings = (
            _FileTriggerSettings if env_file is not None else AgentPaperTriggerSettings
        ).model_validate(environment)
        require_formal_host_configuration_match(settings, trigger_settings)
        await require_formal_host_trigger_profile(trigger_settings, now=snapshot.now)
        if env_file is not None:
            _require_explicit_schwab_config(environment)
        macro_settings = (
            _FileDailySettings if env_file is not None else DailyOnceSettings
        ).model_validate(environment)
        batch_id = _candidate_batch_setting(environment)
        if trigger_settings.agent_paper_formal_model_config_id is None:
            candidates = await _load_loop_candidates(
                trigger_settings,
                account_id=account_id,
                batch_id=batch_id,
                expected_calendar_authority_id=expected_calendar_authority_id,
            )
            for candidate in candidates:
                prior_daily = await prepare_prior_daily_once(
                    trigger_settings,
                    account_id=account_id,
                    repo_root=repo_root,
                    now=snapshot.now,
                    expected_calendar_authority_id=expected_calendar_authority_id,
                    candidate=candidate,
                )
                if prior_daily == "prepared":
                    return None
                if prior_daily == "unavailable":
                    return AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE
                if prior_daily != "ready":
                    return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
            for candidate in candidates:
                macro = await prepare_macro_once(
                    macro_settings,
                    fred_key=SecretStr(environment.get("FRED_API_KEY", "")),
                    account_id=account_id,
                    repo_root=repo_root,
                    now=snapshot.now,
                    expected_calendar_authority_id=expected_calendar_authority_id,
                    candidate=candidate,
                )
                if macro == "prepared":
                    return None
                if macro not in {"ready", "unavailable"}:
                    return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
            for candidate in candidates:
                current_trends = await prepare_current_trends_once(
                    trigger_settings,
                    account_id=account_id,
                    repo_root=repo_root,
                    now=snapshot.now,
                    expected_calendar_authority_id=expected_calendar_authority_id,
                    candidate=candidate,
                )
                if current_trends == "prepared":
                    return None
                if current_trends == "unavailable":
                    return AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE
                if current_trends != "ready":
                    return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        decision_slot_at = floor_decision_slot(snapshot.now)
        try:
            await trigger_once(
                trigger_settings,
                AgentPaperTriggerRequest(
                    action="trigger",
                    account_id=account_id,
                    candidate_batch_id=batch_id,
                    formal_stage_capacity=trigger_settings.agent_paper_formal_model_config_id
                    is not None,
                    formal_model_config_id=trigger_settings.agent_paper_formal_model_config_id,
                    idempotency_key=(
                        f"agent-paper-loop:{floor_decision_slot(snapshot.now).isoformat()}"
                    ),
                ),
                repo_root=repo_root,
                expected_calendar_authority_id=expected_calendar_authority_id,
                **({"process_environment": environment} if env_file is not None else {}),
            )
        except ValueError as error:
            error_code = str(error)
            if error_code not in _SLOT_TERMINAL_TRIGGER_FAILURE_CODES:
                raise
            persisted = await _load_persisted_trigger_failure(
                settings.database_url,
                account_id=account_id,
                decision_slot_at=decision_slot_at,
            )
            if persisted != error_code:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN") from error
        return None
    if action == AgentPaperLoopAction.RUN_RESEARCH_JOB:
        if not isinstance(snapshot.research_job_id, UUID) or not isinstance(
            snapshot.cycle_id, UUID
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if await _codex_sdk_preflight_blocker_async():
            await asyncio.sleep(5)
            return None
        research_settings = (
            _FileEngineSettings.model_validate(
                {
                    **(
                        bound_environment
                        if pinned_calendar is not None
                        else load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
                    ),
                    "OPENAI_API_KEY": None,
                    "NEXUS_PROVIDER_JOBS_ENABLED": "false",
                }
            )
            if pinned_calendar is not None or env_file is not None
            else local_settings(repo_root, backend="codex_sdk")
        )
        if formal_stage_capacity or settings.agent_paper_formal_model_config_id is not None:
            require_formal_host_configuration_match(settings, research_settings)
        try:
            worked = await run_cycle_job_once(
                research_settings,
                job_id=snapshot.research_job_id,
                **({"formal_stage_capacity": True} if formal_stage_capacity else {}),
            )
        except ValueError as error:
            if not formal_stage_capacity or str(error) not in CAPACITY_REJECTION_CODES:
                raise
            await close_capacity_rejected_cycle_once(
                research_settings,
                account_id=account_id,
                cycle_id=snapshot.cycle_id,
                job_id=snapshot.research_job_id,
                code=str(error),
            )
            return None
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
    if action == AgentPaperLoopAction.PUBLISH_EVALUATION_PERFORMANCE:
        if snapshot.performance_due_at is None or snapshot.action_account_id is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        published = await publish_due_performance_once(
            settings,
            account_id=snapshot.action_account_id,
            as_of=snapshot.performance_due_at,
            enqueue_session_summary=False,
        )
        if not published:
            return AgentPaperLoopAction.STOP_PERFORMANCE_UNAVAILABLE
        return None
    if snapshot.cycle_id is None:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    if action == AgentPaperLoopAction.RESUME:
        resumed = await resume_once(
            settings,
            AgentPaperResumeRequest(
                action="resume",
                cycle_id=snapshot.cycle_id,
                idempotency_key=f"agent-paper-loop-resume:{snapshot.cycle_id}",
                formal_stage_capacity=formal_stage_capacity,
            ),
        )
        if (
            resumed is not None
            and resumed.status == "researching"
            and resumed.result_code == "POLICY_UNCONFIRMED"
        ):
            return None
        if resumed is not None and resumed.result_code in {
            "PORTFOLIO_SYNTHESIS_UNAVAILABLE",
            "PORTFOLIO_PROPOSAL_UNAVAILABLE",
            "POLICY_UNCONFIRMED",
            "TARGET_ACTIVATION_PENDING",
        }:
            raise ValueError(resumed.result_code)
        return None
    if action == AgentPaperLoopAction.COLLECT_WINDOW:
        if snapshot.instrument_id is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        environment = (
            (
                bound_environment
                if pinned_calendar is not None
                else load_agent_paper_service_environment(repo_root, {}, env_file=env_file)
            )
            if env_file is not None
            else (
                bound_environment
                if pinned_calendar is not None
                else load_agent_paper_service_environment(repo_root, os.environ)
            )
        )
        if env_file is not None:
            _require_explicit_schwab_config(environment)
        market_settings = (
            _FileMarketSettings if env_file is not None else AgentPaperMarketSettings
        ).model_validate(environment)
        require_formal_host_configuration_match(settings, market_settings)
        await collect_window_once(
            market_settings,
            AgentPaperCollectWindowRequest(
                action="collect-window",
                cycle_id=snapshot.cycle_id,
                instrument_id=snapshot.instrument_id,
            ),
            repo_root=repo_root,
            **({"process_environment": environment} if env_file is not None else {}),
        )
        return None
    if action == AgentPaperLoopAction.ADVANCE_ORDER and snapshot.order_id is not None:
        if snapshot.cycle_id is None or snapshot.market_window_id is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        result = await run_paper_once(
            settings,
            PaperAdvanceRequest(
                action="advance",
                account_id=snapshot.action_account_id or account_id,
                order_id=snapshot.order_id,
                evaluated_at=snapshot.now,
                idempotency_key=(
                    f"agent-paper-loop-advance-v2:{snapshot.order_id}:{snapshot.market_window_id}"
                ),
                agent_cycle_id=snapshot.cycle_id,
                agent_market_window_id=snapshot.market_window_id,
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
    enqueue_session_summary: bool = True,
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
                    enqueue_session_summary=enqueue_session_summary,
                )
        except PaperPerformanceError as error:
            if error.code == "PAPER_PERFORMANCE_PRICE_INCOMPLETE":
                return False
            raise
        return True
    finally:
        await engine.dispose()


async def inspect_agent_version_evaluation_loop(
    settings: EngineSettings,
    *,
    expected_calendar_authority_id: UUID,
) -> AgentPaperLoopSnapshot | None:
    """Return the next exact persisted evaluation action, if one is due."""
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
            evaluations = tuple(
                await session.scalars(
                    select(AgentVersionEvaluationRecord)
                    .outerjoin(
                        AgentVersionEvaluationDecisionRecord,
                        AgentVersionEvaluationDecisionRecord.evaluation_id
                        == AgentVersionEvaluationRecord.id,
                    )
                    .where(AgentVersionEvaluationDecisionRecord.id.is_(None))
                    .order_by(AgentVersionEvaluationRecord.created_at)
                    .limit(2)
                )
            )
            if not evaluations:
                return None
            if len(evaluations) != 1:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            evaluation = evaluations[0]
            if evaluation.calendar_authority_id != expected_calendar_authority_id:
                raise RuntimeError("AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH")
            paired_slots = set(
                await session.scalars(
                    select(AgentVersionCyclePairRecord.decision_slot_at).where(
                        AgentVersionCyclePairRecord.evaluation_id == evaluation.id
                    )
                )
            )
            registered_slots = tuple(evaluation.train_slots) + tuple(evaluation.oos_slots)
            account_ids = (
                evaluation.incumbent_account_id,
                evaluation.challenger_account_id,
            )
            session_closes = await load_oos_session_closes(
                session,
                evaluation=evaluation,
            )
            if not session_closes:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            persisted_snapshot_keys = {
                (snapshot_account_id, snapshot_as_of)
                for snapshot_account_id, snapshot_as_of in (
                    await session.execute(
                        select(
                            PaperPerformanceSnapshotRecord.account_id,
                            PaperPerformanceSnapshotRecord.as_of,
                        ).where(
                            PaperPerformanceSnapshotRecord.account_id.in_(account_ids),
                            PaperPerformanceSnapshotRecord.as_of.in_(session_closes),
                        )
                    )
                ).all()
            }
            due_account_id, next_unpublished_close = _next_evaluation_performance(
                account_ids=account_ids,
                session_closes=session_closes,
                persisted_snapshot_keys=persisted_snapshot_keys,
                now=now,
            )
            slot = next((item for item in registered_slots if item not in paired_slots), None)
            if (
                due_account_id is not None
                and next_unpublished_close is not None
                and (slot is None or slot > next_unpublished_close)
            ):
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=registered_slots[-1],
                    cycle_status="evaluation_performance_due",
                    performance_due_at=next_unpublished_close,
                    action_account_id=due_account_id,
                )
            if slot is None:
                if next_unpublished_close is not None:
                    return AgentPaperLoopSnapshot(
                        now=now,
                        calendar_authority_id=evaluation.calendar_authority_id,
                        evaluation_id=evaluation.id,
                        evaluation_slot_at=next_unpublished_close,
                        cycle_status="evaluation_waiting",
                    )
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=registered_slots[-1],
                    cycle_status="evaluation_complete",
                )
            if now < slot:
                wait_until = (
                    min(slot, next_unpublished_close)
                    if next_unpublished_close is not None
                    else slot
                )
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=wait_until,
                    cycle_status="evaluation_waiting",
                )
            cycles = tuple(
                await session.scalars(
                    select(AgentPaperCycleRecord)
                    .where(
                        AgentPaperCycleRecord.account_id.in_(
                            (
                                evaluation.incumbent_account_id,
                                evaluation.challenger_account_id,
                            )
                        ),
                        AgentPaperCycleRecord.decision_slot_at == slot,
                    )
                    .order_by(AgentPaperCycleRecord.account_id)
                )
            )
            if not cycles:
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=slot,
                    cycle_status="evaluation_pending",
                )
            if len(cycles) != 2:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")

            service = PaperReadService(session, now)
            views = []
            for cycle in cycles:
                views.append((cycle, await service._agent_cycle(cycle)))
            queued: list[tuple[AgentPaperCycleRecord, UUID]] = []
            researching_terminal = False
            for cycle, view in views:
                if view.status != "researching":
                    continue
                events = tuple(
                    await session.scalars(
                        select(AgentPaperCycleEventRecord)
                        .where(AgentPaperCycleEventRecord.cycle_id == cycle.id)
                        .order_by(AgentPaperCycleEventRecord.sequence)
                    )
                )
                run_ids: list[str] = []
                for event in events:
                    raw = event.fact_ids.get("research_run_ids")
                    if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
                        run_ids.extend(raw)
                jobs = (
                    tuple(
                        await session.scalars(
                            select(Job).where(
                                Job.job_type == JobType.AGENT_PAPER_RESEARCH.value,
                                Job.payload["run_id"].as_string().in_(tuple(run_ids)),
                            )
                        )
                    )
                    if run_ids
                    else ()
                )
                job = resolve_cycle_research_job(events=events, jobs=jobs)
                if job is None:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                if job[1] == "queued":
                    queued.append((cycle, job[0]))
                elif job[1] in _TERMINAL_JOB_STATUSES:
                    researching_terminal = True
                elif job[1] == "running":
                    lease = await session.scalar(select(JobLease).where(JobLease.job_id == job[0]))
                    if lease is None:
                        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                    return AgentPaperLoopSnapshot(
                        now=now,
                        calendar_authority_id=evaluation.calendar_authority_id,
                        evaluation_id=evaluation.id,
                        evaluation_slot_at=slot,
                        cycle_id=cycle.id,
                        cycle_status="researching",
                        research_job_status=(
                            "running_expired" if lease.expires_at <= now else "running"
                        ),
                        research_job_id=job[0],
                        research_job_lease_expires_at=lease.expires_at,
                        action_account_id=cycle.account_id,
                    )
                else:
                    return AgentPaperLoopSnapshot(
                        now=now,
                        calendar_authority_id=evaluation.calendar_authority_id,
                        evaluation_id=evaluation.id,
                        evaluation_slot_at=slot,
                        cycle_id=cycle.id,
                        cycle_status="researching",
                        research_job_status=job[1],
                        research_job_id=job[0],
                        action_account_id=cycle.account_id,
                    )
            if queued:
                cycle, job_id = queued[0]
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=slot,
                    cycle_id=cycle.id,
                    cycle_status="researching",
                    research_job_status="queued",
                    research_job_id=job_id,
                    action_account_id=cycle.account_id,
                )
            statuses = {view.status for _, view in views}
            if researching_terminal or statuses & {"decision_recorded", "risk_evaluated"}:
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=slot,
                    cycle_status="completed",
                )
            for cycle, view in views:
                if view.status != "partially_filled":
                    continue
                if len(view.order_ids) != 1:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                order_id = view.order_ids[0]
                order = await session.get(PaperOrderRecord, order_id)
                if order is None or order.account_id != cycle.account_id:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                allows_more_windows = (
                    cycle.execution_price_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
                )
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
                            AgentPaperMarketWindowRecord.cycle_id == cycle.id,
                            AgentPaperMarketWindowRecord.order_id == order_id,
                            AgentPaperMarketWindowRecord.purpose == "paper_fill",
                            PaperExecutionWindowAuthorityRecord.id.is_(None),
                        )
                        .order_by(AgentPaperMarketWindowRecord.window_start_at)
                        .limit(2)
                    )
                )
                if len(unused_windows) > 1:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                if unused_windows:
                    window = unused_windows[0]
                    return AgentPaperLoopSnapshot(
                        now=now,
                        calendar_authority_id=evaluation.calendar_authority_id,
                        evaluation_id=evaluation.id,
                        evaluation_slot_at=slot,
                        cycle_id=cycle.id,
                        cycle_status="partially_filled",
                        market_window_ready=True,
                        market_window_start_at=window.window_start_at,
                        market_window_end_at=window.window_end_at,
                        market_window_id=window.id,
                        has_open_order=True,
                        order_id=order_id,
                        instrument_id=order.instrument_id,
                        action_account_id=cycle.account_id,
                        additional_market_windows_allowed=allows_more_windows,
                    )
                if not allows_more_windows:
                    return AgentPaperLoopSnapshot(
                        now=now,
                        calendar_authority_id=evaluation.calendar_authority_id,
                        evaluation_id=evaluation.id,
                        evaluation_slot_at=slot,
                        cycle_id=cycle.id,
                        cycle_status="partially_filled",
                        has_open_order=True,
                        order_id=order_id,
                        instrument_id=order.instrument_id,
                        action_account_id=cycle.account_id,
                    )
                market_scope = await resolve_agent_paper_market_scope(
                    session,
                    cycle_id=cycle.id,
                    instrument_id=order.instrument_id,
                )
                if market_scope.cycle_id != cycle.id:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=slot,
                    cycle_id=cycle.id,
                    cycle_status="partially_filled",
                    has_open_order=True,
                    order_id=order_id,
                    instrument_id=order.instrument_id,
                    market_window_start_at=market_scope.window_start,
                    market_window_end_at=market_scope.window_end + _MARKET_PUBLICATION_LAG,
                    action_account_id=cycle.account_id,
                    additional_market_windows_allowed=True,
                )
            for cycle, view in views:
                if view.status != "awaiting_market":
                    continue
                if len(view.order_ids) != 1:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                order_id = view.order_ids[0]
                order = await session.get(PaperOrderRecord, order_id)
                if order is None or order.account_id != cycle.account_id:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                if view.execution_market.status != "qualified":
                    market_scope = await resolve_agent_paper_market_scope(
                        session,
                        cycle_id=cycle.id,
                        instrument_id=order.instrument_id,
                    )
                    if market_scope.cycle_id != cycle.id:
                        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                    return AgentPaperLoopSnapshot(
                        now=now,
                        calendar_authority_id=evaluation.calendar_authority_id,
                        evaluation_id=evaluation.id,
                        evaluation_slot_at=slot,
                        cycle_id=cycle.id,
                        cycle_status="awaiting_market",
                        has_open_order=True,
                        order_id=order_id,
                        instrument_id=order.instrument_id,
                        market_window_start_at=market_scope.window_start,
                        market_window_end_at=market_scope.window_end + _MARKET_PUBLICATION_LAG,
                        action_account_id=cycle.account_id,
                    )
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
                            AgentPaperMarketWindowRecord.cycle_id == cycle.id,
                            AgentPaperMarketWindowRecord.order_id == order_id,
                            AgentPaperMarketWindowRecord.purpose == "paper_fill",
                            PaperExecutionWindowAuthorityRecord.id.is_(None),
                        )
                        .order_by(AgentPaperMarketWindowRecord.window_start_at)
                        .limit(2)
                    )
                )
                if len(unused_windows) != 1:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                window = unused_windows[0]
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=slot,
                    cycle_id=cycle.id,
                    cycle_status="awaiting_market",
                    market_window_ready=True,
                    market_window_start_at=window.window_start_at,
                    market_window_end_at=window.window_end_at,
                    market_window_id=window.id,
                    has_open_order=True,
                    order_id=order_id,
                    instrument_id=order.instrument_id,
                    action_account_id=cycle.account_id,
                )
            if statuses <= {
                "completed",
            }:
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=evaluation.calendar_authority_id,
                    evaluation_id=evaluation.id,
                    evaluation_slot_at=slot,
                    cycle_status="completed",
                )
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    finally:
        await engine.dispose()


async def _read_portfolio_security_job(session: AsyncSession, cycle: AgentPaperCycleRecord) -> Job:
    from nexus.jobs.models import job_input_hash
    from nexus.paper.agent_cycle_models import candidate_manifest_hash, parse_candidate_manifest
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
    from nexus.persistence.models.agents import AgentResearchRunRecord

    assert cycle.candidate_batch_id is not None
    batch, _ = await validate_agent_paper_candidate_batch(
        session,
        batch_id=cycle.candidate_batch_id,
        account_id=cycle.account_id,
        as_of=cycle.decision_time,
        universe_snapshot_id=cycle.universe_snapshot_id,
    )
    manifest = parse_candidate_manifest(cycle.candidate_manifest, allow_legacy=False)
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord).where(
                AgentPaperCycleEventRecord.cycle_id == cycle.id,
                AgentPaperCycleEventRecord.result_code == "RESEARCH_JOBS_PREPARED",
            )
        )
    )
    if (
        len(events) != 1
        or not cycle.execution_locked
        or tuple(item.instrument_id for item in manifest) != batch.instrument_ids
        or candidate_manifest_hash(manifest) != cycle.candidate_hash
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    raw_ids = events[0].fact_ids.get("research_run_ids")
    if not isinstance(raw_ids, list) or len(raw_ids) != len(batch.instrument_ids):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    run_ids = tuple(UUID(str(value)) for value in raw_ids)
    runs = tuple(
        await session.scalars(
            select(AgentResearchRunRecord)
            .where(AgentResearchRunRecord.id.in_(run_ids))
            .order_by(AgentResearchRunRecord.instrument_id)
        )
    )
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.job_type == JobType.AGENT_PAPER_RESEARCH.value,
                Job.payload["run_id"].as_string().in_(tuple(str(value) for value in run_ids)),
            )
        )
    )
    if (
        tuple(run.id for run in runs) != run_ids
        or tuple(run.instrument_id for run in runs) != batch.instrument_ids
        or len(jobs) != len(run_ids)
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    by_run = {str(run.id): run for run in runs}
    for job in jobs:
        run = by_run.get(str(job.payload.get("run_id")))
        if run is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        durable_hash = job_input_hash(
            {
                "instrument_id": str(run.instrument_id),
                "data_snapshot_id": str(run.data_snapshot_id),
                "decision_time": run.decision_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "request_hash": run.input_hash,
                "schema_hash": run.schema_hash,
                "model_hash": run.model_hash,
                "prompt_hash": run.prompt_hash,
                "evidence_hash": run.evidence_hash,
                "tool_hash": run.tool_hash,
                "pricing_version": run.pricing_version,
            }
        )
        if (
            run.actor_id != "local-agent-paper-cycle"
            or run.idempotency_key != f"agent-paper:{cycle.id}:{run.instrument_id}"
            or run.decision_time != cycle.decision_time
            or job.payload != {"run_id": str(run.id)}
            or job.queue != QueueName.GENERAL.value
            or job.schema_version != "agent_research_job_v1"
            or job.scope_kind != "instrument"
            or job.scope_id != str(run.instrument_id)
            or job.effective_at != cycle.decision_time
            or job.input_hash != durable_hash
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    resolved = resolve_cycle_research_job(events=events, jobs=jobs)
    if resolved is None:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    return next(job for job in jobs if job.id == resolved[0])


async def _inspect_portfolio_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    now: datetime,
    calendar: ServiceCalendarAuthority,
    performance_due_at: datetime | None,
) -> AgentPaperLoopSnapshot | None:
    """Read internal scheduling authority, not the legacy single-security display DTO."""
    from nexus.agents.portfolio_synthesis_persistence import load_frozen_synthesis_authority
    from nexus.agents.portfolio_synthesis_worker import read_synthesis_job
    from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord

    cycle = await session.scalar(
        select(AgentPaperCycleRecord)
        .where(
            AgentPaperCycleRecord.account_id == account_id,
            AgentPaperCycleRecord.decision_time <= now,
            AgentPaperCycleRecord.created_at <= now,
        )
        .order_by(AgentPaperCycleRecord.decision_slot_at.desc(), AgentPaperCycleRecord.id.desc())
        .limit(1)
    )
    if cycle is None or cycle.candidate_batch_id is None:
        return None
    event = await session.scalar(
        select(AgentPaperCycleEventRecord)
        .where(AgentPaperCycleEventRecord.cycle_id == cycle.id)
        .order_by(AgentPaperCycleEventRecord.sequence.desc())
        .limit(1)
    )
    activation_pending = (
        event is not None
        and event.to_status == "decision_recorded"
        and event.result_code == "TARGET_ACTIVATION_PENDING"
    )
    legacy_policy_block = (
        event is not None
        and event.to_status == "unavailable"
        and event.result_code == "POLICY_UNCONFIRMED"
    )
    proposal_unavailable = (
        event is not None
        and event.to_status == "unavailable"
        and event.result_code == "PORTFOLIO_PROPOSAL_UNAVAILABLE"
    )
    if event is None or (
        event.to_status != "researching"
        and not legacy_policy_block
        and not activation_pending
        and not proposal_unavailable
    ):
        return None
    job = await read_synthesis_job(session, cycle_id=cycle.id)
    if job is None:
        if legacy_policy_block or activation_pending or proposal_unavailable:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        job = await _read_portfolio_security_job(session, cycle)
        lease = None
        status = job.status
        if status == "running":
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            if lease is None:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            if lease.expires_at <= now:
                status = "running_expired"
        return AgentPaperLoopSnapshot(
            now=now,
            calendar_authority_id=calendar.calendar_authority_id,
            market_open_at=calendar.market_open_at,
            decision_window_start_at=calendar.decision_window_start_at,
            decision_window_end_at=calendar.market_close_at,
            next_decision_window_start_at=calendar.next_decision_window_start_at,
            next_market_open_at=calendar.next_market_open_at,
            cycle_id=cycle.id,
            cycle_status=event.to_status,
            cycle_slot_at=cycle.decision_slot_at,
            research_job_id=job.id,
            research_job_status=status,
            research_job_lease_expires_at=None if lease is None else lease.expires_at,
            performance_due_at=performance_due_at,
        )
    run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(str(job.payload["run_id"])))
    assert run is not None
    try:
        frozen = await load_frozen_synthesis_authority(session, run)
    except ValueError as error:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN") from error
    if (
        frozen.account_id != account_id
        or frozen.portfolio_id != cycle.portfolio_id
        or frozen.cycle_id != cycle.id
        or frozen.candidate_batch_id != cycle.candidate_batch_id
        or frozen.decision_time != cycle.decision_time
        or not cycle.execution_locked
        or set(job.payload) != {"run_id"}
        or job.input_hash != run.input_hash
        or job.scope_kind != "portfolio"
        or job.scope_id != str(frozen.portfolio_id)
        or job.effective_at != frozen.decision_time
        or job.queue != QueueName.GENERAL.value
        or job.schema_version not in SYNTHESIS_JOB_VERSIONS
        or job.retry_policy != "never"
        or job.max_attempts != 1
        or job.manual_retry_count != 0
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    policy_block = False
    if run.status == "completed" and job.status == "succeeded":
        from nexus.agents.portfolio_synthesis_contracts import (
            PortfolioSynthesisIntentV1,
            canonical_portfolio_synthesis_hash,
            validate_portfolio_synthesis_intent,
        )
        from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_POLICY_HASH
        from nexus.paper.target_service import load_initial_approval_projection
        from nexus.persistence.models.agents import AgentPortfolioSynthesisOutputRecord
        from nexus.persistence.models.paper import (
            PaperInitialTargetApprovalRecord,
            PaperTargetVersionRecord,
            StrategyDecisionRecord,
        )

        output = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run.id,
            )
        )
        proposal = await session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
            )
        )
        if (
            output is None
            or output.status != "completed"
            or output.created_at > now
            or cycle.sizing_policy_version != "portfolio_allocation_v1"
            or cycle.sizing_policy_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
            or frozen.policy_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        try:
            intent = PortfolioSynthesisIntentV1.model_validate(output.intent)
            validate_portfolio_synthesis_intent(intent, frozen)
        except ValueError as error:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN") from error
        content_hash = canonical_portfolio_synthesis_hash(
            {
                "schema_version": "portfolio_synthesis_terminal_v1",
                "status": "completed",
                "intent": intent.model_dump(mode="json"),
            }
        )
        if (
            output.content_hash != content_hash
            or output.binding_hash
            != canonical_portfolio_synthesis_hash(
                {
                    "schema_version": "portfolio_synthesis_binding_v1",
                    "run_id": run.id,
                    "input_hash": run.input_hash,
                    "content_hash": content_hash,
                }
            )
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if proposal is not None and (
            proposal.account_id != cycle.account_id
            or proposal.portfolio_id != cycle.portfolio_id
            or proposal.strategy_version != "agent_portfolio_proposal_v1"
            or proposal.portfolio_synthesis_output_id != output.id
            or proposal.authority_hash != run.input_hash
            or proposal.strategy_parameters_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
            or proposal.portfolio_projection_hash != frozen.projection_hash
            or proposal.decision_time != cycle.decision_time
            or proposal.created_at > now
        ):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        # Only identity and frozen authority are inspected here. Allocation and
        # current admissibility belong to the coordinator's persisted outcome.
        if frozen.active_target_version_id is None:
            approval = await session.scalar(
                select(PaperInitialTargetApprovalRecord).where(
                    PaperInitialTargetApprovalRecord.account_id == account_id,
                )
            )
            policy_block = approval is None
            if policy_block:
                from nexus.paper.manual_request import manual_request_for_cycle

                try:
                    manual = await manual_request_for_cycle(session, cycle_id=cycle.id)
                except ValueError as error:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN") from error
                # Research completion does not activate a target or authorize orders.
                # Only the fully bound explicit mode bypasses this scheduling block.
                if manual is not None and manual.mode == "research_only":
                    policy_block = False
            if approval is not None:
                try:
                    await load_initial_approval_projection(
                        session,
                        approval=approval,
                        portfolio_id=cycle.portfolio_id,
                        decision_time=cycle.decision_time,
                        now=now,
                    )
                except ValueError as error:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN") from error
        else:
            target = await session.get(PaperTargetVersionRecord, frozen.active_target_version_id)
            target_decision = (
                None
                if target is None
                else await session.get(StrategyDecisionRecord, target.strategy_decision_id)
            )
            if (
                target is None
                or target_decision is None
                or target.account_id != cycle.account_id
                or target.portfolio_id != cycle.portfolio_id
                or target.target_content_hash != frozen.active_target_content_hash
                or target.activated_at > cycle.decision_time
                or target_decision.account_id != cycle.account_id
                or target_decision.portfolio_id != cycle.portfolio_id
                or target_decision.strategy_version != "agent_portfolio_proposal_v1"
            ):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        # A validated partial/all-defer assessment finishes without a target.
        # The coordinator rechecks research and settled jobs before recording it.
        if (
            proposal is None
            and any(source.intent.quality_status == "partial" for source in frozen.security_outputs)
            and all(choice.action == "defer" for choice in intent.choices)
        ):
            from nexus.paper.economic_cycle import read_economic_cycle

            if await read_economic_cycle(session, cycle_id=cycle.id) is None:
                policy_block = False
        if activation_pending:
            if (
                policy_block
                or proposal is None
                or event.fact_ids
                != {
                    "strategy_decision_id": str(proposal.id),
                    "portfolio_synthesis_output_id": str(output.id),
                }
            ):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if legacy_policy_block:
            if proposal is not None or event.fact_ids != {
                "portfolio_synthesis_output_id": str(output.id)
            }:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            policy_block = True
        if proposal_unavailable:
            expected_facts = {"portfolio_synthesis_output_id": str(output.id)}
            rejection = event.fact_ids.get("proposal_rejection_code")
            if rejection is not None:
                if (
                    not isinstance(rejection, str)
                    or rejection not in KNOWN_PORTFOLIO_PROPOSAL_REJECTIONS
                ):
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                expected_facts["proposal_rejection_code"] = rejection
            if proposal is not None or event.fact_ids != expected_facts:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            policy_block = False
        if policy_block and proposal is not None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    elif legacy_policy_block or activation_pending or proposal_unavailable:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    status = job.status
    lease = None
    if status == "running":
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
        if lease is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if lease.expires_at <= now:
            status = "running_expired"
    return AgentPaperLoopSnapshot(
        now=now,
        calendar_authority_id=calendar.calendar_authority_id,
        market_open_at=calendar.market_open_at,
        decision_window_start_at=calendar.decision_window_start_at,
        decision_window_end_at=calendar.market_close_at,
        next_decision_window_start_at=calendar.next_decision_window_start_at,
        next_market_open_at=calendar.next_market_open_at,
        cycle_id=cycle.id,
        cycle_status=event.to_status,
        cycle_result_code=(
            "POLICY_UNCONFIRMED"
            if policy_block
            else "TARGET_ACTIVATION_PENDING"
            if activation_pending
            else "PORTFOLIO_PROPOSAL_UNAVAILABLE"
            if proposal_unavailable
            else None
        ),
        cycle_slot_at=cycle.decision_slot_at,
        research_job_id=job.id,
        research_job_status=status,
        research_job_lease_expires_at=None if lease is None else lease.expires_at,
        performance_due_at=performance_due_at,
    )


async def _read_persisted_trigger_failure(
    session: AsyncSession,
    *,
    account_id: UUID,
    decision_slot_at: datetime,
) -> str | None:
    claim = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{account_id}",
            IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
            IdempotencyRecord.idempotency_key == decision_slot_at.isoformat(),
            IdempotencyRecord.schema_version == "agent_paper_schwab_trigger_v2",
        )
    )
    if claim is None:
        return None
    payload = claim.response_payload
    error_code = payload.get("error_code") if payload.get("outcome") == "failed" else None
    if isinstance(error_code, str) and error_code in _SLOT_TERMINAL_TRIGGER_FAILURE_CODES:
        return error_code
    if payload.get("outcome") == "completed":
        return None
    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")


async def _load_persisted_trigger_failure(
    database_url: str | None,
    *,
    account_id: UUID,
    decision_slot_at: datetime,
) -> str | None:
    if not database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            return await _read_persisted_trigger_failure(
                session,
                account_id=account_id,
                decision_slot_at=decision_slot_at,
            )
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
            current_trigger_slot = floor_decision_slot(now)
            trigger_failure_code = await _read_persisted_trigger_failure(
                session,
                account_id=account_id,
                decision_slot_at=current_trigger_slot,
            )
            trigger_attempt_slot_at: datetime | None = (
                current_trigger_slot if trigger_failure_code is not None else None
            )
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
            synthesis = await _inspect_portfolio_batch(
                session,
                account_id=account_id,
                now=now,
                calendar=calendar,
                performance_due_at=performance_due_at,
            )
            if synthesis is not None:
                return synthesis
            view = await service.latest_agent_cycle()
            if view is None:
                return AgentPaperLoopSnapshot(
                    now=now,
                    calendar_authority_id=calendar.calendar_authority_id,
                    market_open_at=calendar.market_open_at,
                    decision_window_start_at=calendar.decision_window_start_at,
                    decision_window_end_at=calendar.market_close_at,
                    next_decision_window_start_at=calendar.next_decision_window_start_at,
                    next_market_open_at=calendar.next_market_open_at,
                    performance_due_at=performance_due_at,
                    trigger_attempt_slot_at=trigger_attempt_slot_at,
                    trigger_failure_code=trigger_failure_code,
                )

            cycle_record = await session.get(AgentPaperCycleRecord, view.cycle_id)
            if cycle_record is None or cycle_record.account_id != account_id:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")

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
            active_order_id = None
            active_instrument_id = None
            active_order_status = None
            additional_market_windows_allowed = (
                cycle_record.execution_price_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
            )
            if view.status in {"awaiting_market", "partially_filled"}:
                latest_order_events = (
                    select(
                        PaperOrderEventRecord.order_id,
                        func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
                    )
                    .where(
                        PaperOrderEventRecord.order_id.in_(view.order_ids),
                        PaperOrderEventRecord.occurred_at <= now,
                        PaperOrderEventRecord.created_at <= now,
                    )
                    .group_by(PaperOrderEventRecord.order_id)
                    .subquery()
                )
                order_rows = tuple(
                    (
                        order_id,
                        instrument_id,
                        status,
                    )
                    for order_id, instrument_id, status in (
                        await session.execute(
                            select(
                                PaperOrderRecord.id,
                                PaperOrderRecord.instrument_id,
                                PaperOrderEventRecord.to_status,
                            )
                            .join(
                                latest_order_events,
                                latest_order_events.c.order_id == PaperOrderRecord.id,
                            )
                            .join(
                                PaperOrderEventRecord,
                                and_(
                                    PaperOrderEventRecord.order_id
                                    == latest_order_events.c.order_id,
                                    PaperOrderEventRecord.event_seq
                                    == latest_order_events.c.event_seq,
                                ),
                            )
                            .where(PaperOrderRecord.id.in_(view.order_ids))
                            .order_by(PaperOrderRecord.id)
                            .limit(51)
                        )
                    ).all()
                )
                if len(order_rows) != len(view.order_ids):
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                selected_order = select_active_order_identity(
                    order_rows,
                    additional_market_windows_allowed=additional_market_windows_allowed,
                )
                if selected_order is None:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                active_order_id, active_instrument_id = selected_order
                active_order_status = next(
                    status
                    for order_id, _instrument_id, status in order_rows
                    if order_id == active_order_id
                )
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
                            AgentPaperMarketWindowRecord.order_id == active_order_id,
                            AgentPaperMarketWindowRecord.purpose == "paper_fill",
                            PaperExecutionWindowAuthorityRecord.id.is_(None),
                        )
                        .order_by(AgentPaperMarketWindowRecord.window_start_at)
                        .limit(2)
                    )
                )
                if len(unused_windows) > 1:
                    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
                if unused_windows:
                    market_window = unused_windows[0]
                elif (
                    active_order_status == "submitted"
                    or cycle_record.execution_price_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
                ):
                    market_scope = await resolve_agent_paper_market_scope(
                        session,
                        cycle_id=view.cycle_id,
                        instrument_id=active_instrument_id,
                    )
            return snapshot_from_cycle_view(
                now=now,
                view=view,
                research_job=research_job,
                market_scope=market_scope,
                calendar=calendar,
                market_window=market_window,
                performance_due_at=performance_due_at,
                trigger_attempt_slot_at=trigger_attempt_slot_at,
                trigger_failure_code=trigger_failure_code,
                additional_market_windows_allowed=additional_market_windows_allowed,
                active_order_id=active_order_id,
                active_instrument_id=active_instrument_id,
                active_order_status=active_order_status,
            )
    finally:
        await engine.dispose()


def _safe_agent_paper_worker_id(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("agent-paper-loop-"):
        return False
    suffix = value.removeprefix("agent-paper-loop-")
    if ":manual:" in suffix:
        suffix, scope = suffix.split(":manual:", 1)
        if re.fullmatch(r"[0-9a-f]{64}", scope) is None:
            return False
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
    from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL

    process_version = getattr(heartbeat, "process_version", None)
    process_protocol, release_revision = parse_agent_paper_process_version(process_version)
    migration_head = getattr(heartbeat, "migration_head", None)
    if (
        process_protocol not in {AGENT_PAPER_LOOP_PROCESS_VERSION, MANUAL_HOST_PROTOCOL}
        or (process_protocol == MANUAL_HOST_PROTOCOL)
        != (":manual:" in str(getattr(heartbeat, "worker_id", "")))
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
        "release_revision": release_revision,
        "migration_head": migration_head,
    }


async def read_agent_paper_service_heartbeat(database_url: str | None) -> dict[str, object]:
    """Read one persisted liveness fact and classify it against the database clock."""
    from nexus.paper.manual_host_exit import valid_host_exit

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
                        .where(
                            WorkerHeartbeat.queue == QueueName.AGENT_PAPER.value, ~valid_host_exit()
                        )
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
    diagnostics.add_argument("--service-startup-preflight", action="store_true")
    diagnostics.add_argument("--service-recovery-check", action="store_true")
    diagnostics.add_argument("--service-status", action="store_true")
    diagnostics.add_argument("--service-exit-reconciliation", choices=("check", "apply"))
    parser.add_argument("--service-project-name")
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--recovery-generation")
    parser.add_argument("--recovery-attempt")
    args = parser.parse_args(argv)
    if (
        (
            args.service_exit_reconciliation is not None
            and (args.recovery_generation is not None or args.recovery_attempt is not None)
        )
        or (bool(args.recovery_generation) != bool(args.recovery_attempt))
        or (
            args.recovery_generation
            and (
                args.repo_root is None
                or args.env_file is None
                or args.service_preflight
                or args.service_startup_preflight
                or args.service_recovery_check
                or args.service_status
                or args.service_exit_reconciliation
            )
        )
    ):
        print(json.dumps({"error_code": "AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED"}))
        return 1
    if (bool(args.service_project_name) != bool(args.service_exit_reconciliation)) or (
        args.service_exit_reconciliation and args.account_id is not None
    ):
        print(json.dumps({"error_code": "AGENT_PAPER_EXIT_RECONCILIATION_SCOPE_INVALID"}))
        return 1
    root = args.repo_root.resolve() if args.repo_root is not None else Path.cwd()
    env_file = None
    if (
        args.repo_root is not None
        or args.env_file is not None
        or args.service_status
        or args.service_recovery_check
        or args.service_exit_reconciliation
    ):
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
        process_environment: Mapping[str, str] = os.environ
        if env_file is not None:
            process_environment = {
                name: value
                for name in ("NEXUS_RELEASE_REVISION",)
                if (value := os.environ.get(name))
            }
        environment = load_agent_paper_service_environment(
            root,
            process_environment,
            env_file=env_file,
        )
        if args.service_exit_reconciliation:
            from nexus.ops.managed_exit_reconciliation import reconcile_managed_exit

            try:
                reconciliation_settings = _FileEngineSettings.model_validate(environment)
                assert env_file is not None
                result = reconcile_managed_exit(
                    reconciliation_settings,
                    repo_root=root,
                    env_file=env_file,
                    project=args.service_project_name,
                    check_only=args.service_exit_reconciliation == "check",
                )
            except Exception:
                result = {
                    "status": "blocked",
                    "blocking_codes": ["AGENT_PAPER_EXIT_RECONCILIATION_SCOPE_INVALID"],
                    "execution_locked": True,
                    "external_requests": 0,
                    "check_only": args.service_exit_reconciliation == "check",
                    "current_eligible": False,
                }
            print(json.dumps(result, sort_keys=True))
            return (
                0
                if result["status"] in {"eligible_at_check", "reconciled", "already_reconciled"}
                else 1
            )
        if args.service_recovery_check:
            from nexus.ops.agent_paper_recovery import (
                inspect_recovery_readiness,
                require_recovery_process_quiescence,
            )

            failure_code = "AGENT_PAPER_RECOVERY_SCOPE_INVALID"
            try:
                recovery_settings = _FileEngineSettings.model_validate(environment)
                failure_code = "AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED"
                with acquire_agent_paper_lock():
                    require_recovery_process_quiescence()
                    failure_code = "AGENT_PAPER_RECOVERY_UNAVAILABLE"
                    result = asyncio.run(inspect_recovery_readiness(recovery_settings))
            except Exception as error:
                if (
                    isinstance(error, RuntimeError)
                    and str(error) == "AGENT_PAPER_LOOP_ALREADY_RUNNING"
                ):
                    failure_code = "AGENT_PAPER_RECOVERY_HOST_PRESENT"
                result = {
                    "status": "blocked",
                    "blocking_codes": [failure_code],
                    "checked_at": _utc_now().isoformat(),
                    "execution_locked": True,
                    "external_requests": 0,
                }
            print(json.dumps(result, sort_keys=True))
            return 0 if result["status"] == "ready" else 1
        if args.service_startup_preflight:
            try:
                result = agent_paper_service_startup_preflight(environment, repo_root=root)
                if result["status"] == "ready" and not asyncio.run(
                    _agent_paper_startup_persistence_ready(environment.get("DATABASE_URL"))
                ):
                    result = {
                        **result,
                        "status": "blocked",
                        "blocking_codes": ["AGENT_PAPER_DATABASE_UNAVAILABLE"],
                    }
            except Exception:
                result = {
                    "status": "blocked",
                    "execution_locked": True,
                    "external_requests": 0,
                    "blocking_codes": ["AGENT_PAPER_PREFLIGHT_CONFIG_LOAD_FAILED"],
                    "optional_codes": [],
                }
            print(json.dumps(result, sort_keys=True))
            return 0 if result["status"] == "ready" else 1
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
        from nexus.ops.agent_paper_recovery_activation import make_recovery_guard

        recovery = (
            make_recovery_guard(
                settings,
                repo_root=root,
                env_file=env_file,
                generation=args.recovery_generation,
                attempt_id=args.recovery_attempt,
            )
            if args.recovery_generation is not None and env_file is not None
            else None
        )
        recovery_arguments: _RecoveryLoopArguments = {}
        if recovery is not None:
            recovery_arguments["recovery_guard"] = recovery
        if env_file is None:
            loop_result = run_local_agent_paper_loop(
                settings,
                account_id=account_id,
                repo_root=root,
                **recovery_arguments,
            )
        else:
            loop_result = run_local_agent_paper_loop(
                settings,
                account_id=account_id,
                repo_root=root,
                env_file=env_file,
                **recovery_arguments,
            )
        try:
            reason = asyncio.run(loop_result)
        except SQLAlchemyError as error:
            if recovery is not None and recovery.receipt is not None:
                try:
                    recovery.finish_database_error(error)
                except Exception:
                    # Keep the original database failure; an unwritten receipt
                    # must never become proof of a confirmed service exit.
                    error.add_note("AGENT_PAPER_RECOVERY_EXIT_RECEIPT_WRITE_FAILED")
            raise
        if recovery is not None and recovery.receipt is not None:
            recovery.finish(reason.value)
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

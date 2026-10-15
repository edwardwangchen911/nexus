"""Explicit, default-off local entrypoint for one Agent paper-cycle step."""

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, NoReturn
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StrictBool,
    field_validator,
    model_validator,
)
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.jobs.models import JobType, job_input_hash
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.agent_risk_authority import RiskCandidate, collect_source_proofs_batch
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.ops.openai_local import local_settings
from nexus.ops.schwab_market_stream import (
    ProviderMarketStreamError,
    SchwabMarketStreamImportResult,
    capture_and_import_schwab_level_one_batch,
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
    CycleAuthority,
    load_agent_paper_service_calendar_authority,
    load_quasi_realtime_cycle_authority,
    select_agent_paper_service_calendar_authority,
)
from nexus.paper.agent_cycle_models import AgentPaperTriggerReservation
from nexus.paper.agent_market import (
    PaperMarketWindowResult,
    collect_agent_paper_window,
    resolve_agent_paper_market_scope,
)
from nexus.paper.agent_version_models import AgentVersionSlotResult
from nexus.paper.agent_version_runner import advance_agent_version_slot
from nexus.paper.candidate_batch import (
    CandidateBatchResult,
    CandidateInstrumentIdentity,
    create_agent_paper_candidate_batch,
    preflight_agent_paper_candidate_batch,
    resolve_agent_paper_candidate_identities,
    validate_agent_paper_candidate_batch,
)
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import (
    InitialTargetApprovalResult,
    approve_initial_target_projection,
)
from nexus.persistence.models.agent_versions import (
    AgentVersionCyclePairRecord,
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    UniverseMember,
)
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperTargetVersionRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.providers.base import InstrumentId, MarketDataProvider
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.market.alpaca import AlpacaMarketDataProvider
from nexus.providers.market.alpaca_http import (
    AlpacaHttpTransport,
    InstrumentSymbolResolver,
    SqlInstrumentSymbolResolver,
)
from nexus.providers.market.schwab_oauth import SchwabOAuthError, schwab_http_client
from nexus.providers.market.schwab_price_history import SchwabPriceHistoryProvider
from nexus.providers.market.schwab_stream import SchwabStreamError
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.risk.authority import require_schwab_execution_scope_capacity
from nexus.risk.models import PaperPortfolioProjection
from nexus.risk.query_authority_producer import (
    inspect_query_authority_scope,
    persist_query_authorities,
)
from nexus.settings import AgentRuntimeMode, EngineSettings
from nexus.worker import run_agent_worker_once, run_portfolio_synthesis_worker_once

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
        "SCHWAB_MARKET_STREAM_DATABASE_CLOCK_INVALID",
        "SCHWAB_MARKET_STREAM_OBJECT_READBACK_MISMATCH",
        "SCHWAB_MARKET_STREAM_OBJECT_URI_MISMATCH",
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
_SAFE_SCHWAB_CAPTURE_STOP_REASONS = _SAFE_SCHWAB_FAILURE_CODES | {
    "CANCELLED",
    "CLOSE_FAILED",
    "CONNECT_FAILED",
    "CONNECT_TIMEOUT",
    "CONNECT_PROXY_FAILED",
    "CONNECT_TLS_FAILED",
    "CONNECT_DNS_FAILED",
    "CONNECT_HANDSHAKE_FAILED",
    "CONNECT_NETWORK_FAILED",
    "CONNECT_RESET",
    "CONNECT_REFUSED",
    "CONNECT_EOF",
    "CONNECT_RUNTIME_FAILED",
    "CONNECT_TYPE_INVALID",
    "CONNECT_VALUE_INVALID",
    "DISCONNECTED",
    "DURATION_LIMIT",
    "FRAME_LIMIT",
}

_AGENT_VERSION_PREPARATION_ROUTE = "agent-version-evaluation:prepare-slot"
_AGENT_VERSION_PREPARATION_SCHEMA = "agent_version_slot_prepare_v1"
_AGENT_VERSION_PREPARATION_FAILURES = {
    "RISK_INPUT_REQUIRED": "PAIR_RISK_INPUT_REQUIRED",
    "SCHWAB_L1_FRESHNESS_UNPROVEN": "PAIR_SCHWAB_L1_FRESHNESS_UNPROVEN",
    "SCHWAB_L1_INCOMPLETE": "PAIR_SCHWAB_L1_INCOMPLETE",
}


class AgentPaperRunRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["run"]
    account_id: UUID
    decision_time: datetime
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")
    candidate_batch_id: UUID | None = None

    @field_validator("decision_time")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("explicit UTC time is required")
        return value


class AgentPaperTriggerRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["trigger"]
    trigger_mode: Literal["scheduled", "manual", "manual_recovery", "synthesis_recovery"] = (
        "scheduled"
    )
    manual_request_id: UUID | None = None
    manual_execution_id: UUID | None = None
    account_id: UUID
    candidate_batch_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")
    formal_stage_capacity: StrictBool = False
    formal_model_config_id: UUID | None = None

    @model_validator(mode="after")
    def _require_formal_identity(self) -> "AgentPaperTriggerRequest":
        if (
            self.formal_stage_capacity != (self.formal_model_config_id is not None)
            or self.formal_stage_capacity
            and self.trigger_mode not in {"scheduled", "manual"}
        ):
            raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
        if (self.trigger_mode == "manual") != (self.manual_request_id is not None):
            raise ValueError("MANUAL_REQUEST_INVALID")
        if self.trigger_mode == "manual" and not self.formal_stage_capacity:
            raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
        return self

    @model_validator(mode="before")
    @classmethod
    def _require_batch(cls, value: object) -> object:
        if isinstance(value, dict) and value.get("candidate_batch_id") is None:
            raise ValueError("CANDIDATE_BATCH_REQUIRED")
        return value


class AgentPaperResumeRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["resume"]
    cycle_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")
    formal_stage_capacity: StrictBool = False


class AgentPaperCollectWindowRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["collect-window"]
    cycle_id: UUID
    instrument_id: UUID | None = None


class AgentPaperRunResearchRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["run-research"]
    cycle_id: UUID
    formal_stage_capacity: StrictBool = False


class AgentPaperPrepareCandidatesRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["prepare-candidates"]
    account_id: UUID
    universe_snapshot_id: UUID
    instrument_ids: tuple[UUID, ...]
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")


class AgentPaperPrepareCandidatesResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    batch: CandidateBatchResult
    identities: tuple[CandidateInstrumentIdentity, ...]


class AgentPaperInspectProjectionRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["inspect-projection"]
    account_id: UUID
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("explicit UTC time is required")
        return value


class AgentPaperApproveInitialTargetRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["approve-initial-target"]
    account_id: UUID
    projection_as_of: datetime
    expected_projection_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    approved_by: str = Field(min_length=1, max_length=100, pattern=r"\S")
    approved_at: datetime
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")

    @field_validator("projection_as_of", "approved_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("explicit UTC time is required")
        return value


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
    intraday_max_symbols: int = Field(
        default=10,
        ge=1,
        le=30,
        validation_alias="NEXUS_INTRADAY_MAX_SYMBOLS",
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="nexus-engine-agent-paper-once")
    commands = root.add_subparsers(dest="action", required=True)
    run = commands.add_parser("run")
    run.add_argument("--account-id", required=True)
    run.add_argument("--decision-time", required=True)
    run.add_argument("--idempotency-key", required=True)
    run.add_argument("--candidate-batch-id")
    trigger = commands.add_parser("trigger")
    trigger.add_argument("--account-id", required=True)
    trigger.add_argument("--idempotency-key", required=True)
    trigger.add_argument("--candidate-batch-id")
    trigger.add_argument("--formal-stage-capacity", action="store_true")
    trigger.add_argument("--formal-model-config-id")
    manual_recovery = commands.add_parser("trigger-manual-recovery")
    manual_recovery.add_argument("--account-id", required=True)
    manual_recovery.add_argument("--idempotency-key", required=True)
    manual_recovery.add_argument("--candidate-batch-id")
    synthesis_recovery = commands.add_parser("trigger-synthesis-recovery")
    synthesis_recovery.add_argument("--account-id", required=True)
    synthesis_recovery.add_argument("--idempotency-key", required=True)
    synthesis_recovery.add_argument("--candidate-batch-id")
    resume = commands.add_parser("resume")
    resume.add_argument("--cycle-id", required=True)
    resume.add_argument("--idempotency-key", required=True)
    resume.add_argument("--formal-stage-capacity", action="store_true")
    run_research = commands.add_parser("run-research")
    run_research.add_argument("--cycle-id", required=True)
    run_research.add_argument("--formal-stage-capacity", action="store_true")
    prepare_candidates = commands.add_parser("prepare-candidates")
    prepare_candidates.add_argument("--account-id", required=True)
    prepare_candidates.add_argument("--universe-snapshot-id", required=True)
    prepare_candidates.add_argument(
        "--instrument-id", dest="instrument_ids", action="append", required=True
    )
    prepare_candidates.add_argument("--idempotency-key", required=True)
    inspect_projection = commands.add_parser("inspect-projection")
    inspect_projection.add_argument("--account-id", required=True)
    inspect_projection.add_argument("--as-of", required=True)
    approve_initial = commands.add_parser("approve-initial-target")
    approve_initial.add_argument("--account-id", required=True)
    approve_initial.add_argument("--projection-as-of", required=True)
    approve_initial.add_argument(
        "--projection-hash",
        dest="expected_projection_hash",
        required=True,
    )
    approve_initial.add_argument("--approved-by", required=True)
    approve_initial.add_argument("--approved-at", required=True)
    approve_initial.add_argument("--idempotency-key", required=True)
    collect = commands.add_parser("collect-window")
    collect.add_argument("--cycle-id", required=True)
    collect.add_argument("--instrument-id")
    return root


def _require_local_database(settings: EngineSettings) -> str:
    if not settings.database_url:
        raise ValueError("AGENT_PAPER_DATABASE_UNCONFIGURED")
    url = make_url(settings.database_url)
    if (
        url.drivername != "postgresql+asyncpg"
        or url.host not in _LOCAL_HOSTS
        or bool(set(url.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise ValueError("AGENT_PAPER_DATABASE_NOT_LOCAL")
    return settings.database_url


async def inspect_projection_once(
    settings: EngineSettings,
    request: AgentPaperInspectProjectionRequest,
) -> PaperPortfolioProjection:
    database_url = _require_local_database(settings)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            return await load_paper_projection(
                session,
                account_id=request.account_id,
                as_of=request.as_of,
            )
    finally:
        await engine.dispose()


async def approve_initial_target_once(
    settings: EngineSettings,
    request: AgentPaperApproveInitialTargetRequest,
) -> InitialTargetApprovalResult:
    database_url = _require_local_database(settings)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            return await approve_initial_target_projection(
                session,
                account_id=request.account_id,
                projection_as_of=request.projection_as_of,
                expected_projection_hash=request.expected_projection_hash,
                approved_by=request.approved_by,
                approved_at=request.approved_at,
                idempotency_key=request.idempotency_key,
            )
    finally:
        await engine.dispose()


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
                candidate_batch_id=request.candidate_batch_id,
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


async def prepare_candidates_once(
    settings: EngineSettings,
    request: AgentPaperPrepareCandidatesRequest,
) -> AgentPaperPrepareCandidatesResult:
    """Resolve local identities and persist one UUID-only candidate scope."""
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session, session.begin():
            created_at = await session.scalar(select(func.clock_timestamp()))
            if created_at is None:
                raise ValueError("CANDIDATE_BATCH_INVALID")
            replay = await preflight_agent_paper_candidate_batch(
                session,
                account_id=request.account_id,
                universe_snapshot_id=request.universe_snapshot_id,
                instrument_ids=request.instrument_ids,
                idempotency_key=request.idempotency_key,
            )
            identities = (
                replay[1]
                if replay is not None
                else await resolve_agent_paper_candidate_identities(
                    session,
                    universe_snapshot_id=request.universe_snapshot_id,
                    instrument_ids=request.instrument_ids,
                    created_at=created_at,
                )
            )
            print(
                json.dumps(
                    {
                        "candidate_identities": [
                            identity.model_dump(mode="json") for identity in identities
                        ]
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=request.account_id,
                universe_snapshot_id=request.universe_snapshot_id,
                instrument_ids=request.instrument_ids,
                idempotency_key=request.idempotency_key,
                created_at=created_at,
            )
            return AgentPaperPrepareCandidatesResult(batch=batch, identities=identities)
    finally:
        await engine.dispose()


def _validate_trigger_quote(
    quote: LatestQuote | None,
    artifact: DataArtifact | None,
    result: SchwabMarketStreamImportResult,
    *,
    decision_time: datetime,
    symbol: str,
    instrument_id: UUID,
) -> None:
    if (
        quote is None
        or artifact is None
        or result.quote_ids.get(symbol) != quote.id
        or quote.instrument_id != instrument_id
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
    payload: dict[str, object] = {
        "schema": "nexus_schwab_l1_failure_v1",
        "stage": "capture_or_import",
        "provider_error_code": provider_error_code,
    }
    failure_stage = getattr(error, "failure_stage", None)
    if failure_stage in {
        "archive_pack",
        "archive_replay",
        "snapshot_parse",
        "stream_import",
    }:
        payload["stage"] = failure_stage
    if getattr(error, "validation_reason", None) == "CAPTURE_TIME_BOUND":
        payload["validation_reason"] = "CAPTURE_TIME_BOUND"
    capture_summary = getattr(error, "capture_summary", None)
    if isinstance(capture_summary, Mapping):
        safe_capture_summary = {
            "status": capture_summary.get("status"),
            "stop_reason": capture_summary.get("stop_reason"),
            "authenticated": capture_summary.get("authenticated"),
            "subscription_confirmed": capture_summary.get("subscription_confirmed"),
            "observation_count": capture_summary.get("observation_count"),
            "frame_count": capture_summary.get("frame_count"),
        }
        if (
            safe_capture_summary["status"] in {"bounded_complete", "partial", "failed"}
            and safe_capture_summary["stop_reason"] in _SAFE_SCHWAB_CAPTURE_STOP_REASONS
            and type(safe_capture_summary["authenticated"]) is bool
            and type(safe_capture_summary["subscription_confirmed"]) is bool
            and type(safe_capture_summary["observation_count"]) is int
            and safe_capture_summary["observation_count"] >= 0
            and type(safe_capture_summary["frame_count"]) is int
            and safe_capture_summary["frame_count"] >= 0
        ):
            payload["capture_summary"] = safe_capture_summary
    try:
        diagnostic_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        write_private(
            diagnostic_path,
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(),
        )
    except OSError:
        pass
    raise ValueError("SCHWAB_L1_INCOMPLETE") from error


async def prepare_schwab_decision_authority(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    settings: AgentPaperTriggerSettings,
    *,
    account_id: UUID,
    decision_session: date,
    decision_slot_at: datetime,
    idempotency_key: str,
    identities: tuple[CandidateInstrumentIdentity, ...],
    expected_universe_snapshot_id: UUID | None = None,
    initial_time: datetime | None = None,
    repo_root: Path,
    client: httpx.AsyncClient,
    expected_calendar_authority_id: UUID | None = None,
    process_environment: Mapping[str, str] | None = None,
    expand_execution_scope: bool = False,
    manual_market_window: bool = False,
) -> CycleAuthority:
    """Freeze one exact shared provider pass for portfolio or registered evaluation."""
    if initial_time is None:
        initial_time = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(initial_time, datetime):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")

    selected = (
        await select_agent_paper_service_calendar_authority(session, now=initial_time)
        if expected_calendar_authority_id is None
        else None
    )
    calendar_authority_id = (
        selected.calendar_authority_id if selected is not None else expected_calendar_authority_id
    )
    assert calendar_authority_id is not None
    service_calendar = await load_agent_paper_service_calendar_authority(
        session,
        now=initial_time,
        expected_calendar_authority_id=calendar_authority_id,
    )
    if (
        service_calendar.session != decision_session
        or initial_time
        < (
            service_calendar.market_open_at
            if manual_market_window
            else service_calendar.decision_window_start_at
        )
        or initial_time >= service_calendar.market_close_at
    ):
        raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")

    initial_authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=account_id,
        now=initial_time,
        expected_calendar_authority_id=calendar_authority_id,
        expected_universe_snapshot_id=expected_universe_snapshot_id,
    )
    if initial_authority.calendar_authority_id != calendar_authority_id:
        raise ValueError("AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH")
    if (
        expected_universe_snapshot_id is not None
        and initial_authority.universe_snapshot_id != expected_universe_snapshot_id
    ):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    if not identities or len(identities) > 5:
        raise ValueError("CANDIDATE_BATCH_INVALID")
    execution_identities = identities
    if expand_execution_scope:
        from nexus.paper.agent_decision_store import load_portfolio_proposal
        from nexus.paper.projection import load_paper_projection

        projection = await load_paper_projection(session, account_id=account_id, as_of=initial_time)
        current_target = await session.scalar(
            select(PaperTargetVersionRecord)
            .where(
                PaperTargetVersionRecord.account_id == account_id,
                PaperTargetVersionRecord.activated_at <= initial_time,
                PaperTargetVersionRecord.created_at <= initial_time,
            )
            .order_by(PaperTargetVersionRecord.version_seq.desc())
            .limit(1)
        )
        target_ids: set[UUID] = set()
        if current_target is not None:
            current_proposal = await load_portfolio_proposal(
                session,
                account_id=account_id,
                decision_id=current_target.strategy_decision_id,
            )
            if current_proposal is None:
                raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
            target_ids = {
                instrument_id
                for instrument_id, weight in current_proposal.proposal.weights.items()
                if weight > 0
            }
        execution_ids = {
            *(identity.instrument_id for identity in identities),
            *(position.instrument_id for position in projection.positions),
            *(order.instrument_id for order in projection.open_orders),
            *target_ids,
        }
        require_schwab_execution_scope_capacity(
            execution_ids, max_instruments=settings.intraday_max_symbols
        )
        if execution_ids != {identity.instrument_id for identity in identities}:
            rows = tuple(
                (
                    await session.execute(
                        select(InstrumentAlias, Instrument)
                        .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
                        .join(
                            UniverseMember,
                            UniverseMember.instrument_id == InstrumentAlias.instrument_id,
                        )
                        .where(
                            InstrumentAlias.instrument_id.in_(execution_ids),
                            InstrumentAlias.alias_type == "ticker",
                            InstrumentAlias.valid_from <= decision_session,
                            (InstrumentAlias.valid_to.is_(None))
                            | (InstrumentAlias.valid_to >= decision_session),
                            InstrumentAlias.created_at <= initial_time,
                            Instrument.created_at <= initial_time,
                            UniverseMember.universe_snapshot_id
                            == initial_authority.universe_snapshot_id,
                        )
                    )
                ).tuples()
            )
            if (
                len(rows) != len(execution_ids)
                or {alias.instrument_id for alias, _ in rows} != execution_ids
                or len({alias.symbol for alias, _ in rows}) != len(rows)
            ):
                raise ValueError("RISK_INPUT_REQUIRED")
            execution_identities = tuple(
                CandidateInstrumentIdentity(
                    instrument_id=alias.instrument_id,
                    symbol=alias.symbol,
                    asset_type=instrument.asset_type,
                    exchange=instrument.exchange,
                    currency=instrument.currency,
                )
                for alias, instrument in sorted(rows, key=lambda item: item[0].instrument_id.int)
            )
    for identity in execution_identities:
        scope = await inspect_query_authority_scope(
            session,
            decision_session=decision_session,
            symbol=identity.symbol,
            as_of=initial_time,
            universe_snapshot_id=initial_authority.universe_snapshot_id,
            source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        )
        if scope.instrument_id != identity.instrument_id:
            raise ValueError("CANDIDATE_BATCH_INVALID")
    digest = sha256(idempotency_key.encode()).hexdigest()[:16]
    output_parent = repo_root / ".secrets" / "nexus-agent-paper"
    output_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    attempt_directory = output_parent / f"{decision_slot_at.strftime('%Y%m%dT%H%MZ')}-{digest}"
    try:
        proofs = await collect_source_proofs_batch(
            candidates=tuple(
                RiskCandidate(symbol=item.symbol, instrument_id=item.instrument_id)
                for item in execution_identities
            ),
            max_instruments=settings.intraday_max_symbols,
            decision_session=decision_session,
            key_id=settings.alpaca_key,
            secret=settings.alpaca_secret,
            output_dir=attempt_directory,
            cutoff_at=None,
        )
    except ValueError as error:
        raise ValueError("RISK_INPUT_REQUIRED") from error

    try:
        stream = await capture_and_import_schwab_level_one_batch(
            session,
            store,
            repo_root=repo_root,
            client=client,
            bindings={item.symbol: item.instrument_id for item in execution_identities},
            max_instruments=settings.intraday_max_symbols,
            process_environment=process_environment,
        )
    except (
        OSError,
        RuntimeError,
        ProviderMarketStreamError,
        SchwabOAuthError,
        SchwabStreamError,
    ) as error:
        _raise_schwab_l1_incomplete(
            error,
            diagnostic_path=attempt_directory / "schwab-failure.json",
        )
    decision_time = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(decision_time, datetime):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    final_calendar = await load_agent_paper_service_calendar_authority(
        session,
        now=decision_time,
        expected_calendar_authority_id=calendar_authority_id,
    )
    if (
        final_calendar.calendar_authority_id != calendar_authority_id
        or final_calendar.session != decision_session
        or decision_time
        < (
            final_calendar.market_open_at
            if manual_market_window
            else final_calendar.decision_window_start_at
        )
        or decision_time >= final_calendar.market_close_at
    ):
        raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
    artifact = await session.get(DataArtifact, stream.artifact_id)
    if set(stream.quote_ids) != {item.symbol for item in execution_identities}:
        raise ValueError("SCHWAB_L1_INCOMPLETE")
    for identity in execution_identities:
        quote = await session.get(LatestQuote, stream.quote_ids[identity.symbol])
        _validate_trigger_quote(
            quote,
            artifact,
            stream,
            decision_time=decision_time,
            symbol=identity.symbol,
            instrument_id=identity.instrument_id,
        )

    try:
        if set(proofs) != {item.instrument_id for item in execution_identities}:
            raise ValueError("RISK_INPUT_REQUIRED")
        for identity in execution_identities:
            await persist_query_authorities(
                session,
                universe_snapshot_id=initial_authority.universe_snapshot_id,
                decision_session=decision_session,
                symbol=identity.symbol,
                proofs=proofs[identity.instrument_id],
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            )
    except ValueError as error:
        raise ValueError("RISK_INPUT_REQUIRED") from error
    authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=account_id,
        now=decision_time,
        expected_calendar_authority_id=calendar_authority_id,
        expected_universe_snapshot_id=expected_universe_snapshot_id,
    )
    if authority.calendar_authority_id != calendar_authority_id:
        raise ValueError("AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH")
    return authority


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
    process_environment: Mapping[str, str] | None = None,
) -> AgentPaperCycleResult:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    if (
        request.trigger_mode != reservation.trigger_mode
        or request.manual_request_id != reservation.manual_request_id
        or request.manual_execution_id != reservation.manual_execution_id
        or request.formal_stage_capacity != reservation.formal_stage_capacity
        or request.formal_model_config_id != reservation.formal_model_config_id
    ):
        raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    if request.formal_stage_capacity:
        from nexus.agents.codex_formal_stage import require_formal_stage_model_profile

        assert request.formal_model_config_id is not None
        model = await require_formal_stage_model_profile(
            session, model_config_id=request.formal_model_config_id, now=now
        )
        if reservation.formal_model_hash != model.config_hash:
            raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    if request.manual_request_id is not None:
        from nexus.paper.manual_request import require_preparing_manual_request

        await require_preparing_manual_request(
            session,
            request_id=request.manual_request_id,
            execution_id=request.manual_execution_id,
            account_id=request.account_id,
            batch_id=request.candidate_batch_id,
            model_config_id=request.formal_model_config_id,
            now=now,
        )
    batch, identities = await validate_agent_paper_candidate_batch(
        session,
        batch_id=request.candidate_batch_id,
        account_id=request.account_id,
        as_of=now,
    )
    if (
        reservation.candidate_batch_id != batch.batch_id
        or reservation.candidate_batch_hash != batch.batch_hash
    ):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    authority = await prepare_schwab_decision_authority(
        session,
        store,
        settings,
        account_id=request.account_id,
        decision_session=reservation.decision_session,
        decision_slot_at=reservation.decision_slot_at,
        idempotency_key=request.idempotency_key,
        identities=identities,
        initial_time=now,
        expected_universe_snapshot_id=batch.universe_snapshot_id,
        repo_root=repo_root,
        client=client,
        expected_calendar_authority_id=expected_calendar_authority_id,
        process_environment=process_environment,
        expand_execution_scope=True,
        manual_market_window=request.trigger_mode == "manual",
    )
    return await start_schwab_agent_paper_cycle(
        session,
        authority=authority,
        decision_slot_at=reservation.decision_slot_at,
        idempotency_key=request.idempotency_key,
        daily_limit_usd=settings.agent_daily_budget_usd,
        requested_instrument_ids=batch.instrument_ids,
        candidate_batch_id=batch.batch_id,
        execution_scope_max_symbols=settings.intraday_max_symbols,
        formal_stage_capacity=request.formal_stage_capacity,
        formal_model_config_id=request.formal_model_config_id,
        manual_request_id=request.manual_request_id,
        manual_execution_id=request.manual_execution_id,
    )


async def trigger_once(
    settings: AgentPaperTriggerSettings,
    request: AgentPaperTriggerRequest,
    *,
    repo_root: Path,
    expected_calendar_authority_id: UUID | None = None,
    process_environment: Mapping[str, str] | None = None,
) -> AgentPaperCycleResult:
    _validate_trigger_dependencies(settings)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    phase = "trigger_reservation"
    primary_error: Exception | None = None
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
                candidate_batch_id=request.candidate_batch_id,
                require_synthesis_prompt=True,
                formal_stage_capacity=request.formal_stage_capacity,
                formal_model_config_id=request.formal_model_config_id,
                trigger_mode=request.trigger_mode,
                manual_request_id=request.manual_request_id,
                manual_execution_id=request.manual_execution_id,
            )
        if reservation.replay is not None:
            return reservation.replay
        phase = "trigger_provider"
        store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        try:
            async with (
                factory() as session,
                session.begin(),
                schwab_http_client(timeout=30) as client,
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
                    **(
                        {"process_environment": process_environment}
                        if process_environment is not None
                        else {}
                    ),
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
    except Exception as error:
        from nexus.ops.manual_paper_host import annotate_manual_trigger_failure

        primary_error = error
        annotate_manual_trigger_failure(error, phase=phase)
        raise
    finally:
        try:
            await engine.dispose()
        except Exception:
            if primary_error is None:
                raise


def _validate_trigger_dependencies(settings: AgentPaperTriggerSettings) -> None:
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


async def _evaluation_candidate(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
    now: datetime,
) -> CandidateInstrumentIdentity:
    # Registered evaluation scope is separate from ordinary candidate batches.
    rows = list(
        await session.execute(
            select(Instrument, InstrumentAlias)
            .join(InstrumentAlias, InstrumentAlias.instrument_id == Instrument.id)
            .where(
                Instrument.id == evaluation.instrument_id,
                InstrumentAlias.symbol == "AAPL",
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.created_at <= now,
                InstrumentAlias.valid_from <= now.date(),
                (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= now.date())),
            )
        )
    )
    if len(rows) != 1:
        raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
    instrument, alias = rows[0]
    return CandidateInstrumentIdentity(
        instrument_id=instrument.id,
        symbol=alias.symbol,
        asset_type=instrument.asset_type,
        exchange=instrument.exchange,
        currency=instrument.currency,
    )


async def load_agent_version_candidate(
    settings: EngineSettings,
    *,
    evaluation_id: UUID,
    expected_calendar_authority_id: UUID,
    now: datetime,
) -> CandidateInstrumentIdentity:
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        async with async_sessionmaker(engine)() as session:
            evaluation = await session.get(AgentVersionEvaluationRecord, evaluation_id)
            if (
                evaluation is None
                or evaluation.calendar_authority_id != expected_calendar_authority_id
            ):
                raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
            return await _evaluation_candidate(session, evaluation=evaluation, now=now)
    finally:
        await engine.dispose()


async def _reserve_agent_version_slot_preparation(
    session: AsyncSession,
    *,
    evaluation_id: UUID,
    decision_slot_at: datetime,
) -> tuple[UUID, AgentVersionSlotResult | None]:
    request_hash = job_input_hash(
        {
            "schema": _AGENT_VERSION_PREPARATION_SCHEMA,
            "evaluation_id": str(evaluation_id),
            "decision_slot_at": decision_slot_at.isoformat(),
            "source_policy_id": str(PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID),
        }
    )
    claim, inserted = await claim_or_lock(
        session,
        f"local-agent-version-evaluation:{evaluation_id}",
        _AGENT_VERSION_PREPARATION_ROUTE,
        decision_slot_at.isoformat(),
        request_hash,
        _AGENT_VERSION_PREPARATION_SCHEMA,
    )
    if (
        claim.request_hash != request_hash
        or claim.schema_version != _AGENT_VERSION_PREPARATION_SCHEMA
    ):
        raise ValueError("AGENT_VERSION_IDEMPOTENCY_CONFLICT")
    if inserted:
        return claim.id, None
    raw = claim.response_payload.get("result")
    if not isinstance(raw, dict):
        raise ValueError("AGENT_VERSION_RESULT_UNKNOWN")
    try:
        replay = AgentVersionSlotResult.model_validate(raw)
    except ValueError:
        raise ValueError("AGENT_VERSION_RESULT_UNKNOWN") from None
    if replay.evaluation_id != evaluation_id or replay.decision_slot_at != decision_slot_at:
        raise ValueError("AGENT_VERSION_RESULT_UNKNOWN")
    return claim.id, replay.model_copy(update={"replayed": True})


async def _finish_agent_version_slot_preparation(
    session: AsyncSession,
    *,
    claim_id: UUID,
    result: AgentVersionSlotResult,
) -> None:
    claim = await session.scalar(
        select(IdempotencyRecord).where(IdempotencyRecord.id == claim_id).with_for_update()
    )
    if (
        claim is None
        or claim.route != _AGENT_VERSION_PREPARATION_ROUTE
        or claim.schema_version != _AGENT_VERSION_PREPARATION_SCHEMA
        or claim.idempotency_key != result.decision_slot_at.isoformat()
    ):
        raise ValueError("AGENT_VERSION_RESULT_UNKNOWN")
    payload: dict[str, object] = {"result": result.model_dump(mode="json")}
    if claim.response_payload and claim.response_payload != payload:
        raise ValueError("AGENT_VERSION_IDEMPOTENCY_CONFLICT")
    claim.response_payload = payload
    await session.flush()


async def prepare_agent_version_slot_once(
    settings: AgentPaperTriggerSettings,
    *,
    evaluation_id: UUID,
    decision_slot_at: datetime,
    repo_root: Path,
    expected_calendar_authority_id: UUID,
    process_environment: Mapping[str, str] | None = None,
) -> AgentVersionSlotResult:
    """Freeze one shared provider pass, then atomically start both frozen lanes."""
    _validate_trigger_dependencies(settings)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as claim_session, claim_session.begin():
            evaluation = await claim_session.get(AgentVersionEvaluationRecord, evaluation_id)
            if (
                evaluation is None
                or evaluation.calendar_authority_id != expected_calendar_authority_id
                or decision_slot_at
                not in tuple(evaluation.train_slots) + tuple(evaluation.oos_slots)
            ):
                raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
            claim_id, replay = await _reserve_agent_version_slot_preparation(
                claim_session,
                evaluation_id=evaluation_id,
                decision_slot_at=decision_slot_at,
            )
            if replay is not None:
                return replay
            decision = await claim_session.scalar(
                select(AgentVersionEvaluationDecisionRecord.id).where(
                    AgentVersionEvaluationDecisionRecord.evaluation_id == evaluation_id
                )
            )
            pair = await claim_session.scalar(
                select(AgentVersionCyclePairRecord.id).where(
                    AgentVersionCyclePairRecord.evaluation_id == evaluation_id,
                    AgentVersionCyclePairRecord.decision_slot_at == decision_slot_at,
                )
            )
            cycle_count = int(
                await claim_session.scalar(
                    select(func.count())
                    .select_from(AgentPaperCycleRecord)
                    .where(
                        AgentPaperCycleRecord.account_id.in_(
                            (
                                evaluation.incumbent_account_id,
                                evaluation.challenger_account_id,
                            )
                        ),
                        AgentPaperCycleRecord.decision_slot_at == decision_slot_at,
                    )
                )
                or 0
            )
            if decision is not None or pair is not None or cycle_count:
                raise ValueError("AGENT_VERSION_RESULT_UNKNOWN")

        store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        try:
            async with (
                factory() as session,
                session.begin(),
                schwab_http_client(timeout=30) as client,
            ):
                identities = (
                    await _evaluation_candidate(
                        session,
                        evaluation=evaluation,
                        now=decision_slot_at,
                    ),
                )
                authority = await prepare_schwab_decision_authority(
                    session,
                    store,
                    settings,
                    account_id=evaluation.incumbent_account_id,
                    identities=identities,
                    decision_session=decision_slot_at.astimezone(
                        ZoneInfo("America/New_York")
                    ).date(),
                    decision_slot_at=decision_slot_at,
                    idempotency_key=(
                        f"agent-version:{evaluation_id}:{decision_slot_at.isoformat()}"
                    ),
                    repo_root=repo_root,
                    client=client,
                    expected_calendar_authority_id=expected_calendar_authority_id,
                    process_environment=process_environment,
                )
                result = await advance_agent_version_slot(
                    session,
                    evaluation_id=evaluation_id,
                    decision_slot_at=decision_slot_at,
                    daily_limit_usd=settings.agent_daily_budget_usd,
                    now=authority.decision_time,
                )
                await _finish_agent_version_slot_preparation(
                    session,
                    claim_id=claim_id,
                    result=result,
                )
                return result
        except ValueError as error:
            reason_code = _AGENT_VERSION_PREPARATION_FAILURES.get(str(error))
            if reason_code is None:
                raise
            result = AgentVersionSlotResult(
                evaluation_id=evaluation_id,
                decision_slot_at=decision_slot_at,
                phase=("train" if decision_slot_at in evaluation.train_slots else "oos"),
                status="incomparable",
                reason_code=reason_code,
                replayed=False,
            )
            async with factory() as failure_session, failure_session.begin():
                await _finish_agent_version_slot_preparation(
                    failure_session,
                    claim_id=claim_id,
                    result=result,
                )
            return result
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
                session,
                cycle_id=request.cycle_id,
                idempotency_key=request.idempotency_key,
                now=datetime.now(UTC),
                daily_limit_usd=settings.agent_daily_budget_usd,
                formal_stage_capacity=request.formal_stage_capacity,
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
    if not 1 <= len(raw_run_ids) <= 5 or len(set(raw_run_ids)) != len(raw_run_ids):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    try:
        run_ids = tuple(UUID(value) for value in raw_run_ids)
    except ValueError as error:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN") from error
    if len(jobs) != len(run_ids) or not set(output_run_ids) <= set(run_ids):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    by_run: dict[str, object] = {}
    for item in jobs:
        payload = getattr(item, "payload", None)
        item_run = payload.get("run_id") if isinstance(payload, dict) else None
        if item_run not in raw_run_ids or item_run in by_run:
            raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
        by_run[item_run] = item
    pending = []
    for run_id in run_ids:
        item = by_run[str(run_id)]
        if getattr(item, "job_type", None) != JobType.AGENT_PAPER_RESEARCH.value:
            raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
        if run_id in output_run_ids and getattr(item, "status", None) == "succeeded":
            continue
        if run_id in output_run_ids or getattr(item, "status", None) != "queued":
            raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
        pending.append((run_id, item))
    if not pending:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    run_id, job = pending[0]
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
            from nexus.agents.portfolio_synthesis_worker import read_synthesis_job

            synthesis = await read_synthesis_job(session, cycle_id=requested_cycle_id)
            if synthesis is not None:
                if (
                    synthesis.status != "queued"
                    or synthesis.attempt_count != 0
                    or synthesis.manual_retry_count != 0
                    or synthesis.max_attempts != 1
                ):
                    raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
                return synthesis.id
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
    worked = await run_cycle_job_once(
        worker_settings,
        job_id=job_id,
        **({"formal_stage_capacity": True} if request.formal_stage_capacity else {}),
    )
    if not worked:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    return AgentPaperRunResearchResult(cycle_id=request.cycle_id, job_id=job_id)


async def run_cycle_job_once(
    settings: EngineSettings, *, job_id: UUID, formal_stage_capacity: bool = False
) -> bool:
    """Route only this persisted Job type through its dedicated fenced worker."""
    from nexus.persistence.session import session_factory

    if type(formal_stage_capacity) is not bool or not isinstance(job_id, UUID):
        raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
    if formal_stage_capacity:
        _validate_local_database(settings.database_url)
    async with session_factory(settings)() as session:
        job = await session.get(Job, job_id)
        kind = None if job is None else job.job_type
    if kind == JobType.AGENT_PORTFOLIO_SYNTHESIS.value:
        if formal_stage_capacity:
            return await run_portfolio_synthesis_worker_once(
                settings, job_ids=(job_id,), formal_stage_capacity=True
            )
        return await run_portfolio_synthesis_worker_once(settings, job_ids=(job_id,))
    if kind == JobType.AGENT_PAPER_RESEARCH.value:
        if formal_stage_capacity:
            return await run_agent_worker_once(
                settings, job_ids=(job_id,), formal_stage_capacity=True
            )
        return await run_agent_worker_once(settings, job_ids=(job_id,))
    raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")


class _FrozenMarketSymbols:
    """An exact already-resolved identity date; no database access during transport."""

    def __init__(self, symbols: Mapping[UUID, str], as_of: date) -> None:
        self._symbols = dict(symbols)
        self._as_of = as_of

    async def resolve(self, instrument_id: UUID, as_of: date) -> str:
        if as_of != self._as_of or instrument_id not in self._symbols:
            raise ValueError("INSTRUMENT_SYMBOL_AUTHORITY_INVALID")
        return self._symbols[instrument_id]


async def collect_window_once(
    settings: AgentPaperMarketSettings,
    request: AgentPaperCollectWindowRequest,
    *,
    repo_root: Path,
    process_environment: Mapping[str, str] | None = None,
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
            schwab_http_client(timeout=30) as http,
        ):
            scope = await resolve_agent_paper_market_scope(
                resolver_session,
                cycle_id=request.cycle_id,
                instrument_id=request.instrument_id,
            )
            resolver: InstrumentSymbolResolver = SqlInstrumentSymbolResolver(resolver_session)
            if await resolver_session.scalar(
                text("SELECT to_regclass('economic_lane_cycles') IS NOT NULL")
            ):
                from nexus.paper.economic_cycle import read_economic_cycle
                from nexus.persistence.models.economic import (
                    EconomicCommonWindowRecord,
                    EconomicWindowOrderRecord,
                )

                owner = await read_economic_cycle(resolver_session, cycle_id=request.cycle_id)
                if owner is not None:
                    ids = tuple(
                        await resolver_session.scalars(
                            select(EconomicWindowOrderRecord.instrument_id)
                            .join(
                                EconomicCommonWindowRecord,
                                EconomicCommonWindowRecord.id
                                == EconomicWindowOrderRecord.window_id,
                            )
                            .where(
                                EconomicCommonWindowRecord.pair_slot_id == owner[0].pair_slot_id,
                                EconomicCommonWindowRecord.window_start == scope.window_start,
                            )
                            .distinct()
                        )
                    )
                    symbols = {
                        iid: await resolver.resolve(InstrumentId(iid), scope.window_start.date())
                        for iid in ids
                    }
                    resolver = _FrozenMarketSymbols(symbols, scope.window_start.date())
                    await resolver_session.close()
            provider: MarketDataProvider
            if scope.execution_source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID:
                provider = SchwabPriceHistoryProvider(
                    repo_root,
                    http,
                    symbol_resolver=resolver,
                    process_environment=process_environment,
                )
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
            | AgentPaperPrepareCandidatesRequest
            | AgentPaperInspectProjectionRequest
            | AgentPaperApproveInitialTargetRequest
        )
        if action == "run":
            request = AgentPaperRunRequest.model_validate(values)
        elif action in {"trigger", "trigger-manual-recovery", "trigger-synthesis-recovery"}:
            if values.get("candidate_batch_id") is None:
                return _emit_error("CANDIDATE_BATCH_REQUIRED")
            if action == "trigger-manual-recovery":
                values["action"] = "trigger"
                values["trigger_mode"] = "manual_recovery"
            elif action == "trigger-synthesis-recovery":
                values["action"] = "trigger"
                values["trigger_mode"] = "synthesis_recovery"
            request = AgentPaperTriggerRequest.model_validate(values)
        elif action == "resume":
            request = AgentPaperResumeRequest.model_validate(values)
        elif action == "run-research":
            request = AgentPaperRunResearchRequest.model_validate(values)
        elif action == "prepare-candidates":
            request = AgentPaperPrepareCandidatesRequest.model_validate(values)
        elif action == "inspect-projection":
            request = AgentPaperInspectProjectionRequest.model_validate(values)
        elif action == "approve-initial-target":
            request = AgentPaperApproveInitialTargetRequest.model_validate(values)
        else:
            request = AgentPaperCollectWindowRequest.model_validate(values)
        environment = load_local_environment(Path.cwd(), {})
        settings = EngineSettings.model_validate(environment)
        if not settings.agent_paper_cycle_enabled:
            return _emit_error("AGENT_PAPER_CYCLE_DISABLED")
        result: (
            AgentPaperCycleResult
            | PaperMarketWindowResult
            | AgentPaperRunResearchResult
            | AgentPaperPrepareCandidatesResult
            | PaperPortfolioProjection
            | InitialTargetApprovalResult
        )
        if isinstance(request, AgentPaperRunRequest):
            result = asyncio.run(run_once(settings, request))
        elif isinstance(request, AgentPaperTriggerRequest):
            trigger_settings = AgentPaperTriggerSettings.model_validate(environment)
            result = asyncio.run(trigger_once(trigger_settings, request, repo_root=Path.cwd()))
        elif isinstance(request, AgentPaperResumeRequest):
            result = asyncio.run(resume_once(settings, request))
        elif isinstance(request, AgentPaperRunResearchRequest):
            result = asyncio.run(run_research_once(settings, request, repo_root=Path.cwd()))
        elif isinstance(request, AgentPaperPrepareCandidatesRequest):
            result = asyncio.run(prepare_candidates_once(settings, request))
        elif isinstance(request, AgentPaperInspectProjectionRequest):
            result = asyncio.run(inspect_projection_once(settings, request))
        elif isinstance(request, AgentPaperApproveInitialTargetRequest):
            result = asyncio.run(approve_initial_target_once(settings, request))
        else:
            market_settings = AgentPaperMarketSettings.model_validate(environment)
            result = asyncio.run(
                collect_window_once(market_settings, request, repo_root=Path.cwd())
            )
    except Exception as error:
        code = str(error)
        if code not in {
            "ECONOMIC_PAIR_CYCLE_NOT_BOUND",
            "ECONOMIC_PROTOCOL_DATABASE_MISMATCH",
            "ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN",
            "ECONOMIC_WINDOW_REQUIRED",
            "ECONOMIC_WINDOW_INVALID",
            "ECONOMIC_WINDOW_MISSED",
            "ECONOMIC_WINDOW_ORDER_REQUIRED",
            "ECONOMIC_WINDOW_ARCHIVES_REQUIRED",
            "ECONOMIC_WINDOW_ARCHIVE_CONFLICT",
            "ECONOMIC_WINDOW_BINDING_REQUIRED",
            "ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE",
            "ECONOMIC_WINDOW_COLLECTION_TOO_EARLY",
            "ECONOMIC_WINDOW_COLLECTION_UNKNOWN",
            "ECONOMIC_WINDOW_COLLECTION_FAILED",
            "AGENT_PAPER_ACCOUNT_INVALID",
            "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
            "AGENT_PAPER_CALENDAR_UNAVAILABLE",
            "AGENT_PAPER_DATABASE_UNCONFIGURED",
            "AGENT_PAPER_DATABASE_NOT_LOCAL",
            "AGENT_PAPER_MARKET_WINDOW_CLOSED",
            "AGENT_PAPER_MANUAL_RECOVERY_UNAVAILABLE",
            "AGENT_PAPER_SYNTHESIS_RECOVERY_UNAVAILABLE",
            "PORTFOLIO_SYNTHESIS_PROMPT_INVALID",
            "AGENT_PAPER_TIME_IN_FUTURE",
            "AGENT_PAPER_MARKET_CONFIG_INVALID",
            "AGENT_PAPER_OUTSIDE_WINDOW",
            "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
            "AGENT_PAPER_PROVIDER_CAPACITY_INVALID",
            "RISK_INPUT_REQUIRED",
            "SCHWAB_L1_FRESHNESS_UNPROVEN",
            "SCHWAB_L1_INCOMPLETE",
            "RESEARCH_ADMISSION_UNAVAILABLE",
            "PAPER_MARKET_WINDOW_UNAVAILABLE",
            "CANDIDATE_BATCH_INVALID",
            "CANDIDATE_BATCH_REQUIRED",
            "CODEX_FORMAL_CYCLE_PROFILE_INVALID",
            "CODEX_FORMAL_WORKER_ARGUMENT_INVALID",
            "CODEX_FORMAL_WORKER_DATABASE_INVALID",
            "IDEMPOTENCY_CONFLICT",
            "INITIAL_APPROVAL_ACCOUNT_OR_TIME_INVALID",
            "INITIAL_APPROVAL_ALREADY_EXISTS_CONFLICT",
            "INITIAL_APPROVAL_IDEMPOTENCY_CONFLICT",
            "INITIAL_APPROVAL_INVALID",
            "INITIAL_PROJECTION_HASH_INVALID",
            "INITIAL_PROJECTION_OCCUPANCY_AUTHORITY_CONFLICT",
            "INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN",
            "INITIAL_PROJECTION_STALE_OR_HASH_CONFLICT",
        }:
            code = "AGENT_PAPER_RESULT_UNKNOWN"
        return _emit_error(code)
    print(json.dumps(result.model_dump(mode="json"), separators=(",", ":"), sort_keys=True))
    return 0

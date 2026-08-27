"""Strict, redacted contracts for the internal strategy-evolution workbench."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

Hash64 = str
EvolutionViewStatus = Literal["empty", "ready", "partial", "stale"]
EvolutionState = Literal[
    "PROPOSED",
    "REJECTED",
    "ADMITTED",
    "BACKTEST_RUNNING",
    "BACKTEST_FAILED",
    "BACKTEST_PASSED",
    "SHADOW_RUNNING",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTION_ELIGIBLE",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
]
CandidateEvolutionState = Literal[
    "ADMITTED",
    "VALIDATION_FAILED",
    "VALIDATION_PASSED",
    "OOS_NOT_SELECTED",
    "OOS_SELECTED",
    "OOS_RESERVED",
    "BACKTEST_FAILED",
    "BACKTEST_PASSED",
    "SHADOW_RUNNING",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTION_ELIGIBLE",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
]
GateStatus = Literal["PASS", "FAIL", "UNKNOWN", "STALE"]

_CURSOR_DOMAIN = b"nexus-strategy-evolution-cursor-v1\0"
_CURSOR_MAX_AGE = timedelta(minutes=5)
_CURSOR_MAX_LENGTH = 2048


class _StrictSchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class StartExperimentRequest(_StrictSchema):
    """The browser cannot choose a strategy, search axis, model, or data source."""


class VetoExperimentRequest(_StrictSchema):
    reason: Literal["USER_VETO", "EVIDENCE_CONCERN", "OPERATIONAL_RISK"]


class RegistryControlRequest(_StrictSchema):
    action: Literal["pause_auto_promotion", "resume_auto_promotion"]


class RollbackRequest(_StrictSchema):
    reason: Literal[
        "USER_REQUESTED",
        "PERFORMANCE_REGRESSION",
        "DATA_QUALITY_CONCERN",
        "OPERATIONAL_RISK",
    ]


class EvolutionSuccessMeta(_StrictSchema):
    request_id: UUID
    schema_version: Literal["1"] = "1"
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def _utc_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)


class ChampionStateResponse(_StrictSchema):
    strategy_version: str
    generation: int = Field(ge=1)
    registry_version: int = Field(ge=1)
    event_sequence: int = Field(ge=1)
    effective_session: date | None
    auto_promotion_paused: bool
    pending_transition: Literal["PROMOTE", "ROLLBACK"] | None
    pending_effective_session: date | None
    live_execution_enabled: Literal[False] = False


class SearchAxisSummaryResponse(_StrictSchema):
    axis: Literal[
        "min_adv20_usd",
        "min_close",
        "target_weight",
        "ranking_feature",
        "positive_return_filter_enabled",
    ]
    candidate_value_count: int = Field(ge=1, le=8)


class ActiveFamilyResponse(_StrictSchema):
    family_id: UUID
    created_at: datetime
    hypothesis: str | None
    expected_mechanism: str | None
    search_axes: tuple[SearchAxisSummaryResponse, ...]
    candidate_count: int = Field(ge=0, le=24)
    state: EvolutionState
    projection_version: int = Field(ge=1)
    cumulative_oos_trial_count: int = Field(ge=0)
    authority_hash: Hash64 = Field(pattern=r"^[0-9a-f]{64}$")
    protocol_hash: Hash64 = Field(pattern=r"^[0-9a-f]{64}$")
    code_hash: Hash64 = Field(pattern=r"^[0-9a-f]{64}$")
    next_step: str

    @field_validator("created_at")
    @classmethod
    def _created_at_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)


class ExperimentSummaryResponse(_StrictSchema):
    experiment_id: UUID
    family_id: UUID
    candidate_ordinal: int = Field(ge=1, le=24)
    strategy_version: str
    state: CandidateEvolutionState
    created_at: datetime
    candidate_hash: Hash64 = Field(pattern=r"^[0-9a-f]{64}$")
    expression_hash: Hash64 = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("created_at")
    @classmethod
    def _summary_time_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)


class MetricPairResponse(_StrictSchema):
    challenger: str | None
    champion: str | None
    delta: str | None


class ComparisonResponse(_StrictSchema):
    status: Literal["available", "unavailable", "partial"]
    valid_oos_window_count: int | None = Field(default=None, ge=0)
    cumulative_oos_trial_count: int | None = Field(default=None, ge=0)
    annualized_return: MetricPairResponse
    sharpe: MetricPairResponse
    max_drawdown: MetricPairResponse
    double_cost_return: MetricPairResponse
    spy_comparison_complete: bool | None
    equal_weight_comparison_complete: bool | None
    data_mode: Literal["point_in_time", "unavailable"]
    authority_hash: Hash64 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    protocol_hash: Hash64 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    cost_policy_hash: Hash64 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    comparison_hash: Hash64 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class ShadowProgressResponse(_StrictSchema):
    status: Literal["not_started", "running", "failed", "finalized", "stale"]
    completed_sessions: int = Field(ge=0, le=63)
    evaluation_sessions: int = Field(ge=1, le=63)
    grace_sessions: int = Field(ge=0, le=5)
    grace_elapsed_sessions: int = Field(ge=0, le=5)
    checkpoints_completed: tuple[Literal[5, 21, 63], ...]
    full_fill_count: int = Field(ge=0)
    after_cost_excess_return: str | None
    drawdown_difference: str | None
    hard_risk_violation_count: int = Field(ge=0)
    data_complete: bool
    authority_hash: Hash64 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class GateResultResponse(_StrictSchema):
    stage: Literal["historical", "shadow", "handover"]
    gate: str
    status: GateStatus
    observed: tuple[str, ...]
    thresholds: tuple[str, ...]
    reason_code: str


class ExperimentEventResponse(_StrictSchema):
    sequence: int = Field(ge=1)
    event_type: str
    occurred_at: datetime
    event_hash: Hash64 = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("occurred_at")
    @classmethod
    def _event_time_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)


class RegistryEventResponse(_StrictSchema):
    sequence: int = Field(ge=1)
    event_type: Literal["GENESIS", "PROMOTE", "ROLLBACK", "PAUSE", "RESUME", "VETO"]
    target_strategy_version: str
    champion_generation: int = Field(ge=1)
    occurred_at: datetime
    effective_session: date | None
    status: Literal["effective", "pending", "recorded"]
    event_hash: Hash64 = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("occurred_at")
    @classmethod
    def _registry_time_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)


class StrategyEvolutionData(_StrictSchema):
    status: EvolutionViewStatus
    write_enabled: bool
    write_block_reason: str | None
    experiment_start_enabled: bool
    experiment_start_reason: str
    champion: ChampionStateResponse
    active_family: ActiveFamilyResponse | None
    cooldown_until: datetime | None
    experiments: tuple[ExperimentSummaryResponse, ...]
    registry_events: tuple[RegistryEventResponse, ...] = Field(max_length=20)
    next_cursor: str | None
    live_execution_enabled: Literal[False] = False


class StrategyEvolutionEnvelope(_StrictSchema):
    data: StrategyEvolutionData
    meta: EvolutionSuccessMeta


class ExperimentDetailResponse(_StrictSchema):
    experiment: ExperimentSummaryResponse
    family: ActiveFamilyResponse
    comparison: ComparisonResponse
    shadow: ShadowProgressResponse
    gates: tuple[GateResultResponse, ...]
    events: tuple[ExperimentEventResponse, ...]
    integrity_status: Literal["verified", "partial", "stale"]
    write_enabled: bool
    write_block_reason: str | None
    live_execution_enabled: Literal[False] = False


class ExperimentDetailEnvelope(_StrictSchema):
    data: ExperimentDetailResponse
    meta: EvolutionSuccessMeta


class EvolutionActionResponse(_StrictSchema):
    action: Literal[
        "generate_family",
        "veto_experiment",
        "pause_auto_promotion",
        "resume_auto_promotion",
        "rollback_registry",
    ]
    status: Literal[
        "QUEUED",
        "VETOED",
        "PAUSED",
        "EFFECTIVE",
        "PENDING_BOUNDARY",
        "PENDING_ORDERS",
        "FAILED",
    ]
    reason_code: str
    generation: int = Field(ge=1)
    registry_version: int = Field(ge=1)
    resource_id: UUID | None
    pending_effective_session: date | None
    live_execution_enabled: Literal[False] = False


class EvolutionActionEnvelope(_StrictSchema):
    data: EvolutionActionResponse
    meta: EvolutionSuccessMeta


class EvolutionCursorState(_StrictSchema):
    decision_time: datetime
    after_created_at: datetime
    after_experiment_id: UUID


class EvolutionCursorCodec:
    """Short-lived HMAC cursor bound to the experiment ordering contract."""

    def __init__(self, signing_secrets: Sequence[str], request_time: datetime) -> None:
        self._secrets = tuple(secret for secret in signing_secrets if secret)
        if not self._secrets:
            raise ValueError("at least one cursor signing secret is required")
        self._request_time = _require_utc(request_time)

    def encode(self, state: EvolutionCursorState) -> str:
        payload = json.dumps(
            {
                "after_created_at": _time(state.after_created_at),
                "after_experiment_id": str(state.after_experiment_id),
                "decision_time": _time(state.decision_time),
                "scope": "nexus.strategy_evolution.v1",
                "version": 1,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        signature = hmac.new(
            self._secrets[0].encode(), _CURSOR_DOMAIN + payload, hashlib.sha256
        ).digest()
        cursor = f"{_b64encode(payload)}.{_b64encode(signature)}"
        if len(cursor) > _CURSOR_MAX_LENGTH:
            raise RuntimeError("strategy evolution cursor exceeded its fixed bound")
        return cursor

    def decode(self, cursor: str) -> EvolutionCursorState:
        try:
            if not 1 <= len(cursor) <= _CURSOR_MAX_LENGTH:
                raise ValueError
            encoded_payload, encoded_signature = cursor.split(".")
            payload = _b64decode(encoded_payload)
            signature = _b64decode(encoded_signature)
            if len(signature) != hashlib.sha256().digest_size or not any(
                hmac.compare_digest(
                    signature,
                    hmac.new(secret.encode(), _CURSOR_DOMAIN + payload, hashlib.sha256).digest(),
                )
                for secret in self._secrets
            ):
                raise ValueError
            decoded = json.loads(payload)
            if not isinstance(decoded, dict) or set(decoded) != {
                "after_created_at",
                "after_experiment_id",
                "decision_time",
                "scope",
                "version",
            }:
                raise ValueError
            if decoded["scope"] != "nexus.strategy_evolution.v1" or decoded["version"] != 1:
                raise ValueError
            state = EvolutionCursorState(
                decision_time=_parse_time(decoded["decision_time"]),
                after_created_at=_parse_time(decoded["after_created_at"]),
                after_experiment_id=UUID(cast(str, decoded["after_experiment_id"])),
            )
            age = self._request_time - state.decision_time
            if age < timedelta(0) or age > _CURSOR_MAX_AGE:
                raise ValueError
            return state
        except (
            ValueError,
            TypeError,
            KeyError,
            json.JSONDecodeError,
            binascii.Error,
        ):
            raise ValueError("strategy evolution cursor is invalid") from None


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _time(value: datetime) -> str:
    return _require_utc(value).isoformat().replace("+00:00", "Z")


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError
    return _require_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    decoded = base64.b64decode(
        value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
    )
    if _b64encode(decoded) != value:
        raise ValueError("strategy evolution cursor base64 is not canonical")
    return decoded

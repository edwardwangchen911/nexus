"""Frozen contracts and hashes for paired Agent-version evaluation."""

import hashlib
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus.audit.canonical import canonical_json

POLICY_VERSION = "agent_version_evaluation_v1"
TRAIN_SLOT_COUNT = 4
OOS_SLOT_COUNT = 20
MIN_TRAIN_SESSIONS = 2
MIN_OOS_SESSIONS = 10
MAX_OOS_SLOTS_PER_SESSION = 2
MIN_ACTION_DIVERGENCES = 3
MIN_CHALLENGER_FILLS = 1
MIN_EXCESS_RETURN = Decimal("0.005")
MIN_POSITIVE_EXCESS_SESSIONS = 7
MAX_DRAWDOWN_DEGRADATION = Decimal("0.0025")

_NEW_YORK = ZoneInfo("America/New_York")


class AgentVersionEvaluationRequest(BaseModel):
    """One predeclared AAPL train/OOS evaluation request."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    incumbent_version_source_run_id: UUID
    challenger_prompt_version_id: UUID
    challenger_model_config_id: UUID
    initial_capital: Decimal
    train_slots: tuple[datetime, ...]
    oos_slots: tuple[datetime, ...]

    @model_validator(mode="after")
    def _validate_policy(self) -> "AgentVersionEvaluationRequest":
        if not self.initial_capital.is_finite() or self.initial_capital <= 0:
            raise ValueError("AGENT_VERSION_CAPITAL_INVALID")
        if (
            len(self.train_slots) != TRAIN_SLOT_COUNT
            or len(self.oos_slots) != OOS_SLOT_COUNT
        ):
            raise ValueError("AGENT_VERSION_WINDOW_INVALID")
        all_slots = self.train_slots + self.oos_slots
        if any(
            slot.tzinfo is None
            or slot.utcoffset() != UTC.utcoffset(slot)
            or slot.microsecond != 0
            for slot in all_slots
        ):
            raise ValueError("AGENT_VERSION_WINDOW_INVALID")
        if (
            tuple(sorted(self.train_slots)) != self.train_slots
            or tuple(sorted(self.oos_slots)) != self.oos_slots
            or len(set(all_slots)) != len(all_slots)
            or self.train_slots[-1] >= self.oos_slots[0]
            or _session(self.train_slots[-1]) >= _session(self.oos_slots[0])
        ):
            raise ValueError("AGENT_VERSION_WINDOW_INVALID")
        train_sessions = {_session(slot) for slot in self.train_slots}
        oos_counts = Counter(_session(slot) for slot in self.oos_slots)
        if (
            len(train_sessions) < MIN_TRAIN_SESSIONS
            or len(oos_counts) < MIN_OOS_SESSIONS
            or max(oos_counts.values()) > MAX_OOS_SLOTS_PER_SESSION
        ):
            raise ValueError("AGENT_VERSION_WINDOW_INVALID")
        return self


class FrozenAgentVersionSelection(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_run_id: UUID
    prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class AgentVersionEvaluationRegistration(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evaluation_id: UUID
    incumbent_account_id: UUID
    challenger_account_id: UUID
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    replayed: bool
    model_jobs_created: Literal[0] = 0
    execution_locked: Literal[True] = True


class AgentVersionEvaluationProgress(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evaluation_id: UUID
    train_pairs_completed: int = Field(ge=0, le=TRAIN_SLOT_COUNT)
    oos_pairs_completed: int = Field(ge=0, le=OOS_SLOT_COUNT)
    status: Literal["training", "evaluating", "finalized"]


def evaluation_policy_hash() -> str:
    return _sha256(
        {
            "max_drawdown_degradation": _decimal(MAX_DRAWDOWN_DEGRADATION),
            "max_oos_slots_per_session": MAX_OOS_SLOTS_PER_SESSION,
            "min_action_divergences": MIN_ACTION_DIVERGENCES,
            "min_challenger_fills": MIN_CHALLENGER_FILLS,
            "min_excess_return": _decimal(MIN_EXCESS_RETURN),
            "min_oos_sessions": MIN_OOS_SESSIONS,
            "min_positive_excess_sessions": MIN_POSITIVE_EXCESS_SESSIONS,
            "min_train_sessions": MIN_TRAIN_SESSIONS,
            "oos_slot_count": OOS_SLOT_COUNT,
            "policy_version": POLICY_VERSION,
            "train_slot_count": TRAIN_SLOT_COUNT,
        }
    )


def evaluation_request_hash(request: AgentVersionEvaluationRequest) -> str:
    return _sha256(
        {
            "challenger_model_config_id": str(request.challenger_model_config_id),
            "challenger_prompt_version_id": str(request.challenger_prompt_version_id),
            "incumbent_version_source_run_id": str(
                request.incumbent_version_source_run_id
            ),
            "initial_capital": _decimal(request.initial_capital),
            "instrument_id": str(request.instrument_id),
            "oos_slots": [_timestamp(slot) for slot in request.oos_slots],
            "policy_hash": evaluation_policy_hash(),
            "train_slots": [_timestamp(slot) for slot in request.train_slots],
        }
    )


def _session(value: datetime) -> date:
    return value.astimezone(_NEW_YORK).date()


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()

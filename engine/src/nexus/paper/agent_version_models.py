"""Frozen contracts and hashes for paired Agent-version evaluation."""

import hashlib
import re
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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
MIN_SAME_SESSION_SLOT_GAP = timedelta(minutes=15)

_NEW_YORK = ZoneInfo("America/New_York")
type AgentVersionGateKey = Literal[
    "authority",
    "oos_pairs",
    "cost_authority",
    "comparison_authority",
    "hard_risk",
    "ledger_conservation",
    "duplicate_execution",
    "action_divergence",
    "challenger_fills",
    "excess_return",
    "positive_excess_sessions",
    "drawdown_degradation",
]


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
        if len(self.train_slots) != TRAIN_SLOT_COUNT or len(self.oos_slots) != OOS_SLOT_COUNT:
            raise ValueError("AGENT_VERSION_WINDOW_INVALID")
        all_slots = self.train_slots + self.oos_slots
        if any(
            slot.tzinfo is None or slot.utcoffset() != UTC.utcoffset(slot) or slot.microsecond != 0
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
            or has_unsafe_same_session_gap(self.train_slots)
            or has_unsafe_same_session_gap(self.oos_slots)
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


class AgentVersionSlotResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evaluation_id: UUID
    decision_slot_at: datetime
    phase: Literal["train", "oos"]
    incumbent_cycle_id: UUID | None = None
    challenger_cycle_id: UUID | None = None
    incumbent_run_id: UUID | None = None
    challenger_run_id: UUID | None = None
    status: Literal["prepared", "running", "paired", "incomparable"]
    reason_code: str = Field(min_length=1, max_length=100)
    replayed: bool
    execution_locked: Literal[True] = True


class AgentVersionMetricFacts(BaseModel):
    """Decimal-only OOS facts consumed by the frozen v1 gate."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    oos_pair_count: int = Field(ge=0, le=OOS_SLOT_COUNT)
    action_divergence_count: int = Field(ge=0, le=OOS_SLOT_COUNT)
    challenger_fill_count: int = Field(ge=0)
    cumulative_excess_return: Decimal
    positive_excess_session_count: int = Field(ge=0, le=MIN_OOS_SESSIONS)
    drawdown_degradation: Decimal
    hard_risk_violation_count: int = Field(ge=0)
    ledger_conserved: bool
    duplicate_execution_fact_count: int = Field(ge=0)
    cost_authority_complete: bool
    comparison_authority_complete: bool
    authority_complete: bool

    @field_validator("cumulative_excess_return", "drawdown_degradation", mode="before")
    @classmethod
    def _exact_decimal(cls, value: object) -> object:
        if isinstance(value, float):
            raise ValueError("AGENT_VERSION_METRIC_DECIMAL_INVALID")
        return value

    @field_validator("cumulative_excess_return", "drawdown_degradation")
    @classmethod
    def _finite_decimal(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("AGENT_VERSION_METRIC_DECIMAL_INVALID")
        return value


class AgentVersionGate(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    key: AgentVersionGateKey
    status: Literal["PASS", "FAIL", "UNKNOWN"]
    reason_code: str = Field(min_length=1, max_length=100)
    observed: str | None = None
    threshold: str | None = None


class AgentVersionGateReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    status: Literal["PASS", "FAIL", "UNKNOWN"]
    reason_code: str = Field(min_length=1, max_length=100)
    facts: AgentVersionMetricFacts
    gates: tuple[AgentVersionGate, ...]


class AgentVersionEvaluationDecision(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evaluation_id: UUID
    status: Literal["PASS", "FAIL", "UNKNOWN"]
    selected_version_source_run_id: UUID
    reason_code: str = Field(min_length=1, max_length=100)
    metric_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    replayed: bool
    execution_locked: Literal[True] = True


def evaluate_agent_version_metrics(facts: AgentVersionMetricFacts) -> AgentVersionGateReport:
    """Apply the immutable v1 safety and OOS improvement gates in fixed order."""
    checks = (
        _gate("authority", facts.authority_complete, "OOS_AUTHORITY_INCOMPLETE"),
        _gate(
            "oos_pairs",
            facts.oos_pair_count == OOS_SLOT_COUNT,
            "OOS_PAIR_INCOMPLETE",
            observed=str(facts.oos_pair_count),
            threshold=str(OOS_SLOT_COUNT),
        ),
        _gate("cost_authority", facts.cost_authority_complete, "OOS_COST_AUTHORITY_INCOMPLETE"),
        _gate(
            "comparison_authority",
            facts.comparison_authority_complete,
            "OOS_COMPARISON_AUTHORITY_INCOMPLETE",
        ),
        _gate(
            "hard_risk",
            facts.hard_risk_violation_count == 0,
            "HARD_RISK_VIOLATION",
            failure="FAIL",
            observed=str(facts.hard_risk_violation_count),
            threshold="0",
        ),
        _gate(
            "ledger_conservation",
            facts.ledger_conserved,
            "LEDGER_CONSERVATION_FAILED",
            failure="FAIL",
        ),
        _gate(
            "duplicate_execution",
            facts.duplicate_execution_fact_count == 0,
            "DUPLICATE_EXECUTION_FACT",
            failure="FAIL",
            observed=str(facts.duplicate_execution_fact_count),
            threshold="0",
        ),
        _gate(
            "action_divergence",
            facts.action_divergence_count >= MIN_ACTION_DIVERGENCES,
            "OOS_ACTION_DIVERGENCE_INSUFFICIENT",
            observed=str(facts.action_divergence_count),
            threshold=str(MIN_ACTION_DIVERGENCES),
        ),
        _gate(
            "challenger_fills",
            facts.challenger_fill_count >= MIN_CHALLENGER_FILLS,
            "OOS_CHALLENGER_FILL_INSUFFICIENT",
            observed=str(facts.challenger_fill_count),
            threshold=str(MIN_CHALLENGER_FILLS),
        ),
        _gate(
            "excess_return",
            facts.cumulative_excess_return >= MIN_EXCESS_RETURN,
            "OOS_EXCESS_RETURN_INSUFFICIENT",
            observed=_decimal(facts.cumulative_excess_return),
            threshold=_decimal(MIN_EXCESS_RETURN),
        ),
        _gate(
            "positive_excess_sessions",
            facts.positive_excess_session_count >= MIN_POSITIVE_EXCESS_SESSIONS,
            "OOS_POSITIVE_SESSIONS_INSUFFICIENT",
            observed=str(facts.positive_excess_session_count),
            threshold=str(MIN_POSITIVE_EXCESS_SESSIONS),
        ),
        _gate(
            "drawdown_degradation",
            facts.drawdown_degradation <= MAX_DRAWDOWN_DEGRADATION,
            "OOS_DRAWDOWN_DEGRADATION_EXCESSIVE",
            observed=_decimal(facts.drawdown_degradation),
            threshold=_decimal(MAX_DRAWDOWN_DEGRADATION),
        ),
    )
    first = next((gate for gate in checks if gate.status != "PASS"), None)
    return AgentVersionGateReport(
        status="PASS" if first is None else first.status,
        reason_code="OOS_IMPROVEMENT_PROVEN" if first is None else first.reason_code,
        facts=facts,
        gates=checks,
    )


def _gate(
    key: AgentVersionGateKey,
    passed: bool,
    reason_code: str,
    *,
    failure: Literal["FAIL", "UNKNOWN"] = "UNKNOWN",
    observed: str | None = None,
    threshold: str | None = None,
) -> AgentVersionGate:
    return AgentVersionGate(
        key=key,
        status="PASS" if passed else failure,
        reason_code="GATE_PASSED" if passed else reason_code,
        observed=observed,
        threshold=threshold,
    )


def evaluation_policy_hash() -> str:
    return _sha256(evaluation_policy_payload())


def evaluation_policy_payload() -> dict[str, object]:
    """Return the canonical v1 policy payload used by new evaluations."""
    return {
        "max_drawdown_degradation": _decimal(MAX_DRAWDOWN_DEGRADATION),
        "max_oos_slots_per_session": MAX_OOS_SLOTS_PER_SESSION,
        "min_action_divergences": MIN_ACTION_DIVERGENCES,
        "min_challenger_fills": MIN_CHALLENGER_FILLS,
        "min_excess_return": _decimal(MIN_EXCESS_RETURN),
        "min_oos_sessions": MIN_OOS_SESSIONS,
        "min_positive_excess_sessions": MIN_POSITIVE_EXCESS_SESSIONS,
        "min_same_session_slot_gap_seconds": int(
            MIN_SAME_SESSION_SLOT_GAP.total_seconds()
        ),
        "min_train_sessions": MIN_TRAIN_SESSIONS,
        "oos_slot_count": OOS_SLOT_COUNT,
        "policy_version": POLICY_VERSION,
        "train_slot_count": TRAIN_SLOT_COUNT,
    }


def has_unsafe_same_session_gap(slots: Sequence[datetime]) -> bool:
    by_session: dict[date, list[datetime]] = {}
    for slot in slots:
        by_session.setdefault(_session(slot), []).append(slot)
    return any(
        later - earlier < MIN_SAME_SESSION_SLOT_GAP
        for session_slots in by_session.values()
        for earlier, later in zip(
            sorted(session_slots),
            sorted(session_slots)[1:],
            strict=False,
        )
    )


def evaluation_request_hash(request: AgentVersionEvaluationRequest) -> str:
    return _sha256(
        {
            "challenger_model_config_id": str(request.challenger_model_config_id),
            "challenger_prompt_version_id": str(request.challenger_prompt_version_id),
            "incumbent_version_source_run_id": str(request.incumbent_version_source_run_id),
            "initial_capital": _decimal(request.initial_capital),
            "instrument_id": str(request.instrument_id),
            "oos_slots": [_timestamp(slot) for slot in request.oos_slots],
            "policy_hash": evaluation_policy_hash(),
            "train_slots": [_timestamp(slot) for slot in request.train_slots],
        }
    )


def agent_version_market_evidence_hash(candidate_hash: str) -> str:
    """Hash the shared account-neutral candidate authority for one paired slot."""
    if re.fullmatch(r"[0-9a-f]{64}", candidate_hash) is None:
        raise ValueError("AGENT_VERSION_MARKET_EVIDENCE_INVALID")
    return hashlib.sha256(f"agent_version_market_evidence_v1|{candidate_hash}".encode()).hexdigest()


def _session(value: datetime) -> date:
    return value.astimezone(_NEW_YORK).date()


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()

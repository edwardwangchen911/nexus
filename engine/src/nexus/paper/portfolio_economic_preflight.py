"""Compile approved policy without pretending to register or activate an evaluation."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars as xcals  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.agents.codex_sdk import MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS
from nexus.agents.codex_staged import STAGES
from nexus.agents.codex_versions import (
    DEADLINE_BUDGET_CONFIG_VERSION,
    PORTFOLIO_DECISION_SDK_TIMEOUT_SECONDS,
    RESEARCH_STAGE_JOB_TIMEOUT_SECONDS,
    aggregate_input_limit,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class PlanInstrument(StrictModel):
    instrument_id: UUID
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.\-]{0,9}$")


class EconomicPlanRequest(StrictModel):
    """Explicit inputs are planning claims, never business identity or pricing authority."""

    evaluation_key: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    instruments: tuple[PlanInstrument, PlanInstrument]
    start_session: date
    research_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    synthesis_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    global_daily_limit_usd: Decimal = Field(ge=0, allow_inf_nan=False)

    @field_validator("research_cost_usd", "synthesis_cost_usd", "global_daily_limit_usd")
    @classmethod
    def normalize_money(cls, value: Decimal) -> Decimal:
        return value.normalize()

    @model_validator(mode="after")
    def distinct_candidates(self) -> EconomicPlanRequest:
        if (
            len({item.instrument_id for item in self.instruments}) != 2
            or len({item.symbol for item in self.instruments}) != 2
        ):
            raise ValueError("ECONOMIC_PLAN_DUPLICATE_IDENTITY")
        return self


class PlannedSlot(StrictModel):
    ordinal: int
    phase: Literal["training", "oos"]
    session: date
    decision_at: datetime
    market_close_at: datetime


class ApprovedPolicy(StrictModel):
    version: Literal["portfolio_economic_observation_v1"] = "portfolio_economic_observation_v1"
    cash_usd_per_lane: Literal["10000"] = "10000"
    training_days: Literal[4] = 4
    oos_days: Literal[20] = 20
    min_net_excess_percentage_points: Literal["0.20"] = "0.20"
    min_excess_over_hold_percentage_points: Literal["0"] = "0"
    max_drawdown_deterioration_percentage_points: Literal["0"] = "0"
    max_extra_cost_percentage_points: Literal["0.10"] = "0.10"
    min_oos_action_difference_days: Literal[5] = 5
    min_challenger_fill_days: Literal[2] = 2
    positive_excess_in_both_ten_day_halves: Literal[True] = True
    fixed_baseline_required: Literal[True] = True
    formal_tasks_first: Literal[True] = True
    retries: Literal[0] = 0
    auto_promote: Literal[False] = False
    execution_window_rule: Literal["first_common_lawful_five_minute_window"] = (
        "first_common_lawful_five_minute_window"
    )


class EconomicPlan(StrictModel):
    schema_version: Literal["portfolio_economic_preflight_v1"] = "portfolio_economic_preflight_v1"
    status: Literal["compiled_not_registered"] = "compiled_not_registered"
    economic_status: Literal["not_evaluated"] = "not_evaluated"
    execution_locked: Literal[True] = True
    request: EconomicPlanRequest
    policy: ApprovedPolicy
    calendar_package_version: str
    calendar_schedule_hash: str
    research_config_version: str
    slots: tuple[PlannedSlot, ...]
    daily_model_jobs: int
    total_model_jobs: int
    daily_sdk_calls: int
    total_sdk_calls: int
    daily_model_input_cap: int
    daily_wire_input_cap: int
    daily_output_cap: int
    daily_cost_ceiling_usd: Decimal
    serial_invocation_ceiling_seconds: int
    activation_blockers: tuple[str, ...]
    content_hash: str


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def compile_plan(request: EconomicPlanRequest, *, now: datetime) -> EconomicPlan:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("ECONOMIC_PLAN_CLOCK_INVALID")
    calendar = xcals.get_calendar("XNYS")
    start = request.start_session
    if not calendar.is_session(start):
        raise ValueError("ECONOMIC_PLAN_SESSION_INVALID")
    labels = tuple(calendar.sessions_in_range(start, start + timedelta(days=70)))[:24]
    if len(labels) != 24:
        raise ValueError("ECONOMIC_PLAN_CALENDAR_INCOMPLETE")
    slots = tuple(
        PlannedSlot(
            ordinal=index + 1,
            phase="training" if index < 4 else "oos",
            session=label.date(),
            decision_at=datetime.combine(
                label.date(), time(10), ZoneInfo("America/New_York")
            ).astimezone(UTC),
            market_close_at=calendar.session_close(label).to_pydatetime(),
        )
        for index, label in enumerate(labels)
    )
    if slots[0].decision_at <= now:
        raise ValueError("ECONOMIC_PLAN_SESSION_INVALID")
    blockers = [
        "MODEL_AND_CANDIDATE_AUTHORITIES_UNBOUND",
        "CALENDAR_AND_PROTOCOL_NOT_REGISTERED",
        "DUAL_LANE_ENTRYPOINT_UNIMPLEMENTED",
        "ATOMIC_GLOBAL_BUDGET_RESERVATION_UNIMPLEMENTED",
        "COMMON_EXECUTION_WINDOW_UNVERIFIED",
        "FIXED_STRATEGY_BASELINE_UNIMPLEMENTED",
        "CLOSE_PRICE_AND_COST_AUTHORITIES_UNBOUND",
        "REAL_EVALUATION_ACTIVATION_NOT_AUTHORIZED",
    ]
    cost = 4 * request.research_cost_usd + 2 * request.synthesis_cost_usd
    daily_calls = 4 * len(STAGES) + 2
    if cost > request.global_daily_limit_usd:
        blockers.append("DAILY_LIMIT_BELOW_EVALUATION_CEILING")
    # Existing portfolio_synthesis_worker.CodexSynthesisTransport invokes one
    # 45-second invoke_codex_portfolio call using the same 96k model reservation.
    # This is not the different, two-turn portfolio_decision transport.
    payload = {
        "request": request.model_dump(mode="json"),
        "policy": ApprovedPolicy().model_dump(mode="json"),
        "calendar_package_version": version("exchange-calendars"),
        "calendar_schedule_hash": _hash([slot.model_dump(mode="json") for slot in slots]),
        "research_config_version": DEADLINE_BUDGET_CONFIG_VERSION,
        "slots": [slot.model_dump(mode="json") for slot in slots],
        "daily_model_jobs": 6,
        "total_model_jobs": 144,
        "daily_sdk_calls": daily_calls,
        "total_sdk_calls": daily_calls * len(slots),
        "daily_model_input_cap": 6 * aggregate_input_limit(DEADLINE_BUDGET_CONFIG_VERSION),
        "daily_wire_input_cap": (
            4 * aggregate_input_limit(DEADLINE_BUDGET_CONFIG_VERSION) + 2 * MAX_INPUT_TOKENS
        ),
        "daily_output_cap": 6 * MAX_OUTPUT_TOKENS,
        "daily_cost_ceiling_usd": str(cost.normalize()),
        # Invocation caps only: excludes queueing, admission, archive publication,
        # DB and cleanup. It cannot prove a common execution window is feasible.
        "serial_invocation_ceiling_seconds": (
            4 * RESEARCH_STAGE_JOB_TIMEOUT_SECONDS + 2 * PORTFOLIO_DECISION_SDK_TIMEOUT_SECONDS
        ),
        "activation_blockers": blockers,
    }
    return EconomicPlan.model_validate(payload | {"content_hash": _hash(payload)})


def save_plan(path: Path, plan: EconomicPlan) -> Literal["created", "existing"]:
    """Exclusive receipt write; no overwrite, promotion, database or external calls."""
    parent = path.parent
    parent_info = parent.lstat()
    if (
        not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.getuid()
        or parent_info.st_mode & 0o777 != 0o700
    ):
        raise ValueError("ECONOMIC_PLAN_DIRECTORY_INVALID")
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        try:
            # O_NOFOLLOW also rejects a replacement symlink between stat and open.
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "r") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                    raise ValueError("invalid receipt ownership/type")
                if info.st_mode & 0o777 != 0o600:
                    raise ValueError("invalid receipt permissions")
                existing = EconomicPlan.model_validate_json(stream.read(1024 * 1024))
        except (OSError, ValueError) as error:
            raise ValueError("ECONOMIC_PLAN_EXISTING_INVALID") from error
        if existing != plan:
            raise ValueError("ECONOMIC_PLAN_IDEMPOTENCY_CONFLICT")
        return "existing"
    with os.fdopen(fd, "w") as stream:
        stream.write(plan.model_dump_json(indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return "created"

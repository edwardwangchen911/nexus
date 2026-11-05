"""Finite diagnostic facts, never evidence of zero usage or permission to retry."""

from typing import Literal, get_args
from uuid import UUID

from nexus.agents.budget import BudgetExhausted
from nexus.paper.research_failure import PRECLAIM_CODES

AdmissionFailurePhase = Literal[
    "pool_capacity",
    "profile_validation",
    "prompt_selection",
    "snapshot_preparation",
    "research_admission",
    "prompt_binding",
    "stage_registration",
    "reservation_commit",
]

ADMISSION_FAILURE_CODES = PRECLAIM_CODES | frozenset(
    {
        "UNKNOWN",
        "SINGLE_SOL_NATURAL_BUDGET_PROTECTED",
        "SINGLE_SOL_BUDGET_SCOPE_INVALID",
        "SINGLE_SOL_BUDGET_STALE_SNAPSHOT",
        "SINGLE_SOL_ECONOMIC_SCOPE_CONFLICT",
        "SINGLE_SOL_BUDGET_WINDOW_CLOSED",
        "CODEX_CAPACITY_PRECLAIM_TIMEOUT",
        "CODEX_STAGE_BUDGET_CLOCK_INVALID",
        "CODEX_FORMAL_CYCLE_PROFILE_INVALID",
        "CODEX_FORMAL_ADMISSION_ARGUMENT_INVALID",
        "AGENT_PAPER_RESEARCH_CONTEXT_INVALID",
        "PLATFORM_NATIVE_PREPARATION_REQUIRED",
        "COMPARISON_ROUND_BUDGET_EXHAUSTED",
        "DAILY_RESEARCH_BUDGET_EXHAUSTED",
        "RESEARCH_PRICING_STALE",
    }
)


def admission_failure_facts(
    error: ValueError, *, phase: AdmissionFailurePhase, instrument_id: UUID | None
) -> dict[str, object]:
    raw = error.args[0] if len(error.args) == 1 else None
    code = raw if type(raw) is str and raw in ADMISSION_FAILURE_CODES else "UNKNOWN"
    if isinstance(error, BudgetExhausted) and type(raw) is str:
        code = {
            "daily research budget exhausted": "DAILY_RESEARCH_BUDGET_EXHAUSTED",
            "stale pricing cannot reserve research budget": "RESEARCH_PRICING_STALE",
        }.get(raw, code)
    return {
        "admission_failure_version": 1,
        "failure_phase": phase,
        "failure_code": code,
        "failure_instrument_id": None if instrument_id is None else str(instrument_id),
    }


def read_admission_failure(facts: dict[str, object]) -> dict[str, object] | None:
    """Missing legacy facts remain missing; arbitrary stored text never reaches the API."""
    phase, code = facts.get("failure_phase"), facts.get("failure_code")
    if (
        type(facts.get("admission_failure_version")) is not int
        or facts["admission_failure_version"] != 1
        or type(phase) is not str
        or phase not in get_args(AdmissionFailurePhase)
        or type(code) is not str
        or code not in ADMISSION_FAILURE_CODES
    ):
        return None
    instrument = facts.get("failure_instrument_id")
    if instrument is not None:
        if type(instrument) is not str:
            return None
        try:
            instrument = UUID(instrument)
        except ValueError:
            return None
    return {"phase": phase, "code": code, "instrument_id": instrument}

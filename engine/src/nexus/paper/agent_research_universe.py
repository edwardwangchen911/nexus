"""Versioned, deterministic research rotation for Agent paper cycles."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

type ResearchSymbol = Literal["AAPL", "MSFT", "JPM"]


@dataclass(frozen=True)
class ResearchUniverse:
    version: str
    symbols: tuple[ResearchSymbol, ...]


@dataclass(frozen=True)
class ResearchTarget:
    version: str
    ordinal: int
    symbol: ResearchSymbol
    attempt: int


@dataclass(frozen=True)
class ResearchCycleOutcome:
    version: str
    ordinal: int
    symbol: str
    attempt: int
    status: str
    result_code: str


AGENT_PAPER_RESEARCH_UNIVERSE_V1 = ResearchUniverse(
    version="agent_paper_research_v1",
    symbols=("AAPL", "MSFT", "JPM"),
)

_CANDIDATE_FAILURE_CODES = frozenset(
    {
        "CANDIDATE_EVIDENCE_UNAVAILABLE",
        "RESEARCH_OUTPUT_NOT_ACTIONABLE",
        "AGENT_DECISION_CONFLICT",
    }
)


def research_failure_result_code(target: ResearchTarget, result_code: str) -> str:
    if target.attempt == 3 and result_code in _CANDIDATE_FAILURE_CODES:
        return "RESEARCH_BLOCKED"
    return result_code


def research_target_facts(target: ResearchTarget) -> dict[str, object]:
    return {
        "research_universe_version": target.version,
        "research_universe_ordinal": target.ordinal,
        "research_symbol": target.symbol,
        "research_attempt": target.attempt,
    }


def research_target_from_events(events: Sequence[object]) -> ResearchTarget | None:
    if not events:
        raise ValueError("AGENT_PAPER_ROTATION_INVALID")
    first = events[0]
    facts = getattr(first, "fact_ids", None)
    if not isinstance(facts, Mapping):
        raise ValueError("AGENT_PAPER_ROTATION_INVALID")
    keys = {
        "research_universe_version",
        "research_universe_ordinal",
        "research_symbol",
        "research_attempt",
    }
    present = keys & facts.keys()
    if not present:
        return None
    version = facts.get("research_universe_version")
    ordinal = facts.get("research_universe_ordinal")
    symbol = facts.get("research_symbol")
    attempt = facts.get("research_attempt")
    universe = AGENT_PAPER_RESEARCH_UNIVERSE_V1
    if (
        getattr(first, "sequence", None) != 1
        or getattr(first, "to_status", None) != "prepared"
        or getattr(first, "result_code", None) != "CYCLE_PREPARED"
        or version != universe.version
        or type(ordinal) is not int
        or not 0 <= ordinal < len(universe.symbols)
        or symbol != universe.symbols[ordinal]
        or type(attempt) is not int
        or not 1 <= attempt <= 3
    ):
        raise ValueError("AGENT_PAPER_ROTATION_INVALID")
    return ResearchTarget(
        version=version,
        ordinal=ordinal,
        symbol=cast(ResearchSymbol, symbol),
        attempt=attempt,
    )


def research_cycle_outcome_from_events(
    events: Sequence[object],
) -> ResearchCycleOutcome | None:
    target = research_target_from_events(events)
    if target is None:
        return None
    terminal = events[-1]
    status = getattr(terminal, "to_status", None)
    result_code = getattr(terminal, "result_code", None)
    if status not in {"completed", "unavailable", "failed"}:
        return None
    if not isinstance(result_code, str) or not result_code:
        raise ValueError("AGENT_PAPER_ROTATION_INVALID")
    return ResearchCycleOutcome(
        version=target.version,
        ordinal=target.ordinal,
        symbol=target.symbol,
        attempt=target.attempt,
        status=status,
        result_code=result_code,
    )


def choose_research_target(
    outcomes: tuple[ResearchCycleOutcome, ...],
    *,
    universe: ResearchUniverse = AGENT_PAPER_RESEARCH_UNIVERSE_V1,
) -> ResearchTarget:
    """Reduce ordered terminal outcomes into the next auditable target."""
    ordinal = 0
    attempt = 1
    for outcome in outcomes:
        if outcome.version != universe.version:
            continue
        if (
            outcome.ordinal != ordinal
            or outcome.symbol != universe.symbols[ordinal]
            or outcome.attempt != attempt
            or outcome.status not in {"completed", "unavailable", "failed"}
        ):
            raise ValueError("AGENT_PAPER_ROTATION_INVALID")
        if outcome.status == "completed":
            ordinal = (ordinal + 1) % len(universe.symbols)
            attempt = 1
            continue
        if outcome.result_code == "RESEARCH_BLOCKED":
            if attempt != 3:
                raise ValueError("AGENT_PAPER_ROTATION_INVALID")
            ordinal = (ordinal + 1) % len(universe.symbols)
            attempt = 1
            continue
        if outcome.result_code in _CANDIDATE_FAILURE_CODES:
            if attempt >= 3:
                raise ValueError("AGENT_PAPER_ROTATION_INVALID")
            attempt += 1

    return ResearchTarget(
        version=universe.version,
        ordinal=ordinal,
        symbol=universe.symbols[ordinal],
        attempt=attempt,
    )

"""Frozen contracts and canonical hashes for Agent paper-cycle coordination."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from nexus.agents.funnel import Candidate, select_research_funnel

type CycleStatus = Literal[
    "prepared",
    "researching",
    "decision_recorded",
    "risk_evaluated",
    "awaiting_market",
    "partially_filled",
    "completed",
    "unavailable",
    "failed",
]


class CandidateAuthority(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    symbol: str
    inclusion_reason: str
    research_snapshot_id: UUID
    paper_holding: bool = False
    hard_risk: bool = False
    data_exception: bool = False


class CandidateManifestEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    symbol: str
    inclusion_reason: str
    research_snapshot_id: UUID


class AgentPaperCycleResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    status: CycleStatus
    result_code: str
    research_run_ids: tuple[UUID, ...] = ()
    strategy_decision_id: UUID | None = None
    batch_id: UUID | None = None
    order_ids: tuple[UUID, ...] = ()
    replayed: bool


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def select_candidate_manifest(
    candidates: tuple[CandidateAuthority, ...],
) -> tuple[CandidateManifestEntry, ...]:
    by_id: dict[UUID, CandidateAuthority] = {}
    for candidate in candidates:
        prior = by_id.get(candidate.instrument_id)
        if prior is not None and prior != candidate:
            raise ValueError("CANDIDATE_AUTHORITY_CONFLICT")
        by_id[candidate.instrument_id] = candidate
    funnel = select_research_funnel(
        tuple(
            Candidate(
                instrument_id=row.instrument_id,
                hard_risk=row.hard_risk,
                data_exception=row.data_exception,
                paper_holding=row.paper_holding,
            )
            for row in by_id.values()
        ),
        debate_capacity=5,
    )
    return tuple(
        CandidateManifestEntry(
            instrument_id=row.instrument_id,
            symbol=by_id[row.instrument_id].symbol,
            inclusion_reason=by_id[row.instrument_id].inclusion_reason,
            research_snapshot_id=by_id[row.instrument_id].research_snapshot_id,
        )
        for row in funnel.full_debate
    )


def candidate_manifest_hash(manifest: tuple[CandidateManifestEntry, ...]) -> str:
    payload = [
        row.model_dump(mode="json")
        for row in sorted(manifest, key=lambda item: item.instrument_id.int)
    ]
    return hashlib.sha256(_canonical_json(payload).encode()).hexdigest()


def canonical_cycle_event_hash(
    *,
    cycle_id: UUID,
    sequence: int,
    from_status: CycleStatus | None,
    to_status: CycleStatus,
    result_code: str,
    fact_ids: dict[str, object],
    occurred_at: datetime,
) -> str:
    if occurred_at.tzinfo is None or occurred_at.utcoffset() != UTC.utcoffset(occurred_at):
        raise ValueError("cycle event time must use UTC")
    payload = {
        "schema": "agent_paper_cycle_event_sha256_v1",
        "cycle_id": str(cycle_id),
        "sequence": sequence,
        "from_status": from_status,
        "to_status": to_status,
        "result_code": result_code,
        "fact_ids": fact_ids,
        "occurred_at": occurred_at.isoformat().replace("+00:00", "Z"),
    }
    return hashlib.sha256(_canonical_json(payload).encode()).hexdigest()

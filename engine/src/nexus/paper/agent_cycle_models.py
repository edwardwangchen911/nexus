"""Frozen contracts and canonical hashes for Agent paper-cycle coordination."""

import hashlib
import json
from datetime import UTC, date, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StringConstraints

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

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


def floor_decision_slot(value: datetime) -> datetime:
    """Return the containing five-minute UTC decision slot."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    utc_value = value.astimezone(UTC)
    return utc_value.replace(
        minute=utc_value.minute - utc_value.minute % 5,
        second=0,
        microsecond=0,
    )


class CandidateManifestEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    symbol: Literal["AAPL"]
    inclusion_reason: str
    research_snapshot_id: UUID
    prior_daily_price_authority_id: UUID
    decision_quote_id: UUID
    decision_trade_id: UUID | None
    decision_artifact_id: UUID
    decision_evidence_hash: Hash64
    risk_query_authority_ids: tuple[UUID, UUID, UUID]


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


class AgentPaperTriggerReservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    claim_id: UUID
    decision_session: date
    decision_slot_at: datetime
    replay: AgentPaperCycleResult | None = None


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


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

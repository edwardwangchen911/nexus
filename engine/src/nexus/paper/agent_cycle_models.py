"""Frozen contracts and canonical hashes for Agent paper-cycle coordination."""

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Annotated, Literal, overload
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from nexus.paper.agent_research_universe import (
    AGENT_PAPER_RESEARCH_UNIVERSE_V1,
    ResearchSymbol,
    ResearchTarget,
)

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
    symbol: ResearchSymbol
    inclusion_reason: str
    research_snapshot_id: UUID
    prior_daily_price_authority_id: UUID
    decision_quote_id: UUID
    decision_trade_id: UUID | None
    decision_artifact_id: UUID
    decision_evidence_hash: Hash64
    risk_query_authority_ids: tuple[UUID, UUID, UUID]


class LegacyCandidateManifestEntry(BaseModel):
    """Original four-field manifest retained only for persisted legacy cycles."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    symbol: str
    inclusion_reason: str
    research_snapshot_id: UUID


type StoredCandidateManifestEntry = CandidateManifestEntry | LegacyCandidateManifestEntry


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
    research_universe_version: Literal["agent_paper_research_v1"] = "agent_paper_research_v1"
    research_universe_ordinal: int = Field(default=0, ge=0, le=2)
    research_symbol: ResearchSymbol = "AAPL"
    research_attempt: int = Field(default=1, ge=1, le=3)
    replay: AgentPaperCycleResult | None = None

    @model_validator(mode="after")
    def validate_research_target(self) -> "AgentPaperTriggerReservation":
        if (
            self.research_universe_version != AGENT_PAPER_RESEARCH_UNIVERSE_V1.version
            or self.research_symbol
            != AGENT_PAPER_RESEARCH_UNIVERSE_V1.symbols[self.research_universe_ordinal]
        ):
            raise ValueError("AGENT_PAPER_ROTATION_INVALID")
        return self

    @property
    def research_target(self) -> ResearchTarget:
        return ResearchTarget(
            version=self.research_universe_version,
            ordinal=self.research_universe_ordinal,
            symbol=self.research_symbol,
            attempt=self.research_attempt,
        )


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


@overload
def parse_candidate_manifest(
    rows: Sequence[object], *, allow_legacy: Literal[False]
) -> tuple[CandidateManifestEntry, ...]: ...


@overload
def parse_candidate_manifest(
    rows: Sequence[object], *, allow_legacy: Literal[True]
) -> tuple[StoredCandidateManifestEntry, ...]: ...


@overload
def parse_candidate_manifest(
    rows: Sequence[object], *, allow_legacy: bool
) -> tuple[StoredCandidateManifestEntry, ...]: ...


def parse_candidate_manifest(
    rows: Sequence[object], *, allow_legacy: bool
) -> tuple[StoredCandidateManifestEntry, ...]:
    """Parse persisted manifests without upgrading or inventing historical evidence."""
    parsed: list[StoredCandidateManifestEntry] = []
    for row in rows:
        try:
            parsed.append(CandidateManifestEntry.model_validate(row))
        except ValueError:
            if not allow_legacy:
                raise
            parsed.append(LegacyCandidateManifestEntry.model_validate(row))
    return tuple(parsed)


def candidate_manifest_hash(manifest: tuple[StoredCandidateManifestEntry, ...]) -> str:
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

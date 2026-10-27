"""Closed non-trading identity for independent research and monitoring.

An absent predecessor is an explicit frozen fact, never a missing field. Historical
observations are research inputs and cannot initialize a real-time price latch.
"""

import hashlib
import json
from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.monitoring.disclosure_context import DisclosureMonitoringContext
from nexus.persistence.models.data import DataSnapshot

PURPOSE: Literal["independent_monitor_research_v1"] = "independent_monitor_research_v1"
SOURCE = "nexus_independent_monitor_research"
MODE: Literal["research_and_monitor"] = "research_and_monitor"
CONTRACT_VERSION = 3


class IndependentPredecessor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    kind: Literal["price", "disclosure"]
    plan_id: UUID | None
    generation: int | None = Field(ge=1, strict=True)

    @model_validator(mode="after")
    def exact_identity_or_absence(self) -> Self:
        if (self.plan_id is None) != (self.generation is None):
            raise ValueError("INDEPENDENT_MONITOR_PREDECESSOR_INVALID")
        return self


class IndependentMonitorContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["independent_monitor_research_v1"] = PURPOSE
    request_id: UUID
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    account_id: UUID
    candidate_batch_id: UUID
    batch_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_ids: tuple[UUID, ...] = Field(min_length=1, max_length=5)
    instrument_id: UUID
    decision_time: datetime
    source_snapshot_id: UUID
    source_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    predecessors: tuple[IndependentPredecessor, ...] = Field(min_length=2, max_length=2)
    disclosure_monitoring: DisclosureMonitoringContext
    execution_locked: Literal[True] = True
    formal_consumption_allowed: Literal[False] = False

    @field_validator("decision_time")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def exact_scope(self) -> Self:
        if (
            self.instrument_id not in self.candidate_ids
            or len(set(self.candidate_ids)) != len(self.candidate_ids)
            or {p.kind for p in self.predecessors} != {"price", "disclosure"}
            or self.disclosure_monitoring.instrument_id != self.instrument_id
            or self.disclosure_monitoring.research_as_of != self.decision_time
        ):
            raise ValueError("INDEPENDENT_MONITOR_CONTEXT_INVALID")
        return self

    def predecessor(self, kind: Literal["price", "disclosure"]) -> IndependentPredecessor:
        return next(p for p in self.predecessors if p.kind == kind)


def content(context: IndependentMonitorContext) -> bytes:
    return json.dumps(context.model_dump(mode="json"), sort_keys=True).encode()


def is_independent_snapshot(snapshot: DataSnapshot | None, manifest: object = None) -> bool:
    """Any marker blocks formal use; qualification requires all markers to agree."""
    return bool(
        snapshot is not None
        and (
            snapshot.source == SOURCE
            or snapshot.schema_version == PURPOSE
            or snapshot.payload.get("purpose") == PURPOSE
            or "independent_monitor_context" in snapshot.payload
        )
        or isinstance(manifest, list)
        and any(isinstance(item, dict) and item.get("source") == SOURCE for item in manifest)
    )


def require_independent_snapshot(snapshot: DataSnapshot | None) -> IndependentMonitorContext:
    invalid = "INDEPENDENT_MONITOR_CONTEXT_INVALID"
    if (
        snapshot is None
        or snapshot.source != SOURCE
        or snapshot.schema_version != PURPOSE
        or snapshot.payload.get("purpose") != PURPOSE
        or any(
            key in snapshot.payload
            for key in (
                "portfolio_context",
                "agent_paper_context",
                "monitor_context",
                "daily_monitoring",
            )
        )
        or snapshot.payload.get("as_of") != snapshot.observed_at.isoformat()
        or max(snapshot.available_at, snapshot.received_at) > snapshot.observed_at
        or snapshot.revision
        != hashlib.sha256(json.dumps(snapshot.payload, sort_keys=True).encode()).hexdigest()
    ):
        raise ValueError(invalid)
    try:
        context = IndependentMonitorContext.model_validate_json(
            json.dumps(snapshot.payload["independent_monitor_context"])
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(invalid) from error
    if (
        str(context.instrument_id) != snapshot.provider_object_id
        or snapshot.payload.get("instrument_id") != str(context.instrument_id)
        or context.decision_time != snapshot.observed_at
    ):
        raise ValueError(invalid)
    return context

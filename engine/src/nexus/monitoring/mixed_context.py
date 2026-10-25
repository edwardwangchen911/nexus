"""Version two single-stock input keeps sealed mixed evidence distinct from price-v1."""

import hashlib
import json
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.paper_context import AgentPaperPortfolioContext
from nexus.domain.time import require_utc
from nexus.monitoring.batch_inputs import BatchInputs, read_batch_inputs
from nexus.monitoring.contracts import PriceObservation
from nexus.monitoring.disclosure_context import (
    DisclosureMonitoringContext,
    validate_disclosure_baseline,
)
from nexus.paper.agent_cycle_authority import CycleAuthority, validate_candidate_manifest
from nexus.paper.agent_cycle_models import CandidateManifestEntry
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.strategy.intents import ResearchIntent

SOURCE = "nexus_monitor_increment_v2"
PURPOSE: Literal["monitor_increment_v2"] = "monitor_increment_v2"


class MixedTriggerCheck(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    trigger_id: UUID
    check_id: UUID
    kind: Literal["price", "disclosure"]
    event_key: str
    cause: Literal["initial_match", "crossed", "disclosure_added"]
    checked_at: datetime
    observation: dict[str, object]

    @field_validator("checked_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class MixedMonitorContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["monitor_increment_v2"] = PURPOSE
    account_id: UUID
    instrument_id: UUID
    parent_run_id: UUID
    parent_output_id: UUID
    parent_output_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_intent: ResearchIntent
    plan_id: UUID
    plan_generation: int = Field(ge=1)
    trigger_checks: tuple[MixedTriggerCheck, ...] = Field(min_length=1, max_length=32)
    decision_time: datetime
    decision_quote: PriceObservation
    candidate: CandidateManifestEntry
    authority: CycleAuthority
    paper_portfolio: AgentPaperPortfolioContext
    batch_id: UUID
    materials_snapshot_id: UUID
    materials_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    materials: BatchInputs
    disclosure_monitoring: DisclosureMonitoringContext
    execution_locked: Literal[True] = True

    @field_validator("decision_time")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)


def content(context: MixedMonitorContext) -> bytes:
    return json.dumps(context.model_dump(mode="json"), sort_keys=True).encode()


async def read_mixed_context(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID, as_of: datetime
) -> tuple[MixedMonitorContext, ResearchEvidenceRef] | None:
    from decimal import Decimal, localcontext

    from nexus.monitoring.observations import read_latest_price
    from nexus.paper.projection import load_paper_projection

    snapshot = await session.get(DataSnapshot, snapshot_id)
    if (
        snapshot is None
        or snapshot.source != SOURCE
        or snapshot.schema_version != PURPOSE
        or snapshot.provider_object_id != str(instrument_id)
        or snapshot.observed_at != as_of
        or max(snapshot.created_at, snapshot.available_at, snapshot.received_at) > as_of
        or snapshot.payload.get("purpose") != PURPOSE
        or "agent_paper_context" in snapshot.payload
        or "portfolio_context" in snapshot.payload
        or hashlib.sha256(json.dumps(snapshot.payload, sort_keys=True).encode()).hexdigest()
        != snapshot.revision
    ):
        return None
    try:
        context = MixedMonitorContext.model_validate_json(
            json.dumps(snapshot.payload["monitor_context"])
        )
        material_row = await session.get(DataSnapshot, context.materials_snapshot_id)
        material = await read_batch_inputs(
            session, snapshot_id=context.materials_snapshot_id, as_of=as_of
        )
        if (
            material_row is None
            or material_row.revision != context.materials_hash
            or material != context.materials
            or material is None
            or material.batch_id != context.batch_id
            or material.account_id != context.account_id
            or material.instrument_id != instrument_id
            or material.prepared_at > as_of
        ):
            return None
        if (
            context.instrument_id != instrument_id
            or context.decision_time != as_of
            or context.authority.account_id != context.account_id
            or context.authority.decision_time != as_of
            or context.candidate.instrument_id != instrument_id
            or context.decision_quote.quote_id != context.candidate.decision_quote_id
            or context.decision_quote.artifact_id != context.candidate.decision_artifact_id
        ):
            return None
        parents = [p for p in material.parents if p["plan_id"] == str(context.plan_id)]
        if len(parents) != 1:
            return None
        parent = parents[0]
        if (
            parent["run_id"] != str(context.parent_run_id)
            or parent["output_id"] != str(context.parent_output_id)
            or parent["output_hash"] != context.parent_output_hash
            or parent["generation"] != context.plan_generation
            or parent["intent"] != context.parent_intent.model_dump(mode="json")
        ):
            return None
        expected = [
            {k: v for k, v in member.items() if k != "plan_id"} for member in material.members
        ]
        if context.trigger_checks != tuple(
            MixedTriggerCheck.model_validate(item) for item in expected
        ):
            return None
        if (
            not await validate_candidate_manifest(
                session,
                authority=context.authority,
                manifest=(context.candidate,),
                requested_instrument_ids=(instrument_id,),
                max_instruments=1,
            )
            or await read_latest_price(session, instrument_id=instrument_id, now=as_of)
            != context.decision_quote
        ):
            return None
        if (
            context.disclosure_monitoring.instrument_id != instrument_id
            or context.disclosure_monitoring.research_as_of != as_of
            or not await validate_disclosure_baseline(session, context.disclosure_monitoring)
        ):
            return None
        projection = await load_paper_projection(
            session, account_id=context.account_id, as_of=as_of
        )
        position = next((p for p in projection.positions if p.instrument_id == instrument_id), None)
        if projection.nav <= 0:
            return None
        with localcontext() as arithmetic:
            arithmetic.prec = 38
            weight = (
                Decimal(0)
                if position is None
                else position.quantity * position.reference_close / projection.nav
            )
            cash = (projection.available_cash + projection.reserved_cash) / projection.nav
        if (
            context.paper_portfolio.projection_hash != projection.projection_hash
            or context.paper_portfolio.candidate_position_weight != str(weight)
            or context.paper_portfolio.cash_weight != str(cash)
        ):
            return None
        output = await session.get(AgentResearchOutputRecord, context.parent_output_id)
        if output is None or output.content_hash != context.parent_output_hash:
            return None
        raw = content(context)
        digest = hashlib.sha256(raw).hexdigest()
        evidence = await session.scalar(
            select(ResearchEvidenceRef).where(
                ResearchEvidenceRef.source == SOURCE,
                ResearchEvidenceRef.provider_object_id == str(context.trigger_checks[0].trigger_id),
                ResearchEvidenceRef.revision == digest,
                ResearchEvidenceRef.content_hash == digest,
                ResearchEvidenceRef.byte_length == len(raw),
                ResearchEvidenceRef.available_at == as_of,
                ResearchEvidenceRef.created_at <= as_of,
                ResearchEvidenceRef.location == f"memory://sha256/{digest}",
            )
        )
        return (context, evidence) if evidence is not None else None
    except (ValidationError, ValueError, TypeError, KeyError):
        return None


def model_view(context: MixedMonitorContext) -> dict[str, object]:
    """Transmit each original thesis/audit once; custody is proved by the reader.

    Document text and hashes remain complete. Archive bindings stay in the exact
    immutable material snapshot; they are not model-authored filing evidence.
    """
    value = context.model_dump(mode="json")
    materials = dict(value["materials"])
    theses: dict[str, dict[str, object]] = {}
    parents = []
    for parent in materials["parents"]:
        identity = parent["output_id"]
        theses.setdefault(
            identity,
            {
                key: item
                for key, item in parent.items()
                if key not in {"kind", "plan_id", "generation"}
            },
        )
        parents.append(
            {key: item for key, item in parent.items() if key not in {"intent", "role_outputs"}}
        )
    materials["parents"] = parents
    materials["parent_theses"] = list(theses.values())
    contents = []
    for item in materials["contents"]:
        archive = {key: part for key, part in item["archive"].items() if key != "originals"}
        contents.append({**item, "archive": archive})
    materials["contents"] = contents
    value["materials"] = materials
    # Keep the primary thesis at the established field, without sending it twice.
    for thesis in materials["parent_theses"]:
        if thesis["output_id"] == str(context.parent_output_id):
            thesis.pop("intent")
            thesis["intent_ref"] = "parent_intent"
    return value

"""Frozen, redacted market and portfolio context for one Agent paper decision."""

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution_authority import MAX_FROZEN_EVIDENCE
from nexus.paper.agent_cycle_models import CandidateManifestEntry
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.risk.canonical import canonical_paper_projection_hash
from nexus.risk.models import PaperPortfolioProjection

SOURCE = "nexus_agent_paper_context"
SNAPSHOT_SOURCE = "nexus_agent_paper"
SNAPSHOT_SCHEMA = "agent_paper_research_v1"
_LEGACY_SNAPSHOT_AUTHORITY = ("nexus_research", "research_filings_v1")


class FrozenAgentPaperContextStatus(StrEnum):
    MISSING = "missing"
    VALID = "valid"
    INVALID = "invalid"


class AgentPaperDecisionQuote(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: Literal["schwab"] = "schwab"
    feed: Literal["l1"] = "l1"
    delivery_mode: Literal["real_time"] = "real_time"
    delayed: Literal[False] = False
    bid: str
    ask: str
    observed_at: datetime
    available_at: datetime
    received_at: datetime


class AgentPaperPriorDaily(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: Literal["alpaca"] = "alpaca"
    feed: Literal["sip"] = "sip"
    delivery_mode: Literal["delayed"] = "delayed"
    adjustment: Literal["all"] = "all"
    session: str
    close: str


class AgentPaperPortfolioContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    candidate_position_weight: str
    cash_weight: str
    projection_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class AgentPaperResearchContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    instrument_id: UUID
    decision_time: datetime
    decision_quote: AgentPaperDecisionQuote
    prior_daily: AgentPaperPriorDaily
    paper_portfolio: AgentPaperPortfolioContext
    execution_locked: Literal[True] = True


def _decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000000000001")), "f")


def _content(context: AgentPaperResearchContext) -> bytes:
    return json.dumps(
        context.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


async def prepare_agent_paper_research_snapshot(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    decision_time: datetime,
    candidate: CandidateManifestEntry,
    projection: PaperPortfolioProjection,
) -> DataSnapshot:
    """Add exact cycle facts to an existing slow research snapshot without mutating it."""
    base = await session.get(DataSnapshot, candidate.research_snapshot_id)
    quote = await session.get(LatestQuote, candidate.decision_quote_id)
    artifact = await session.get(DataArtifact, candidate.decision_artifact_id)
    prior_authority = await session.get(
        PriceBarSetAuthority, candidate.prior_daily_price_authority_id
    )
    if base is None or quote is None or artifact is None or prior_authority is None:
        raise ValueError("AGENT_PAPER_RESEARCH_CONTEXT_INVALID")
    prior_manifest = await session.get(
        PriceSeriesManifest, prior_authority.price_series_manifest_id
    )
    prior_bars = tuple(
        await session.scalars(
            select(PriceBarRecord).where(
                PriceBarRecord.price_series_manifest_id == prior_authority.price_series_manifest_id,
                PriceBarRecord.instrument_id == candidate.instrument_id,
            )
        )
    )
    base_evidence = tuple(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord)
            .where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == base.id,
                AgentDataSnapshotEvidenceRecord.instrument_id == candidate.instrument_id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                AgentDataSnapshotEvidenceRecord.available_at <= decision_time,
            )
            .order_by(
                AgentDataSnapshotEvidenceRecord.available_at,
                AgentDataSnapshotEvidenceRecord.evidence_ref_id,
            )
        )
    )
    position = next(
        (item for item in projection.positions if item.instrument_id == candidate.instrument_id),
        None,
    )
    if (
        base.source != "nexus_research"
        or base.schema_version != "research_filings_v1"
        or base.provider_object_id != str(candidate.instrument_id)
        or base.available_at > decision_time
        or base.received_at > decision_time
        or not base_evidence
        or len(base_evidence) > MAX_FROZEN_EVIDENCE - 2
        or projection.as_of != decision_time
        or projection.nav <= 0
        # Research may observe occupied resources; activation/execution still owns
        # that guard. Freeze the full projection digest, including open orders.
        or canonical_paper_projection_hash(projection) != projection.projection_hash
        or quote.instrument_id != candidate.instrument_id
        or quote.source != "schwab"
        or quote.feed != "l1"
        or quote.delivery_mode != "real_time"
        or quote.event_kind != "quote"
        or quote.evidence_status != "verified"
        or quote.data_artifact_id != artifact.id
        or quote.bid is None
        or quote.ask is None
        or quote.bid <= 0
        or quote.ask < quote.bid
        or quote.available_at > decision_time
        or quote.received_at > decision_time
        or artifact.source != "schwab"
        or artifact.schema_version != "schwab_level_one_stream_v1"
        or artifact.sha256 != candidate.decision_evidence_hash
        or prior_manifest is None
        or prior_manifest.source != "alpaca"
        or prior_manifest.feed != "sip"
        or prior_manifest.delivery_mode != "delayed"
        or prior_manifest.adjustment != "all"
        or prior_manifest.interval != "1Day"
        or len(prior_bars) != 1
        or prior_bars[0].session != prior_authority.last_session
        or prior_bars[0].close <= 0
    ):
        raise ValueError("AGENT_PAPER_RESEARCH_CONTEXT_INVALID")
    prior = prior_bars[0]
    with localcontext() as decimal_context:
        decimal_context.prec = 28
        candidate_weight = (
            Decimal(0)
            if position is None
            else position.quantity * position.reference_close / projection.nav
        )
        cash_weight = (projection.available_cash + projection.reserved_cash) / projection.nav
    context = AgentPaperResearchContext(
        cycle_id=cycle_id,
        instrument_id=candidate.instrument_id,
        decision_time=decision_time,
        decision_quote=AgentPaperDecisionQuote(
            bid=_decimal(quote.bid),
            ask=_decimal(quote.ask),
            observed_at=quote.observed_at,
            available_at=quote.available_at,
            received_at=quote.received_at,
        ),
        prior_daily=AgentPaperPriorDaily(
            session=prior.session.isoformat(),
            close=_decimal(prior.close),
        ),
        paper_portfolio=AgentPaperPortfolioContext(
            candidate_position_weight=_decimal(candidate_weight),
            cash_weight=_decimal(cash_weight),
            projection_hash=projection.projection_hash,
        ),
    )
    content = _content(context)
    digest = hashlib.sha256(content).hexdigest()
    object_id = f"{cycle_id}:{candidate.instrument_id}"
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(
            source=SOURCE,
            provider_object_id=object_id,
            revision=digest,
            available_at=decision_time,
            content_hash=digest,
            byte_length=len(content),
            location=f"memory://sha256/{digest}",
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
            ResearchEvidenceRef.revision == digest,
        )
    )
    if evidence is None:
        raise ValueError("AGENT_PAPER_RESEARCH_CONTEXT_INVALID")
    payload = {
        **{
            key: value
            for key, value in base.payload.items()
            if key not in {"portfolio_context", "agent_paper_context"}
        },
        "evidence_ids": [str(item.evidence_ref_id) for item in base_evidence],
        "agent_paper_context": context.model_dump(mode="json"),
    }
    snapshot_revision = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    snapshot_id = await session.scalar(
        insert(DataSnapshot)
        .values(
            source=SNAPSHOT_SOURCE,
            provider_object_id=str(candidate.instrument_id),
            revision=snapshot_revision,
            observed_at=decision_time,
            available_at=decision_time,
            received_at=decision_time,
            schema_version=SNAPSHOT_SCHEMA,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
        .returning(DataSnapshot.id)
    )
    snapshot = (
        await session.get(DataSnapshot, snapshot_id)
        if snapshot_id is not None
        else await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SNAPSHOT_SOURCE,
                DataSnapshot.provider_object_id == str(candidate.instrument_id),
                DataSnapshot.revision == snapshot_revision,
            )
        )
    )
    if snapshot is None:
        raise ValueError("AGENT_PAPER_RESEARCH_CONTEXT_INVALID")
    rows = [
        {
            "data_snapshot_id": snapshot.id,
            "instrument_id": candidate.instrument_id,
            "evidence_ref_id": item.evidence_ref_id,
            "authority_kind": "research",
            "selected_at": None,
            "source": item.source,
            "provider_object_id": item.provider_object_id,
            "revision": item.revision,
            "available_at": item.available_at,
            "location": item.location,
            "content_hash": item.content_hash,
        }
        for item in base_evidence
    ]
    await session.execute(
        insert(AgentDataSnapshotEvidenceRecord).values(rows).on_conflict_do_nothing()
    )
    await session.flush()
    return snapshot


async def read_agent_paper_research_context(
    session: AsyncSession,
    *,
    snapshot_id: UUID,
    instrument_id: UUID,
    as_of: datetime,
) -> tuple[AgentPaperResearchContext, ResearchEvidenceRef] | None:
    snapshot = await session.get(DataSnapshot, snapshot_id)
    if (
        snapshot is None
        or (snapshot.source, snapshot.schema_version)
        not in {
            (SNAPSHOT_SOURCE, SNAPSHOT_SCHEMA),
            _LEGACY_SNAPSHOT_AUTHORITY,
        }
        or (
            (snapshot.source, snapshot.schema_version) == _LEGACY_SNAPSHOT_AUTHORITY
            and "portfolio_context" in snapshot.payload
        )
        or snapshot.provider_object_id != str(instrument_id)
        or snapshot.available_at > as_of
        or snapshot.received_at > as_of
        or hashlib.sha256(json.dumps(snapshot.payload, sort_keys=True).encode()).hexdigest()
        != snapshot.revision
    ):
        return None
    try:
        context = AgentPaperResearchContext.model_validate(snapshot.payload["agent_paper_context"])
    except (KeyError, TypeError, ValidationError):
        return None
    if context.instrument_id != instrument_id or context.decision_time != as_of:
        return None
    content = _content(context)
    digest = hashlib.sha256(content).hexdigest()
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == f"{context.cycle_id}:{instrument_id}",
            ResearchEvidenceRef.revision == digest,
            ResearchEvidenceRef.content_hash == digest,
            ResearchEvidenceRef.available_at == context.decision_time,
            ResearchEvidenceRef.location == f"memory://sha256/{digest}",
            ResearchEvidenceRef.byte_length == len(content),
        )
    )
    return (context, cast(ResearchEvidenceRef, evidence)) if evidence is not None else None


async def revalidate_frozen_agent_paper_context(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    evidence_records: tuple[ResearchEvidenceRef, ...],
) -> FrozenAgentPaperContextStatus:
    """Require the exact context evidence frozen into this run, if present."""
    context_records = tuple(record for record in evidence_records if record.source == SOURCE)
    context = await read_agent_paper_research_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if not context_records and context is None:
        return FrozenAgentPaperContextStatus.MISSING
    if context is None:
        return FrozenAgentPaperContextStatus.INVALID
    cycle = await session.get(AgentPaperCycleRecord, context[0].cycle_id)
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord).where(
                AgentPaperCycleEventRecord.cycle_id == context[0].cycle_id
            )
        )
    )
    run_bound = any(
        isinstance((run_ids := event.fact_ids.get("research_run_ids")), list)
        and str(run.id) in run_ids
        for event in events
        if isinstance(event.fact_ids, dict)
    )
    if (
        len(context_records) != 1
        or context_records[0].id != context[1].id
        or cycle is None
        or cycle.decision_time != run.decision_time
        or context[0].decision_time != run.decision_time
        or not run_bound
    ):
        return FrozenAgentPaperContextStatus.INVALID
    return FrozenAgentPaperContextStatus.VALID


def paper_contexts_match_projection(
    contexts: Sequence[AgentPaperResearchContext], projection: PaperPortfolioProjection
) -> bool:
    """Bind deterministic sizing to the exact portfolio shown to every paper run."""
    return bool(contexts) and all(
        context.paper_portfolio.projection_hash == projection.projection_hash
        for context in contexts
    )

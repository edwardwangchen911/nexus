"""Freeze and revalidate independent request provenance, without external collection."""

import hashlib
import json
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.research_only import require_research_only_snapshot
from nexus.monitoring.disclosure_context import (
    freeze_disclosure_baseline,
    validate_disclosure_baseline,
)
from nexus.monitoring.independent_context import (
    CONTRACT_VERSION,
    MODE,
    PURPOSE,
    SOURCE,
    IndependentMonitorContext,
    IndependentPredecessor,
    content,
    require_independent_snapshot,
)
from nexus.monitoring.storage import lock_security
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.manual_paper import (
    ManualPaperRequest,
)
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.research import ResearchEvidenceRef


async def predecessors(
    session: AsyncSession, *, account_id: UUID, instrument_id: UUID, as_of: datetime
) -> tuple[IndependentPredecessor, ...]:
    result = []
    for kind in ("price", "disclosure"):
        row = await session.scalar(
            select(MonitorPlan)
            .where(
                MonitorPlan.account_id == account_id,
                MonitorPlan.instrument_id == instrument_id,
                MonitorPlan.kind == kind,
                MonitorPlan.created_at <= as_of,
            )
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        result.append(
            IndependentPredecessor(
                kind=kind,
                plan_id=None if row is None else row.id,
                generation=None if row is None else row.generation,
            )
        )
    return tuple(result)


async def freeze_independent_snapshot(
    session: AsyncSession, *, request: ManualPaperRequest, base: DataSnapshot
) -> DataSnapshot:
    invalid = "INDEPENDENT_MONITOR_CONTEXT_INVALID"
    require_research_only_snapshot(base)
    if request.contract_version != CONTRACT_VERSION or request.mode != MODE:
        raise ValueError(invalid)
    batch = await load_agent_paper_candidate_batch(session, batch_id=request.candidate_batch_id)
    instrument_id = UUID(base.provider_object_id)
    if (
        batch is None
        or batch.batch_hash != request.batch_hash
        or batch.account_id != request.account_id
        or instrument_id not in batch.instrument_ids
        or base.observed_at < request.requested_at
    ):
        raise ValueError(invalid)
    await lock_security(session, account_id=request.account_id, instrument_id=instrument_id)
    context = IndependentMonitorContext(
        request_id=request.id,
        request_hash=request.request_hash,
        account_id=request.account_id,
        candidate_batch_id=request.candidate_batch_id,
        batch_hash=request.batch_hash,
        candidate_ids=batch.instrument_ids,
        instrument_id=instrument_id,
        decision_time=base.observed_at,
        source_snapshot_id=base.id,
        source_snapshot_hash=base.revision,
        predecessors=await predecessors(
            session,
            account_id=request.account_id,
            instrument_id=instrument_id,
            as_of=base.observed_at,
        ),
        disclosure_monitoring=await freeze_disclosure_baseline(
            session, instrument_id=instrument_id, as_of=base.observed_at
        ),
    )
    # Research originals are an exact copy. Native price authority is separately
    # frozen by admission and revalidated by execution/source qualification; it
    # is not a research-context reference or part of the original payload list.
    rows = tuple(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == base.id,
                AgentDataSnapshotEvidenceRecord.instrument_id == instrument_id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
            )
        )
    )
    if (
        not rows
        or len(rows) >= 100
        or {str(r.evidence_ref_id) for r in rows}
        != set(cast(list[str], base.payload["evidence_ids"]))
    ):
        raise ValueError(invalid)
    raw = content(context)
    digest = hashlib.sha256(raw).hexdigest()
    ref = ResearchEvidenceRef(
        source=SOURCE,
        provider_object_id=f"{request.id}:{instrument_id}",
        revision=digest,
        available_at=base.observed_at,
        content_hash=digest,
        byte_length=len(raw),
        location=f"memory://sha256/{digest}",
    )
    session.add(ref)
    await session.flush()
    payload = base.payload | dict(
        purpose=PURPOSE,
        independent_monitor_context=context.model_dump(mode="json"),
        evidence_ids=[str(r.evidence_ref_id) for r in rows] + [str(ref.id)],
    )
    snapshot = DataSnapshot(
        source=SOURCE,
        provider_object_id=str(instrument_id),
        revision=hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        observed_at=base.observed_at,
        available_at=base.observed_at,
        received_at=base.observed_at,
        schema_version=PURPOSE,
        payload=payload,
    )
    session.add(snapshot)
    await session.flush()
    evidence_rows: tuple[AgentDataSnapshotEvidenceRecord | ResearchEvidenceRef, ...] = (*rows, ref)
    for row in evidence_rows:
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=snapshot.id,
                instrument_id=instrument_id,
                evidence_ref_id=row.id
                if isinstance(row, ResearchEvidenceRef)
                else row.evidence_ref_id,
                authority_kind="research",
                selected_at=None,
                source=row.source,
                provider_object_id=row.provider_object_id,
                revision=row.revision,
                available_at=row.available_at,
                location=row.location,
                content_hash=row.content_hash,
            )
        )
    await session.flush()
    require_independent_snapshot(snapshot)
    return snapshot


async def read_independent_context(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID, as_of: datetime
) -> tuple[IndependentMonitorContext, ResearchEvidenceRef] | None:
    snapshot = await session.get(DataSnapshot, snapshot_id)
    try:
        context = require_independent_snapshot(snapshot)
    except (ValueError, TypeError):
        return None
    if (
        snapshot is None
        or snapshot.created_at > as_of
        or context.instrument_id != instrument_id
        or context.decision_time != as_of
    ):
        return None
    request = await session.get(ManualPaperRequest, context.request_id)
    batch = await load_agent_paper_candidate_batch(session, batch_id=context.candidate_batch_id)
    base = await session.get(DataSnapshot, context.source_snapshot_id)
    from nexus.paper.manual_request import manual_request_hash

    if (
        request is None
        or request.contract_version != CONTRACT_VERSION
        or request.mode != MODE
        or request.request_hash != context.request_hash
        or request.account_id != context.account_id
        or request.batch_hash != context.batch_hash
        or request.candidate_batch_id != context.candidate_batch_id
        or request.requested_at > as_of
        or request.request_hash
        != manual_request_hash(
            account_id=request.account_id,
            batch_id=request.candidate_batch_id,
            mode=MODE,
            contract_version=CONTRACT_VERSION,
            research_profile=request.research_profile,
        )
        or batch is None
        or batch.account_id != context.account_id
        or batch.batch_hash != context.batch_hash
        or batch.instrument_ids != context.candidate_ids
        or base is None
        or base.revision != context.source_snapshot_hash
        or base.observed_at != as_of
        or base.created_at > as_of
    ):
        return None
    try:
        require_research_only_snapshot(base)
    except ValueError:
        return None
    if context.predecessors != await predecessors(
        session, account_id=context.account_id, instrument_id=instrument_id, as_of=as_of
    ) or not await validate_disclosure_baseline(session, context.disclosure_monitoring):
        return None
    rows = tuple(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot_id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
            )
        )
    )
    original = tuple(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == base.id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
            )
        )
    )

    def binding(row: AgentDataSnapshotEvidenceRecord) -> tuple[object, ...]:
        return (
            row.instrument_id,
            row.evidence_ref_id,
            row.authority_kind,
            row.selected_at,
            row.source,
            row.provider_object_id,
            row.revision,
            row.available_at,
            row.location,
            row.content_hash,
        )

    own = [r for r in rows if r.source == SOURCE]
    if (
        len(own) != 1
        or {binding(r) for r in rows if r.source != SOURCE} != {binding(r) for r in original}
        or {str(r.evidence_ref_id) for r in rows}
        != set(cast(list[str], snapshot.payload["evidence_ids"]))
    ):
        return None
    ref = await session.get(ResearchEvidenceRef, own[0].evidence_ref_id)
    digest = hashlib.sha256(content(context)).hexdigest()
    if (
        ref is None
        or ref.source != SOURCE
        or ref.provider_object_id != f"{request.id}:{instrument_id}"
        or ref.revision != digest
        or ref.content_hash != digest
        or ref.byte_length != len(content(context))
        or ref.location != f"memory://sha256/{digest}"
        or ref.available_at != as_of
        or ref.created_at > as_of
        or own[0].content_hash != digest
        or own[0].revision != digest
    ):
        return None
    return context, ref


async def require_independent_run(
    session: AsyncSession, run: AgentResearchRunRecord
) -> IndependentMonitorContext:
    invalid = "INDEPENDENT_MONITOR_SOURCE_INVALID"
    bound = await read_independent_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if bound is None:
        raise ValueError(invalid)
    context = bound[0]
    request = await session.get(ManualPaperRequest, context.request_id)
    from nexus.paper.research_request import read_research_manifest

    manifest = None if request is None else await read_research_manifest(session, request)
    if manifest is None or run.id not in {m.run_id for m in manifest.members}:
        raise ValueError(invalid)
    return context

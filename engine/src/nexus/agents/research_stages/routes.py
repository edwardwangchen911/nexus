"""Immutable preparation-time version selection for new research identities only."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.paper_context import (
    FrozenAgentPaperContextStatus,
    read_agent_paper_research_context,
    revalidate_frozen_agent_paper_context,
)
from nexus.agents.research_stages.protocol import (
    ProtocolRejected,
    ensure_schema,
    required,
    research_run_id,
)
from nexus.monitoring.independent_storage import read_independent_context
from nexus.paper.research_request import read_research_manifest
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.monitoring import MonitorIncrement
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job

VERSION = "research-stages-v1"


async def owner(session: AsyncSession, run: AgentResearchRunRecord, job: Job) -> tuple[UUID, str]:
    from nexus.agents.comparison_routes import comparison_owner

    comparison = await comparison_owner(session, run, job)
    if comparison is not None:
        return comparison, "comparison"
    increment = await session.scalar(
        select(MonitorIncrement).where(
            MonitorIncrement.run_id == run.id, MonitorIncrement.job_id == job.id
        )
    )
    if increment is not None:
        from nexus.monitoring.admission import require_increment_binding

        await require_increment_binding(session, run)
        return increment.id, "increment"
    independent = await read_independent_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if independent is not None:
        request = required(await session.get(ManualPaperRequest, independent[0].request_id))
        manifest = await read_research_manifest(session, request)
        if manifest is None or not any(
            m.run_id == run.id and m.job_id == job.id for m in manifest.members
        ):
            raise ProtocolRejected("ROOT_MANIFEST_BINDING_INVALID")
        return request.id, "independent"
    paper = await read_agent_paper_research_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if paper is not None:
        cycle = await session.get(AgentPaperCycleRecord, paper[0].cycle_id)
        evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
        if (
            cycle is None
            or run.actor_id != "local-agent-paper-cycle"
            or await revalidate_frozen_agent_paper_context(
                session, run=run, evidence_records=tuple(evidence)
            )
            != FrozenAgentPaperContextStatus.VALID
        ):
            raise ProtocolRejected("ROOT_MANIFEST_BINDING_INVALID")
        return cycle.id, "formal"
    raise ProtocolRejected("RESEARCH_PROTOCOL_OWNER_UNAVAILABLE")


async def ensure_routes(session: AsyncSession) -> None:
    """Schema is migration-owned; no implicit upgrade or route registration."""
    await ensure_schema(session)


async def register_prepared_runs(session: AsyncSession, run_ids: Sequence[UUID]) -> None:
    """Called inside the original preparation transaction, before Jobs become visible."""
    await ensure_routes(session)
    for run_id in run_ids:
        run = required(await session.get(AgentResearchRunRecord, run_id))
        job = (
            await session.scalars(
                select(Job).where(Job.payload["run_id"].astext == str(run_id)).with_for_update()
            )
        ).one()
        existing = await selected_route(session, job.id)
        if existing is not None:
            # Exact immutable selection is read-only on replay, never a new send right.
            continue
        if job.job_type != "agent.research.comparison" and not await formal_stage_job_qualified(
            session, job
        ):
            raise ProtocolRejected("STAGED_PREPARATION_NOT_QUALIFIED")
        scope, kind = await owner(session, run, job)
        if kind == "comparison":
            await session.execute(
                text(
                    "INSERT INTO research_stage_scope(id,comparison_id) "
                    "VALUES(:scope,:scope) ON CONFLICT(id) DO NOTHING"
                ),
                {"scope": scope},
            )
        else:
            await _register_business_scope(session, scope, kind)
        await session.execute(
            text("INSERT INTO research_stage_route VALUES(:run,:job,:scope,:kind,:version)"),
            {"run": run.id, "job": job.id, "scope": scope, "kind": kind, "version": VERSION},
        )


async def _register_business_scope(session: AsyncSession, scope: UUID, kind: str) -> None:
    await session.execute(
        text(
            "INSERT INTO research_stage_scope(id,cycle_id,increment_id,request_id) "
            "VALUES(:scope,:cycle,:inc,:request) ON CONFLICT(id) DO NOTHING"
        ),
        {
            "scope": scope,
            "cycle": scope if kind == "formal" else None,
            "inc": scope if kind == "increment" else None,
            "request": scope if kind == "independent" else None,
        },
    )


async def selected_route(session: AsyncSession, job_id: UUID) -> Mapping[str, Any] | None:
    if await session.scalar(text("SELECT to_regclass('research_stage_route')")) is None:
        return None
    row = (
        (
            await session.execute(
                text("SELECT * FROM research_stage_route WHERE job_id=:job"), {"job": job_id}
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    job = required(await session.get(Job, job_id))
    run = required(await session.get(AgentResearchRunRecord, research_run_id(job)))
    scope, kind = await owner(session, run, job)
    if (row["run_id"], row["scope_id"], row["kind"], row["version"]) != (
        run.id,
        scope,
        kind,
        VERSION,
    ):
        raise ProtocolRejected("STAGED_ROUTE_BINDING_INVALID")
    return dict(row)


async def register_new_prepared_runs(session: AsyncSession, run_ids: Sequence[UUID]) -> None:
    for run_id in run_ids:
        job = (
            await session.scalars(select(Job).where(Job.payload["run_id"].astext == str(run_id)))
        ).one()
        if await selected_route(session, job.id) is not None:
            continue
        born = await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM research_stage_prepared_job WHERE job_id=:job AND "
                "pg_xact_status(xmin::text::xid8)='in progress')"
            ),
            {"job": job.id},
        )
        if born and await formal_stage_job_qualified(session, job):
            await register_prepared_runs(session, (run_id,))

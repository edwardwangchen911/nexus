"""Stable v3 historical facts, separate from current maintenance eligibility."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.research_stages.routes import selected_route
from nexus.jobs.models import job_input_hash
from nexus.paper.manual_request import read_manual_request
from nexus.paper.research_request import require_research_jobs_settled
from nexus.persistence.models.manual_paper import ManualPaperRequest

BLOCKED = "INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"

# Exact historical identity only. Future monitoring checks, new requests and the
# global audit tail must never become members of this frozen acceptance proof.
FACT_FILTERS = {
    "manual_paper_requests": "id=:request",
    "manual_paper_request_events": "request_id=:request",
    "research_request_manifests": "request_id=:request",
    "research_request_members": (
        "manifest_id IN (SELECT id FROM research_request_manifests WHERE request_id=:request)"
    ),
    "agent_research_runs": "id=ANY(:runs)",
    "agent_research_events": "run_id=ANY(:runs)",
    "agent_research_outputs": "run_id=ANY(:runs)",
    "agent_daily_budget_reservations": "run_id=ANY(:runs)",
    "jobs": "id=ANY(:jobs) OR payload->>'run_id'=ANY(:run_strings)",
    "job_attempts": "job_id=ANY(:jobs)",
    "job_leases": "job_id=ANY(:jobs)",
    "research_stage_scope": "id=:request OR request_id=:request",
    "research_stage_route": "scope_id=:request OR run_id=ANY(:runs) OR job_id=ANY(:jobs)",
    "research_stage_member": "scope_id=:request OR run_id=ANY(:runs) OR job_id=ANY(:jobs)",
    "research_stage_prepared_job": "job_id=ANY(:jobs)",
    **{
        f"research_stage_{name}": "run_id=ANY(:runs)"
        for name in ("authorization", "observation", "verification", "cleanup", "terminal")
    },
}


async def independent_unknown_proof(
    session: AsyncSession, *, row: ManualPaperRequest, now: datetime
) -> dict[str, object]:
    """Validate native financial closure and bind all current facts, including absence."""
    view = await read_manual_request(session, row)
    if (
        row.contract_version != 3
        or row.mode != "research_and_monitor"
        or view.cycle_id is not None
        or view.status != "unknown"
        or view.code != "MANUAL_RESULT_UNKNOWN"
        or view.research is None
    ):
        raise ValueError(BLOCKED)
    statuses = tuple(
        await session.scalars(
            text(
                "SELECT status FROM manual_paper_request_events "
                "WHERE request_id=:id ORDER BY sequence"
            ),
            {"id": row.id},
        )
    )
    if statuses != ("queued", "preparing", "active", "unknown"):
        raise ValueError(BLOCKED)
    unknown = sorted(str(m.run_id) for m in view.research.members if not m.usage_known)
    if not unknown:
        raise ValueError(BLOCKED)
    for member in view.research.members:
        route = await selected_route(session, member.job_id)
        if route is None or route["kind"] != "independent" or route["scope_id"] != row.id:
            raise ValueError(BLOCKED)
    # This is only a financial/cleanup proof, not an admission exemption.
    await require_research_jobs_settled(
        session, row=row, view=view.research, now=now, allow_unknown_usage=True
    )
    params = {
        "request": row.id,
        "runs": [m.run_id for m in view.research.members],
        "run_strings": [str(m.run_id) for m in view.research.members],
        "jobs": [m.job_id for m in view.research.members],
    }
    facts = {}
    for table, predicate in FACT_FILTERS.items():
        rows = sorted(
            await session.scalars(
                text(f"SELECT to_jsonb(t)::text FROM {table} t WHERE {predicate}"), params
            )
        )
        facts[table] = job_input_hash({"rows": rows})
    return {
        "account_id": str(row.account_id),
        "request_id": str(row.id),
        "contract_version": 3,
        "unknown_run_ids": unknown,
        "facts": facts,
        "disposition": "historical_occupancy_only_usage_and_budget_preserved",
    }


async def load_independent_request(
    session: AsyncSession, *, request_id: UUID, account_id: UUID
) -> ManualPaperRequest:
    row = await session.scalar(
        select(ManualPaperRequest)
        .where(ManualPaperRequest.id == request_id, ManualPaperRequest.account_id == account_id)
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise ValueError(BLOCKED)
    return row

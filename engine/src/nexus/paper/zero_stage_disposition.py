"""Narrow historical disposition; never a claim of cleanup or zero provider cost."""

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.public import _selected_member_settled
from nexus.paper.manual_request import ManualRequestView
from nexus.persistence.models.manual_paper import ManualPaperRequest

SCHEMA = "independent_zero_stage_accept_v1"
BLOCKED = "INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"


async def selected(session: AsyncSession, row: ManualPaperRequest) -> bool:
    """Select an explicit stored shape before validation, not after old proof failure."""
    return row.contract_version == 3 and bool(
        await session.scalar(
            text(
                "SELECT 1 FROM research_request_manifests f "
                "JOIN research_request_members m ON m.manifest_id=f.id "
                "JOIN research_stage_terminal t ON t.run_id=m.run_id "
                "WHERE f.request_id=:id AND NOT t.known AND t.evidence='[]'::jsonb LIMIT 1"
            ),
            {"id": row.id},
        )
    )


async def proof(
    session: AsyncSession, *, row: ManualPaperRequest, view: ManualRequestView, now: datetime
) -> dict[str, object]:
    if (row.contract_version, row.mode, view.status) != (3, "research_and_monitor", "unknown"):
        raise ValueError(BLOCKED)
    if view.research is None or len(view.research.members) != 3:
        raise ValueError(BLOCKED)
    unknown = [m for m in view.research.members if not m.usage_known]
    if len(unknown) != 1:
        raise ValueError(BLOCKED)
    target = unknown[0]
    terminal = await read_terminal_proof(
        session, run_id=target.run_id, now=now, close_empty_unknown=True
    )
    if (
        terminal is None
        or not terminal.empty_authorizations
        or terminal.known
        or terminal.budget_settled
        or terminal.cleanup_confirmed
        or terminal.orphan
    ):
        raise ValueError(BLOCKED)
    # Full terminal reader proves the exact reservation is still held. Neither
    # that amount nor empty authorization bounds the unknown supplier invoice.
    for member in view.research.members:
        if member.run_id != target.run_id:
            await _selected_member_settled(
                session,
                owner_id=row.id,
                kind="independent",
                member=member,
                now=now,
                allow_unknown_usage=False,
            )
    return dict(
        kind="zero_stage_local_quiescent_v1",
        run_id=str(target.run_id),
        cleanup_confirmed=False,
        usage_unknown=True,
        budget_preserved=True,
    )

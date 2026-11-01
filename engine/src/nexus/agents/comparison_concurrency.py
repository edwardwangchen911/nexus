"""Serialize experimental work with existing accounting locks and terminal proofs."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import _lock_budget_day
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected
from nexus.agents.research_usage import require_research_accounting_clear


async def require_comparison_exclusive(session: AsyncSession, experiment_id: UUID) -> None:
    now = await session.scalar(text("SELECT clock_timestamp()"))
    await _lock_budget_day(session, now.date().isoformat())
    now = await session.scalar(text("SELECT clock_timestamp()"))
    await require_research_accounting_clear(session)
    rows = (
        (
            await session.execute(
                text("""SELECT c.run_id,c.job_id,j.status,j.started_at,j.finished_at,j.attempt_count
      FROM research_comparison_chain c JOIN jobs j ON j.id=c.job_id
      WHERE c.experiment_id<>:id"""),
                {"id": experiment_id},
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        if (
            row["status"] not in ("succeeded", "failed", "dead_letter", "cancelled", "skipped")
            or row["finished_at"] is None
        ):
            raise ValueError("COMPARISON_OTHER_WORK_UNCONFIRMED")
        # A legitimate zero-send close intentionally retains its route without
        # a member/terminal. The public stage reader must keep rejecting that
        # shape; only this exact, audited native proof can classify it first.
        if (
            row["status"] == "failed"
            and row["started_at"] is None
            and row["attempt_count"] == 0
            and await session.scalar(
                text("SELECT public.research_comparison_preclaim_bound(:job,:at)"),
                {"job": row["job_id"], "at": row["finished_at"]},
            )
            is True
        ):
            continue
        try:
            proof = await read_terminal_proof(session, run_id=row["run_id"], now=now)
        except ProtocolRejected:
            raise ValueError("COMPARISON_OTHER_WORK_UNCONFIRMED") from None
        if proof is None or not proof.cleanup_confirmed or proof.known and not proof.budget_settled:
            raise ValueError("COMPARISON_OTHER_WORK_UNCONFIRMED")

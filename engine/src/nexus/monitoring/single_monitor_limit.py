"""The one authorized v6 lineage gets at most one committed increment forever."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def require_single_monitor_increment_unused(
    session: AsyncSession, *, parent_run_id: UUID
) -> None:
    """Caller holds the normal account fence; SQL insertion is the atomic backstop."""
    root = await session.scalar(
        text("SELECT public.single_monitor_root_request(:run)"), {"run": parent_run_id}
    )
    if root is not None and await session.scalar(
        text("SELECT 1 FROM public.single_monitor_increment_claims WHERE root_request_id=:root"),
        {"root": root},
    ):
        raise ValueError("SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED")

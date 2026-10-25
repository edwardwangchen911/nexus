"""Validate completed seed facts before an existing legacy-fixture DDL boundary."""

from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_GUARDS = {
    "agent_research_runs": ("trg_agent_research_runs_monitor_frozen_members",),
    "data_snapshots": (
        "trg_data_snapshots_monitor_frozen_members",
        "trg_daily_monitor_snapshot",
        "trg_daily_v2_snapshot",
    ),
}


async def validate_pending_monitor_guards(
    session: AsyncSession, *, table: Literal["agent_research_runs", "data_snapshots"]
) -> None:
    """Drain only these initially deferred guards, then restore their fixture mode.

    Call after a complete valid seed and before the fixture's pre-existing trigger
    bypass. This executes validation; it neither disables nor drops any guard.
    These callers have not overridden the guards' initially deferred mode.
    """
    await session.flush()
    names = ", ".join(_GUARDS[table])
    await session.execute(text(f"SET CONSTRAINTS {names} IMMEDIATE"))
    await session.execute(text(f"SET CONSTRAINTS {names} DEFERRED"))

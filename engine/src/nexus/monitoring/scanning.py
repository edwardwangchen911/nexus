"""One bounded, current market-session scan of installed plans; no provider or model."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.monitoring.observations import read_latest_price
from nexus.monitoring.storage import lock_security, record_check
from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.paper import PaperAccountRecord


async def scan_account(
    session: AsyncSession, *, account_id: UUID, calendar_authority_id: UUID, now: datetime
) -> dict[str, object]:
    now = require_utc(now)
    account = await session.get(PaperAccountRecord, account_id)
    if account is None or account.capability != "simulation_write_internal":
        raise ValueError("MONITOR_ACCOUNT_UNAVAILABLE")
    calendar = await load_agent_paper_service_calendar_authority(
        session, now=now, expected_calendar_authority_id=calendar_authority_id
    )
    counts = {"checks": 0, "true": 0, "false": 0, "unknown": 0, "expired": 0, "not_effective": 0}
    result: dict[str, object] = {
        "status": "scanned",
        "observed_at": now.isoformat(),
        "model_requests": 0,
        "provider_requests": 0,
        "execution_locked": True,
    }
    if not calendar.market_open_at <= now < calendar.market_close_at:
        return {**result, "status": "market_closed", **counts}
    plans = tuple(
        await session.scalars(
            select(MonitorPlan)
            .where(MonitorPlan.account_id == account_id)
            .distinct(MonitorPlan.instrument_id)
            .order_by(MonitorPlan.instrument_id, MonitorPlan.generation.desc())
            .limit(6)
        )
    )
    if len(plans) > 5:
        raise ValueError("MONITOR_SCOPE_EXCEEDED")
    scheduled = now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
    for selected in plans:
        # Lock in instrument order, then reload: installation may have advanced
        # the generation after the bounded scope query and before this lock.
        await lock_security(session, account_id=account_id, instrument_id=selected.instrument_id)
        plan = await session.scalar(
            select(MonitorPlan)
            .where(
                MonitorPlan.account_id == account_id,
                MonitorPlan.instrument_id == selected.instrument_id,
            )
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        assert plan is not None  # Plans are append-only.
        if now >= plan.valid_until:
            counts["expired"] += 1
            continue
        if now < plan.effective_at:
            counts["not_effective"] += 1
            continue
        observation = await read_latest_price(session, instrument_id=plan.instrument_id, now=now)
        check = await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=scheduled,
            checked_at=now,
            observation=observation,
        )
        counts["checks"] += 1
        counts[check.result] += 1
    return {**result, **counts}

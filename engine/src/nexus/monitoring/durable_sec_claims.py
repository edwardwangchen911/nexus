"""Shared transactional claims; these never grant research or notification permission alone."""

from datetime import datetime, time, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.monitoring.storage import lock_security
from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.sec_monitor import (
    SecAutoIncrementDayClaim,
    SecDiscoveryCheck,
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecMonitorRule,
    SecMonitorRuleState,
    SecNotificationClaim,
)

type DurableFact = (
    SecMonitorRule
    | SecMonitorRuleState
    | SecDiscoveryCheck
    | SecDiscoveryEvent
    | SecDiscoveryEventRevision
    | SecNotificationClaim
    | SecAutoIncrementDayClaim
)
_FACT_TABLES = frozenset(
    cls.__tablename__
    for cls in (
        SecMonitorRule,
        SecMonitorRuleState,
        SecDiscoveryCheck,
        SecDiscoveryEvent,
        SecDiscoveryEventRevision,
        SecNotificationClaim,
        SecAutoIncrementDayClaim,
    )
)


async def audit_fact(session: AsyncSession, row: DurableFact) -> None:
    """Hash the exact persisted SQL JSON representation, not a Python reconstruction."""
    table = row.__tablename__
    if table not in _FACT_TABLES:
        raise ValueError("DURABLE_SEC_FACT_INVALID")
    await session.flush()
    if isinstance(row, SecAutoIncrementDayClaim):
        where = "account_id=:account AND instrument_id=:instrument AND session_date=:day"
        params = dict(account=row.account_id, instrument=row.instrument_id, day=row.session_date)
        row_id = f"{row.account_id}:{row.instrument_id}:{row.session_date.isoformat()}"
    else:
        where, params, row_id = "id=:id", dict(id=row.id), str(row.id)
    result = (
        await session.execute(
            text(
                f"SELECT to_jsonb(f)-'created_at' AS binding, created_at "
                f"FROM {table} f WHERE {where}"
            ),
            params,
        )
    ).one_or_none()
    if (
        result is None
        or not isinstance(result.binding, dict)
        or not isinstance(result.created_at, datetime)
    ):
        raise ValueError("DURABLE_SEC_FACT_INVALID")
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.durable_fact",
        schema_version="durable_sec_v1",
        occurred_at=result.created_at,
        payload={"table": table, "row_id": row_id, "binding": result.binding},
    )


async def _rule_state(
    session: AsyncSession,
    account_id: UUID,
    instrument_id: UUID,
    now: datetime,
) -> tuple[SecMonitorRule | None, bool]:
    await lock_security(session, account_id=account_id, instrument_id=instrument_id)
    rule = await session.scalar(
        select(SecMonitorRule)
        .where(
            SecMonitorRule.account_id == account_id,
            SecMonitorRule.instrument_id == instrument_id,
        )
        .execution_options(populate_existing=True)
    )
    if rule is None:
        return None, False
    state = await session.scalar(
        select(SecMonitorRuleState)
        .where(
            SecMonitorRuleState.rule_id == rule.id,
        )
        .order_by(SecMonitorRuleState.sequence.desc())
        .limit(1)
        .execution_options(populate_existing=True)
    )
    active = (
        rule.created_at <= now
        and rule.effective_at <= now
        and state is not None
        and state.created_at <= now
        and state.effective_at <= now
        and state.state == "active"
    )
    return rule, active


async def claim_notice(
    session: AsyncSession,
    *,
    account_id: UUID,
    instrument_id: UUID,
    event_key: str,
    outbox_id: UUID,
    legacy_trigger_id: UUID | None,
    now: datetime,
) -> bool:
    now = require_utc(now)
    rule, active = await _rule_state(session, account_id, instrument_id, now)
    if rule is None:
        return True
    if not active:
        return False
    old = await session.scalar(
        select(SecNotificationClaim)
        .where(
            SecNotificationClaim.account_id == account_id,
            SecNotificationClaim.instrument_id == instrument_id,
            SecNotificationClaim.event_key == event_key,
        )
        .execution_options(populate_existing=True)
    )
    if old is not None:
        return old.outbox_id == outbox_id and old.legacy_trigger_id == legacy_trigger_id
    prior = await session.scalar(
        select(MonitorTrigger.id)
        .where(
            MonitorTrigger.account_id == account_id,
            MonitorTrigger.instrument_id == instrument_id,
            MonitorTrigger.kind == "disclosure",
            MonitorTrigger.event_key == event_key,
        )
        .limit(1)
    )
    if prior is not None:
        return False
    event = await session.scalar(
        select(SecDiscoveryEvent)
        .where(
            SecDiscoveryEvent.account_id == account_id,
            SecDiscoveryEvent.instrument_id == instrument_id,
            SecDiscoveryEvent.event_key == event_key,
        )
        .execution_options(populate_existing=True)
    )
    if event is None:
        return False
    conflict = await session.scalar(
        select(SecDiscoveryEventRevision.id)
        .where(
            SecDiscoveryEventRevision.event_id == event.id,
        )
        .limit(1)
    )
    if conflict is not None:
        return False
    row = SecNotificationClaim(
        id=new_uuid7(),
        account_id=account_id,
        instrument_id=instrument_id,
        event_key=event_key,
        outbox_id=outbox_id,
        legacy_trigger_id=legacy_trigger_id,
        created_at=now,
    )
    session.add(row)
    await audit_fact(session, row)
    return True


async def claim_increment_day(
    session: AsyncSession,
    member: MonitorIncrement,
    *,
    calendar_authority_id: UUID,
    now: datetime,
) -> SecAutoIncrementDayClaim | None:
    now = require_utc(now)
    rule, active = await _rule_state(session, member.account_id, member.instrument_id, now)
    if rule is None:
        return None
    if not active:
        raise ValueError("DURABLE_SEC_RULE_INACTIVE")
    calendar = await load_agent_paper_service_calendar_authority(
        session,
        now=member.research_as_of,
        expected_calendar_authority_id=calendar_authority_id,
    )
    ny = ZoneInfo("America/New_York")
    day = require_utc(member.research_as_of).astimezone(ny).date()
    if (
        member.id is None
        or member.research_as_of > now
        or calendar.session != day
        or member.session_date != day
        or not calendar.market_open_at <= member.research_as_of < calendar.market_close_at
    ):
        raise ValueError("DURABLE_SEC_SESSION_INVALID")
    old = await session.get(
        SecAutoIncrementDayClaim,
        (member.account_id, member.instrument_id, day),
        populate_existing=True,
    )
    if old is not None:
        if (old.increment_id, old.run_id, old.calendar_authority_id) == (
            member.id,
            member.run_id,
            calendar_authority_id,
        ):
            return old
        raise ValueError("DURABLE_SEC_DAY_ALREADY_CLAIMED")
    start = datetime.combine(day, time.min, tzinfo=ny)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=ny)
    prior = await session.scalar(
        select(MonitorIncrement.id)
        .where(
            MonitorIncrement.account_id == member.account_id,
            MonitorIncrement.instrument_id == member.instrument_id,
            or_(
                MonitorIncrement.session_date == day,
                (MonitorIncrement.research_as_of >= start)
                & (MonitorIncrement.research_as_of < end),
            ),
        )
        .limit(1)
    )
    if prior is not None:
        raise ValueError("DURABLE_SEC_DAY_ALREADY_CLAIMED")
    row = SecAutoIncrementDayClaim(
        account_id=member.account_id,
        instrument_id=member.instrument_id,
        session_date=day,
        calendar_authority_id=calendar_authority_id,
        increment_id=member.id,
        run_id=member.run_id,
        created_at=now,
    )
    session.add(row)
    await audit_fact(session, row)
    return row

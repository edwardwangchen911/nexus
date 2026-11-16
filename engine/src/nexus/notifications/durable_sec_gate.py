"""Pause-aware notice delivery; only security/rule locks span the bounded HTTP send."""

from sqlalchemy import String, and_, exists, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement

from nexus.monitoring.storage import lock_security
from nexus.notifications.durable_sec import DURABLE_SEC_EVENT, DurableSecNotification
from nexus.outbox.models import OutboxMessage, Publisher
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.runtime import OutboxEvent
from nexus.persistence.models.sec_monitor import (
    SecMonitorRule,
    SecMonitorRuleState,
    SecNotificationClaim,
)


def notice_claim_condition() -> ColumnElement[bool]:
    latest = (
        select(SecMonitorRuleState.state)
        .where(SecMonitorRuleState.rule_id == SecMonitorRule.id)
        .order_by(SecMonitorRuleState.sequence.desc())
        .limit(1)
        .correlate(SecMonitorRule)
        .scalar_subquery()
    )
    scope = and_(
        SecMonitorRule.account_id == SecNotificationClaim.account_id,
        SecMonitorRule.instrument_id == SecNotificationClaim.instrument_id,
    )
    claimed = exists(
        select(SecNotificationClaim.id)
        .where(SecNotificationClaim.outbox_id == OutboxEvent.id)
        .correlate(OutboxEvent)
    )
    blocked = exists(
        select(SecNotificationClaim.id)
        .join(SecMonitorRule, scope)
        .where(
            SecNotificationClaim.outbox_id == OutboxEvent.id,
            or_(latest.is_(None), latest != "active"),
        )
        .correlate(OutboxEvent)
    )
    legacy_scope = exists(
        select(MonitorTrigger.id)
        .join(
            SecMonitorRule,
            and_(
                SecMonitorRule.account_id == MonitorTrigger.account_id,
                SecMonitorRule.instrument_id == MonitorTrigger.instrument_id,
            ),
        )
        .where(MonitorTrigger.outbox_id == OutboxEvent.id, MonitorTrigger.kind == "disclosure")
        .correlate(OutboxEvent)
    )
    durable = and_(
        OutboxEvent.event_type == DURABLE_SEC_EVENT[0],
        OutboxEvent.schema_version == DURABLE_SEC_EVENT[1],
    )
    exact = exists(
        select(SecNotificationClaim.id)
        .join(SecMonitorRule, scope)
        .where(
            SecNotificationClaim.outbox_id == OutboxEvent.id,
            SecNotificationClaim.event_key == OutboxEvent.payload["event_key"].astext,
            SecMonitorRule.id.cast(String) == OutboxEvent.payload["rule_id"].astext,
            SecNotificationClaim.instrument_id.cast(String)
            == OutboxEvent.payload["instrument_id"].astext,
            SecNotificationClaim.legacy_trigger_id.is_(None),
            latest == "active",
        )
        .correlate(OutboxEvent)
    )
    return and_(~blocked, or_(and_(durable, exact), and_(~durable, or_(claimed, ~legacy_scope))))


class DurableSecPublisher:
    def __init__(self, factory: async_sessionmaker[AsyncSession], publisher: Publisher):
        self.factory, self.publisher = factory, publisher

    async def __call__(self, message: OutboxMessage) -> None:
        async with self.factory() as session, session.begin():
            if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
                raise ValueError("DURABLE_SEC_NOTICE_ISOLATION_INVALID")
            claims = tuple(
                await session.scalars(
                    select(SecNotificationClaim).where(
                        SecNotificationClaim.outbox_id == message.event_id
                    )
                )
            )
            triggers = tuple(
                await session.scalars(
                    select(MonitorTrigger).where(
                        MonitorTrigger.outbox_id == message.event_id,
                        MonitorTrigger.kind == "disclosure",
                    )
                )
            )
            scopes = {(r.account_id, r.instrument_id) for r in claims}
            scopes.update((r.account_id, r.instrument_id) for r in triggers)
            for account_id, instrument_id in sorted(scopes, key=lambda pair: tuple(map(str, pair))):
                await lock_security(session, account_id=account_id, instrument_id=instrument_id)
                rule = await session.scalar(
                    select(SecMonitorRule).where(
                        SecMonitorRule.account_id == account_id,
                        SecMonitorRule.instrument_id == instrument_id,
                    )
                )
                if rule is not None:
                    await session.execute(
                        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,0))"),
                        {"scope": f"sec-durable-rule:{rule.id}"},
                    )
            row = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.id == message.event_id,
                    OutboxEvent.status == "delivering",
                    OutboxEvent.available_at > text("pg_catalog.clock_timestamp()"),
                    notice_claim_condition(),
                )
                .execution_options(populate_existing=True)
            )
            if row is None or (
                row.event_type,
                row.schema_version,
                row.aggregate_id,
                row.payload,
            ) != (
                message.event_type,
                message.schema_version,
                message.aggregate_id,
                message.payload,
            ):
                raise ValueError("DURABLE_SEC_NOTICE_NOT_ACTIVE")
            if (message.event_type, message.schema_version) == DURABLE_SEC_EVENT:
                value = DurableSecNotification.model_validate(message.payload)
                if value.discovery_id != message.aggregate_id:
                    raise ValueError("DURABLE_SEC_NOTICE_INVALID")
            # No account or budget lock is acquired here. Pause takes the same
            # security/rule locks, so its commit orders before or after this send.
            await self.publisher(message)

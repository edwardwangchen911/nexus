"""Pause-aware notice delivery; only security/rule locks span the bounded HTTP send."""

from sqlalchemy import String, and_, exists, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement

from nexus.monitoring.storage import lock_security
from nexus.notifications.durable_sec import DURABLE_SEC_EVENT, DurableSecNotification
from nexus.outbox.models import OutboxMessage, Publisher
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from nexus.persistence.models.sec_conflicts import SecDisclosureKeyConflict
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
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
            ~exists(
                select(SecDiscoveryEventRevision.id)
                .where(SecDiscoveryEventRevision.event_id == OutboxEvent.aggregate_id)
                .correlate(OutboxEvent)
            ),
        )
        .correlate(OutboxEvent)
    )
    conflicted_claim = exists(
        select(SecNotificationClaim.id)
        .join(
            SecDisclosureKeyConflict,
            and_(
                SecDisclosureKeyConflict.account_id == SecNotificationClaim.account_id,
                SecDisclosureKeyConflict.instrument_id == SecNotificationClaim.instrument_id,
                SecDisclosureKeyConflict.event_key == SecNotificationClaim.event_key,
            ),
        )
        .where(SecNotificationClaim.outbox_id == OutboxEvent.id)
        .correlate(OutboxEvent)
    )
    conflicted_legacy = exists(
        select(MonitorTrigger.id)
        .join(
            SecDisclosureKeyConflict,
            and_(
                SecDisclosureKeyConflict.account_id == MonitorTrigger.account_id,
                SecDisclosureKeyConflict.instrument_id == MonitorTrigger.instrument_id,
                SecDisclosureKeyConflict.event_key == MonitorTrigger.event_key,
            ),
        )
        .where(MonitorTrigger.outbox_id == OutboxEvent.id, MonitorTrigger.kind == "disclosure")
        .correlate(OutboxEvent)
    )
    legacy_revision = exists(
        select(MonitorTrigger.id)
        .join(
            SecDiscoveryEvent,
            and_(
                SecDiscoveryEvent.account_id == MonitorTrigger.account_id,
                SecDiscoveryEvent.instrument_id == MonitorTrigger.instrument_id,
                SecDiscoveryEvent.event_key == MonitorTrigger.event_key,
            ),
        )
        .join(SecDiscoveryEventRevision, SecDiscoveryEventRevision.event_id == SecDiscoveryEvent.id)
        .where(MonitorTrigger.outbox_id == OutboxEvent.id, MonitorTrigger.kind == "disclosure")
        .correlate(OutboxEvent)
    )
    claim_revision = exists(
        select(SecNotificationClaim.id)
        .join(
            SecDiscoveryEvent,
            and_(
                SecDiscoveryEvent.account_id == SecNotificationClaim.account_id,
                SecDiscoveryEvent.instrument_id == SecNotificationClaim.instrument_id,
                SecDiscoveryEvent.event_key == SecNotificationClaim.event_key,
            ),
        )
        .join(SecDiscoveryEventRevision, SecDiscoveryEventRevision.event_id == SecDiscoveryEvent.id)
        .where(SecNotificationClaim.outbox_id == OutboxEvent.id)
        .correlate(OutboxEvent)
    )
    return and_(
        ~blocked,
        ~conflicted_claim,
        ~conflicted_legacy,
        ~legacy_revision,
        ~claim_revision,
        or_(and_(durable, exact), and_(~durable, or_(claimed, ~legacy_scope))),
    )


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
            protected = (message.event_type, message.schema_version) == DURABLE_SEC_EVENT
            for account_id, instrument_id in sorted(scopes, key=lambda pair: tuple(map(str, pair))):
                await lock_security(session, account_id=account_id, instrument_id=instrument_id)
                rule = await session.scalar(
                    select(SecMonitorRule).where(
                        SecMonitorRule.account_id == account_id,
                        SecMonitorRule.instrument_id == instrument_id,
                    )
                )
                if rule is not None:
                    protected = True
                    await session.execute(
                        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,0))"),
                        {"scope": f"sec-durable-rule:{rule.id}"},
                    )
            if protected and message.delivery is None:
                raise ValueError("DURABLE_SEC_NOTICE_NOT_ACTIVE")
            query = select(OutboxEvent)
            if message.delivery is not None:
                delivery = message.delivery
                query = query.join(
                    OutboxDeliveryAttempt, OutboxDeliveryAttempt.outbox_event_id == OutboxEvent.id
                ).where(
                    OutboxEvent.attempt_count == delivery.attempt_no,
                    OutboxEvent.available_at == delivery.expires_at,
                    OutboxDeliveryAttempt.attempt_no == delivery.attempt_no,
                    OutboxDeliveryAttempt.worker_id == delivery.worker_id,
                    OutboxDeliveryAttempt.lease_token == delivery.lease_token,
                    OutboxDeliveryAttempt.outcome == "delivering",
                    OutboxDeliveryAttempt.finished_at.is_(None),
                )
            row = await session.scalar(
                query.where(
                    OutboxEvent.id == message.event_id,
                    OutboxEvent.status == "delivering",
                    OutboxEvent.available_at > text("pg_catalog.clock_timestamp()"),
                    notice_claim_condition(),
                )
                .with_for_update(of=OutboxEvent)
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

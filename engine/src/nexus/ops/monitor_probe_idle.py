"""Recheck durable monitor activity under the same account lock used for new claims."""

from collections import defaultdict
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.ops import monitor_batch_prepare as material
from nexus.ops import monitor_disclosure_refresh as disclosure
from nexus.ops import monitor_market_prepare as market
from nexus.ops import monitor_price_refresh as price
from nexus.persistence.models.monitoring import MonitorObservationBatch, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent

INVALID = "HOST_READ_PROBE_STATE_INVALID"


async def require_monitor_probe_idle(session: AsyncSession, account: UUID) -> None:
    if (
        await session.scalar(
            select(MonitorTrigger.id)
            .where(
                MonitorTrigger.account_id == account,
                MonitorTrigger.status.in_(("admitted", "unknown")),
            )
            .limit(1)
        )
        is not None
    ):
        raise ValueError(INVALID)
    items = await session.scalars(
        select(AuditEvent)
        .where(
            AuditEvent.payload["account_id"].astext == str(account),
            AuditEvent.action.in_(
                (
                    price._ACTION,
                    market._ACTION,
                    material.CLAIM,
                    material.RESULT,
                    disclosure.CLAIM,
                    disclosure.RESULT,
                )
            ),
        )
        .order_by(AuditEvent.event_seq)
    )
    by_price: dict[str, list[AuditEvent]] = defaultdict(list)
    by_market: dict[str, list[AuditEvent]] = defaultdict(list)
    by_disclosure: dict[str, list[AuditEvent]] = defaultdict(list)
    batches: set[UUID] = set()
    for item in items:
        payload = item.payload
        if item.action == price._ACTION:
            by_price[str(payload.get("scheduled_at"))].append(item)
        elif item.action == market._ACTION:
            by_market[str(payload.get("batch_id"))].append(item)
        elif item.action in (material.CLAIM, material.RESULT):
            if item.action == material.RESULT and payload.get("status") == "unknown":
                raise ValueError(INVALID)
            batches.add(UUID(str(payload["batch_id"])))
        else:
            by_disclosure[str(payload.get("operation_id"))].append(item)
    for events in by_price.values():
        result = price._prior(tuple(events))
        if (
            result is None
            or result.get("status") not in {"prepared", "blocked"}
            or not isinstance(result.get("provider_requests"), int)
        ):
            raise ValueError(INVALID)
    for events in by_market.values():
        if market._history_state(tuple(events), account_id=account) not in {"prepared", "blocked"}:
            raise ValueError(INVALID)
    for batch_id in batches:
        batch = await session.get(MonitorObservationBatch, batch_id)
        if batch is None or batch.account_id != account:
            raise ValueError(INVALID)
        open_claim, latest = await material._history(session, batch)
        if (
            open_claim
            or latest is None
            or latest.get("status") not in {"prepared", "blocked", "preparing"}
        ):
            raise ValueError(INVALID)
    for events in by_disclosure.values():
        if len(events) != 2:
            raise ValueError(INVALID)
        claim, terminal = events
        if (
            claim.action != disclosure.CLAIM
            or terminal.action != disclosure.RESULT
            or any(terminal.payload.get(k) != v for k, v in claim.payload.items())
            or terminal.payload.get("status") not in {"refreshed", "failed"}
            or not isinstance(terminal.payload.get("provider_requests"), int)
        ):
            raise ValueError(INVALID)
        for event in events:
            canonical = canonical_audit_event(
                AuditChainInput(
                    event_seq=event.event_seq,
                    occurred_at=event.occurred_at,
                    actor_id=event.actor_id,
                    action=event.action,
                    schema_version=event.schema_version,
                    payload=event.payload,
                )
            )
            if (
                event.actor_id != "research-monitor"
                or event.schema_version != disclosure.SCHEMA
                or event.canonical_event != canonical
                or event.event_hash != audit_event_hash(event.previous_hash, canonical)
            ):
                raise ValueError(INVALID)

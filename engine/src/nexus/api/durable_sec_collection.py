"""Project an existing SEC collection receipt without fetching or interpreting filings."""

from datetime import datetime, timedelta
from re import fullmatch
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.durable_sec import DurableSecCollectionView
from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.models.sec_monitor import SecMonitorRule
from nexus.research.durable_sec_index_storage import SCHEMA as CAPTURE_SCHEMA
from nexus.research.durable_sec_index_storage import SOURCE as CAPTURE_SOURCE


async def read_latest_sec_collection(
    session: AsyncSession, rule: SecMonitorRule, now: datetime
) -> DurableSecCollectionView | None:
    from nexus.ops.monitor_disclosure_refresh import CLAIM, FEED, RESULT, SCHEMA

    visible = (
        AuditEvent.actor_id == "research-monitor",
        AuditEvent.schema_version == SCHEMA,
        AuditEvent.created_at <= now,
        AuditEvent.occurred_at <= now,
    )
    claim = await session.scalar(
        select(AuditEvent)
        .where(
            *visible,
            AuditEvent.action == CLAIM,
            AuditEvent.occurred_at >= rule.effective_at,
            AuditEvent.payload["rule_id"].astext == str(rule.id),
            AuditEvent.payload["account_id"].astext == str(rule.account_id),
            AuditEvent.payload["instrument_id"].astext == str(rule.instrument_id),
            AuditEvent.payload["issuer_cik"].astext == rule.issuer_cik,
            AuditEvent.payload["feed"].astext == FEED,
            AuditEvent.payload["durable_sec"].astext == "true",
        )
        .order_by(AuditEvent.event_seq.desc())
        .limit(1)
    )
    if claim is None or claim.payload.get("durable_sec") is not True:
        return None
    try:
        scheduled = require_utc(datetime.fromisoformat(str(claim.payload["scheduled_at"])))
        operation = UUID(str(claim.payload["operation_id"]))
        identity = UUID(str(claim.payload["identity_snapshot_id"]))
    except (KeyError, ValueError):
        return None
    if (
        scheduled.minute % 5
        or scheduled.second
        or scheduled.microsecond
        or not scheduled <= claim.occurred_at < scheduled + timedelta(minutes=5)
    ):
        return None
    unknown = DurableSecCollectionView(
        scheduled_at=scheduled, claimed_at=claim.occurred_at, finished_at=None, status="unknown"
    )
    markers = (
        await session.scalars(
            select(AuditEvent)
            .where(
                *visible,
                AuditEvent.action.in_((CLAIM, RESULT)),
                AuditEvent.payload["operation_id"].astext == str(operation),
            )
            .order_by(AuditEvent.event_seq)
            .limit(3)
        )
    ).all()
    if len(markers) != 2 or markers[0].id != claim.id or markers[1].action != RESULT:
        return unknown
    receipt = markers[1]
    if receipt.occurred_at < claim.occurred_at or any(
        receipt.payload.get(key) != value for key, value in claim.payload.items()
    ):
        return unknown
    if (
        receipt.payload.get("status") == "failed"
        and receipt.payload.get("reason") == "SEC_INDEX_REFRESH_FAILED"
    ):
        return unknown.model_copy(update={"status": "failed", "finished_at": receipt.occurred_at})
    if receipt.payload.get("status") != "refreshed":
        return unknown
    try:
        snapshot_id = UUID(str(receipt.payload["snapshot_id"]))
        revision = receipt.payload["snapshot_hash"]
    except (KeyError, ValueError):
        return unknown
    if not isinstance(revision, str) or fullmatch(r"[0-9a-f]{64}", revision) is None:
        return unknown
    index = DataSnapshot.payload["index"]
    archived = await session.scalar(
        select(DataSnapshot.id).where(
            DataSnapshot.id == snapshot_id,
            DataSnapshot.source == CAPTURE_SOURCE,
            DataSnapshot.schema_version == CAPTURE_SCHEMA,
            DataSnapshot.revision == revision,
            DataSnapshot.created_at <= receipt.occurred_at,
            DataSnapshot.available_at <= receipt.occurred_at,
            DataSnapshot.observed_at <= receipt.occurred_at,
            DataSnapshot.received_at <= receipt.occurred_at,
            index["instrument_id"].astext == str(rule.instrument_id),
            index["issuer_cik"].astext == rule.issuer_cik,
            index["identity_snapshot_id"].astext == str(identity),
        )
    )
    if archived is None:
        return unknown
    return unknown.model_copy(update={"status": "archived", "finished_at": receipt.occurred_at})

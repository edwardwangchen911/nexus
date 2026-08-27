"""Bounded deterministic audit export over an allowlist-only projection."""

from __future__ import annotations

import gzip
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from sqlalchemy import not_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from nexus.audit.canonical import canonical_json
from nexus.audit.redaction import (
    AuditExportSource,
    AuditKind,
    redact_audit_event,
)
from nexus.persistence.models.runtime import AuditEvent

_MAX_ROWS = 100_000
_KIND_PREFIXES: dict[AuditKind, tuple[str, ...]] = {
    "quality": ("quality.", "source."),
    "risk": ("risk.", "paper.risk.", "drawdown."),
    "agent_cost": ("agent.budget.", "agent.cost."),
    "job": ("job.", "outbox."),
    "reconciliation": ("reconciliation.", "broker.reconciliation."),
    "evolution": ("strategy.",),
    "audit": (),
}


class AuditExportLimitError(RuntimeError):
    """The export would exceed its explicit row bound."""


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must use canonical UTC")
    return value.astimezone(UTC)


def audit_kind_condition(kind: AuditKind) -> ColumnElement[bool]:
    all_prefixes = tuple(
        prefix
        for current_kind, prefixes in _KIND_PREFIXES.items()
        if current_kind != "audit"
        for prefix in prefixes
    )
    if kind == "audit":
        return not_(or_(*(AuditEvent.action.startswith(prefix) for prefix in all_prefixes)))
    return or_(
        *(AuditEvent.action.startswith(prefix) for prefix in _KIND_PREFIXES[kind])
    )


def redacted_audit_document(event: AuditEvent) -> dict[str, object]:
    return redact_audit_event(
        AuditExportSource(
            event_seq=event.event_seq,
            occurred_at=event.occurred_at,
            actor_id=event.actor_id,
            action=event.action,
            schema_version=event.schema_version,
            payload=event.payload,
            previous_hash=event.previous_hash,
            event_hash=event.event_hash,
        )
    )


def render_redacted_audit_gzip(events: Sequence[AuditEvent]) -> bytes:
    """Render ordered source rows as deterministic UTF-8 gzip NDJSON."""
    body = b"".join(
        (canonical_json(redacted_audit_document(event)) + "\n").encode("utf-8")
        for event in events
    )
    return gzip.compress(body, compresslevel=9, mtime=0)


async def export_audit_ndjson_gzip(
    session: AsyncSession,
    *,
    from_at: datetime,
    to_at: datetime,
    kind: AuditKind | None = None,
    max_rows: int = _MAX_ROWS,
) -> bytes:
    """Export a half-open, at-most-31-day range without private payload fields."""
    from_at = _utc(from_at, field="from_at")
    to_at = _utc(to_at, field="to_at")
    if to_at <= from_at:
        raise ValueError("audit export range must be increasing")
    if to_at - from_at > timedelta(days=31):
        raise ValueError("audit export range cannot exceed 31 days")
    if not 1 <= max_rows <= _MAX_ROWS:
        raise ValueError("audit export max_rows is invalid")
    conditions = [
        AuditEvent.occurred_at >= from_at,
        AuditEvent.occurred_at < to_at,
    ]
    if kind is not None:
        if kind not in _KIND_PREFIXES:
            raise ValueError("audit export kind is invalid")
        conditions.append(audit_kind_condition(kind))
    events = tuple(
        (
            await session.scalars(
                select(AuditEvent)
                .where(*conditions)
                .order_by(AuditEvent.event_seq)
                .limit(max_rows + 1)
            )
        ).all()
    )
    if len(events) > max_rows:
        raise AuditExportLimitError("audit export row limit exceeded")
    return render_redacted_audit_gzip(events)

"""Immutable, unactivated economic protocols; no account, budget or order creation."""

from __future__ import annotations

import re
from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.paper.portfolio_economic_preflight import EconomicPlan, EconomicPlanRequest, compile_plan
from nexus.persistence.base import Base
from nexus.persistence.models.runtime import AuditEvent

PROTOCOL_ACTION = "portfolio.economic.protocol_registered"
PROTOCOL_SCHEMA = "portfolio_economic_protocol_v1"
_ACTOR = "local-economic-protocol-cli"
_ISOLATED_DATABASE = re.compile(
    r"^(nexus_test_[0-9a-f]{32}|nexus_diag_economic_protocol_[a-z0-9_]+)$"
)


async def require_isolated_database(session: AsyncSession, *, expected: str) -> None:
    actual = await session.scalar(text("SELECT current_database()"))
    if actual != expected or not _ISOLATED_DATABASE.fullmatch(expected):
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    for name in (
        "agent_paper_cycles",
        "agent_research_runs",
        "agent_portfolio_synthesis_runs",
        "jobs",
        "paper_orders",
        "broker_account_bindings",
        "account_external_id_envelopes",
    ):
        if await session.scalar(select(func.count()).select_from(Base.metadata.tables[name])):
            raise ValueError("ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN")


def decode_protocol(event: AuditEvent) -> EconomicPlan:
    """Verify stored frozen facts and audit payload, never recompile old slots."""
    if (
        event.action != PROTOCOL_ACTION
        or event.schema_version != PROTOCOL_SCHEMA
        or event.actor_id != _ACTOR
    ):
        raise ValueError("ECONOMIC_PROTOCOL_EVIDENCE_INVALID")
    plan = EconomicPlan.model_validate(event.payload)
    from nexus.paper.portfolio_economic_preflight import _hash

    body = plan.model_dump(
        mode="json",
        exclude={"schema_version", "status", "economic_status", "execution_locked", "content_hash"},
    )
    if _hash(body) != plan.content_hash:
        raise ValueError("ECONOMIC_PROTOCOL_EVIDENCE_INVALID")
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
        canonical != event.canonical_event
        or audit_event_hash(event.previous_hash, canonical) != event.event_hash
    ):
        raise ValueError("ECONOMIC_PROTOCOL_EVIDENCE_INVALID")
    if len(plan.slots) != 24 or event.occurred_at >= plan.slots[0].decision_at:
        raise ValueError("ECONOMIC_PROTOCOL_EVIDENCE_INVALID")
    return plan


async def register_protocol(
    session: AsyncSession,
    request: EconomicPlanRequest,
    *,
    expected_database: str,
) -> AuditEvent:
    await require_isolated_database(session, expected=expected_database)
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock(hashtextextended('portfolio-economic-protocol-v1', 2026))"
        )
    )
    events = tuple(
        await session.scalars(
            select(AuditEvent).where(AuditEvent.action == PROTOCOL_ACTION).limit(2)
        )
    )
    if events:
        if len(events) != 1:
            raise ValueError("ECONOMIC_PROTOCOL_EVIDENCE_INVALID")
        existing = decode_protocol(events[0])
        if existing.request != request:
            raise ValueError("ECONOMIC_PROTOCOL_IDEMPOTENCY_CONFLICT")
        return events[0]
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    plan = compile_plan(request, now=now)
    return await append_audit_event(
        session,
        actor_id=_ACTOR,
        action=PROTOCOL_ACTION,
        schema_version=PROTOCOL_SCHEMA,
        payload=plan.model_dump(mode="json"),
        occurred_at=now,
    )


async def read_protocol(
    session: AsyncSession, *, as_of: datetime
) -> tuple[str, AuditEvent | None, EconomicPlan | None]:
    events = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == PROTOCOL_ACTION,
                AuditEvent.occurred_at <= as_of,
                AuditEvent.created_at <= as_of,
            )
            .limit(2)
        )
    )
    if not events:
        return "not_implemented", None, None
    try:
        if len(events) != 1:
            raise ValueError("multiple protocols")
        return "registered_not_activated", events[0], decode_protocol(events[0])
    except ValueError:
        return "unavailable", None, None

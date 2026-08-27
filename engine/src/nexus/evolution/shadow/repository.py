"""Locked reads and append-only event writes for Strategy Shadow."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.backtest.engine import exchange_calendar_content_hash
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.evolution import StrategyExperimentEventRecord


async def load_exact_calendar_sessions(
    session: AsyncSession,
    *,
    calendar_authority_id: UUID,
    after: date,
) -> tuple[date, ...]:
    calendar = await session.get(ExchangeCalendarAuthorityRecord, calendar_authority_id)
    rows = tuple(
        await session.scalars(
            select(ExchangeCalendarSessionRecord)
            .where(ExchangeCalendarSessionRecord.calendar_authority_id == calendar_authority_id)
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    if calendar is None or len(rows) < 68:
        raise ValueError("STRATEGY_SHADOW_CALENDAR_INCOMPLETE")
    all_sessions = tuple(item.session for item in rows)
    decision_times = {item.session: item.decision_at for item in rows}
    if (
        calendar.completeness_status != "complete"
        or all_sessions[0] != calendar.range_start
        or all_sessions[-1] != calendar.range_end
        or exchange_calendar_content_hash(all_sessions, decision_times) != calendar.content_hash
    ):
        raise ValueError("STRATEGY_SHADOW_CALENDAR_INVALID")
    window = tuple(item for item in all_sessions if item > after)[:68]
    if len(window) != 68:
        raise ValueError("STRATEGY_SHADOW_CALENDAR_INCOMPLETE")
    return window


async def append_strategy_experiment_event(
    session: AsyncSession,
    *,
    family_id: UUID,
    event_type: str,
    idempotency_key: str,
    occurred_at: datetime,
    payload: dict[str, object],
) -> StrategyExperimentEventRecord:
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-experiment-event:' || :family_id, 0))"
        ),
        {"family_id": str(family_id)},
    )
    existing = await session.scalar(
        select(StrategyExperimentEventRecord).where(
            StrategyExperimentEventRecord.idempotency_key == idempotency_key
        )
    )
    if existing is not None:
        return existing
    prior = await session.scalar(
        select(StrategyExperimentEventRecord)
        .where(StrategyExperimentEventRecord.family_id == family_id)
        .order_by(StrategyExperimentEventRecord.sequence.desc())
        .limit(1)
    )
    sequence = 1 if prior is None else prior.sequence + 1
    previous_hash = "0" * 64 if prior is None else prior.event_hash
    identity = {
        "schema_version": "strategy_experiment_event_v1",
        "family_id": str(family_id),
        "sequence": sequence,
        "event_type": event_type,
        "idempotency_key": idempotency_key,
        "actor_kind": "service",
        "occurred_at": occurred_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "payload": payload,
        "previous_hash": previous_hash,
    }
    event = StrategyExperimentEventRecord(
        id=new_uuid7(),
        family_id=family_id,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind="service",
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=previous_hash,
        event_hash=hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
        created_at=occurred_at,
    )
    session.add(event)
    await session.flush()
    return event

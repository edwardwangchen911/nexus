"""Append-only repository helpers for bounded strategy-evolution admission."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyExperimentEventRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyRegistryHeadRecord,
)

_ZERO_HASH = "0" * 64


async def find_family_by_idempotency(
    session: AsyncSession, idempotency_key: str
) -> StrategyExperimentFamilyRecord | None:
    return cast(
        StrategyExperimentFamilyRecord | None,
        await session.scalar(
            select(StrategyExperimentFamilyRecord).where(
                StrategyExperimentFamilyRecord.idempotency_key == idempotency_key
            )
        ),
    )


async def lock_internal_paper_registry_head(
    session: AsyncSession,
) -> StrategyRegistryHeadRecord | None:
    return cast(
        StrategyRegistryHeadRecord | None,
        await session.scalar(
            select(StrategyRegistryHeadRecord)
            .where(StrategyRegistryHeadRecord.channel == "internal_paper")
            .with_for_update()
        ),
    )


async def active_lineage_projection(
    session: AsyncSession, lineage_id: UUID
) -> StrategyExperimentStateProjectionRecord | None:
    terminal_states = (
        "REJECTED",
        "BACKTEST_FAILED",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    )
    return cast(
        StrategyExperimentStateProjectionRecord | None,
        await session.scalar(
            select(StrategyExperimentStateProjectionRecord)
            .where(
                StrategyExperimentStateProjectionRecord.research_lineage_id
                == lineage_id,
                StrategyExperimentStateProjectionRecord.current_state.not_in(
                    terminal_states
                ),
            )
            .with_for_update()
        ),
    )


async def latest_family_created_at(
    session: AsyncSession, lineage_id: UUID
) -> datetime | None:
    return cast(
        datetime | None,
        await session.scalar(
            select(func.max(StrategyExperimentFamilyRecord.created_at)).where(
                StrategyExperimentFamilyRecord.research_lineage_id == lineage_id
            )
        ),
    )


async def cumulative_lineage_trial_count(session: AsyncSession, lineage_id: UUID) -> int:
    value = await session.scalar(
        select(func.max(StrategyExperimentStateProjectionRecord.cumulative_oos_trial_count)).where(
            StrategyExperimentStateProjectionRecord.research_lineage_id == lineage_id
        )
    )
    return int(value or 0)


async def publish_challenger_definition(
    session: AsyncSession,
    *,
    document: Mapping[str, object],
    expression_hash: str,
    published_at: datetime,
) -> StrategyDefinitionRecord:
    strategy_version = document.get("version")
    if not isinstance(strategy_version, str):
        raise ValueError("candidate strategy version is missing")
    existing = await session.scalar(
        select(StrategyDefinitionRecord).where(
            StrategyDefinitionRecord.strategy_version == strategy_version
        )
    )
    normalized = dict(document)
    if existing is not None:
        if (
            existing.document != normalized
            or existing.expression_hash != expression_hash
            or existing.schema_version != "strategy_dsl_v1"
            or existing.lifecycle != "challenger"
        ):
            raise ValueError("candidate strategy identity conflicts with an immutable definition")
        return existing
    record = StrategyDefinitionRecord(
        id=new_uuid7(),
        strategy_version=strategy_version,
        schema_version="strategy_dsl_v1",
        expression_hash=expression_hash,
        document=normalized,
        lifecycle="challenger",
        published_at=published_at,
        created_at=published_at,
    )
    session.add(record)
    await session.flush()
    return record


async def append_admitted_event(
    session: AsyncSession,
    *,
    family_id: UUID,
    idempotency_key: str,
    occurred_at: datetime,
    payload: dict[str, object],
) -> StrategyExperimentEventRecord:
    identity = {
        "schema_version": "strategy_experiment_event_v1",
        "family_id": str(family_id),
        "sequence": 1,
        "event_type": "ADMITTED",
        "idempotency_key": idempotency_key,
        "actor_kind": "service",
        "occurred_at": occurred_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "payload": payload,
        "previous_hash": _ZERO_HASH,
    }
    event = StrategyExperimentEventRecord(
        id=new_uuid7(),
        family_id=family_id,
        schema_version="strategy_experiment_event_v1",
        sequence=1,
        event_type="ADMITTED",
        idempotency_key=idempotency_key,
        actor_kind="service",
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=_ZERO_HASH,
        event_hash=hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
        created_at=occurred_at,
    )
    session.add(event)
    await session.flush()
    return event

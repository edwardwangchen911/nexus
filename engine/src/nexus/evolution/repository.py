"""Append-only repository helpers for bounded strategy-evolution admission."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyExperimentCandidateProjectionRecord,
    StrategyExperimentEventRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageProjectionRecord,
)

_ZERO_HASH = "0" * 64

CandidateState = Literal[
    "ADMITTED",
    "VALIDATION_FAILED",
    "VALIDATION_PASSED",
    "OOS_NOT_SELECTED",
    "OOS_SELECTED",
    "OOS_RESERVED",
    "BACKTEST_FAILED",
    "BACKTEST_PASSED",
    "SHADOW_RUNNING",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTION_ELIGIBLE",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
]
FamilyAggregateState = Literal[
    "ADMITTED",
    "BACKTEST_RUNNING",
    "BACKTEST_FAILED",
    "BACKTEST_PASSED",
    "SHADOW_RUNNING",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTION_ELIGIBLE",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
]

_CANDIDATE_STATES = frozenset(
    {
        "ADMITTED",
        "VALIDATION_FAILED",
        "VALIDATION_PASSED",
        "OOS_NOT_SELECTED",
        "OOS_SELECTED",
        "OOS_RESERVED",
        "BACKTEST_FAILED",
        "BACKTEST_PASSED",
        "SHADOW_RUNNING",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTION_ELIGIBLE",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    }
)
_CANDIDATE_TRANSITIONS = {
    "ADMITTED": frozenset(
        {"VALIDATION_FAILED", "VALIDATION_PASSED", "VETOED", "STALE_CHAMPION_BASELINE"}
    ),
    "VALIDATION_PASSED": frozenset(
        {"OOS_NOT_SELECTED", "OOS_SELECTED", "VETOED", "STALE_CHAMPION_BASELINE"}
    ),
    "OOS_SELECTED": frozenset({"OOS_RESERVED", "VETOED", "STALE_CHAMPION_BASELINE"}),
    "OOS_RESERVED": frozenset(
        {"BACKTEST_FAILED", "BACKTEST_PASSED", "VETOED", "STALE_CHAMPION_BASELINE"}
    ),
    "BACKTEST_PASSED": frozenset(
        {"SHADOW_RUNNING", "VETOED", "STALE_CHAMPION_BASELINE"}
    ),
    "SHADOW_RUNNING": frozenset(
        {
            "SHADOW_FAILED",
            "EXPIRED",
            "VETOED",
            "PROMOTION_ELIGIBLE",
            "STALE_CHAMPION_BASELINE",
        }
    ),
    "PROMOTION_ELIGIBLE": frozenset(
        {"PROMOTED", "VETOED", "STALE_CHAMPION_BASELINE"}
    ),
}


def require_candidate_transition(before: str, after: str) -> None:
    """Reject skipped, reversed, and terminal candidate transitions."""
    if before not in _CANDIDATE_STATES or after not in _CANDIDATE_STATES:
        raise ValueError("CANDIDATE_STATE_UNKNOWN")
    if after not in _CANDIDATE_TRANSITIONS.get(before, ()):
        raise ValueError("CANDIDATE_STATE_TRANSITION_INVALID")


def aggregate_family_state(states: Sequence[str]) -> FamilyAggregateState:
    """Derive the family projection from all independently replayed candidates."""
    if not states or any(state not in _CANDIDATE_STATES for state in states):
        raise ValueError("CANDIDATE_STATE_UNKNOWN")
    distinct = frozenset(states)
    for dominant in (
        "PROMOTED",
        "PROMOTION_ELIGIBLE",
        "SHADOW_RUNNING",
        "BACKTEST_PASSED",
    ):
        if dominant in distinct:
            return cast(FamilyAggregateState, dominant)
    if distinct == {"ADMITTED"}:
        return "ADMITTED"
    if distinct & {"ADMITTED", "VALIDATION_PASSED", "OOS_SELECTED", "OOS_RESERVED"}:
        return "BACKTEST_RUNNING"
    for terminal in (
        "STALE_CHAMPION_BASELINE",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "BACKTEST_FAILED",
    ):
        if terminal in distinct:
            return cast(FamilyAggregateState, terminal)
    return "BACKTEST_FAILED"


@dataclass(frozen=True)
class VerifiedExperimentAuthority:
    family: StrategyExperimentFamilyRecord
    experiments: tuple[StrategyExperimentRecord, ...]
    events: tuple[StrategyExperimentEventRecord, ...]
    candidate_projections: tuple[StrategyExperimentCandidateProjectionRecord, ...]
    family_projection: StrategyExperimentStateProjectionRecord
    lineage_projection: StrategyResearchLineageProjectionRecord

    def candidate_projection(
        self, experiment_id: UUID
    ) -> StrategyExperimentCandidateProjectionRecord:
        for projection in self.candidate_projections:
            if projection.experiment_id == experiment_id:
                return projection
        raise ValueError("STRATEGY_EXPERIMENT_CHAIN_INVALID")


async def load_verified_experiment_authority(
    session: AsyncSession,
    family_id: UUID,
    *,
    lock: bool = False,
) -> VerifiedExperimentAuthority:
    """Load and replay one family, every candidate, and both mutable projections."""
    family_query = select(StrategyExperimentFamilyRecord).where(
        StrategyExperimentFamilyRecord.id == family_id
    )
    family_projection_query = select(StrategyExperimentStateProjectionRecord).where(
        StrategyExperimentStateProjectionRecord.family_id == family_id
    )
    experiments_query = (
        select(StrategyExperimentRecord)
        .where(StrategyExperimentRecord.family_id == family_id)
        .order_by(StrategyExperimentRecord.candidate_ordinal)
    )
    candidates_query = (
        select(StrategyExperimentCandidateProjectionRecord)
        .where(StrategyExperimentCandidateProjectionRecord.family_id == family_id)
        .order_by(StrategyExperimentCandidateProjectionRecord.experiment_id)
    )
    events_query = (
        select(StrategyExperimentEventRecord)
        .where(StrategyExperimentEventRecord.family_id == family_id)
        .order_by(StrategyExperimentEventRecord.sequence)
    )
    if lock:
        family_query = family_query.with_for_update()
        family_projection_query = family_projection_query.with_for_update()
        experiments_query = experiments_query.with_for_update()
        candidates_query = candidates_query.with_for_update()
        events_query = events_query.with_for_update()
    family = await session.scalar(family_query)
    projection = await session.scalar(family_projection_query)
    experiments = tuple(await session.scalars(experiments_query))
    candidate_projections = tuple(await session.scalars(candidates_query))
    events = tuple(await session.scalars(events_query))
    if family is None or projection is None:
        raise ValueError("STRATEGY_EXPERIMENT_CHAIN_INVALID")
    lineage_query = select(StrategyResearchLineageProjectionRecord).where(
        StrategyResearchLineageProjectionRecord.lineage_id == family.research_lineage_id
    )
    if lock:
        lineage_query = lineage_query.with_for_update()
    lineage_projection = await session.scalar(lineage_query)
    if lineage_projection is None:
        raise ValueError("STRATEGY_EXPERIMENT_CHAIN_INVALID")
    authority = VerifiedExperimentAuthority(
        family=family,
        experiments=experiments,
        events=events,
        candidate_projections=candidate_projections,
        family_projection=projection,
        lineage_projection=lineage_projection,
    )
    if not _verified_experiment_authority_is_valid(authority):
        raise ValueError("STRATEGY_EXPERIMENT_CHAIN_INVALID")
    return authority


async def append_candidate_event(
    session: AsyncSession,
    *,
    authority: VerifiedExperimentAuthority,
    experiment_id: UUID,
    event_type: CandidateState,
    idempotency_key: str,
    occurred_at: datetime,
    payload: dict[str, object],
    actor_kind: str = "service",
) -> StrategyExperimentEventRecord:
    """Append one legal candidate event and advance candidate/family projections."""
    candidate = authority.candidate_projection(experiment_id)
    require_candidate_transition(candidate.current_state, event_type)
    previous = authority.events[-1]
    sequence = previous.sequence + 1
    identity = _experiment_event_identity(
        family_id=authority.family.id,
        experiment_id=experiment_id,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=previous.event_hash,
    )
    event = StrategyExperimentEventRecord(
        id=new_uuid7(),
        family_id=authority.family.id,
        experiment_id=experiment_id,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=previous.event_hash,
        event_hash=hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
        created_at=occurred_at,
    )
    session.add(event)
    await session.flush()
    candidate.current_state = event_type
    candidate.event_sequence = sequence
    candidate.version += 1
    candidate.updated_at = occurred_at
    await session.flush()
    states = [
        event_type if item.experiment_id == experiment_id else item.current_state
        for item in authority.candidate_projections
    ]
    authority.family_projection.current_state = aggregate_family_state(states)
    authority.family_projection.event_sequence = sequence
    authority.family_projection.cumulative_oos_trial_count = (
        authority.lineage_projection.cumulative_oos_trial_count
    )
    authority.family_projection.version += 1
    authority.family_projection.updated_at = occurred_at
    await session.flush()
    return event


async def append_family_event(
    session: AsyncSession,
    *,
    authority: VerifiedExperimentAuthority,
    event_type: str,
    idempotency_key: str,
    occurred_at: datetime,
    payload: dict[str, object],
    actor_kind: str = "service",
) -> StrategyExperimentEventRecord:
    """Append a family-only fact without pretending it changes a candidate state."""
    previous = authority.events[-1]
    sequence = previous.sequence + 1
    identity = _experiment_event_identity(
        family_id=authority.family.id,
        experiment_id=None,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=previous.event_hash,
    )
    event = StrategyExperimentEventRecord(
        id=new_uuid7(),
        family_id=authority.family.id,
        experiment_id=None,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=previous.event_hash,
        event_hash=hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
        created_at=occurred_at,
    )
    session.add(event)
    await session.flush()
    authority.family_projection.current_state = aggregate_family_state(
        [item.current_state for item in authority.candidate_projections]
    )
    authority.family_projection.event_sequence = sequence
    authority.family_projection.cumulative_oos_trial_count = (
        authority.lineage_projection.cumulative_oos_trial_count
    )
    authority.family_projection.version += 1
    authority.family_projection.updated_at = occurred_at
    await session.flush()
    return event


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
                StrategyExperimentStateProjectionRecord.research_lineage_id == lineage_id,
                StrategyExperimentStateProjectionRecord.current_state.not_in(terminal_states),
            )
            .with_for_update()
        ),
    )


async def latest_family_created_at(session: AsyncSession, lineage_id: UUID) -> datetime | None:
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
    experiment_id: UUID | None = None,
    sequence: int = 1,
    previous_hash: str = _ZERO_HASH,
) -> StrategyExperimentEventRecord:
    identity = _experiment_event_identity(
        family_id=family_id,
        experiment_id=experiment_id,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type="ADMITTED",
        idempotency_key=idempotency_key,
        actor_kind="service",
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=previous_hash,
    )
    event = StrategyExperimentEventRecord(
        id=new_uuid7(),
        family_id=family_id,
        experiment_id=experiment_id,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type="ADMITTED",
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


def _experiment_event_identity(
    *,
    family_id: UUID,
    experiment_id: UUID | None,
    schema_version: str,
    sequence: int,
    event_type: str,
    idempotency_key: str,
    actor_kind: str,
    occurred_at: datetime,
    payload: dict[str, object],
    previous_hash: str,
) -> dict[str, object]:
    identity: dict[str, object] = {
        "schema_version": schema_version,
        "family_id": str(family_id),
        "sequence": sequence,
        "event_type": event_type,
        "idempotency_key": idempotency_key,
        "actor_kind": actor_kind,
        "occurred_at": occurred_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "payload": payload,
        "previous_hash": previous_hash,
    }
    if experiment_id is not None:
        identity["experiment_id"] = str(experiment_id)
    return identity


def _event_chain_content_is_valid(
    family_id: UUID, events: Sequence[StrategyExperimentEventRecord]
) -> bool:
    if not events:
        return False
    prior_hash = _ZERO_HASH
    for index, event in enumerate(events, start=1):
        identity = _experiment_event_identity(
            family_id=family_id,
            experiment_id=event.experiment_id,
            schema_version=event.schema_version,
            sequence=event.sequence,
            event_type=event.event_type,
            idempotency_key=event.idempotency_key,
            actor_kind=event.actor_kind,
            occurred_at=event.occurred_at,
            payload=event.payload,
            previous_hash=event.previous_hash,
        )
        expected = hashlib.sha256(canonical_json(identity).encode()).hexdigest()
        if (
            event.family_id != family_id
            or event.sequence != index
            or event.previous_hash != prior_hash
            or event.event_hash != expected
        ):
            return False
        prior_hash = event.event_hash
    return True


def _verified_experiment_authority_is_valid(
    authority: VerifiedExperimentAuthority,
) -> bool:
    family = authority.family
    experiments = authority.experiments
    projections = authority.candidate_projections
    family_projection = authority.family_projection
    if (
        family.admission_status != "ADMITTED"
        or family.candidate_count != len(experiments)
        or len(experiments) != len(projections)
        or family_projection.family_id != family.id
        or family_projection.research_lineage_id != family.research_lineage_id
        or authority.lineage_projection.lineage_id != family.research_lineage_id
        or family_projection.cumulative_oos_trial_count
        > authority.lineage_projection.cumulative_oos_trial_count
        or not _event_chain_content_is_valid(family.id, authority.events)
        or family_projection.event_sequence != authority.events[-1].sequence
    ):
        return False
    experiments_by_id = {item.id: item for item in experiments}
    projections_by_id = {item.experiment_id: item for item in projections}
    if (
        len(experiments_by_id) != len(experiments)
        or set(experiments_by_id) != set(projections_by_id)
        or any(item.family_id != family.id for item in experiments)
        or any(item.family_id != family.id for item in projections)
    ):
        return False
    replay: dict[UUID, tuple[CandidateState, int, int]] = {}
    legacy_admitted_sequences = {
        event.sequence
        for event in authority.events
        if event.experiment_id is None and event.event_type == "ADMITTED"
    }
    for event in authority.events:
        if event.experiment_id is None:
            continue
        if (
            event.experiment_id not in experiments_by_id
            or event.event_type not in _CANDIDATE_STATES
        ):
            return False
        prior = replay.get(event.experiment_id)
        if prior is None:
            if event.event_type != "ADMITTED":
                return False
            replay[event.experiment_id] = ("ADMITTED", event.sequence, 1)
            continue
        try:
            require_candidate_transition(prior[0], event.event_type)
        except ValueError:
            return False
        replay[event.experiment_id] = (
            cast(CandidateState, event.event_type),
            event.sequence,
            prior[2] + 1,
        )
    states: list[str] = []
    for experiment_id, projection in projections_by_id.items():
        expected = replay.get(experiment_id)
        if expected is None:
            if (
                projection.current_state != "ADMITTED"
                or projection.event_sequence not in legacy_admitted_sequences
                or projection.version != 1
            ):
                return False
        elif (
            projection.current_state != expected[0]
            or projection.event_sequence != expected[1]
            or projection.version != expected[2]
        ):
            return False
        states.append(projection.current_state)
    try:
        aggregate = aggregate_family_state(states)
    except ValueError:
        return False
    terminal = {
        "BACKTEST_FAILED",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    }
    return family_projection.current_state == aggregate and (
        aggregate in terminal
        or family_projection.cumulative_oos_trial_count
        == authority.lineage_projection.cumulative_oos_trial_count
    )


def experiment_chain_is_valid(
    family_id: UUID,
    events: Sequence[StrategyExperimentEventRecord],
    projection: StrategyExperimentStateProjectionRecord,
) -> bool:
    """Verify a family's event content, sequence, and current-state projection."""
    if not events or projection.event_sequence != events[-1].sequence:
        return False
    compatible_last_events = {
        "PROPOSED": {"PROPOSED"},
        "REJECTED": {"REJECTED"},
        "ADMITTED": {"ADMITTED"},
        "BACKTEST_RUNNING": {"BACKTEST_RUNNING"},
        "BACKTEST_FAILED": {"BACKTEST_FAILED"},
        "BACKTEST_PASSED": {"BACKTEST_PASSED"},
        "SHADOW_RUNNING": {"SHADOW_RUNNING"},
        "SHADOW_FAILED": {"SHADOW_FAILED"},
        "EXPIRED": {"EXPIRED"},
        "VETOED": {"VETOED"},
        "PROMOTION_ELIGIBLE": {"PROMOTION_ELIGIBLE"},
        "PROMOTED": {"PROMOTED"},
        "STALE_CHAMPION_BASELINE": {"STALE_CHAMPION_BASELINE"},
    }
    if events[-1].event_type not in compatible_last_events.get(projection.current_state, set()):
        return False
    return _event_chain_content_is_valid(family_id, events)


async def experiment_projection_is_valid(
    session: AsyncSession,
    projection: StrategyExperimentStateProjectionRecord,
) -> bool:
    try:
        authority = await load_verified_experiment_authority(
            session, projection.family_id
        )
    except ValueError:
        return False
    stored = authority.family_projection
    return (
        stored.current_state == projection.current_state
        and stored.event_sequence == projection.event_sequence
        and stored.cumulative_oos_trial_count
        == projection.cumulative_oos_trial_count
        and stored.version == projection.version
    )

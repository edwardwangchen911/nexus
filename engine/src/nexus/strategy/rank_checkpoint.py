"""Atomic forward decisions and rank evidence; no trading or source-policy bypass."""

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.paper.execution_service import _decimal
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
)
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    StrategyDecisionRecord,
    StrategyRankCheckpointRecord,
)
from nexus.strategy.baseline import HeldRankObservation, PositionRankState, StrategyDataSnapshot
from nexus.strategy.dsl import (
    baseline_feature_registry,
    compile_strategy,
    evolution_feature_registry_v1,
)
from nexus.strategy.forward import (
    PersistedForwardStrategyDecision,
    _active_registry_event,
    build_forward_strategy_decision,
    persist_forward_strategy_decision,
)
from nexus.strategy.interpreter import evaluate_compiled_strategy_with_rank_observations
from nexus.strategy.rank_continuity import PositionContinuity, load_held_position_continuity

_SCHEMA = "rank_checkpoint_v1"


@dataclass(frozen=True)
class RankCheckpointAuthority:
    account_id: UUID
    portfolio_id: UUID
    decision_time: datetime
    execution_session: date
    source_policy_id: UUID
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID
    portfolio_projection_hash: str
    authority_hash: str


class _Observations(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    held_rank: tuple[HeldRankObservation, ...]
    continuity: tuple[PositionContinuity, ...]


@dataclass(frozen=True)
class PersistedRankCheckpoint:
    forward: PersistedForwardStrategyDecision
    checkpoint_hash: str
    input_snapshot_hash: str
    held_rank_observations: tuple[HeldRankObservation, ...]


def _normalize(value: object) -> object:
    if isinstance(value, Decimal):
        return _decimal(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_normalize(item) for item in value]
    return value


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(_normalize(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _checkpoint_hash(
    decision_hash: str, parent_hash: str | None, snapshot_hash: str, observations: dict[str, object]
) -> str:
    return _hash(
        {
            "schema": _SCHEMA,
            "decision_hash": decision_hash,
            "parent_checkpoint_hash": parent_hash,
            "input_snapshot_hash": snapshot_hash,
            "observations": observations,
        }
    )


def _derive_rank_states(
    positions: tuple[PositionContinuity, ...], observations: tuple[HeldRankObservation, ...]
) -> tuple[PositionRankState, ...]:
    previous = {item.instrument_id: item for item in observations}
    states = []
    for position in positions:
        if position.quantity_at_cutoff == 0:
            continue
        prior = previous.get(position.instrument_id)
        if position.continuous_since_previous:
            if prior is None or prior.status != "known" or prior.evaluated_streak is None:
                raise ValueError("RANK_CHECKPOINT_STATE_UNAVAILABLE")
            streak = prior.evaluated_streak
        elif position.kind in {"opened", "reopened"} and position.opening_fill_id is not None:
            streak = 0
        else:
            raise ValueError("RANK_CHECKPOINT_STATE_UNAVAILABLE")
        states.append(
            PositionRankState(
                instrument_id=position.instrument_id, consecutive_outside_top20_weeks=streak
            )
        )
    return tuple(states)


async def _read_checkpoint(
    session: AsyncSession, decision: StrategyDecisionRecord
) -> tuple[StrategyRankCheckpointRecord, _Observations]:
    record = await session.get(StrategyRankCheckpointRecord, decision.id)
    if record is None:
        raise ValueError("RANK_CHECKPOINT_HISTORY_UNAVAILABLE")
    parent = (
        await session.get(StrategyRankCheckpointRecord, record.previous_decision_id)
        if record.previous_decision_id
        else None
    )
    if record.previous_decision_id and parent is None:
        raise ValueError("RANK_CHECKPOINT_HISTORY_UNAVAILABLE")
    if record.schema_version != _SCHEMA or record.checkpoint_hash != _checkpoint_hash(
        decision.decision_hash,
        parent.checkpoint_hash if parent else None,
        record.input_snapshot_hash,
        record.observations,
    ):
        raise ValueError("RANK_CHECKPOINT_HASH_MISMATCH")
    return record, _Observations.model_validate(record.observations)


async def evaluate_and_persist_rank_checkpoint(
    session: AsyncSession,
    *,
    snapshot: StrategyDataSnapshot,
    authority: RankCheckpointAuthority,
) -> PersistedRankCheckpoint | None:
    """Derive rank inputs and persist both facts in a caller-owned transaction.

    Supplied rank counters are not authority: they are replaced from event/checkpoint
    evidence. Price features remain the frozen caller snapshot and retain all original
    forward source/quality/adjustment admission checks. This function never commits.
    """
    cutoff = require_utc(authority.decision_time)
    if (
        cutoff.date() != snapshot.decision_session
        or snapshot.next_session != authority.execution_session
    ):
        raise ValueError("RANK_CHECKPOINT_SESSION_MISMATCH")
    if authority.execution_session.isocalendar()[:2] == snapshot.decision_session.isocalendar()[:2]:
        return None
    async with session.begin_nested():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
            {"scope": f"internal-paper:{authority.account_id}"},
        )
        account = await session.scalar(
            select(PaperAccountRecord)
            .where(PaperAccountRecord.id == authority.account_id)
            .with_for_update()
        )
        if account is None or account.portfolio_id != authority.portfolio_id:
            raise ValueError("RANK_CHECKPOINT_ACCOUNT_MISMATCH")
        existing = await session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.account_id == authority.account_id,
                StrategyDecisionRecord.decision_session == snapshot.decision_session,
            )
        )
        existing_checkpoint = (await _read_checkpoint(session, existing))[0] if existing else None
        if existing is not None:
            definition = await session.get(
                StrategyDefinitionRecord, existing.strategy_definition_id
            )
            registry_event = await session.get(
                StrategyRegistryEventRecord, existing.registry_event_id
            )
            generation = existing.champion_generation
        else:
            head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
            if head is None:
                raise ValueError("RANK_CHECKPOINT_REGISTRY_UNAVAILABLE")
            definition = await session.get(StrategyDefinitionRecord, head.active_definition_id)
            registry_event = await _active_registry_event(session, head=head, cutoff=cutoff)
            generation = head.active_generation
        if definition is None or registry_event is None:
            raise ValueError("RANK_CHECKPOINT_REGISTRY_UNAVAILABLE")
        if (
            registry_event.to_definition_id != definition.id
            or registry_event.champion_generation != generation
            or registry_event.channel != "internal_paper"
            or registry_event.event_type not in {"GENESIS", "PROMOTE", "ROLLBACK"}
            or (
                registry_event.event_type != "GENESIS"
                and (
                    definition.published_at > cutoff
                    or registry_event.occurred_at > cutoff
                    or registry_event.effective_session is None
                    or registry_event.effective_session > snapshot.decision_session
                )
            )
        ):
            raise ValueError("RANK_CHECKPOINT_REGISTRY_UNAVAILABLE")
        compiled = compile_strategy(
            definition.document,
            baseline_feature_registry()
            if definition.strategy_version == "baseline_v1"
            else evolution_feature_registry_v1(),
        )
        if compiled.expression_hash != definition.expression_hash:
            raise ValueError("RANK_CHECKPOINT_DEFINITION_MISMATCH")
        later = await session.scalar(
            select(StrategyDecisionRecord.id)
            .where(
                StrategyDecisionRecord.account_id == authority.account_id,
                StrategyDecisionRecord.decision_session > snapshot.decision_session,
            )
            .limit(1)
        )
        if later is not None and existing is None:
            raise ValueError("RANK_CHECKPOINT_BACKDATED_DECISION")
        parent = await session.scalar(
            select(StrategyDecisionRecord)
            .where(
                StrategyDecisionRecord.account_id == authority.account_id,
                StrategyDecisionRecord.strategy_definition_id == definition.id,
                StrategyDecisionRecord.champion_generation == generation,
                StrategyDecisionRecord.decision_session < snapshot.decision_session,
            )
            .order_by(StrategyDecisionRecord.decision_session.desc())
            .limit(1)
        )
        parent_record, parent_observations = (
            await _read_checkpoint(session, parent) if parent else (None, None)
        )
        if parent is not None and parent.decision_time >= cutoff:
            raise ValueError("RANK_CHECKPOINT_HISTORY_TIME_MISMATCH")
        continuity = await load_held_position_continuity(
            session,
            account_id=authority.account_id,
            previous_cutoff=parent.decision_time if parent else None,
            cutoff=cutoff,
        )
        current_projection = await load_paper_projection(
            session, account_id=authority.account_id, as_of=cutoff
        )
        if current_projection.projection_hash != authority.portfolio_projection_hash:
            raise ValueError("RANK_CHECKPOINT_PROJECTION_MISMATCH")
        current_quantities = {
            p.instrument_id: p.quantity for p in current_projection.positions if p.quantity
        }
        event_quantities = {
            p.instrument_id: p.quantity_at_cutoff
            for p in continuity.positions
            if p.quantity_at_cutoff
        }
        if current_quantities != event_quantities or set(snapshot.current_positions) != set(
            event_quantities
        ):
            raise ValueError("RANK_CHECKPOINT_POSITION_MISMATCH")
        if parent is None:
            if continuity.positions or current_quantities:
                raise ValueError("RANK_CHECKPOINT_GENESIS_NOT_EMPTY")
        else:
            previous_projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=parent.decision_time
            )
            if previous_projection.projection_hash != parent.portfolio_projection_hash:
                raise ValueError("RANK_CHECKPOINT_HISTORY_PROJECTION_MISMATCH")
            previous_quantities = {
                p.instrument_id: p.quantity for p in previous_projection.positions if p.quantity
            }
            previous_events = {
                p.instrument_id: p.quantity_at_previous
                for p in continuity.positions
                if p.quantity_at_previous
            }
            if previous_quantities != previous_events:
                raise ValueError("RANK_CHECKPOINT_HISTORY_POSITION_MISMATCH")
            assert parent_observations is not None
            prefixes = {p.instrument_id: p for p in parent_observations.continuity}
            for position in continuity.positions:
                previous = prefixes.get(position.instrument_id)
                if position.previous_event_seq and (
                    previous is None or previous.prefix_hash != position.previous_prefix_hash
                ):
                    raise ValueError("RANK_CHECKPOINT_HISTORY_PREFIX_MISMATCH")
            if not set(prefixes) <= {p.instrument_id for p in continuity.positions}:
                raise ValueError("RANK_CHECKPOINT_HISTORY_PREFIX_MISMATCH")
        states = _derive_rank_states(
            continuity.positions, parent_observations.held_rank if parent_observations else ()
        )
        frozen = snapshot.model_copy(
            update={
                "position_rank_states": states,
                "current_positions": tuple(sorted(snapshot.current_positions, key=str)),
                "instruments": tuple(
                    sorted(snapshot.instruments, key=lambda item: str(item.instrument_id))
                ),
            }
        )
        observed = evaluate_compiled_strategy_with_rank_observations(
            compiled, frozen, frozen.decision_session
        )
        decision = build_forward_strategy_decision(
            observed.decision,
            account_id=authority.account_id,
            portfolio_id=authority.portfolio_id,
            current_positions=frozen.current_positions,
            whitelisted_instrument_ids=tuple(i.instrument_id for i in frozen.instruments),
            decision_time=cutoff,
            execution_session=authority.execution_session,
            source_policy_id=authority.source_policy_id,
            calendar_authority_id=authority.calendar_authority_id,
            universe_snapshot_id=authority.universe_snapshot_id,
            quality_report_id=authority.quality_report_id,
            portfolio_projection_hash=authority.portfolio_projection_hash,
            authority_hash=authority.authority_hash,
            strategy_parameters_hash=definition.expression_hash,
            strategy_definition_id=definition.id,
            registry_event_id=registry_event.id,
            champion_generation=generation,
        )
        if decision is None:
            return None
        input_hash = _hash(frozen.model_dump(mode="python"))
        observations = _Observations(
            held_rank=observed.held_rank_observations, continuity=continuity.positions
        ).model_dump(mode="json")
        checkpoint_hash = _checkpoint_hash(
            decision.decision_hash,
            parent_record.checkpoint_hash if parent_record else None,
            input_hash,
            observations,
        )
        if existing_checkpoint is not None and (
            existing_checkpoint.checkpoint_hash != checkpoint_hash
            or existing_checkpoint.previous_decision_id != (parent.id if parent else None)
        ):
            raise ValueError("RANK_CHECKPOINT_CONTENT_CONFLICT")
        persisted = await persist_forward_strategy_decision(session, decision)
        if existing_checkpoint is None:
            session.add(
                StrategyRankCheckpointRecord(
                    decision_id=persisted.decision_id,
                    previous_decision_id=parent.id if parent else None,
                    schema_version=_SCHEMA,
                    input_snapshot_hash=input_hash,
                    observations=observations,
                    checkpoint_hash=checkpoint_hash,
                )
            )
            await session.flush()
        return PersistedRankCheckpoint(
            persisted, checkpoint_hash, input_hash, observed.held_rank_observations
        )

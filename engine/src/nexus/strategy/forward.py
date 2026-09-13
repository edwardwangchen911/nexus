"""Immutable forward strategy decisions consumed by deterministic risk."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import DataSnapshot, QualityReport, SourcePolicy
from nexus.persistence.models.evolution import (
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
)
from nexus.persistence.models.market import UniverseMember, UniverseSnapshot
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PaperAccountRecord,
    StrategyDecisionRecord,
    StrategyDecisionTargetRecord,
)
from nexus.quality.source_policy import (
    SOURCE_POLICY_V2_ID,
    load_source_policy_v2,
    source_policy_v2_supports_adjusted_prices,
)
from nexus.risk.models import PaperDecisionAuthority

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_QUANTUM = Decimal("0.000000000001")
_MAX_ABSOLUTE = Decimal("1e26")


class ForwardDecisionConflictError(ValueError):
    """The immutable decision key already exists with different content."""


@dataclass(frozen=True)
class PersistedForwardStrategyDecision:
    decision_id: UUID
    decision: ForwardStrategyDecision


def _exact_decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, float):
        raise ValueError(f"{field} must not contain a binary float")
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"{field} must be a Decimal")
    result = Decimal(value)
    if not result.is_finite() or abs(result) >= _MAX_ABSOLUTE:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)")
    try:
        with localcontext() as context:
            context.prec = 50
            canonical = result.quantize(_QUANTUM)
    except InvalidOperation as error:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)") from error
    if canonical != result:
        raise ValueError(f"{field} must have no more than 12 decimal places")
    return result


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ForwardStrategyTarget(_FrozenModel):
    instrument_id: UUID
    target_weight: Decimal
    exit: bool
    strategy_priority: int = Field(ge=0, le=2)
    reason: str = Field(min_length=1, max_length=100)

    @field_validator("target_weight", mode="before")
    @classmethod
    def _weight_is_exact(cls, value: object) -> Decimal:
        return _exact_decimal(value, field="target_weight")

    @model_validator(mode="after")
    def _valid_target(self) -> ForwardStrategyTarget:
        if not Decimal(0) <= self.target_weight <= Decimal(1):
            raise ValueError("target weight must be between zero and one")
        if self.exit and (self.target_weight != Decimal(0) or self.strategy_priority != 0):
            raise ValueError("exit targets require zero weight and priority zero")
        if not self.exit and self.strategy_priority == 0:
            raise ValueError("non-exit targets cannot use exit priority")
        return self


class ForwardStrategyDecision(_FrozenModel):
    account_id: UUID
    portfolio_id: UUID
    strategy_id: str
    strategy_version: str
    strategy_definition_id: UUID
    registry_event_id: UUID
    champion_generation: int = Field(ge=1)
    decision_session: date
    decision_time: datetime
    execution_session: date
    source_policy_id: UUID
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID
    portfolio_projection_hash: Hash64
    authority_hash: Hash64
    strategy_parameters_hash: Hash64
    decision_hash: Hash64
    cash_weight: Decimal
    reason: Literal["WEEKLY_REBALANCE"]
    targets: tuple[ForwardStrategyTarget, ...]

    @property
    def execution_source_policy_id(self) -> None:
        """Registered decisions intentionally have no separate fill-price policy."""
        return None

    @field_validator("cash_weight", mode="before")
    @classmethod
    def _cash_weight_is_exact(cls, value: object) -> Decimal:
        return _exact_decimal(value, field="cash_weight")

    @field_validator("decision_time")
    @classmethod
    def _decision_time_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("decision time must be timezone-aware")
        if value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("decision time must use UTC")
        return value

    @model_validator(mode="after")
    def _valid_decision(self) -> ForwardStrategyDecision:
        if not self.strategy_id.strip() or not self.strategy_version.strip():
            raise ValueError("strategy identity must be nonempty")
        expected_strategy_id = (
            "baseline" if self.strategy_version == "baseline_v1" else "registered"
        )
        if self.strategy_id != expected_strategy_id:
            raise ValueError("strategy identity must match the content-addressed version")
        if self.execution_session <= self.decision_session:
            raise ValueError("execution session must follow decision session")
        instrument_ids = [target.instrument_id for target in self.targets]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("forward decision targets must be unique")
        total_weight = sum(
            (target.target_weight for target in self.targets),
            start=self.cash_weight,
        )
        if total_weight != Decimal(1):
            raise ValueError("target and cash weights must equal one")
        return self


def build_forward_strategy_decision(
    decision: object,
    *,
    account_id: UUID,
    portfolio_id: UUID,
    current_positions: Sequence[UUID],
    whitelisted_instrument_ids: Sequence[UUID],
    decision_time: datetime,
    execution_session: date | None,
    source_policy_id: UUID,
    calendar_authority_id: UUID,
    universe_snapshot_id: UUID,
    quality_report_id: UUID,
    portfolio_projection_hash: str,
    authority_hash: str,
    strategy_parameters_hash: str,
    strategy_definition_id: UUID,
    registry_event_id: UUID,
    champion_generation: int,
) -> ForwardStrategyDecision | None:
    """Adapt a verified pure strategy result without accepting caller targets."""
    from nexus.risk.canonical import canonical_forward_strategy_decision_hash
    from nexus.strategy.baseline import StrategyDecision
    from nexus.strategy.interpreter import CompiledStrategyDecision

    if not isinstance(decision, StrategyDecision | CompiledStrategyDecision):
        raise TypeError("decision must be a verified strategy decision")
    if decision.reason == "NOT_WEEK_END":
        if decision.targets or decision.exits or decision.cash_weight is not None:
            raise ValueError("NOT_WEEK_END must not contain consumable targets")
        return None
    if execution_session is None:
        raise ValueError("execution session is required")
    if decision_time.tzinfo is None or decision_time.utcoffset() is None:
        raise ValueError("decision time must be timezone-aware")
    if decision.cash_weight is None:
        raise ValueError("weekly decision requires cash weight")

    current = tuple(current_positions)
    if len(current) != len(set(current)):
        raise ValueError("current positions must be unique")
    whitelist = tuple(whitelisted_instrument_ids)
    if len(whitelist) != len(set(whitelist)):
        raise ValueError("whitelisted instruments must be unique")

    target_ids = [target.instrument_id for target in decision.targets]
    exit_ids = list(decision.exits)
    if len(target_ids) != len(set(target_ids)):
        raise ValueError("target instruments must be unique")
    if len(exit_ids) != len(set(exit_ids)):
        raise ValueError("exit instruments must be unique")
    if set(target_ids) & set(exit_ids):
        raise ValueError("target and exit instruments must not overlap")
    if not set(exit_ids) <= set(current):
        raise ValueError("exit instruments must be current positions")
    if not (set(target_ids) | set(exit_ids)) <= set(whitelist):
        raise ValueError("forward instruments must be whitelisted")

    current_set = set(current)
    reason_prefix = "BASELINE" if decision.strategy_version == "baseline_v1" else "STRATEGY"
    mapped_targets = [
        ForwardStrategyTarget(
            instrument_id=target.instrument_id,
            target_weight=target.weight,
            exit=False,
            strategy_priority=1 if target.instrument_id in current_set else 2,
            reason=(
                f"{reason_prefix}_CURRENT_TARGET"
                if target.instrument_id in current_set
                else f"{reason_prefix}_NEW_TARGET"
            ),
        )
        for target in decision.targets
    ]
    mapped_targets.extend(
        ForwardStrategyTarget(
            instrument_id=instrument_id,
            target_weight=Decimal(0),
            exit=True,
            strategy_priority=0,
            reason=f"{reason_prefix}_EXIT",
        )
        for instrument_id in decision.exits
    )
    draft = ForwardStrategyDecision(
        account_id=account_id,
        portfolio_id=portfolio_id,
        strategy_id="baseline" if decision.strategy_version == "baseline_v1" else "registered",
        strategy_version=decision.strategy_version,
        strategy_definition_id=strategy_definition_id,
        registry_event_id=registry_event_id,
        champion_generation=champion_generation,
        decision_session=decision.decision_session,
        decision_time=decision_time.astimezone(UTC),
        execution_session=execution_session,
        source_policy_id=source_policy_id,
        calendar_authority_id=calendar_authority_id,
        universe_snapshot_id=universe_snapshot_id,
        quality_report_id=quality_report_id,
        portfolio_projection_hash=portfolio_projection_hash,
        authority_hash=authority_hash,
        strategy_parameters_hash=strategy_parameters_hash,
        decision_hash="0" * 64,
        cash_weight=decision.cash_weight,
        reason="WEEKLY_REBALANCE",
        targets=tuple(sorted(mapped_targets, key=lambda target: target.instrument_id.int)),
    )
    return draft.model_copy(
        update={"decision_hash": canonical_forward_strategy_decision_hash(draft)}
    )


async def evaluate_registered_forward_strategy_decision(
    session: AsyncSession,
    *,
    snapshot: object,
    account_id: UUID,
    portfolio_id: UUID,
    decision_time: datetime,
    execution_session: date | None,
    source_policy_id: UUID,
    calendar_authority_id: UUID,
    universe_snapshot_id: UUID,
    quality_report_id: UUID,
    portfolio_projection_hash: str,
    authority_hash: str,
) -> ForwardStrategyDecision | None:
    """Evaluate only the active content-addressed registry definition."""
    from nexus.strategy.baseline import StrategyDataSnapshot
    from nexus.strategy.dsl import (
        baseline_feature_registry,
        compile_strategy,
        evolution_feature_registry_v1,
    )
    from nexus.strategy.interpreter import evaluate_compiled_strategy

    if not isinstance(snapshot, StrategyDataSnapshot):
        raise TypeError("snapshot must be a StrategyDataSnapshot")
    head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
    if head is None:
        raise ValueError("internal-paper strategy registry is unavailable at decision time")
    definition = await session.get(StrategyDefinitionRecord, head.active_definition_id)
    event = await _active_registry_event(session, head=head, cutoff=decision_time)
    if definition is None or event is None:
        raise ValueError("active internal-paper strategy authority is incomplete")
    if event.event_type != "GENESIS" and definition.published_at > decision_time:
        raise ValueError("active internal-paper strategy definition is not yet available")
    registry = (
        baseline_feature_registry()
        if definition.strategy_version == "baseline_v1"
        else evolution_feature_registry_v1()
    )
    compiled = compile_strategy(definition.document, registry)
    if compiled.expression_hash != definition.expression_hash:
        raise ValueError("active internal-paper strategy definition is invalid")
    evaluated = evaluate_compiled_strategy(
        compiled,
        snapshot,
        snapshot.decision_session,
    )
    return build_forward_strategy_decision(
        evaluated,
        account_id=account_id,
        portfolio_id=portfolio_id,
        current_positions=snapshot.current_positions,
        whitelisted_instrument_ids=tuple(item.instrument_id for item in snapshot.instruments),
        decision_time=decision_time,
        execution_session=execution_session,
        source_policy_id=source_policy_id,
        calendar_authority_id=calendar_authority_id,
        universe_snapshot_id=universe_snapshot_id,
        quality_report_id=quality_report_id,
        portfolio_projection_hash=portfolio_projection_hash,
        authority_hash=authority_hash,
        strategy_parameters_hash=definition.expression_hash,
        strategy_definition_id=definition.id,
        registry_event_id=event.id,
        champion_generation=head.active_generation,
    )


async def require_paper_decision_authority(
    session: AsyncSession,
    decision: PaperDecisionAuthority,
) -> None:
    account = await session.get(PaperAccountRecord, decision.account_id)
    if account is None or account.portfolio_id != decision.portfolio_id:
        raise ValueError("paper account does not match forward decision")

    source_policy = await session.get(SourcePolicy, decision.source_policy_id)
    calendar = await session.get(ExchangeCalendarAuthorityRecord, decision.calendar_authority_id)
    universe = await session.get(UniverseSnapshot, decision.universe_snapshot_id)
    agent_paper = decision.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
    quality = (
        None
        if decision.quality_report_id is None
        else await session.get(QualityReport, decision.quality_report_id)
    )
    if (
        source_policy is None
        or calendar is None
        or universe is None
        or agent_paper != (decision.quality_report_id is None)
        or (not agent_paper and quality is None)
    ):
        raise ValueError("forward decision authority is incomplete")
    quality_snapshot = (
        None if quality is None else await session.get(DataSnapshot, quality.data_snapshot_id)
    )
    if not agent_paper and (
        quality_snapshot is None or quality is None or quality.status != "PASS"
    ):
        raise ValueError("forward decision quality authority is incomplete")

    available_times: tuple[datetime | None, ...] = (
        source_policy.created_at,
        calendar.published_at,
        universe.published_at,
    )
    if quality is not None and quality_snapshot is not None:
        available_times += (quality.created_at, quality_snapshot.available_at)
    if any(
        available_at is None or available_at > decision.decision_time
        for available_at in available_times
    ):
        raise ValueError("forward decision authority was available after decision time")

    calendar_sessions = tuple(
        (
            await session.scalars(
                select(ExchangeCalendarSessionRecord)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == decision.calendar_authority_id
                )
                .order_by(ExchangeCalendarSessionRecord.session)
            )
        ).all()
    )
    matching_indexes = [
        index
        for index, row in enumerate(calendar_sessions)
        if row.session == decision.decision_session
    ]
    if len(matching_indexes) != 1:
        raise ValueError("decision session is missing from calendar authority")
    decision_index = matching_indexes[0]
    if (
        calendar_sessions[decision_index].decision_at != decision.decision_time
        or decision_index + 1 >= len(calendar_sessions)
        or calendar_sessions[decision_index + 1].session != decision.execution_session
    ):
        raise ValueError("execution session is not the next authority session")

    target_ids = {target.instrument_id for target in decision.targets}
    member_ids = set(
        (
            await session.scalars(
                select(UniverseMember.instrument_id).where(
                    UniverseMember.universe_snapshot_id == decision.universe_snapshot_id,
                    UniverseMember.instrument_id.in_(target_ids),
                )
            )
        ).all()
    )
    if member_ids != target_ids:
        raise ValueError("forward decision contains a non-whitelist instrument")


def _domain_from_records(
    record: StrategyDecisionRecord,
    targets: Sequence[StrategyDecisionTargetRecord],
) -> ForwardStrategyDecision:
    if (
        record.decision_origin != "registered_strategy"
        or record.strategy_definition_id is None
        or record.registry_event_id is None
        or record.champion_generation is None
        or record.quality_report_id is None
    ):
        raise ValueError("stored decision is not a registered strategy decision")
    decision = ForwardStrategyDecision(
        account_id=record.account_id,
        portfolio_id=record.portfolio_id,
        strategy_id=record.strategy_id,
        strategy_version=record.strategy_version,
        strategy_definition_id=record.strategy_definition_id,
        registry_event_id=record.registry_event_id,
        champion_generation=record.champion_generation,
        decision_session=record.decision_session,
        decision_time=record.decision_time,
        execution_session=record.execution_session,
        source_policy_id=record.source_policy_id,
        calendar_authority_id=record.calendar_authority_id,
        universe_snapshot_id=record.universe_snapshot_id,
        quality_report_id=record.quality_report_id,
        portfolio_projection_hash=record.portfolio_projection_hash,
        authority_hash=record.authority_hash,
        strategy_parameters_hash=record.strategy_parameters_hash,
        decision_hash=record.decision_hash,
        cash_weight=record.cash_weight,
        reason="WEEKLY_REBALANCE",
        targets=tuple(
            ForwardStrategyTarget(
                instrument_id=target.instrument_id,
                target_weight=target.target_weight,
                exit=target.exit,
                strategy_priority=target.strategy_priority,
                reason=target.reason,
            )
            for target in sorted(targets, key=lambda item: item.instrument_id.int)
        ),
    )
    from nexus.risk.canonical import (
        canonical_forward_strategy_decision_hash,
        canonical_legacy_forward_strategy_decision_hash,
    )

    valid_hashes = {canonical_forward_strategy_decision_hash(decision)}
    if decision.champion_generation == 1 and decision.strategy_version == "baseline_v1":
        valid_hashes.add(canonical_legacy_forward_strategy_decision_hash(decision))
    if decision.decision_hash not in valid_hashes:
        raise ValueError("stored forward decision hash does not match content")
    return decision


async def load_forward_strategy_decision(
    session: AsyncSession,
    *,
    account_id: UUID,
    strategy_version: str,
    decision_session: date,
) -> PersistedForwardStrategyDecision | None:
    """Load and revalidate one immutable consumable decision."""
    record = await session.scalar(
        select(StrategyDecisionRecord).where(
            StrategyDecisionRecord.account_id == account_id,
            StrategyDecisionRecord.decision_session == decision_session,
        )
    )
    if record is None:
        return None
    if record.strategy_version != strategy_version:
        raise ForwardDecisionConflictError("forward decision strategy version conflict")
    targets = tuple(
        (
            await session.scalars(
                select(StrategyDecisionTargetRecord).where(
                    StrategyDecisionTargetRecord.strategy_decision_id == record.id
                )
            )
        ).all()
    )
    return PersistedForwardStrategyDecision(
        decision_id=record.id,
        decision=_domain_from_records(record, targets),
    )


async def persist_forward_strategy_decision(
    session: AsyncSession,
    decision: ForwardStrategyDecision,
) -> PersistedForwardStrategyDecision:
    """Insert an immutable forward decision or return its identical prior fact."""
    from nexus.risk.canonical import canonical_forward_strategy_decision_hash

    if canonical_forward_strategy_decision_hash(decision) != decision.decision_hash:
        raise ValueError("forward decision hash does not match content")
    existing = await load_forward_strategy_decision(
        session,
        account_id=decision.account_id,
        strategy_version=decision.strategy_version,
        decision_session=decision.decision_session,
    )
    if existing is not None:
        if existing.decision.decision_hash != decision.decision_hash:
            raise ForwardDecisionConflictError("forward decision content conflict")
        return existing

    if decision.source_policy_id != SOURCE_POLICY_V2_ID:
        raise ValueError("new forward decision requires exact source_policy_v2")
    await load_source_policy_v2(
        session,
        instrument_tick_size=None,
        decision_time=decision.decision_time,
    )
    if not source_policy_v2_supports_adjusted_prices():
        raise ValueError("adjusted price authority is unavailable")
    await require_paper_decision_authority(session, decision)
    registry = await session.get(StrategyRegistryHeadRecord, "internal_paper")
    if registry is None:
        raise ValueError("internal-paper strategy registry is incomplete")
    active_definition = await session.get(StrategyDefinitionRecord, registry.active_definition_id)
    active_event = await _active_registry_event(
        session,
        head=registry,
        cutoff=decision.decision_time,
    )
    if (
        active_definition is None
        or decision.strategy_id
        != ("baseline" if decision.strategy_version == "baseline_v1" else "registered")
        or active_definition.strategy_version != decision.strategy_version
        or active_definition.id != decision.strategy_definition_id
        or active_definition.expression_hash != decision.strategy_parameters_hash
        or registry.active_generation != decision.champion_generation
        or active_event is None
        or active_event.id != decision.registry_event_id
    ):
        raise ValueError("forward decision registry binding is stale or invalid")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 1709))"),
        {
            "lock_key": (
                f"forward-strategy-decision:{decision.account_id}:"
                f"{decision.decision_session.isoformat()}"
            )
        },
    )
    concurrent = await load_forward_strategy_decision(
        session,
        account_id=decision.account_id,
        strategy_version=decision.strategy_version,
        decision_session=decision.decision_session,
    )
    if concurrent is not None:
        if concurrent.decision.decision_hash != decision.decision_hash:
            raise ForwardDecisionConflictError("forward decision content conflict")
        return concurrent
    decision_id = new_uuid7()
    session.add(
        StrategyDecisionRecord(
            id=decision_id,
            account_id=decision.account_id,
            portfolio_id=decision.portfolio_id,
            strategy_id=decision.strategy_id,
            strategy_version=decision.strategy_version,
            strategy_definition_id=decision.strategy_definition_id,
            registry_event_id=decision.registry_event_id,
            champion_generation=decision.champion_generation,
            decision_session=decision.decision_session,
            decision_time=decision.decision_time,
            execution_session=decision.execution_session,
            source_policy_id=decision.source_policy_id,
            calendar_authority_id=decision.calendar_authority_id,
            universe_snapshot_id=decision.universe_snapshot_id,
            quality_report_id=decision.quality_report_id,
            portfolio_projection_hash=decision.portfolio_projection_hash,
            authority_hash=decision.authority_hash,
            strategy_parameters_hash=decision.strategy_parameters_hash,
            decision_hash=decision.decision_hash,
            cash_weight=decision.cash_weight,
            reason=decision.reason,
        )
    )
    session.add_all(
        StrategyDecisionTargetRecord(
            id=new_uuid7(),
            strategy_decision_id=decision_id,
            account_id=decision.account_id,
            instrument_id=target.instrument_id,
            target_weight=target.target_weight,
            exit=target.exit,
            strategy_priority=target.strategy_priority,
            reason=target.reason,
        )
        for target in decision.targets
    )
    await session.flush()
    return PersistedForwardStrategyDecision(
        decision_id=decision_id,
        decision=decision,
    )


async def _active_registry_event(
    session: AsyncSession,
    *,
    head: StrategyRegistryHeadRecord,
    cutoff: datetime,
) -> StrategyRegistryEventRecord | None:
    event = await session.scalar(
        select(StrategyRegistryEventRecord)
        .where(
            StrategyRegistryEventRecord.channel == "internal_paper",
            StrategyRegistryEventRecord.event_type.in_(("GENESIS", "PROMOTE", "ROLLBACK")),
            StrategyRegistryEventRecord.to_definition_id == head.active_definition_id,
            StrategyRegistryEventRecord.champion_generation == head.active_generation,
        )
        .order_by(StrategyRegistryEventRecord.sequence.desc())
        .limit(1)
    )
    if event is None:
        return None
    if event.event_type != "GENESIS" and event.occurred_at > cutoff:
        return None
    if (
        event.event_type in {"PROMOTE", "ROLLBACK"}
        and event.effective_session is not None
        and event.effective_session > cutoff.date()
    ):
        return None
    return event

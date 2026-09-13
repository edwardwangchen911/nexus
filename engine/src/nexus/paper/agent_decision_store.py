"""PostgreSQL authority and persistence for Agent-origin paper decisions."""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import load_actionable_research_output
from nexus.domain.ids import new_uuid7
from nexus.paper.agent_decision import (
    AgentPaperDecision,
    AgentPaperDecisionBinding,
    canonical_agent_binding_hash,
    canonical_agent_paper_decision_hash,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import SourcePolicy
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
    StrategyDecisionTargetRecord,
)
from nexus.strategy.forward import (
    ForwardDecisionConflictError,
    ForwardStrategyTarget,
    PersistedForwardStrategyDecision,
    load_forward_strategy_decision,
    require_paper_decision_authority,
)


@dataclass(frozen=True)
class PersistedAgentPaperDecision:
    decision_id: UUID
    decision: AgentPaperDecision


PersistedPaperDecision = PersistedForwardStrategyDecision | PersistedAgentPaperDecision


def _binding_valid(decision: AgentPaperDecision, binding: AgentPaperDecisionBinding) -> bool:
    return binding.binding_hash == canonical_agent_binding_hash(
        cycle_id=decision.cycle_id,
        decision_hash=decision.decision_hash,
        binding=binding,
    )


async def require_agent_source_policies(
    session: AsyncSession, decision: AgentPaperDecision
) -> None:
    expected = (
        (
            decision.source_policy_id,
            "paper_decision_price",
            "paper_decision_price_policy_v1",
            "70737603eb58331d60668bb6a039e4910d856a2c992e972fc8af0adcf05db579",
        ),
        (
            decision.execution_source_policy_id,
            "paper_execution_price",
            "paper_execution_price_policy_v1",
            "41efc2c8625321bc8d9d941500fd3f809868a12fcc13ee2fefdfa4bf52705682",
        ),
    )
    for policy_id, key, version, content_hash in expected:
        policy = await session.get(SourcePolicy, policy_id, populate_existing=True)
        if (
            policy is None
            or policy.key != key
            or policy.version != version
            or policy.schema_version != "1"
            or policy.created_at > decision.decision_time
            or policy.payload.get("fallback") != "disabled"
            or policy.payload.get("selected_source") != "alpaca"
            or policy.payload.get("content_hash") != content_hash
        ):
            raise ValueError("Agent decision source policy is missing or invalid")


async def require_agent_decision_bindings(
    session: AsyncSession, decision: AgentPaperDecision
) -> None:
    if not 1 <= len(decision.bindings) <= 5:
        raise ValueError("Agent decision requires one to five output bindings")
    output_ids = [binding.output_id for binding in decision.bindings]
    if len(output_ids) != len(set(output_ids)):
        raise ValueError("Agent decision output bindings must be unique")
    for binding in decision.bindings:
        output = await session.get(
            AgentResearchOutputRecord,
            binding.output_id,
            populate_existing=True,
        )
        if output is None:
            raise ValueError("Agent decision output binding is missing")
        admitted = await load_actionable_research_output(
            session,
            run_id=output.run_id,
            decision_time=decision.decision_time,
        )
        if (
            admitted.output_id != binding.output_id
            or admitted.instrument_id != binding.instrument_id
            or admitted.intent_hash != binding.intent_hash
            or admitted.sizing_action != binding.sizing_action
            or not _binding_valid(decision, binding)
        ):
            raise ValueError("Agent decision output binding is invalid")


async def _load_agent_paper_decision(
    session: AsyncSession,
    record: StrategyDecisionRecord,
) -> PersistedAgentPaperDecision:
    cycle = await session.scalar(
        select(AgentPaperCycleRecord).where(
            AgentPaperCycleRecord.account_id == record.account_id,
            AgentPaperCycleRecord.decision_session == record.decision_session,
        )
    )
    if cycle is None or record.execution_source_policy_id is None:
        raise ValueError("Agent paper decision authority is missing")
    targets = tuple(
        await session.scalars(
            select(StrategyDecisionTargetRecord)
            .where(StrategyDecisionTargetRecord.strategy_decision_id == record.id)
            .order_by(
                StrategyDecisionTargetRecord.strategy_priority,
                StrategyDecisionTargetRecord.instrument_id,
            )
        )
    )
    binding_rows = tuple(
        (
            await session.execute(
                select(
                    StrategyDecisionAgentOutputRecord,
                    AgentResearchRunRecord.instrument_id,
                )
                .join(
                    AgentResearchOutputRecord,
                    AgentResearchOutputRecord.id
                    == StrategyDecisionAgentOutputRecord.agent_research_output_id,
                )
                .join(
                    AgentResearchRunRecord,
                    AgentResearchRunRecord.id == AgentResearchOutputRecord.run_id,
                )
                .where(StrategyDecisionAgentOutputRecord.strategy_decision_id == record.id)
                .order_by(AgentResearchRunRecord.instrument_id)
            )
        ).all()
    )
    decision = AgentPaperDecision(
        cycle_id=cycle.id,
        account_id=record.account_id,
        portfolio_id=record.portfolio_id,
        decision_session=record.decision_session,
        decision_time=record.decision_time,
        execution_session=record.execution_session,
        source_policy_id=record.source_policy_id,
        execution_source_policy_id=record.execution_source_policy_id,
        calendar_authority_id=record.calendar_authority_id,
        universe_snapshot_id=record.universe_snapshot_id,
        quality_report_id=record.quality_report_id,
        portfolio_projection_hash=record.portfolio_projection_hash,
        authority_hash=record.authority_hash,
        sizing_policy_hash=record.strategy_parameters_hash,
        decision_hash=record.decision_hash,
        cash_weight=record.cash_weight,
        targets=tuple(
            ForwardStrategyTarget(
                instrument_id=target.instrument_id,
                target_weight=target.target_weight,
                exit=target.exit,
                strategy_priority=target.strategy_priority,
                reason=target.reason,
            )
            for target in targets
        ),
        bindings=tuple(
            AgentPaperDecisionBinding(
                output_id=binding.agent_research_output_id,
                instrument_id=instrument_id,
                intent_hash=binding.intent_hash,
                sizing_action=binding.sizing_action,
                binding_hash=binding.binding_hash,
            )
            for binding, instrument_id in binding_rows
        ),
    )
    if (
        record.decision_origin != "agent_research"
        or record.strategy_id != "agent_research"
        or record.strategy_version != "agent_paper_decision_v1"
        or record.reason != "AGENT_RESEARCH"
        or cycle.sizing_policy_hash != decision.sizing_policy_hash
        or not 1 <= len(decision.bindings) <= 5
        or canonical_agent_paper_decision_hash(decision) != decision.decision_hash
        or any(not _binding_valid(decision, binding) for binding in decision.bindings)
    ):
        raise ValueError("stored Agent paper decision is invalid")
    return PersistedAgentPaperDecision(record.id, decision)


async def load_paper_decision(
    session: AsyncSession,
    *,
    account_id: UUID,
    decision_id: UUID,
) -> PersistedPaperDecision | None:
    """Load either decision origin without weakening either authority contract."""
    record = await session.get(StrategyDecisionRecord, decision_id, populate_existing=True)
    if record is None or record.account_id != account_id:
        return None
    if record.decision_origin == "registered_strategy":
        return await load_forward_strategy_decision(
            session,
            account_id=account_id,
            strategy_version=record.strategy_version,
            decision_session=record.decision_session,
        )
    if record.decision_origin == "agent_research":
        return await _load_agent_paper_decision(session, record)
    raise ValueError("paper decision origin is invalid")


async def persist_agent_paper_decision(
    session: AsyncSession,
    decision: AgentPaperDecision,
) -> PersistedAgentPaperDecision:
    """Persist one Agent decision after revalidating all database-owned authority."""
    if canonical_agent_paper_decision_hash(decision) != decision.decision_hash:
        raise ValueError("Agent decision hash does not match content")
    await require_agent_source_policies(session, decision)
    await require_paper_decision_authority(session, decision)
    await require_agent_decision_bindings(session, decision)
    cycle = await session.get(AgentPaperCycleRecord, decision.cycle_id, populate_existing=True)
    if (
        cycle is None
        or cycle.account_id != decision.account_id
        or cycle.portfolio_id != decision.portfolio_id
        or cycle.decision_session != decision.decision_session
        or cycle.decision_time != decision.decision_time
        or cycle.execution_session != decision.execution_session
        or cycle.decision_price_policy_id != decision.source_policy_id
        or cycle.execution_price_policy_id != decision.execution_source_policy_id
        or cycle.universe_snapshot_id != decision.universe_snapshot_id
        or cycle.quality_report_id != decision.quality_report_id
        or cycle.sizing_policy_hash != decision.sizing_policy_hash
    ):
        raise ValueError("Agent paper cycle does not match decision")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 1709))"),
        {
            "lock_key": (
                f"forward-strategy-decision:{decision.account_id}:"
                f"{decision.decision_session.isoformat()}"
            )
        },
    )
    existing = await session.scalar(
        select(StrategyDecisionRecord).where(
            StrategyDecisionRecord.account_id == decision.account_id,
            StrategyDecisionRecord.decision_session == decision.decision_session,
        )
    )
    if existing is not None:
        persisted = await load_paper_decision(
            session,
            account_id=decision.account_id,
            decision_id=existing.id,
        )
        if (
            not isinstance(persisted, PersistedAgentPaperDecision)
            or persisted.decision.decision_hash != decision.decision_hash
        ):
            raise ForwardDecisionConflictError("paper decision content conflict")
        return persisted

    decision_id = new_uuid7()
    session.add(
        StrategyDecisionRecord(
            id=decision_id,
            account_id=decision.account_id,
            portfolio_id=decision.portfolio_id,
            strategy_id="agent_research",
            strategy_version="agent_paper_decision_v1",
            strategy_definition_id=None,
            registry_event_id=None,
            champion_generation=None,
            decision_origin="agent_research",
            decision_session=decision.decision_session,
            decision_time=decision.decision_time,
            execution_session=decision.execution_session,
            source_policy_id=decision.source_policy_id,
            execution_source_policy_id=decision.execution_source_policy_id,
            calendar_authority_id=decision.calendar_authority_id,
            universe_snapshot_id=decision.universe_snapshot_id,
            quality_report_id=decision.quality_report_id,
            portfolio_projection_hash=decision.portfolio_projection_hash,
            authority_hash=decision.authority_hash,
            strategy_parameters_hash=decision.sizing_policy_hash,
            decision_hash=decision.decision_hash,
            cash_weight=decision.cash_weight,
            reason="AGENT_RESEARCH",
        )
    )
    # The binding guard reads the decision origin from PostgreSQL, so make the
    # immutable root visible before dependent rows are checked.
    await session.flush()
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
    session.add_all(
        StrategyDecisionAgentOutputRecord(
            strategy_decision_id=decision_id,
            agent_research_output_id=binding.output_id,
            intent_hash=binding.intent_hash,
            sizing_action=binding.sizing_action,
            binding_hash=binding.binding_hash,
        )
        for binding in decision.bindings
    )
    await session.flush()
    return PersistedAgentPaperDecision(decision_id, decision)

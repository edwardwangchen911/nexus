"""PostgreSQL authority and persistence for Agent-origin paper decisions."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_candidate import require_candidate_consumer_profile
from nexus.agents.persistence import load_actionable_research_output
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_persistence import load_frozen_synthesis_authority
from nexus.agents.portfolio_synthesis_worker import read_synthesis_job
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.paper.agent_decision import (
    AgentPaperDecision,
    AgentPaperDecisionBinding,
    canonical_agent_binding_hash,
    canonical_agent_paper_decision_hash,
)
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_POLICY_HASH
from nexus.paper.portfolio_proposal import (
    PortfolioBaselineV1,
    PortfolioProposalV1,
    build_portfolio_proposal,
    canonical_portfolio_target_content_hash,
    initial_projection_baseline,
)
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import (
    load_initial_approval_projection,
    lock_portfolio_account,
)
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import SourcePolicy
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    PaperInitialTargetApprovalRecord,
    PaperTargetVersionRecord,
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
    expected: tuple[tuple[UUID, str, str, str, str | None], ...]
    if (
        decision.source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
        and decision.execution_source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
    ):
        expected = (
            (
                decision.source_policy_id,
                "paper_decision_price",
                "paper_schwab_l1_decision_v1",
                "fcbb81143639771e8c9228d768707cb11b720a7e69fcd0598aef2af958dba885",
                None,
            ),
            (
                decision.execution_source_policy_id,
                "paper_execution_price",
                "paper_schwab_price_history_execution_v1",
                "b53d7d39a12ae0debf366b6bdb36c8e86f67229517d4b490b68878875a883dac",
                "schwab",
            ),
        )
    else:
        expected = (
            (
                decision.source_policy_id,
                "paper_decision_price",
                "paper_decision_price_policy_v1",
                "70737603eb58331d60668bb6a039e4910d856a2c992e972fc8af0adcf05db579",
                "alpaca",
            ),
            (
                decision.execution_source_policy_id,
                "paper_execution_price",
                "paper_execution_price_policy_v1",
                "41efc2c8625321bc8d9d941500fd3f809868a12fcc13ee2fefdfa4bf52705682",
                "alpaca",
            ),
        )
    for policy_id, key, version, content_hash, selected_source in expected:
        policy = await session.get(SourcePolicy, policy_id, populate_existing=True)
        if policy is None:
            raise ValueError("Agent decision source policy is missing or invalid")
        price_source = policy.payload.get("price_source")
        schwab = policy.payload.get("schwab")
        if (
            policy.key != key
            or policy.version != version
            or policy.schema_version != "1"
            or policy.created_at > decision.decision_time
            or policy.payload.get("fallback") != "disabled"
            or (
                selected_source is not None
                and policy.payload.get("selected_source") != selected_source
            )
            or policy.payload.get("content_hash") != content_hash
            or (
                policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
                and (
                    policy.payload.get("market_context_source") != "schwab"
                    or not isinstance(price_source, dict)
                    or price_source.get("source") != "alpaca"
                    or not isinstance(schwab, dict)
                    or schwab.get("delayed") is not False
                )
            )
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
            decision_time=max(decision.decision_time, output.created_at),
            allow_safe_no_trade=True,
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
    cycle = (
        None
        if record.agent_paper_cycle_id is None
        else await session.get(AgentPaperCycleRecord, record.agent_paper_cycle_id)
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
        {"lock_key": f"agent-paper-decision:{decision.cycle_id}"},
    )
    existing = await session.scalar(
        select(StrategyDecisionRecord).where(
            StrategyDecisionRecord.agent_paper_cycle_id == decision.cycle_id,
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
            agent_paper_cycle_id=decision.cycle_id,
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


@dataclass(frozen=True)
class PersistedPortfolioProposal:
    decision_id: UUID
    account_id: UUID
    cycle_id: UUID
    proposal: PortfolioProposalV1


async def _load_portfolio_target_baseline(
    session: AsyncSession,
    *,
    target: PaperTargetVersionRecord,
    account_id: UUID,
    candidate_stage_capacity: bool,
    formal_stage_capacity: bool,
) -> PersistedPortfolioProposal | None:
    """Read the prior target using its own authority, without upgrading its identity."""
    prior_formal = False
    if formal_stage_capacity:
        from nexus.agents.codex_versions import (
            BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
            CANDIDATE_BUDGET_CONFIG_VERSION,
            FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
            FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
            FULL_INPUT_CAPACITY_CONFIG_VERSION,
        )

        decision = await session.get(StrategyDecisionRecord, target.strategy_decision_id)
        prior_run = (
            None
            if decision is None or decision.account_id != account_id
            else await session.scalar(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id == decision.agent_paper_cycle_id
                )
            )
        )
        if prior_run is None or prior_run.pricing_version == CANDIDATE_BUDGET_CONFIG_VERSION:
            raise ValueError("PORTFOLIO_PROPOSAL_TARGET_CONFLICT")
        prior_formal = prior_run.pricing_version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
        if prior_run.pricing_version in {
            FULL_INPUT_CAPACITY_CONFIG_VERSION,
            BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
            FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        }:
            from nexus.paper.management_read_authority import formal_proposal_read_profile

            prior_formal = await formal_proposal_read_profile(
                session,
                account_id=account_id,
                decision_id=target.strategy_decision_id,
                read_as_of=target.created_at,
            )
            if not prior_formal:
                raise ValueError("PORTFOLIO_PROPOSAL_TARGET_CONFLICT")
    return await load_portfolio_proposal(
        session,
        account_id=account_id,
        decision_id=target.strategy_decision_id,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=prior_formal,
    )


async def _portfolio_bindings(
    session: AsyncSession,
    *,
    frozen: PortfolioSynthesisInputV1,
    decision_hash: str,
    at: datetime,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> tuple[AgentPaperDecisionBinding, ...]:
    bindings = []
    for source in frozen.security_outputs:
        output = await session.get(AgentResearchOutputRecord, source.research_output_id)
        research = (
            None if output is None else await session.get(AgentResearchRunRecord, output.run_id)
        )
        if (
            output is None
            or research is None
            or output.content_hash != source.content_hash
            or research.instrument_id != source.instrument_id
            or research.decision_time != frozen.decision_time
            or research.idempotency_key != f"agent-paper:{frozen.cycle_id}:{source.instrument_id}"
        ):
            raise ValueError("PORTFOLIO_PROPOSAL_RESEARCH_CONFLICT")
        admitted = await load_actionable_research_output(
            session,
            run_id=research.id,
            decision_time=at,
            allow_safe_no_trade=True,
            candidate_stage_capacity=candidate_stage_capacity,
            formal_stage_capacity=formal_stage_capacity,
        )
        if (
            admitted.output_id != source.research_output_id
            or admitted.intent_hash != source.intent_hash
            or admitted.evidence_ref_ids != source.evidence_ref_ids
        ):
            raise ValueError("PORTFOLIO_PROPOSAL_RESEARCH_CONFLICT")
        binding = AgentPaperDecisionBinding(
            output_id=output.id,
            instrument_id=source.instrument_id,
            intent_hash=source.intent_hash,
            sizing_action=admitted.sizing_action,
            binding_hash="0" * 64,
        )
        bindings.append(
            binding.model_copy(
                update={
                    "binding_hash": canonical_agent_binding_hash(
                        cycle_id=frozen.cycle_id, decision_hash=decision_hash, binding=binding
                    )
                }
            )
        )
    return tuple(bindings)


async def _rebuild_portfolio_proposal(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    output_id: UUID,
    now: datetime | None,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> PortfolioProposalV1:
    output = await session.get(
        AgentPortfolioSynthesisOutputRecord, output_id, populate_existing=True
    )
    run = (
        None
        if output is None
        else await session.get(
            AgentPortfolioSynthesisRunRecord, output.run_id, populate_existing=True
        )
    )
    if (
        output is None
        or run is None
        or run.cycle_id != cycle.id
        or output.status != "completed"
        or run.status != "completed"
        or cycle.candidate_batch_id is None
        or cycle.sizing_policy_version != "portfolio_allocation_v1"
        or cycle.sizing_policy_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
    ):
        raise ValueError("PORTFOLIO_PROPOSAL_AUTHORITY_CONFLICT")
    frozen = await load_frozen_synthesis_authority(session, run)
    if formal_stage_capacity:
        from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
        from nexus.paper.agent_cycle import _require_cycle_candidate_profile

        formal_model_id = await _require_cycle_candidate_profile(
            session,
            cycle=cycle,
            candidate_stage_capacity=False,
            formal_stage_capacity=True,
            now=frozen.admitted_at if now is None else now,
        )
        formal_model = await require_formal_stage_model_profile(
            session,
            model_config_id=formal_model_id,
            now=frozen.admitted_at if now is None else now,
        )
        if (
            run.model_config_id != formal_model_id
            or run.model_hash != formal_model.config_hash
            or run.pricing_version != formal_model.version
        ):
            raise ValueError("PORTFOLIO_PROPOSAL_AUTHORITY_CONFLICT")
    job = await read_synthesis_job(session, cycle_id=cycle.id)
    if (
        job is None
        or job.status != "succeeded"
        or frozen.account_id != cycle.account_id
        or frozen.portfolio_id != cycle.portfolio_id
        or frozen.candidate_batch_id != cycle.candidate_batch_id
        or frozen.decision_time != cycle.decision_time
        or frozen.policy_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
    ):
        raise ValueError("PORTFOLIO_PROPOSAL_AUTHORITY_CONFLICT")
    if now is not None and not output.created_at <= require_utc(now) < frozen.expires_at:
        raise ValueError("PORTFOLIO_PROPOSAL_EXPIRED")
    intent = PortfolioSynthesisIntentV1.model_validate(output.intent)
    validate_portfolio_synthesis_intent(intent, frozen)
    content_hash = canonical_portfolio_synthesis_hash(
        {
            "schema_version": "portfolio_synthesis_terminal_v1",
            "status": "completed",
            "intent": intent.model_dump(mode="json"),
        }
    )
    if (
        output.content_hash != content_hash
        or output.binding_hash
        != canonical_portfolio_synthesis_hash(
            {
                "schema_version": "portfolio_synthesis_binding_v1",
                "run_id": run.id,
                "input_hash": run.input_hash,
                "content_hash": content_hash,
            }
        )
    ):
        raise ValueError("PORTFOLIO_PROPOSAL_OUTPUT_CONFLICT")
    projection = await load_paper_projection(
        session, account_id=cycle.account_id, as_of=cycle.decision_time
    )
    if projection.projection_hash != frozen.projection_hash:
        raise ValueError("PORTFOLIO_PROPOSAL_PROJECTION_CONFLICT")
    if now is not None:
        current_target = await session.scalar(
            select(PaperTargetVersionRecord.id)
            .where(PaperTargetVersionRecord.account_id == cycle.account_id)
            .order_by(PaperTargetVersionRecord.version_seq.desc())
            .limit(1)
        )
        if current_target != frozen.active_target_version_id:
            raise ValueError("PORTFOLIO_PROPOSAL_BASELINE_CONFLICT")
    if frozen.active_target_version_id is None:
        approval = await session.scalar(
            select(PaperInitialTargetApprovalRecord).where(
                PaperInitialTargetApprovalRecord.account_id == cycle.account_id
            )
        )
        if approval is None:
            raise ValueError("POLICY_UNCONFIRMED")
        approved_projection = await load_initial_approval_projection(
            session,
            approval=approval,
            portfolio_id=cycle.portfolio_id,
            decision_time=cycle.decision_time,
            now=now,
        )
        baseline = initial_projection_baseline(
            approved_projection, approval_id=approval.id, approval_hash=approval.approval_hash
        )
    else:
        target = await session.get(PaperTargetVersionRecord, frozen.active_target_version_id)
        prior = (
            None
            if target is None
            else await _load_portfolio_target_baseline(
                session,
                target=target,
                account_id=cycle.account_id,
                candidate_stage_capacity=candidate_stage_capacity,
                formal_stage_capacity=formal_stage_capacity,
            )
        )
        if (
            target is None
            or prior is None
            or target.account_id != cycle.account_id
            or target.portfolio_id != cycle.portfolio_id
            or target.target_content_hash != frozen.active_target_content_hash
            or target.target_content_hash != canonical_portfolio_target_content_hash(prior.proposal)
            or prior.proposal.weights
            != {item.instrument_id: item.target_weight for item in frozen.active_target_weights}
            or prior.proposal.cash_weight != frozen.active_target_cash_weight
        ):
            raise ValueError("PORTFOLIO_PROPOSAL_TARGET_CONFLICT")
        baseline = PortfolioBaselineV1(
            weights=prior.proposal.weights,
            cash_weight=prior.proposal.cash_weight,
            roles=prior.proposal.roles,
            target_version_id=target.id,
            content_hash=target.target_content_hash,
        )
    proposal = build_portfolio_proposal(
        active_target=baseline,
        synthesis=intent,
        synthesis_output_id=output.id,
        synthesis_output_hash=output.content_hash,
        expires_at=frozen.expires_at,
    )
    await _portfolio_bindings(
        session,
        frozen=frozen,
        decision_hash=proposal.decision_hash,
        at=frozen.admitted_at if now is None else now,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    return proposal


def _target_priority(proposal: PortfolioProposalV1, instrument_id: UUID) -> int:
    if proposal.weights[instrument_id] == 0:
        return 0
    return (
        1
        if proposal.weights[instrument_id] <= proposal.baseline.weights.get(instrument_id, 0)
        else 2
    )


async def load_portfolio_proposal(
    session: AsyncSession,
    *,
    account_id: UUID,
    decision_id: UUID,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> PersistedPortfolioProposal | None:
    if type(formal_stage_capacity) is not bool or (
        formal_stage_capacity and candidate_stage_capacity is not False
    ):
        raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    record = await session.get(StrategyDecisionRecord, decision_id, populate_existing=True)
    if record is None or record.account_id != account_id:
        return None
    cycle = (
        None
        if record.agent_paper_cycle_id is None
        else await session.get(AgentPaperCycleRecord, record.agent_paper_cycle_id)
    )
    if (
        cycle is None
        or record.strategy_version != "agent_portfolio_proposal_v1"
        or record.portfolio_synthesis_output_id is None
    ):
        raise ValueError("PORTFOLIO_PROPOSAL_RECORD_CONFLICT")
    proposal = await _rebuild_portfolio_proposal(
        session,
        cycle=cycle,
        output_id=record.portfolio_synthesis_output_id,
        now=None,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    rows = tuple(
        await session.scalars(
            select(StrategyDecisionTargetRecord).where(
                StrategyDecisionTargetRecord.strategy_decision_id == record.id
            )
        )
    )
    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord).where(
            AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id
        )
    )
    if run is None:
        raise ValueError("PORTFOLIO_PROPOSAL_AUTHORITY_CONFLICT")
    frozen = await load_frozen_synthesis_authority(session, run)
    bindings = await _portfolio_bindings(
        session,
        frozen=frozen,
        decision_hash=proposal.decision_hash,
        at=frozen.admitted_at,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    bound_rows = tuple(
        await session.scalars(
            select(StrategyDecisionAgentOutputRecord).where(
                StrategyDecisionAgentOutputRecord.strategy_decision_id == record.id
            )
        )
    )
    if (
        record.decision_hash != proposal.decision_hash
        or record.cash_weight != proposal.cash_weight
        or record.strategy_parameters_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
        or record.authority_hash != proposal.synthesis.input_hash
        or record.portfolio_id != cycle.portfolio_id
        or record.decision_time != cycle.decision_time
        or record.decision_session != cycle.decision_session
        or record.execution_session != cycle.execution_session
        or record.source_policy_id != cycle.decision_price_policy_id
        or record.execution_source_policy_id != cycle.execution_price_policy_id
        or record.calendar_authority_id != frozen.calendar_authority_id
        or record.universe_snapshot_id != cycle.universe_snapshot_id
        or record.portfolio_projection_hash != frozen.projection_hash
        or {
            row.agent_research_output_id: (row.intent_hash, row.sizing_action, row.binding_hash)
            for row in bound_rows
        }
        != {
            binding.output_id: (binding.intent_hash, binding.sizing_action, binding.binding_hash)
            for binding in bindings
        }
        or {row.instrument_id: row.target_weight for row in rows} != proposal.weights
        or any(
            row.reason != proposal.reasons[row.instrument_id]
            or row.strategy_priority != _target_priority(proposal, row.instrument_id)
            or row.exit != (row.target_weight == 0)
            for row in rows
        )
    ):
        raise ValueError("PORTFOLIO_PROPOSAL_RECORD_CONFLICT")
    return PersistedPortfolioProposal(record.id, account_id, cycle.id, proposal)


async def persist_portfolio_proposal(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    synthesis_output_id: UUID,
    now: datetime,
    proposal: PortfolioProposalV1 | None = None,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> PersistedPortfolioProposal:
    if type(formal_stage_capacity) is not bool or (
        formal_stage_capacity and candidate_stage_capacity is not False
    ):
        raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    cycle = await session.get(AgentPaperCycleRecord, cycle_id, populate_existing=True)
    if cycle is None:
        raise ValueError("PORTFOLIO_PROPOSAL_CYCLE_CONFLICT")
    await lock_portfolio_account(session, cycle.account_id)
    existing = await session.scalar(
        select(StrategyDecisionRecord).where(
            StrategyDecisionRecord.agent_paper_cycle_id == cycle_id
        )
    )
    if existing is not None:
        stored = await load_portfolio_proposal(
            session,
            account_id=cycle.account_id,
            decision_id=existing.id,
            candidate_stage_capacity=candidate_stage_capacity,
            formal_stage_capacity=formal_stage_capacity,
        )
        if (
            stored is None
            or stored.proposal.synthesis_output_id != synthesis_output_id
            or proposal is not None
            and proposal != stored.proposal
        ):
            raise ValueError("PORTFOLIO_PROPOSAL_CONTENT_CONFLICT")
        return stored
    rebuilt = await _rebuild_portfolio_proposal(
        session,
        cycle=cycle,
        output_id=synthesis_output_id,
        now=now,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    if proposal is not None and proposal != rebuilt:
        raise ValueError("PORTFOLIO_PROPOSAL_CONTENT_CONFLICT")
    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord).where(
            AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id
        )
    )
    if run is None:
        raise ValueError("PORTFOLIO_PROPOSAL_AUTHORITY_CONFLICT")
    frozen = await load_frozen_synthesis_authority(session, run)
    decision_id = new_uuid7()
    session.add(
        StrategyDecisionRecord(
            id=decision_id,
            account_id=cycle.account_id,
            portfolio_id=cycle.portfolio_id,
            strategy_id="agent_research",
            strategy_version="agent_portfolio_proposal_v1",
            strategy_definition_id=None,
            registry_event_id=None,
            champion_generation=None,
            decision_origin="agent_research",
            agent_paper_cycle_id=cycle.id,
            portfolio_synthesis_output_id=synthesis_output_id,
            decision_session=cycle.decision_session,
            decision_time=cycle.decision_time,
            execution_session=cycle.execution_session,
            source_policy_id=cycle.decision_price_policy_id,
            execution_source_policy_id=cycle.execution_price_policy_id,
            calendar_authority_id=frozen.calendar_authority_id,
            universe_snapshot_id=cycle.universe_snapshot_id,
            quality_report_id=None,
            portfolio_projection_hash=frozen.projection_hash,
            authority_hash=run.input_hash,
            strategy_parameters_hash=PORTFOLIO_ALLOCATION_POLICY_HASH,
            decision_hash=rebuilt.decision_hash,
            cash_weight=rebuilt.cash_weight,
            reason="AGENT_RESEARCH",
            created_at=now,
        )
    )
    await session.flush()
    session.add_all(
        StrategyDecisionTargetRecord(
            strategy_decision_id=decision_id,
            account_id=cycle.account_id,
            instrument_id=key,
            target_weight=weight,
            exit=weight == 0,
            strategy_priority=_target_priority(rebuilt, key),
            reason=rebuilt.reasons[key],
            created_at=now,
        )
        for key, weight in rebuilt.weights.items()
    )
    bindings = await _portfolio_bindings(
        session,
        frozen=frozen,
        decision_hash=rebuilt.decision_hash,
        at=now,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    session.add_all(
        StrategyDecisionAgentOutputRecord(
            strategy_decision_id=decision_id,
            agent_research_output_id=binding.output_id,
            intent_hash=binding.intent_hash,
            sizing_action=binding.sizing_action,
            binding_hash=binding.binding_hash,
        )
        for binding in bindings
    )
    await session.flush()
    return PersistedPortfolioProposal(decision_id, cycle.account_id, cycle.id, rebuilt)

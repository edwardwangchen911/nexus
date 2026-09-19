"""Strict immutable authority loaders for portfolio-management reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_persistence import _frozen_run
from nexus.paper.agent_decision_store import (
    PersistedPortfolioProposal,
    load_portfolio_proposal,
)
from nexus.paper.candidate_batch import CandidateBatchResult, load_agent_paper_candidate_batch
from nexus.paper.portfolio_proposal import canonical_portfolio_target_content_hash
from nexus.paper.target_service import canonical_target_activation_hash
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)


@dataclass(frozen=True)
class SynthesisReadAuthority:
    run: AgentPortfolioSynthesisRunRecord
    output: AgentPortfolioSynthesisOutputRecord
    frozen: PortfolioSynthesisInputV1
    intent: PortfolioSynthesisIntentV1 | None


@dataclass(frozen=True)
class ProposalReadAuthority:
    decision: StrategyDecisionRecord
    persisted: PersistedPortfolioProposal


@dataclass(frozen=True)
class TargetReadAuthority:
    target: PaperTargetVersionRecord
    proposal: ProposalReadAuthority
    previous: PaperTargetVersionRecord | None


async def load_candidate_batch_authority(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    read_as_of: datetime,
) -> CandidateBatchResult | None:
    if cycle.candidate_batch_id is None:
        return None
    batch = await load_agent_paper_candidate_batch(session, batch_id=cycle.candidate_batch_id)
    if (
        batch is None
        or batch.account_id != cycle.account_id
        or batch.universe_snapshot_id != cycle.universe_snapshot_id
        or batch.created_at > read_as_of
    ):
        raise ValueError("PORTFOLIO_CANDIDATE_BATCH_AUTHORITY_CONFLICT")
    return batch


async def load_synthesis_read_authority(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    read_as_of: datetime,
) -> SynthesisReadAuthority | None:
    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord).where(
            AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id,
            AgentPortfolioSynthesisRunRecord.created_at <= read_as_of,
        )
    )
    if run is None:
        return None
    frozen = _frozen_run(run)
    if (
        frozen.account_id != cycle.account_id
        or frozen.portfolio_id != cycle.portfolio_id
        or frozen.candidate_batch_id != cycle.candidate_batch_id
        or frozen.decision_time != cycle.decision_time
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_AUTHORITY_CONFLICT")
    output = await session.scalar(
        select(AgentPortfolioSynthesisOutputRecord).where(
            AgentPortfolioSynthesisOutputRecord.run_id == run.id,
            AgentPortfolioSynthesisOutputRecord.created_at <= read_as_of,
        )
    )
    if output is None:
        if run.status in {"completed", "abstained", "failed"} or run.completed_at is not None:
            raise ValueError("PORTFOLIO_SYNTHESIS_TERMINAL_OUTPUT_MISSING")
        return None
    intent = (
        None
        if output.intent is None
        else PortfolioSynthesisIntentV1.model_validate(output.intent)
    )
    content_hash = canonical_portfolio_synthesis_hash(
        {
            "schema_version": "portfolio_synthesis_terminal_v1",
            "status": output.status,
            "intent": None if intent is None else intent.model_dump(mode="json"),
        }
    )
    binding_hash = canonical_portfolio_synthesis_hash(
        {
            "schema_version": "portfolio_synthesis_binding_v1",
            "run_id": run.id,
            "input_hash": run.input_hash,
            "content_hash": content_hash,
        }
    )
    if (
        run.status != output.status
        or (output.status == "completed") != (intent is not None)
        or output.content_hash != content_hash
        or output.binding_hash != binding_hash
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_OUTPUT_CONFLICT")
    if intent is not None:
        validate_portfolio_synthesis_intent(intent, frozen)
    return SynthesisReadAuthority(run=run, output=output, frozen=frozen, intent=intent)


async def load_proposal_read_authority(
    session: AsyncSession,
    *,
    account_id: UUID,
    decision_id: UUID,
    read_as_of: datetime,
) -> ProposalReadAuthority | None:
    decision = await session.scalar(
        select(StrategyDecisionRecord).where(
            StrategyDecisionRecord.id == decision_id,
            StrategyDecisionRecord.account_id == account_id,
            StrategyDecisionRecord.created_at <= read_as_of,
        )
    )
    if decision is None:
        return None
    persisted = await load_portfolio_proposal(
        session,
        account_id=account_id,
        decision_id=decision.id,
    )
    if persisted is None or persisted.cycle_id != decision.agent_paper_cycle_id:
        raise ValueError("PORTFOLIO_PROPOSAL_AUTHORITY_CONFLICT")
    return ProposalReadAuthority(decision=decision, persisted=persisted)


async def load_target_read_authority(
    session: AsyncSession,
    *,
    account_id: UUID,
    target_id: UUID,
    read_as_of: datetime,
) -> TargetReadAuthority | None:
    target = await session.scalar(
        select(PaperTargetVersionRecord).where(
            PaperTargetVersionRecord.id == target_id,
            PaperTargetVersionRecord.account_id == account_id,
            PaperTargetVersionRecord.created_at <= read_as_of,
            PaperTargetVersionRecord.activated_at <= read_as_of,
        )
    )
    if target is None:
        return None
    proposal = await load_proposal_read_authority(
        session,
        account_id=account_id,
        decision_id=target.strategy_decision_id,
        read_as_of=read_as_of,
    )
    previous = (
        None
        if target.previous_target_version_id is None
        else await session.scalar(
            select(PaperTargetVersionRecord).where(
                PaperTargetVersionRecord.id == target.previous_target_version_id,
                PaperTargetVersionRecord.account_id == account_id,
                PaperTargetVersionRecord.created_at <= read_as_of,
                PaperTargetVersionRecord.activated_at <= read_as_of,
            )
        )
    )
    if (
        proposal is None
        or target.portfolio_id != proposal.decision.portfolio_id
        or target.target_content_hash
        != canonical_portfolio_target_content_hash(proposal.persisted.proposal)
        or target.activation_hash
        != canonical_target_activation_hash(
            account_id=target.account_id,
            portfolio_id=target.portfolio_id,
            version_seq=target.version_seq,
            strategy_decision_id=target.strategy_decision_id,
            previous_target_version_id=target.previous_target_version_id,
            target_content_hash=target.target_content_hash,
            activated_at=target.activated_at,
        )
        or (target.version_seq == 1) != (previous is None)
        or previous is not None
        and (
            previous.version_seq != target.version_seq - 1
            or previous.activated_at > target.activated_at
        )
    ):
        raise ValueError("PORTFOLIO_TARGET_AUTHORITY_CONFLICT")
    return TargetReadAuthority(target=target, proposal=proposal, previous=previous)

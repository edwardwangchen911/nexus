"""Qualified historical no-trade pair results; no window or execution writes."""

from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_synthesis_contracts import canonical_portfolio_synthesis_hash
from nexus.paper.agent_cycle_models import CycleStatus, canonical_cycle_event_hash
from nexus.paper.agent_decision_store import load_portfolio_proposal
from nexus.paper.economic_cycle import EconomicLaneRef, read_economic_cycle, require_economic_lane
from nexus.paper.portfolio_proposal import (
    PortfolioProposalV1,
    canonical_portfolio_target_content_hash,
)
from nexus.paper.service import load_paper_order_batch_result
from nexus.paper.target_service import TargetActivationResult, canonical_target_activation_hash
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisRunRecord,
    AgentResearchOutputRecord,
)
from nexus.persistence.models.economic import EconomicLaneCycleRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    PaperOrderRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job

_UNKNOWN = "ECONOMIC_PAIR_RESULT_UNKNOWN"


async def _activation(
    session: AsyncSession,
    link: EconomicLaneCycleRecord,
    decision: StrategyDecisionRecord,
    proposal: PortfolioProposalV1,
    target_id: UUID,
) -> tuple[PaperTargetVersionRecord, IdempotencyRecord]:
    claims = tuple(
        await session.scalars(
            select(IdempotencyRecord)
            .where(
                IdempotencyRecord.actor_id == f"internal-paper:{link.account_id}",
                IdempotencyRecord.route == "paper-target-activation-v1",
                IdempotencyRecord.response_payload["strategy_decision_id"].as_string()
                == str(decision.id),
            )
            .order_by(IdempotencyRecord.created_at, IdempotencyRecord.id)
        )
    )
    if not claims:
        raise ValueError(_UNKNOWN)
    target = await session.get(PaperTargetVersionRecord, target_id)
    if (
        target is None
        or target.account_id != link.account_id
        or target.portfolio_id != decision.portfolio_id
        or target.target_content_hash != canonical_portfolio_target_content_hash(proposal)
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
    ):
        raise ValueError(_UNKNOWN)
    canonical = None
    for claim in claims:
        result = TargetActivationResult.model_validate(claim.response_payload)
        if (
            claim.schema_version != "target_activation_result_v1"
            or claim.response_payload != result.model_dump(mode="json")
            or claim.request_hash
            != canonical_portfolio_synthesis_hash(
                {
                    "schema_version": "paper_target_activation_request_v1",
                    "account_id": link.account_id,
                    "strategy_decision_id": decision.id,
                    "request_time": claim.created_at,
                }
            )
            or result.account_id != link.account_id
            or result.strategy_decision_id != decision.id
            or result.target_version_id != target.id
            or result.activated_at != target.activated_at
            or result.result_code
            != ("NO_ORDER_REQUIRED" if result.batch_id is None else "PLAN_CREATED")
        ):
            raise ValueError(_UNKNOWN)
        if canonical is not None and result != canonical:
            raise ValueError(_UNKNOWN)
        canonical = result
    assert canonical is not None
    if canonical.batch_id is not None:
        batch = await load_paper_order_batch_result(
            session,
            account_id=link.account_id,
            strategy_decision_id=decision.id,
            batch_id=canonical.batch_id,
        )
        if batch is None or batch.order_ids:
            raise ValueError(_UNKNOWN)
    return target, claims[0]


async def read_no_trade_pair(
    session: AsyncSession, *, pair_slot_id: UUID
) -> dict[str, object] | None:
    """Only exact completed NO_ORDER_REQUIRED facts qualify; missing is unknown."""
    await require_economic_lane(session, ref=EconomicLaneRef(pair_slot_id, "incumbent"))
    links = tuple(
        await session.scalars(
            select(EconomicLaneCycleRecord)
            .where(EconomicLaneCycleRecord.pair_slot_id == pair_slot_id)
            .order_by(EconomicLaneCycleRecord.lane)
        )
    )
    if len(links) != 2 or {link.lane for link in links} != {"incumbent", "challenger"}:
        return None
    lanes = []
    for lane_name in ("incumbent", "challenger"):
        link = next(item for item in links if item.lane == lane_name)
        if await read_economic_cycle(session, cycle_id=link.cycle_id) is None:
            raise ValueError(_UNKNOWN)
        events = tuple(
            await session.scalars(
                select(AgentPaperCycleEventRecord)
                .where(AgentPaperCycleEventRecord.cycle_id == link.cycle_id)
                .order_by(AgentPaperCycleEventRecord.sequence)
            )
        )
        if not events or (events[-1].to_status, events[-1].result_code) != (
            "completed",
            "NO_ORDER_REQUIRED",
        ):
            return None
        for seq, event in enumerate(events, 1):
            if (
                event.sequence != seq
                or event.from_status != (None if seq == 1 else events[seq - 2].to_status)
                or event.event_hash
                != canonical_cycle_event_hash(
                    cycle_id=link.cycle_id,
                    sequence=event.sequence,
                    from_status=cast(CycleStatus | None, event.from_status),
                    to_status=cast(CycleStatus, event.to_status),
                    result_code=event.result_code,
                    fact_ids=event.fact_ids,
                    occurred_at=event.occurred_at,
                )
            ):
                raise ValueError(_UNKNOWN)
        facts = {}
        for event in events:
            facts.update(event.fact_ids)
        decision = await session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.agent_paper_cycle_id == link.cycle_id
            )
        )
        if (
            decision is None
            or str(decision.id) != facts.get("strategy_decision_id")
            or decision.account_id != link.account_id
        ):
            raise ValueError(_UNKNOWN)
        if (
            await session.scalar(
                select(PaperOrderRecord.id)
                .where(PaperOrderRecord.strategy_decision_id == decision.id)
                .limit(1)
            )
            is not None
        ):
            raise ValueError(_UNKNOWN)
        try:
            stored = await load_portfolio_proposal(
                session, account_id=link.account_id, decision_id=decision.id
            )
            if stored is None:
                raise ValueError(_UNKNOWN)
            target, activation = await _activation(
                session, link, decision, stored.proposal, UUID(str(facts.get("target_version_id")))
            )
        except ValueError as error:
            raise ValueError(_UNKNOWN) from error
        run = await session.scalar(
            select(AgentPortfolioSynthesisRunRecord).where(
                AgentPortfolioSynthesisRunRecord.cycle_id == link.cycle_id
            )
        )
        if run is None or run.completed_at is None:
            raise ValueError(_UNKNOWN)
        outputs = [
            {
                "id": str(decision.portfolio_synthesis_output_id),
                "hash": stored.proposal.synthesis_output_hash,
            }
        ]
        run_ids = [str(run.id)]
        times = [
            run.created_at,
            run.completed_at,
            decision.created_at,
            target.created_at,
            activation.created_at,
            events[-1].created_at,
            events[-1].occurred_at,
        ]
        for output_id in run.research_output_ids:
            output = await session.get(AgentResearchOutputRecord, output_id)
            if output is None:
                raise ValueError(_UNKNOWN)
            run_ids.append(str(output.run_id))
            outputs.append({"id": str(output.id), "hash": output.content_hash})
            times.append(output.created_at)
        jobs = tuple(
            await session.scalars(select(Job).where(Job.payload["run_id"].as_string().in_(run_ids)))
        )
        if (
            len(jobs) != len(run_ids)
            or {str(job.payload.get("run_id")) for job in jobs} != set(run_ids)
            or any(job.status != "succeeded" or job.finished_at is None for job in jobs)
        ):
            raise ValueError(_UNKNOWN)
        times.extend(job.finished_at for job in jobs if job.finished_at is not None)
        lanes.append(
            {
                "lane": lane_name,
                "status": "no_trade",
                "order_count": 0,
                "cycle_id": str(link.cycle_id),
                "event_sequence": events[-1].sequence,
                "event_hash": events[-1].event_hash,
                "decision_id": str(decision.id),
                "decision_hash": decision.decision_hash,
                "target_version_id": str(target.id),
                "activation_receipt_id": str(activation.id),
                "activation_request_hash": activation.request_hash,
                "run_id": str(run.id),
                "input_hash": run.input_hash,
                "outputs": outputs,
                "eligible_at": max(times).isoformat(),
            }
        )
    return {
        "status": "completed",
        "pair_slot_id": str(pair_slot_id),
        "window_id": None,
        "lanes": lanes,
        "orders": [],
        "replayed": True,
        "comparison_status": "not_evaluated",
        "economic_status": "not_evaluated",
        "execution_locked": True,
    }

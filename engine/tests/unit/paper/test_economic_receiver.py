from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.paper.economic_budget_hold import AllocationRequest
from nexus.paper.economic_receiver import require_economic_receiver
from nexus.paper.economic_version_binding import VersionBinding
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord, AgentResearchRunRecord

from engine.tests.unit.paper.test_economic_slot_budget import facts


def binding(plan):
    now = datetime(2026, 10, 8, tzinfo=UTC)
    return VersionBinding(
        protocol_hash=plan.content_hash,
        proposal_hash="b" * 64,
        publication_event_hash="c" * 64,
        shared_model_id=UUID(int=10),
        shared_model_version="codex-sdk-chatgpt-v6-stage-time-budget",
        shared_model_hash="d" * 64,
        incumbent_prompt_id=UUID(int=11),
        incumbent_prompt_hash="e" * 64,
        challenger_prompt_id=UUID(int=12),
        challenger_prompt_hash="f" * 64,
        pricing_available_at=now,
        pricing_expires_at=now + timedelta(days=60),
        configured_cost_usd="1",
        content_hash="a" * 64,
    )


def receiver(action="research_1", lane="incumbent"):
    plan, _ = facts()
    bound = binding(plan)
    allocation = AllocationRequest(hold_event_id=UUID(int=30), lane=lane, action=action)
    if action == "synthesis":
        run = AgentPortfolioSynthesisRunRecord(
            model_config_id=bound.shared_model_id,
            model_hash=bound.shared_model_hash,
            pricing_version=bound.shared_model_version,
            prompt_version_id=bound.incumbent_prompt_id,
            prompt_hash=bound.incumbent_prompt_hash,
        )
    else:
        run = AgentResearchRunRecord(
            instrument_id=plan.request.instruments[int(action[-1]) - 1].instrument_id,
            model_config_id=bound.shared_model_id,
            model_hash=bound.shared_model_hash,
            pricing_version=bound.shared_model_version,
        )
    return plan, bound, allocation, run


@pytest.mark.parametrize("action", ["research_1", "research_2"])
@pytest.mark.parametrize("lane", ["incumbent", "challenger"])
def test_research_uses_exact_candidate_and_shared_model(action, lane):
    require_economic_receiver(*receiver(action, lane))


@pytest.mark.parametrize(
    "field,value",
    [
        ("instrument_id", UUID(int=900)),
        ("model_config_id", UUID(int=900)),
        ("model_hash", "0" * 64),
        ("pricing_version", "older-model"),
    ],
)
def test_research_receiver_identity_mismatch_is_rejected(field, value):
    plan, bound, allocation, run = receiver()
    setattr(run, field, value)
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
        require_economic_receiver(plan, bound, allocation, run)


def test_first_candidate_cannot_receive_second_candidate_allocation():
    plan, bound, allocation, run = receiver("research_1")
    allocation = allocation.model_copy(update={"action": "research_2"})
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
        require_economic_receiver(plan, bound, allocation, run)


@pytest.mark.parametrize("action", ["research_1", "research_2"])
def test_research_allocation_preserves_frozen_protocol_order(action):
    plan, bound, allocation, run = receiver(action)
    plan = plan.model_copy(
        update={
            "request": plan.request.model_copy(
                update={"instruments": tuple(reversed(plan.request.instruments))}
            )
        }
    )
    run.instrument_id = plan.request.instruments[int(action[-1]) - 1].instrument_id
    require_economic_receiver(plan, bound, allocation, run)


def test_other_lane_synthesis_prompt_cannot_receive_challenger_allocation():
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
        require_economic_receiver(*receiver("synthesis", "challenger"))


def test_correct_prompt_without_valid_frozen_inputs_is_rejected():
    plan, bound, allocation, run = receiver("synthesis")
    run.frozen_input = {}
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
        require_economic_receiver(plan, bound, allocation, run)

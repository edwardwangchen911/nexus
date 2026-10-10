"""Verify receiver identities before diagnostic budget transfer; no activation authority."""

from nexus.agents.portfolio_synthesis_persistence import _frozen_run
from nexus.paper.economic_budget_hold import AllocationRequest
from nexus.paper.economic_version_binding import VersionBinding
from nexus.paper.portfolio_economic_preflight import EconomicPlan
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord, AgentResearchRunRecord


def require_economic_receiver(
    protocol: EconomicPlan,
    binding: VersionBinding,
    allocation: AllocationRequest,
    run: AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord,
) -> None:
    """Research shares role prompts; only synthesis uses the lane-specific prompt."""
    invalid = "ECONOMIC_TRANSFER_RECEIVER_INVALID"
    if (
        binding.protocol_hash != protocol.content_hash
        or run.model_config_id != binding.shared_model_id
        or run.model_hash != binding.shared_model_hash
        or run.pricing_version != binding.shared_model_version
    ):
        raise ValueError(invalid)
    candidates = tuple(item.instrument_id for item in protocol.request.instruments)
    if allocation.action != "synthesis":
        if not isinstance(run, AgentResearchRunRecord):
            raise ValueError(invalid)
        index = 0 if allocation.action == "research_1" else 1
        if run.instrument_id != candidates[index]:
            raise ValueError(invalid)
        return
    if not isinstance(run, AgentPortfolioSynthesisRunRecord):
        raise ValueError(invalid)
    prompt_id, prompt_hash = (
        (binding.incumbent_prompt_id, binding.incumbent_prompt_hash)
        if allocation.lane == "incumbent"
        else (binding.challenger_prompt_id, binding.challenger_prompt_hash)
    )
    if run.prompt_version_id != prompt_id or run.prompt_hash != prompt_hash:
        raise ValueError(invalid)
    try:
        frozen = _frozen_run(run, economic_authority=True)
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError(invalid) from error
    if frozen.candidate_instrument_ids != tuple(sorted(candidates, key=lambda item: item.int)):
        raise ValueError(invalid)

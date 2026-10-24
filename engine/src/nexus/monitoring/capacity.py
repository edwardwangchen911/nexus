"""Exactly one bound incremental research member uses the normal capacity gate."""

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.synthesis import domain_evidence
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.settings import EngineSettings


async def prepare_monitor_capacity_pool(
    session: AsyncSession,
    settings: EngineSettings,
    run: AgentResearchRunRecord,
    *,
    deadline: float,
) -> StructuredStagedCodexCommittee:
    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.monitoring.admission import require_increment_binding
    from nexus.worker import _committee

    invalid = "MONITOR_INCREMENT_POOL_BINDING_INVALID"
    binding = await require_increment_binding(session, run)
    if (
        binding.run_id != run.id
        or binding.instrument_id != run.instrument_id
        or binding.research_as_of != run.decision_time
    ):
        raise ValueError(invalid)
    evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    if not evidence:
        raise ValueError(invalid)
    committee = await _committee(
        session,
        settings,
        run,
        responses_client=None,
        tools=None,
        deadline=deadline,
        formal_stage_capacity=True,
    )
    if not isinstance(committee, StructuredStagedCodexCommittee):
        raise ValueError(invalid)
    await preflight_capacity_pool(
        [
            CapacityPoolMember(
                committee,
                ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                ),
                tuple(domain_evidence(ref) for ref in evidence),
            )
        ],
        expected_instrument_ids=(binding.instrument_id,),
    )
    return committee

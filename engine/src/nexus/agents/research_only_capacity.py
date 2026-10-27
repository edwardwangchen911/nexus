"""Whole-request input capacity using immutable pure-research membership."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.research_only import require_observation_snapshot
from nexus.agents.synthesis import domain_evidence
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.settings import EngineSettings


async def prepare_research_only_capacity_pool(
    session: AsyncSession,
    settings: EngineSettings,
    run: AgentResearchRunRecord,
    *,
    deadline: float,
) -> StructuredStagedCodexCommittee:
    invalid = "RESEARCH_ONLY_POOL_BINDING_INVALID"
    try:
        require_observation_snapshot(await session.get(DataSnapshot, run.data_snapshot_id))
    except (ValueError, AttributeError) as error:
        raise ValueError(invalid) from error
    from nexus.paper.research_request import read_research_manifest
    from nexus.persistence.models.manual_paper import (
        ManualPaperRequest,
        ResearchRequestManifest,
        ResearchRequestMember,
    )
    from nexus.worker import _committee

    request = await session.scalar(
        select(ManualPaperRequest)
        .join(
            ResearchRequestManifest,
            ResearchRequestManifest.request_id == ManualPaperRequest.id,
        )
        .join(
            ResearchRequestMember, ResearchRequestMember.manifest_id == ResearchRequestManifest.id
        )
        .where(
            ResearchRequestMember.run_id == run.id,
        )
    )
    if request is None:
        raise ValueError(invalid)
    manifest = await read_research_manifest(session, request)
    if (
        manifest is None
        or not 1 <= len(manifest.members) <= 5
        or run.id not in {item.run_id for item in manifest.members}
    ):
        raise ValueError(invalid)
    from nexus.agents.research_dispatch import require_dispatch_pool, uses_two_flight

    parallel = uses_two_flight(request.research_profile)
    if parallel:
        await require_dispatch_pool(session, request, manifest, run.id)
    members = []
    selected = None
    for item in manifest.members:
        member = await session.get(AgentResearchRunRecord, item.run_id, populate_existing=True)
        if (
            member is None
            or member.status
            not in ({"prepared", "running", "completed"} if parallel else {"prepared", "completed"})
            or member.model_config_id != run.model_config_id
            or member.model_hash != run.model_hash
            or member.pricing_version != run.pricing_version
            or member.decision_time != manifest.research_as_of
        ):
            raise ValueError(invalid)
        require_observation_snapshot(await session.get(DataSnapshot, member.data_snapshot_id))
        evidence = await load_frozen_evidence(session, member, allow_fixture_evidence=False)
        if not evidence:
            raise ValueError(invalid)
        committee = await _committee(
            session,
            settings,
            member,
            responses_client=None,
            tools=None,
            deadline=deadline,
            formal_stage_capacity=True,
        )
        if not isinstance(committee, StructuredStagedCodexCommittee):
            raise ValueError(invalid)
        members.append(
            CapacityPoolMember(
                committee,
                ResearchRequest(
                    instrument_id=member.instrument_id,
                    data_snapshot_id=member.data_snapshot_id,
                    decision_time=member.decision_time,
                ),
                tuple(domain_evidence(e) for e in evidence),
            )
        )
        if member.id == run.id:
            selected = committee
    await preflight_capacity_pool(
        members, expected_instrument_ids=tuple(item.instrument_id for item in manifest.members)
    )
    if selected is None:
        raise ValueError(invalid)
    return selected

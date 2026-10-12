"""Read a complete persisted cycle as a hypothetical v9 capacity qualification."""

import asyncio
from typing import cast
from uuid import UUID

from sqlalchemy import text

from nexus.agents.capacity_cycle import derive_cycle_members
from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
from nexus.agents.codex_staged import StagedCodexCommittee, StructuredStagedCodexCommittee
from nexus.agents.codex_versions import (
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_MAX_WIRE_BYTES,
)
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.paper_context import (
    FrozenAgentPaperContextStatus,
    read_agent_paper_research_context,
    revalidate_frozen_agent_paper_context,
)
from nexus.agents.synthesis import domain_evidence
from nexus.paper.agent_cycle import _events, _fact_uuids
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def inspect_cycle(settings: EngineSettings, *, cycle_id: UUID) -> dict[str, object]:
    from nexus.ops.research_workload_inspection import _inspection_committee

    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            if cycle is None:
                raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
            events = await _events(session, cycle_id)
            ids = _fact_uuids(events, "research_run_ids")
            if not 1 <= len(ids) <= 5 or len(set(ids)) != len(ids):
                raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
            runs = []
            for rid in ids:
                run = await session.get(AgentResearchRunRecord, rid)
                if run is None:
                    raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
                runs.append(run)
            paired = derive_cycle_members(cycle, events, runs)
            members = []
            for row, run in paired:
                model = await session.get(AgentModelConfigRecord, run.model_config_id)
                if (
                    model is None
                    or model.config_hash != run.model_hash
                    or model.version != run.pricing_version
                ):
                    raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
                evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
                if (
                    await revalidate_frozen_agent_paper_context(
                        session, run=run, evidence_records=evidence
                    )
                    != FrozenAgentPaperContextStatus.VALID
                ):
                    raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
                context = await read_agent_paper_research_context(
                    session,
                    snapshot_id=run.data_snapshot_id,
                    instrument_id=run.instrument_id,
                    as_of=run.decision_time,
                )
                if context is None or context[0].cycle_id != cycle.id:
                    raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
                base = await _inspection_committee(session, settings, run, model=model)
                if not isinstance(base, StagedCodexCommittee):
                    raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
                committee = StructuredStagedCodexCommittee(
                    prompts=base._prompts,
                    tools=base._tools,
                    deadline=asyncio.get_running_loop().time() + 195,
                    aggregate_input_tokens=96000,
                    max_wire_bytes=FULL_INPUT_CAPACITY_MAX_WIRE_BYTES,
                    inline_citations=True,
                    paper_stage_scope=True,
                    stage_time_budget=True,
                    stage_budget_version=FULL_INPUT_CAPACITY_CONFIG_VERSION,
                )
                request = ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                )
                members.append(
                    CapacityPoolMember(
                        committee, request, tuple(domain_evidence(r) for r in evidence)
                    )
                )
            result = await preflight_capacity_pool(
                members, expected_instrument_ids=tuple(row.instrument_id for row, _ in paired)
            )
            measured = cast(list[dict[str, object]], result["members"])
            return {
                "status": "capacity_observed",
                "profile": FULL_INPUT_CAPACITY_CONFIG_VERSION,
                "provenance": "readonly_hypothetical_profile",
                "registered": False,
                "enabled": False,
                "model_calls": 0,
                "business_writes": 0,
                "execution_locked": True,
                "candidate_count": len(members),
                "manifest_hash": result["manifest_hash"],
                "aggregate_completion_proven": False,
                "candidates": [
                    {
                        "symbol": row.symbol,
                        "source_model_version": run.pricing_version,
                        "wire_upper_bytes": metric["wire_upper_bytes"],
                        "research_allowance_units": metric["research_allowance_units"],
                    }
                    for (row, run), metric in zip(paired, measured, strict=True)
                ],
            }
    finally:
        await factory.kw["bind"].dispose()

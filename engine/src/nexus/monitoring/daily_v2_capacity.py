"""Read-only daily material preview; never a substitute for full-pool preflight."""

import asyncio
from typing import cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_capacity import candidate_capacity_envelope
from nexus.agents.codex_formal_stage import formal_preparation_model_matches
from nexus.agents.codex_sdk import (
    MAX_INPUT_TOKENS,
    codex_input_allowance,
    codex_output_schema,
    codex_request_wire,
)
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.codex_versions import (
    CAPACITY_PRECLAIM_TIMEOUT_SECONDS,
    FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS,
    FORMAL_RESEARCH_JOB_TIMEOUT_SECONDS,
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_MAX_WIRE_BYTES,
    ISOLATED_CAPACITY_CONFIG_VERSIONS,
)
from nexus.agents.committee import PromptAsset
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_prompts
from nexus.agents.models import ResearchRequest
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.agents.synthesis import domain_evidence
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot


class DailyEventCapacityExceeded(ValueError):
    """Only an intact daily event bundle may request an atomic whole-bundle gap."""

    reason = "MONITOR_CONTENT_CAPACITY"

    def __init__(self, instrument_id: UUID, pending_count: int) -> None:
        self.instrument_id = instrument_id
        self.pending_count = pending_count
        super().__init__(self.reason)


async def preview_daily_event_capacity(session: AsyncSession, run: AgentResearchRunRecord) -> None:
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    paper = None if snapshot is None else snapshot.payload.get("agent_paper_context")
    if not isinstance(paper, dict) or all(
        paper.get(key) is None for key in ("pending_monitoring_v2", "pending_monitoring_v2_gap")
    ):
        return
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    if model is None or model.version not in (
        ISOLATED_CAPACITY_CONFIG_VERSIONS | {FORMAL_STAGE_CAPACITY_CONFIG_VERSION}
    ):
        return
    if (
        not formal_preparation_model_matches(model)
        or model.version != run.pricing_version
        or model.config_hash != run.model_hash
    ):
        raise ValueError("CODEX_FORMAL_AUTHORITY_INVALID")
    bound = await read_agent_paper_research_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if bound is None or (
        bound[0].pending_monitoring_v2 is None and bound[0].pending_monitoring_v2_gap is None
    ):
        raise ValueError("MONITOR_DAILY_EVENT_CONTEXT_INVALID")
    context = bound[0].pending_monitoring_v2
    evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    prompts = await load_frozen_prompts(session, run)
    if not evidence or prompts is None:
        raise ValueError("WORKLOAD_RUN_AUTHORITY_INVALID")
    # Mirror worker._committee's formal preclaim composition. This deadline is
    # local to a discarded preview; no Job/SDK budget or pool authority is granted.
    deadline = asyncio.get_running_loop().time() + CAPACITY_PRECLAIM_TIMEOUT_SECONDS
    deadline -= FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS - FORMAL_RESEARCH_JOB_TIMEOUT_SECONDS
    committee = StructuredStagedCodexCommittee(
        prompts={
            role: PromptAsset(
                role=role,
                version=item.version,
                content=item.content,
                content_hash=item.content_hash,
            )
            for role, item in prompts.items()
        },
        tools=await database_read_only_tools(
            session, run, allow_fixture_evidence=False, allow_fixture_price_evidence=False
        ),
        deadline=deadline,
        aggregate_input_tokens=model.max_input_tokens,
        max_wire_bytes=FULL_INPUT_CAPACITY_MAX_WIRE_BYTES
        if model.version in ISOLATED_CAPACITY_CONFIG_VERSIONS
        else 64000,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=model.version,
    )
    request = ResearchRequest(
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time,
    )
    refs = tuple(domain_evidence(row) for row in evidence)
    inputs = await committee._inputs(request)
    envelope = candidate_capacity_envelope(
        committee, request=request, evidence_refs=refs, inputs=inputs
    )
    reason = None
    if cast(int, envelope["wire_upper_bytes"]) > committee._max_wire_bytes:
        reason = "CODEX_CAPACITY_POOL_WIRE_EXCEEDED"
    else:
        payload, output_model = committee.build_stage_request(
            stage="research", request=request, evidence_refs=refs, frozen_inputs=inputs, previous=[]
        )
        allowance = codex_input_allowance(
            codex_request_wire(payload), codex_output_schema(output_model)
        )
        if allowance > MAX_INPUT_TOKENS or allowance > committee._aggregate_input_tokens:
            reason = "CODEX_CAPACITY_POOL_INPUT_EXCEEDED"
    if reason is not None:
        if context is not None:
            raise DailyEventCapacityExceeded(run.instrument_id, len(context.trigger_checks))
        raise ValueError(reason)

"""Read-only daily material preview; never a substitute for full-pool preflight."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar, Token
from math import isfinite
from types import TracebackType
from typing import Self, cast
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


def _monotonic() -> float:
    return asyncio.get_running_loop().time()


class DailyEventCapacityScope:
    """One lazy absolute allowance, shared by a pool and its unpublished rebuilds."""

    def __init__(self, deadline: float | None = None) -> None:
        if deadline is not None and (type(deadline) not in {int, float} or not isfinite(deadline)):
            raise ValueError("CODEX_STAGE_BUDGET_CLOCK_INVALID")
        self.deadline = deadline
        self._timeout: asyncio.Timeout | None = None
        self._token: Token[DailyEventCapacityScope | None] | None = None

    async def __aenter__(self) -> Self:
        if self._timeout is not None:
            raise RuntimeError("daily capacity scope cannot be reused")
        self._timeout = asyncio.timeout_at(self.deadline)
        await self._timeout.__aenter__()
        self._token = _current_scope.set(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._timeout is not None and self._token is not None
        try:
            try:
                await self._timeout.__aexit__(exc_type, exc_value, traceback)
            except TimeoutError:
                if self._timeout.expired():
                    raise ValueError("CODEX_CAPACITY_PRECLAIM_TIMEOUT") from None
                raise
            if exc_type is None:
                self.check_deadline()
        finally:
            _current_scope.reset(self._token)

    def activate(self) -> None:
        if self._timeout is None:
            raise RuntimeError("daily capacity scope must be entered")
        if self.deadline is None:
            self.deadline = _monotonic() + CAPACITY_PRECLAIM_TIMEOUT_SECONDS
            self._timeout.reschedule(self.deadline)
        self.check_deadline()

    def check_deadline(self) -> None:
        if self.deadline is not None and _monotonic() >= self.deadline:
            raise ValueError("CODEX_CAPACITY_PRECLAIM_TIMEOUT")


_current_scope: ContextVar[DailyEventCapacityScope | None] = ContextVar(
    "daily_event_capacity_scope", default=None
)


class DailyEventCapacityExceeded(ValueError):
    """Only an intact daily event bundle may request an atomic whole-bundle gap."""

    reason = "MONITOR_CONTENT_CAPACITY"

    def __init__(self, instrument_id: UUID, pending_count: int) -> None:
        self.instrument_id = instrument_id
        self.pending_count = pending_count
        super().__init__(self.reason)


async def preview_daily_event_capacity(session: AsyncSession, run: AgentResearchRunRecord) -> None:
    scope = _current_scope.get()
    if scope is None:
        async with DailyEventCapacityScope():
            return await preview_daily_event_capacity(session, run)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    paper = None if snapshot is None else snapshot.payload.get("agent_paper_context")
    if not isinstance(paper, dict) or all(
        paper.get(key) is None for key in ("pending_monitoring_v2", "pending_monitoring_v2_gap")
    ):
        return
    scope.activate()
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
    scope.check_deadline()
    assert scope.deadline is not None
    deadline = scope.deadline
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
    scope.check_deadline()
    envelope = candidate_capacity_envelope(
        committee, request=request, evidence_refs=refs, inputs=inputs
    )
    scope.check_deadline()
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
        scope.check_deadline()
        if allowance > MAX_INPUT_TOKENS or allowance > committee._aggregate_input_tokens:
            reason = "CODEX_CAPACITY_POOL_INPUT_EXCEEDED"
    if reason is not None:
        if context is not None:
            raise DailyEventCapacityExceeded(run.instrument_id, len(context.trigger_checks))
        raise ValueError(reason)
